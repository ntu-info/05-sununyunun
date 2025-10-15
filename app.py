# app.py
from flask import Flask, jsonify, abort, send_file, request, make_response 
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

_engine = None

def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL (or DATABASE_URL) environment variable.")
    # Normalize old 'postgres://' scheme to 'postgresql://'
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    _engine = create_engine(
        db_url,
        pool_pre_ping=True,
    )
    return _engine

def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    @app.get("/terms/<term>/studies", endpoint="terms_studies")
    def get_studies_by_term(term):
        return term

    @app.get("/locations/<coords>/studies", endpoint="locations_studies")
    def get_studies_by_coordinates(coords):
        x, y, z = map(int, coords.split("_"))
        return jsonify([x, y, z])

    @app.get("/test_db", endpoint="test_db")
    
    def test_db():
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}

        try:
            with eng.begin() as conn:
                # Ensure we are in the correct schema
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()

                # Counts
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()

                # Samples
                try:
                    rows = conn.execute(text(
                        "SELECT study_id, ST_X(geom) AS x, ST_Y(geom) AS y, ST_Z(geom) AS z FROM ns.coordinates LIMIT 3"
                    )).mappings().all()
                    payload["coordinates_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["coordinates_sample"] = []

                try:
                    # Select a few columns if they exist; otherwise select a generic subset
                    rows = conn.execute(text("SELECT * FROM ns.metadata LIMIT 3")).mappings().all()
                    payload["metadata_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["metadata_sample"] = []

                try:
                    rows = conn.execute(text(
                        "SELECT study_id, contrast_id, term, weight FROM ns.annotations_terms LIMIT 3"
                    )).mappings().all()
                    payload["annotations_terms_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["annotations_terms_sample"] = []

            payload["ok"] = True
            return jsonify(payload), 200

        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500
    
    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_terms(term_a, term_b):
        eng = get_engine()
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))

                # 找出有 term_a 但沒有 term_b 的研究
                query = text("""
                    SELECT DISTINCT a.study_id
                    FROM ns.annotations_terms AS a
                    WHERE a.term ILIKE :term_a
                    AND a.study_id NOT IN (
                        SELECT study_id FROM ns.annotations_terms WHERE term ILIKE :term_b
                    )
                    LIMIT 50;
                """)

                results = conn.execute(query, {"term_a": f"%{term_a}%", "term_b": f"%{term_b}%"}).fetchall()
                study_ids = [r[0] for r in results]

                if not study_ids:
                    data = {"term_a": term_a, "term_b": term_b, "count": 0, "studies": []}
                else:
                    meta_query = text("""
                        SELECT study_id, title
                        FROM ns.metadata
                        WHERE study_id = ANY(:ids)
                        LIMIT 50;
                    """)
                    meta_results = conn.execute(meta_query, {"ids": study_ids}).mappings().all()
                    studies = [dict(r) for r in meta_results]
                    data = {"term_a": term_a, "term_b": term_b, "count": len(studies), "studies": studies}

                #  HTML / JSON 自動切換
                if request.accept_mimetypes.accept_html:
                    html = f"""
                    <h2>🧠 Dissociate by Terms</h2>
                    <p><b>Term A:</b> {term_a}</p>
                    <p><b>Term B:</b> {term_b}</p>
                    <p><b>Count:</b> {data['count']}</p>
                    <ul>
                        {''.join(f"<li>{s['study_id']}: {s.get('title','(no title)')}</li>" for s in data['studies'])}
                    </ul>
                    """
                    return make_response(html, 200)
                else:
                    return jsonify(data), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        
    # 🧠 Dissociate by coordinates endpoint
    
    @app.get("/dissociate/locations/<coords_a>/<coords_b>", endpoint="dissociate_locations")
    def dissociate_locations(coords_a, coords_b):
        try:
            x1, y1, z1 = map(float, coords_a.split("_"))
            x2, y2, z2 = map(float, coords_b.split("_"))
        except ValueError:
            return jsonify({"error": "Invalid coordinate format. Use x_y_z with underscores."}), 400

        eng = get_engine()
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))

                # Helper: find studies containing a coordinate point (using ST_Equals for exact match)
                def get_studies(x, y, z):
                    query = text("""
                        SELECT DISTINCT study_id
                        FROM ns.coordinates
                        WHERE ST_X(geom) = :x
                          AND ST_Y(geom) = :y
                          AND ST_Z(geom) = :z
                        LIMIT 100;
                    """)
                    results = conn.execute(query, {"x": x, "y": y, "z": z}).fetchall()
                    return [r[0] for r in results]

                studies_a = set(get_studies(x1, y1, z1))
                studies_b = set(get_studies(x2, y2, z2))

                dissoc_a_b = list(studies_a - studies_b)
                dissoc_b_a = list(studies_b - studies_a)

                # Optional: fetch titles from metadata
                if dissoc_a_b or dissoc_b_a:
                    all_ids = list(set(dissoc_a_b) | set(dissoc_b_a))  # <-- 修正這裡
                    meta_query = text("""
                        SELECT study_id, title
                        FROM ns.metadata
                        WHERE study_id = ANY(:ids)
                    """)
                    meta_results = conn.execute(meta_query, {"ids": all_ids}).mappings().all()
                    meta_dict = {r["study_id"]: r["title"] for r in meta_results}
                else:
                    meta_dict = {}


                response = {
                    "coords_a": [x1, y1, z1],
                    "coords_b": [x2, y2, z2],
                    "A_minus_B": [{"study_id": sid, "title": meta_dict.get(sid)} for sid in dissoc_a_b],
                    "B_minus_A": [{"study_id": sid, "title": meta_dict.get(sid)} for sid in dissoc_b_a]
                }

                return jsonify(response), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500


    return app
# WSGI entry point (no __main__)
app = create_app()
