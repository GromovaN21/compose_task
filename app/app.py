import os
from datetime import time

from flask import Flask, request, jsonify, render_template_string
from sqlalchemy import create_engine, Table, Column, Integer, MetaData, select, insert, update
from sqlalchemy.exc import SQLAlchemyError
import redis

# Config from environment (reasonable defaults for local dev)
APP_NAME = os.getenv("APP_NAME", "My Flask App")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/postgres")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
VISITS_KEY = "visits_count"
REDIS_TTL = 10  # seconds

# Initialize Flask
app = Flask(__name__)

# Initialize DB (SQLAlchemy Core, compact)
engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()
visits_table = Table(
    "visits",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("count", Integer, nullable=False, default=0),
)
# Create table if missing
metadata.create_all(engine)

# Ensure there's a single row with id=1 to hold the counter
def ensure_counter_row():

    with engine.connect() as conn:
        stmt = select(visits_table.c.id).limit(1)
        r = conn.execute(stmt).first()
        if r is None:
            conn.execute(insert(visits_table).values(id=1, count=0))
            conn.commit()

ensure_counter_row()

# Initialize Redis
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# Helper: read count from Postgres
def get_count_from_db():
    try:
        with engine.connect() as conn:
            r = conn.execute(select(visits_table.c.count).where(visits_table.c.id == 1)).first()
            return int(r[0]) if r else 0
    except SQLAlchemyError:
        return None

# Helper: increment counter in DB (atomic using UPDATE ... RETURNING)
def increment_count_db():
    try:
        with engine.begin() as conn:
            stmt = (
                update(visits_table)
                .where(visits_table.c.id == 1)
                .values(count=visits_table.c.count + 1)
                .returning(visits_table.c.count)
            )
            r = conn.execute(stmt).first()
            return int(r[0]) if r else None
    except SQLAlchemyError:
        return None

# Root endpoint: increments visits and shows HTML with app name and count
@app.route("/", methods=["GET"])
def index():
    new_count = increment_count_db()
    # Invalidate cache in Redis to keep /visits coherent (optional strategy)
    try:
        redis_client.delete(VISITS_KEY)
    except Exception:
        pass
    # Simple inline HTML template
    html = """
    <!doctype html>
    <html>
      <head><meta charset="utf-8"><title>{{ app_name }}</title></head>
      <body>
        <h1>{{ app_name }}</h1>
        <!--<p>Total visits: <strong>{{ count }}</strong></p>-->
      </body>
    </html>
    """
    return render_template_string(html, app_name=APP_NAME, count=new_count if new_count is not None else "error")

# /visits endpoint: use Redis cache (10s TTL) else read DB and cache result
@app.route("/visits", methods=["GET"])
def visits():
    cached = False
    try:
        cached_val = redis_client.get(VISITS_KEY)
    except Exception:
        cached_val = None

    if cached_val is not None:
        try:
            total = int(cached_val)
            cached = True
        except ValueError:
            total = None
    else:
        total = get_count_from_db()
        if total is not None:
            try:
                redis_client.setex(VISITS_KEY, REDIS_TTL, str(total))
            except Exception:
                pass

    return jsonify({"total": total if total is not None else 0, "cached": cached})

# /health endpoint: reports simple connectivity checks for Postgres and Redis
@app.route("/health", methods=["GET"])
def health():
    # DB check
    db_status = "unknown"
    try:
        with engine.connect() as conn:
            conn.execute(select(1))
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {type(e).__name__}"

    # Redis check
    redis_status = "unknown"
    try:
        if redis_client.ping():
            redis_status = "ok"
        else:
            redis_status = "error"
    except Exception as e:
        redis_status = f"error: {type(e).__name__}"

    return jsonify({"status": "ok", "db": db_status, "redis": redis_status})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))