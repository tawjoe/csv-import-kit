from fastapi import FastAPI, UploadFile, File, HTTPException
from uuid import uuid4
import hashlib
import os
import psycopg  # Postgres ドライバ（読み：さいこっぐ）

app = FastAPI()

# DSN（でぃーえすえぬ：DBの住所）。環境変数 DB_DSN があれば優先
DB_DSN = os.getenv("DB_DSN", "postgresql://app:app@127.0.0.1:5432/csvkit")
MAX_UPLOAD_BYTES = 2_000_000

def sha256_hex(data: bytes) -> str:
    "ファイルの指紋（SHA-256）を16進で返す"
    return hashlib.sha256(data).hexdigest()

@app.on_event("startup")
def init_db():
    "起動時にテーブルが無ければ作る（MVPの最小DDL）"
    with psycopg.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.execute("""
            create table if not exists imports(
              id uuid primary key,
              filename text not null,
              size integer not null,
              hash char(64) not null,
              user_id text not null,
              status text not null,
              created_at timestamptz not null default now(),
              unique(user_id, hash)   -- 冪等キー：同じ人×同じ内容は1件だけ
            );
        """)
        conn.commit()

@app.get("/healthz")
def healthz():
    "アプリ＆DBの生存確認"
    try:
        with psycopg.connect(DB_DSN) as conn, conn.cursor() as cur:
            cur.execute("select 1;")
        return {"ok": True, "db": True}
    except Exception:
        return {"ok": True, "db": False}

@app.post("/imports", status_code=202)
async def create_import(file: UploadFile = File(...), user_id: str = "anon"):
    # 1) 受け取ってサイズガード
    content = await file.read()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "file too large (>2MB)")

    # 2) 冪等キー（ユーザー×指紋）
    file_hash = sha256_hex(content)

    # 3) 新規IDを用意 → INSERT。重複なら DO NOTHING して既存を取り出す
    iid = str(uuid4())
    with psycopg.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.execute("""
            insert into imports(id, filename, size, hash, user_id, status)
            values (%s, %s, %s, %s, %s, %s)
            on conflict (user_id, hash) do nothing
            returning id;
        """, (iid, file.filename, len(content), file_hash, user_id, "received"))
        row = cur.fetchone()

        if row:
            conn.commit()
            return {"import_id": row[0], "status": "received"}

        # 重複（duplicate）なので既存IDを返す
        cur.execute("select id from imports where user_id=%s and hash=%s;", (user_id, file_hash))
        row2 = cur.fetchone()
        conn.commit()
        if not row2:
            raise HTTPException(500, "idempotency lookup failed")
        return {"import_id": row2[0], "status": "duplicate"}

@app.get("/imports/{iid}")
def get_import(iid: str):
    "受付明細の照会"
    with psycopg.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.execute("""
            select id, filename, size, user_id, status, created_at
              from imports
             where id = %s;
        """, (iid,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "not found")
        id_, filename, size, user_id, status, created_at = row
        return {
            "id": id_,
            "filename": filename,
            "size": size,
            "user_id": user_id,
            "status": status,
            "created_at": created_at.isoformat()
        }
