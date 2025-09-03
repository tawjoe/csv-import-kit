from fastapi import FastAPI, UploadFile, File, HTTPException
from uuid import uuid4
import hashlib  # 指紋（ハッシュ）を作る道具

# ① アプリ本体は最初に作る（ここより前で @app... は使わない）
app = FastAPI()

# ② “状態（ステート）”の置き場は先に用意
IMPORTS: dict[str, dict] = {}        # import_id -> 明細
PROCESSED: dict[str, str] = {}       # (user_id:hash) -> import_id
MAX_UPLOAD_BYTES = 2_000_000         # 安全弁：2MB上限

# ③ ヘルパ：バイト列の指紋（SHA-256）を16進文字列に
def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

# ④ ヘルスチェック（動作確認用）
@app.get("/healthz")
def healthz():
    return {"ok": True}

# ⑤ CSV受付：冪等性（user_id + ファイル指紋）とサイズ上限を実装
@app.post("/imports", status_code=202)
async def create_import(file: UploadFile = File(...), user_id: str = "anon"):
    # 1) まず読み込む（MVPは全読み。大きくなったら分割読みへ発展）
    content = await file.read()

    # 2) サイズ上限（安全ゲート）
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, f"file too large (>{MAX_UPLOAD_BYTES} bytes)")

    # 3) 指紋（ハッシュ）を計算 → “誰のどのファイルか”の鍵を作る
    file_hash = sha256_hex(content)
    key = f"{user_id}:{file_hash}"

    # 4) すでに受理済みなら、同じ受付番号（import_id）を返す
    if key in PROCESSED:
        iid = PROCESSED[key]
        return {"import_id": iid, "status": "duplicate"}

    # 5) 初回なら import_id を発行して登録
    iid = str(uuid4())
    PROCESSED[key] = iid
    IMPORTS[iid] = {
        "status": "received",
        "filename": file.filename,
        "size": len(content),
        "errors": [],
        "hash": file_hash,
        "user_id": user_id,
    }
    return {"import_id": iid, "status": "received"}

# ⑥ 受付状況の参照
@app.get("/imports/{iid}")
def get_import(iid: str):
    imp = IMPORTS.get(iid)
    if not imp:
        raise HTTPException(404, "not found")
    return imp
