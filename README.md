# CSV Import Kit (FastAPI)

- /healthz : 200 OK（ヘルスチェック）
- /imports : CSV を multipart/form-data で POST → import_id を返す最小MVP
- ねらい：運用に効く最小骨（あとで冪等性・サイズ制限・DBを差し込み）

## 起動
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload

## 動作確認
curl.exe http://127.0.0.1:8000/healthz
curl.exe --form "file=@samples/customers_ok.csv;type=text/csv" "http://127.0.0.1:8000/imports?user_id=demo"
