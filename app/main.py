from fastapi import FastAPI, UploadFile, File, HTTPException
from uuid import uuid4

app = FastAPI()

@app.get('/healthz')
def healthz():
    return {'ok': True}

IMPORTS = {}

@app.post('/imports', status_code=202)
async def create_import(file: UploadFile = File(...)):
    iid = str(uuid4())
    content = await file.read()
    IMPORTS[iid] = {'status': 'received', 'filename': file.filename, 'size': len(content), 'errors': []}
    return {'import_id': iid}

@app.get('/imports/{iid}')
def get_import(iid: str):
    imp = IMPORTS.get(iid)
    if not imp:
        raise HTTPException(404, 'not found')
    return imp
