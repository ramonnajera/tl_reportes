from fastapi import FastAPI, UploadFile, File, APIRouter
import pandas as pd
import io
import numpy as np
from datetime import datetime, timedelta

app = FastAPI()

df = None

router = APIRouter()

@app.post("/read/")
async def create_upload_file(file: UploadFile = File(...)):
    global df
    try:
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
        df = df.drop(columns=['Password [Required]', 'Password Hash Function [UPLOAD ONLY]', 'New Primary Email [UPLOAD ONLY]'])
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.replace({np.nan: None})

        return {"message": "Archivo subido y cargado correctamente"}
    except Exception as e:
        return {"error": str(e)}

@router.get("/repetidos/", tags=["reportes"])
async def get_repetidos():
    global df
    if df is None:
        return {"error": "No hay DataFrame cargado"}
    df_repetidos = df.copy()
    usuarios_repetidos = df_repetidos[df_repetidos.duplicated(subset=['Email Address [Required]'], keep=False)]['Email Address [Required]'].tolist()
    return {"usuarios_repetidos": usuarios_repetidos}

@router.get("/total/", tags=["reportes"])
async def get_total():
    global df
    if df is None:
        return {"error": "No hay DataFrame cargado"}
    df_total = df.copy()
    unique_users = df_total['Email Address [Required]'].nunique() # Usuarios únicos
    return {"usuarios_total": unique_users}

@router.get("/inactivos/", tags=["reportes"])
async def get_inactivos():
    global df
    if df is None:
        return {"error": "No hay DataFrame cargado"}
    df_inactivos = df.copy()  # Crear una copia para esta ruta
    df_inactivos['Last Sign In [READ ONLY]'] = pd.to_datetime(df_inactivos['Last Sign In [READ ONLY]'], errors='coerce')
    five_years_ago = datetime.now() - timedelta(days=5*365)
    inactive_users = df_inactivos[(df_inactivos['Last Sign In [READ ONLY]'] < five_years_ago) & (df_inactivos['Last Sign In [READ ONLY]'].notna())]  # Usuarios inactivos
    return {"usuarios_inactivos": inactive_users.to_dict('records')}

@router.get("/sin_entrar/", tags=["reportes"])
async def get_sin_entrar():
    global df
    if df is None:
        return {"error": "No hay DataFrame cargado"}
    df_sin_entrar = df.copy()  # Crear una copia para esta ruta
    never_logged_in_users = df_sin_entrar[df_sin_entrar['Last Sign In [READ ONLY]'] == 'Never logged in']  # Usuarios que nunca han iniciado sesión
    return {"usuarios_sin_entrar": never_logged_in_users.to_dict('records')}

app.include_router(router, prefix="/reportes")