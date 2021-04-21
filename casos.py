# imports naturales
import os
import shutil
import subprocess
import pandas as pd
# import from
from datetime import datetime

# parametros
csv_files = os.path.join(os.getcwd(), "target", "csv")
parquet_files = os.path.join(os.getcwd(), "target", "parquet")

# configuracion del espacio
if not os.path.exists(parquet_files):
    os.makedirs(parquet_files)

# procesar los casos por pais

columnas = {
    "Fecha": "fecha",
    "Casos nuevos con sintomas": "nuevos_con_sintomas",
    "Casos totales": "casos",
    "Casos recuperados": "recuperados",
    "Fallecidos": "fallecidos",
    "Casos activos": "activos",
    "Casos nuevos sin sintomas": "nuevos_sin_sintomas",
    "Casos nuevos totales": "nuevos",
    "Casos activos por FD": "acivos_fd",
    "Casos activos por FIS": "activos_fis",
    "Casos recuperados por FIS": "recuperados_fis",
    "Casos recuperados por FD": "recuperados_fd",
    "Casos confirmados recuperados": "confirmados_recuperados",
    "Casos activos confirmados": "activos_confirmados",
    "Casos probables acumulados": "probables_acumulados",
    "Casos activos probables": "activos_probables",
    "Casos nuevos sin notificar": "activos_sin_notificar",
    "Casos confirmados por antigeno": "confirmados_antigeno",
    "Casos con sospecha de reinfeccion": "reinfectados_sospecha",
    "Casos nuevos confirmados por antigeno": "nuevos_antigeno"
}
df = pd.read_csv(os.path.join(csv_files, "casos_por_pais.csv"))
df.rename(columns = columnas, inplace = True)
df.to_parquet(path = os.path.join(parquet_files, "casos_por_pais.parquet"))

# procesar los casos por region

columnas = {
"Region": "region_1",
"Casos totales acumulados": "casos_acumulados",
"Casos nuevos totales": "nuevos",
"Casos nuevos con sintomas": "nuevos_con_sintomas",
"Casos nuevos sin sintomas*": "nuevos_sin_sintomas",
"Casos nuevos sin notificar": "nuevos_sin_notificar",
"Fallecidos totales": "fallecidos",
"% Total": "porcentaje_total_1",
"Fecha": "fecha",
"Casos activos confirmados": "activos_confirmados",
"Casos confirmados recuperados": "confirmados_recuperados",
"Casos probables acumulados": "probables_acumulados",
"Casos activos probables": "activos_probables",
"Casos  nuevos": "nuevos",
"Casos  totales": "casos",
"%  Casos  totales**": "porcentaje_totales",
"Fallecidos": "fallecidos",
"Casos  totales  acumulados": "casos_acumulados",
"Casos  nuevos  totales": "nuevos",
"Casos  nuevos  con  sintomas": "nuevos_con_sintomas",
"Casos  nuevos  sin  sintomas*": "nuevos_sin_sintomas",
"%  Total": "porcentaje_total_2",
"Casos probables acumulados ": "probables_acumulados",
"Casos activos probables ": "activos_probables",
"Casos nuevos": "nuevos",
"Casos totales": "totales",
"Casos confirmados por antigeno": "confirmados_antigeno",
"Casos con sospecha de reinfeccion": "sospecha_reinfeccion",
"Casos nuevos confirmados por antigeno": "nuevos_antigeno",
" Casos nuevos": "nuevos",
" Casos totales": "casos",
" Casos recuperados": "recuperados",
" Casos fallecidos": "fallecidos",
"Casos totales acumulados ": "casos_totales",
"Casos nuevos totales ": "nuevos_totales",
"Casos nuevos con sintomas ": "nuevos_con_sintomas",
"Casos nuevos sin sintomas* ": "nuevos_sin_sintomas",
"Fallecidos totales ": "fallecidos_totales",
"% Total ": "porcentaje_total_3",
"Casos  nuevos  sin  sintomas": "nuevos_sin_sintomas",
"Tasa  *100000": "tasa_cien_mil",
"Incremento  diario": "incremento_diario",
"Región": "region_2"
}