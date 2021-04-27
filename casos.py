# imports naturales
import os
import shutil
import subprocess
import numpy as np
import pandas as pd
# import from
from datetime import datetime

# parametros
csv_files = os.path.join(os.getcwd(), "target", "csv")
parquet_files = os.path.join(os.getcwd(), "target", "parquet")

# funciones
def obtener_valor_unico(*columnas, type:int = 1):
    for fila in columnas:
        try:
            if type == 1:
                resp = next(num for num in fila if num is not np.nan and num >= 0)
            if type == 2:
                resp = next(num for num in fila if num is not np.nan)
        except:
            resp = None
    return resp

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
del df

# procesar los casos por region

columnas = {
    "Fecha": "fecha",
    "Region": "region_1",
    "Región": "region_2",
    "Casos nuevos totales": "nuevos_1",
    "Casos  nuevos": "nuevos_2",
    " Casos nuevos": "nuevos_3",
    "Casos nuevos": "nuevos_4",
    "Casos  nuevos  totales": "nuevos_5",
    "Casos nuevos totales ": "nuevos_6",
    "Casos totales acumulados": "casos_1",
    "Casos  totales": "casos_2",
    " Casos totales": "casos_3",
    "Casos totales": "casos_4",
    "Casos  totales  acumulados": "casos_5",
    "Casos totales acumulados ": "casos_6",
    "Casos nuevos con sintomas": "nuevos_con_sintomas_1",
    "Casos  nuevos  con  sintomas": "nuevos_con_sintomas_2",
    "Casos nuevos con sintomas ": "nuevos_con_sintomas_3",
    "Casos nuevos sin sintomas*": "nuevos_sin_sintomas_1",
    "Casos  nuevos  sin  sintomas*": "nuevos_sin_sintomas_2",
    "Casos nuevos sin sintomas* ": "nuevos_sin_sintomas_3",
    "Casos  nuevos  sin  sintomas": "nuevos_sin_sintomas_4",
    " Casos fallecidos": "fallecidos_1",
    "Fallecidos": "fallecidos_2",
    "Fallecidos totales": "fallecidos_3",
    "Fallecidos totales ": "fallecidos_4",
    "Casos probables acumulados": "probables_acumulados_1",
    "Casos probables acumulados ": "probables_acumulados_2",
    "%  Casos  totales**": "porcentaje_total_1",
    "% Total": "porcentaje_total_2",
    "%  Total": "porcentaje_total_3",
    "% Total ": "porcentaje_total_4",
    "Casos confirmados por antigeno": "confirmados_antigeno_1",
    "Casos nuevos confirmados por antigeno": "confirmados_antigeno_2",
    "Casos activos probables": "activos_probables_1",
    "Casos activos probables ": "activos_probables_2",
    "Casos confirmados recuperados": "recuperados_1",
    " Casos recuperados": "recuperados_2",
    "Casos activos confirmados": "activos_confirmados",
    "Casos nuevos sin notificar": "nuevos_sin_notificar",
    "Casos con sospecha de reinfeccion": "sospecha_reinfeccion",
    "Tasa  *100000": "tasa_cien_mil",
    "Incremento  diario": "incremento_diario"
}
columnas_sort = ["fecha", 
    "region", "region_3", "region_4", "region_1", "region_2",
    "nuevos","nuevos_1", "nuevos_2", "nuevos_3", "nuevos_4", "nuevos_5", "nuevos_6", 
    "casos", "casos_1", "casos_2", "casos_3", "casos_4", "casos_5", "casos_6",
    "nuevos_con_sintomas", "nuevos_con_sintomas_1", "nuevos_con_sintomas_2", "nuevos_con_sintomas_3", 
    "nuevos_sin_sintomas", "nuevos_sin_sintomas_1", "nuevos_sin_sintomas_2", "nuevos_sin_sintomas_3", "nuevos_sin_sintomas_4", 
    "fallecidos", "fallecidos_1", "fallecidos_2", "fallecidos_3", "fallecidos_4", 
    "probables_acumulados", "probables_acumulados_1", "probables_acumulados_2",
    "porcentaje_total", "porcentaje_total_1", "porcentaje_total_2", "porcentaje_total_3", "porcentaje_total_4",
    "confirmados_antigeno", "confirmados_antigeno_1", "confirmados_antigeno_2", 
    "activos_probables", "activos_probables_1", "activos_probables_2",
    "recuperados", "recuperados_1", "recuperados_2",
    "activos_confirmados", "nuevos_sin_notificar", "sospecha_reinfeccion",
    "tasa_cien_mil", "incremento_diario"]
columnas_keep = ["fecha", "region", "nuevos", "casos", "nuevos_con_sintomas", "nuevos_sin_sintomas", "fallecidos", 
    "probables_acumulados", "porcentaje_total", "confirmados_antigeno", "activos_probables", "recuperados", 
    "activos_confirmados", "nuevos_sin_notificar", "sospecha_reinfeccion", "tasa_cien_mil", "incremento_diario"]

df = pd.read_csv(os.path.join(csv_files, "casos_por_region.csv"))
df_regiones = pd.read_csv(os.path.join(csv_files, "regiones.csv"))

df.rename(columns = columnas, inplace = True)

df["nuevos"] = df.apply(lambda row: obtener_valor_unico([row.nuevos_1, row.nuevos_2, row.nuevos_3, row.nuevos_4, row.nuevos_5, row.nuevos_6]), axis = 1)
df["casos"] = df.apply(lambda row: obtener_valor_unico([row.casos_1, row.casos_2, row.casos_3, row.casos_4, row.casos_5, row.casos_6]), axis = 1)
df["nuevos_con_sintomas"] = df.apply(lambda row: obtener_valor_unico([row.nuevos_con_sintomas_1, row.nuevos_con_sintomas_2, row.nuevos_con_sintomas_3]), axis = 1)
df["nuevos_sin_sintomas"] = df.apply(lambda row: obtener_valor_unico([row.nuevos_sin_sintomas_1, row.nuevos_sin_sintomas_2, row.nuevos_sin_sintomas_3, row.nuevos_sin_sintomas_4]), axis = 1)
df["fallecidos"] = df.apply(lambda row: obtener_valor_unico([row.fallecidos_1, row.fallecidos_2, row.fallecidos_3, row.fallecidos_4]), axis = 1)
df["probables_acumulados"] = df.apply(lambda row: obtener_valor_unico([row.probables_acumulados_1, row.probables_acumulados_2]), axis = 1)
df["porcentaje_total"] = df.apply(lambda row: obtener_valor_unico([row.porcentaje_total_1, row.porcentaje_total_2, row.porcentaje_total_3, row.porcentaje_total_4]), axis = 1)
df["confirmados_antigeno"] = df.apply(lambda row: obtener_valor_unico([row.confirmados_antigeno_1, row.confirmados_antigeno_2]), axis = 1)
df["activos_probables"] = df.apply(lambda row: obtener_valor_unico([row.activos_probables_1, row.activos_probables_2]), axis = 1)
df["recuperados"] = df.apply(lambda row: obtener_valor_unico([row.recuperados_1, row.recuperados_2]), axis = 1)

region_1 ={
    'Arica y Parinacota': "ARICA Y PARINACOTA",
    'Tarapacá': "TARAPACÁ",
    'Antofagasta': "ANTOFAGASTA",
    'Atacama': "ATACAMA",
    'Coquimbo': "COQUIMBO",
    'Valparaíso': "VALPARAÍSO",
    'Metropolitana': "METROPOLITANA DE SANTIAGO",
    "O'Higgins": "LIBERTADOR GENERAL BERNARDO O'HIGGINS",
    'Maule': "MAULE",
    'Ñuble': "ÑUBLE",
    'Biobío': "BIOBÍO",
    'Araucania': "LA ARAUCANÍA",
    'Los Ríos': "LOS RÍOS",
    'Los Lagos': "LOS LAGOS",
    'Aysén': "AYSÉN DEL GENERAL CARLOS IBÁÑEZ DEL CAMPO",
    'Magallanes': "MAGALLANES Y DE LA ANTÁRTICA CHILENA",
    'Total': "TOTAL",
    'O’Higgins': "LIBERTADOR GENERAL BERNARDO O'HIGGINS",
    'Araucanía': "LA ARAUCANÍA" ,
    'Se desconoce región de origen': "DESCONOCIDA",
    'Arica  y  Parinacota': "ARICA Y PARINACOTA",
    'Tarapaca': "TARAPACÁ" ,
    'Valparaiso': "VALPARAÍSO",
    'Nuble': "ÑUBLE",
    'Biobio': "BIOBÍO",
    'Los  Rios': "LOS RÍOS",
    'Los  Lagos': "LOS LAGOS",
    'Aysen': "AYSÉN DEL GENERAL CARLOS IBÁÑEZ DEL CAMPO",
    'Arica y Paricota': "ARICA Y PARINACOTA",
    'Metropolita': "METROPOLITANA DE SANTIAGO",
    'Los Rios': "LOS RÍOS"
}

region_2 = {
    'Arica  y  Parinacota': "ARICA Y PARINACOTA",
    'Tarapacá': "TARAPACÁ",
    'Antofagasta': "ANTOFAGASTA",
    'Atacama': "ATACAMA",
    'Coquimbo': "COQUIMBO",
    'Valparaíso': "VALPARAÍSO",
    'Metropolitana': "METROPOLITANA DE SANTIAGO",
    'O’Higgins': "LIBERTADOR GENERAL BERNARDO O'HIGGINS",
    'Maule': "MAULE",
    'Ñuble': "ÑUBLE",
    'Biobío': "BIOBÍO",
    'Araucanía': "LA ARAUCANÍA",
    'Los  Ríos': "LOS RÍOS",
    'Los  Lagos': "LOS LAGOS",
    'Aysén': "AYSÉN DEL GENERAL CARLOS IBÁÑEZ DEL CAMPO",
    'Magallanes': "MAGALLANES Y DE LA ANTÁRTICA CHILENA",
    'Total': "TOTAL"
}

df["region_3"] = df.region_1.map(region_1)
df["region_4"] = df.region_2.map(region_2)

df["region"] = df.apply(lambda row: obtener_valor_unico([row.region_3, row.region_4], type = 2), axis = 1)
df_keep = df[columnas_keep].copy()
del df

df_keep_2 = df_keep.merge(
    right = df_regiones,
    how = "left",
    left_on = "region",
    right_on = "NOM_REGION"
). \
    drop(columns = ["NOM_REGION"]). \
    rename(columns = {"REGION": "cod_region"})

df_keep_2.to_parquet(path = os.path.join(parquet_files, "casos_por_region.parquet"))
del df_keep, df_keep_2, df_regiones

# procesar casos por comuna

columnas = {
    "Region": "region",
    "Codigo region": "cod_region",
    "Comuna": "comuna",
    "Codigo comuna": "cod_comuna",
    "Poblacion": "poblacion",
    "Casos Confirmados": "casos",
    "Fecha": "fecha"
}
df = pd.read_csv(os.path.join(csv_files, "casos_por_comuna.csv"))
df_comunas = pd.read_csv(os.path.join(csv_files, "comunas.csv"))
df_regiones = pd.read_csv(os.path.join(csv_files, "regiones.csv"))
df.rename(columns = columnas, inplace = True)
df.drop(columns = ["region", "comuna"], inplace = True)
df_1 = df.merge(
    right = df_comunas,
    left_on = "cod_comuna",
    right_on = "COMUNA",
    how = "left"
). \
    merge(
        right = df_regiones,
        left_on = "cod_region",
        right_on = "REGION",
        how = "left"
    ). \
    drop(columns = ["COMUNA", "REGION"]). \
    rename(columns = {"NOM_COMUNA": "comuna", "NOM_REGION": "region"})
df_1["comuna"] = df_1.apply(lambda row : "DESCONOCIDA" if row.comuna is np.nan else row.comuna, axis = 1)
df_1.to_parquet(path = os.path.join(parquet_files, "casos_por_comuna.parquet"))
del df, df_1, df_comunas, df_regiones

# procesar paso a paso

columnas = ["codigo_region", "codigo_comuna", "zona", "Fecha", "Paso"]
df = pd.read_csv(os.path.join(csv_files, "paso_a_paso_comuna.csv"))
df_comunas = pd.read_csv(os.path.join(csv_files, "comunas.csv"))
df_regiones = pd.read_csv(os.path.join(csv_files, "regiones.csv"))
df_1 = df[columnas]. \
    merge(
        right = df_regiones,
        left_on = "codigo_region",
        right_on = "REGION",
        how = "inner"
    ). \
    merge(
        right = df_comunas,
        left_on = "codigo_comuna",
        right_on = "COMUNA",
        how = "inner"
    ). \
    drop(columns = ["REGION", "COMUNA"]). \
    rename(
        columns = {
            "Fecha": "fecha", 
            "Paso": "paso",
            "NOM_REGION": "region",
            "NOM_COMUNA": "comuna"
        }
    )
df_1["zona"] = df_1.apply(lambda row: row.zona.upper(), axis = 1)
df_1.to_parquet(path = os.path.join(parquet_files, "paso_a_paso_por_comuna.parquet"))
del df, df_1, df_comunas, df_regiones
