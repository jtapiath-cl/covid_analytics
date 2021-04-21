"""
Script para obtener los datos de origen necesarios para el análisis de datos basal. En este proceso se obtienen los siguientes
sets de datos:

* Detalle de casos a nivel nacional (producto de datos 5)
* Detalle de casos a nivel regional (producto de datos 4)
* Detalle de casos a nivel comunal (producto de datos 2)
* Cantidad de examenes PCR aplicados por region (producto de datos 7)
* Cantidad de examenes PCR notificados como positivos por comuna (producto de datos 63)
* Cantidad de examenes PCR aplicados como Busqueda Activa de Casos (BAC) por comuna (producto de datos 64)
* Estado en el plan Paso a Paso por comuna (producto de datos 74)

El proceso obtiene los datos desde el repositorio de GitHub del MinCiencia, y luego se extraen los datos en bruto de los
productos de datos especificados. Finalmente, se procesan y renombran los archivos CSV necesarios y se eliminan los archivos
transitorios (el repositorio se mantiene persistente para minimizar los tiempos de proceso).
"""

# imports naturales
import os
import glob
import shutil
import patoolib
import subprocess
import urllib.request
import pandas as pd
# import from
from git import Repo
from datetime import datetime

# configuraciones previas
def print_log(log_string:str):
    """
    Función que imprime el log de ejecución en formato YYYY-MM-DD HH:MM:SS || user || mensaje.

    Parámetros:
    - log_string (str): mensaje a imprimir en la pantalla
    """
    user = subprocess.check_output(["whoami"]).decode().split()[0]
    print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} || {user} || {log_string}")
    return None

def copiar_carpeta(files_from:str, files_to: str):
    """
    Función que copia los datos desde el origen, en el repo de GitHub, hacia la ubicación de destino. Si la carpeta
    de destino no existe, la función la crea.

    Parámetros:
    - files_from (str): ubicación de la carpeta de origen
    - files_to (str): ubicación de la carpeta de destino
    """
    if not os.path.exists(files_to):
        os.makedirs(files_to)
    shutil.copytree(src = files_from, dst = files_to, dirs_exist_ok = True)
    return None

def agregar_fecha_df(file:str):
    """
    Función que añade una columna a un DataFrame de Pandas con la fecha que viene en el nombre del archivo.

    Parámetros
    - file (str): Ubicación del archivo CSV a leer
    """
    fecha = os.path.basename(file)[0:10].replace("-", "")
    df_temp = pd.read_csv(file)
    df_temp["Fecha"] = fecha
    return df_temp

def procesar_producto(target:str, grupo:str, destination:str):
    """
    Función que procesa los archivos CSV correspondientes a un producto de datos, concatenándolos, procesándolos y
    escribiendo un archivo CSV final en una ubicación final.

    Parámetros:
    - target (str): ubicación del producto de datos
    - grupo (str): agrupacion geográfica correspondiente al producto. Por ejemplo, casos _por región_ o _por comuna_
    - destination (str): ubicación de destino del archivo final
    """
    os.chdir(path = target)
    files = glob.glob(os.path.join(os.getcwd(), "*.csv"))
    df = pd.concat([agregar_fecha_df(file) for file in files])
    print_log("Escribiendo datos en archivo final...")
    df.to_csv(os.path.join(destination, f"casos_por_{grupo}.csv"), index = False)
    return None

def procesar_producto_2(target:str, grupo:str, archivo:str, destination:str):
    """
    Función que procesa el archivo CSV correspondiente a un producto de datos, procesándolo y escribiendo un archivo CSV final
    en una ubicación final.

    Parámetros:
    - target (str): ubicación del producto de datos
    - grupo (str): agrupacion geográfica correspondiente al producto. Por ejemplo, casos _por región_ o _por comuna_
    - destination (str): ubicación de destino del archivo final
    """
    df = pd.read_csv(os.path.join(target, f"{archivo}.csv"))
    if "Fecha" in df.columns:
        df["Fecha"] = df["Fecha"].str.replace("-", "")
    elif "fecha" in df.columns:
        df["fecha"] = df["fecha"].str.replace("-", "")
        df.rename(columns = {"fecha": "Fecha"}, inplace = True)
    df.to_csv(os.path.join(destination, f"{grupo}.csv"), index = False)
    return None

def procesar_externo(input_dir:str, input_file:str, output_dir:str, output_file:str):
    df = pd.read_csv(os.path.join(input_dir, input_file), sep = ";")
    df.to_csv(os.path.join(output_dir, f"{output_file}.csv"), sep = ",", index = False)
    return None

# parametros
url_covid = "https://github.com/MinCiencia/Datos-COVID19.git"
url_comunas = "https://www.ine.cl/docs/default-source/censo-de-poblacion-y-vivienda/bbdd/censo-2017/csv/csv-identificaci%C3%B3n-geogr%C3%A1fica-censo-2017.rar?sfvrsn=1ae6f56c_2&download=true"
dir_covid = "source"
dir_target = "target"
curr_wd = os.getcwd()
path_covid = os.path.join(curr_wd, dir_covid)
path_target = os.path.join(curr_wd, dir_target)
## ubicaciones fisicas de los archivos
base_dir = os.path.join(curr_wd, "target", "output")
output_dir = os.path.join(curr_wd, "target", "csv")
download_dir = os.path.join(curr_wd, "target")
geografia_dir = os.path.join(curr_wd, "target", "geografia")
prod02 = os.path.join(curr_wd, "source", "output", "producto2")
prod04 = os.path.join(curr_wd, "source", "output", "producto4")
prod05 = os.path.join(curr_wd, "source", "output", "producto5")
prod07 = os.path.join(curr_wd, "source", "output", "producto7")
prod63 = os.path.join(curr_wd, "source", "output", "producto63")
prod64 = os.path.join(curr_wd, "source", "output", "producto64")
prod74 = os.path.join(curr_wd, "source", "output", "producto74")
target02 = os.path.join(path_target, "output", "producto2")
target04 = os.path.join(path_target, "output", "producto4")
target05 = os.path.join(path_target, "output", "producto5")
target07 = os.path.join(path_target, "output", "producto7")
target63 = os.path.join(path_target, "output", "producto63")
target64 = os.path.join(path_target, "output", "producto64")
target74 = os.path.join(path_target, "output", "producto74")
geografia_rar = os.path.join(download_dir, "csv_geografica.rar")

# obtener repo de MinCiencia
if not os.path.exists(path_covid):
    print_log("Clonando repo...")
    print_log("Creando carpeta")
    os.mkdir(path_covid)
    print_log("Clonando repositorio desde URI")
    Repo.clone_from(url = url_covid, to_path = path_covid)
    print_log("Listo!")
else:
    print_log("Actualizando repo...")
    repo = Repo(dir_covid)
    o = repo.remotes.origin
    o.pull()
    print_log("Listo!")

# extraer productos para datos de casos
## extraccion de archivos
if not os.path.exists(path_target):
    print_log("Extraxendo datos objetivo para analisis de casos...")
    print_log("Creando carpeta")
    os.mkdir(path_target)
    print_log("Moviendo archivos de 'producto2'")
    copiar_carpeta(files_from = prod02, files_to = target02)
    print_log("Moviendo archivos de 'producto4'")
    copiar_carpeta(files_from = prod04, files_to = target04)
    print_log("Moviendo archivos de 'producto5'")
    copiar_carpeta(files_from = prod05, files_to = target05)
    print_log("Listo!")
else:
    print_log("Extrayendo datos objetivo para analisis de casos...")
    print_log("Moviendo archivos de 'producto2'")
    copiar_carpeta(files_from = prod02, files_to = target02)
    print_log("Moviendo archivos de 'producto4'")
    copiar_carpeta(files_from = prod04, files_to = target04)
    print_log("Moviendo archivos de 'producto5'")
    copiar_carpeta(files_from = prod05, files_to = target05)
    print_log("Listo!")

## procesado del prod02
print_log("Procesando datos de 'producto02'...")
if not os.path.exists(output_dir):
    os.mkdir(os.path.join(output_dir))
procesar_producto(target = target02, grupo = "comuna", destination = output_dir)
print_log("Listo!")

## procesado del prod04
print_log("Procesando datos de 'producto04'...")
procesar_producto(target = target04, grupo = "region", destination = output_dir)
print_log("Listo!")

## procesado del prod05
print_log("Procesando datos de 'producto05'...")
procesar_producto_2(target = target05, grupo = "casos_por_pais", archivo = "TotalesNacionales_T", destination = output_dir)
print_log("Listo!")

# extraer productos para datos de examenes
## extraccion de archivos
print_log("Extrayendo datos objetivo para analisis de examenes...")
print_log("Moviendo archivos de 'producto07")
copiar_carpeta(files_from = prod07, files_to = target07)
print_log("Moviendo archivos de 'producto63'")
copiar_carpeta(files_from = prod63, files_to = target63)
print_log("Moviendo archivos de 'product64'")
copiar_carpeta(files_from = prod64, files_to = target64)
print_log("Listo!")

## procesado del prod07
print_log("Procesando datos del 'producto07'...")
procesar_producto_2(target = target07, grupo = "examenes_totales_por_region", archivo = "PCR_std", destination = output_dir)
print_log("Listo!")

## procesado del prod63
print_log("Procesando datos del 'producto63'...")
procesar_producto_2(target = target63, grupo = "examenes_notificados_por_comuna", archivo = "NNotificacionPorComuna_std", destination = output_dir)
print_log("Listo!")

## procesado del prod64
print_log("Procesando datos del 'producto64'...")
procesar_producto_2(target = target64, grupo = "examenes_bac_por_comuna", archivo = "BACPorComuna_std", destination = output_dir)
print_log("Listo!")

# extraer otros productos de datos
# extraer productos para datos de examenes
## extraccion de archivos
print_log("Extrayendo datos objetivo para analisis adicionales...")
print_log("Moviendo archivos de 'producto74'")
copiar_carpeta(files_from = prod74, files_to = target74)
print_log("Listo!")

## procesado del prod74
print_log("Procesando datos del 'producto74'...")
procesar_producto_2(target = target74, grupo = "paso_a_paso_comuna", archivo = "paso_a_paso_std", destination = output_dir)
print_log("Listo!")

# descargar archivo de comunas del censo 2017
print_log("Descargando archivos del Censo 2017...")
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

urllib.request.urlretrieve(url_comunas, geografia_rar)
print_log("Listo!")

## procesando archivos de geografia del censo 2017
print_log("Procesando archivos del Censo 2017...")
if not os.path.exists(geografia_dir):
    os.makedirs(geografia_dir)

patoolib.extract_archive(geografia_rar, outdir = geografia_dir)

procesar_externo(input_dir = geografia_dir, input_file = "microdato_censo2017-comunas.csv", output_dir = output_dir, output_file = "comunas")
procesar_externo(input_dir = geografia_dir, input_file = "microdato_censo2017-regiones.csv", output_dir = output_dir, output_file = "regiones")

print_log("Listo!")

# borrando archivos temporales
print_log("Borrando archivos creados en el proceso...")
try:
    shutil.rmtree(base_dir)
    shutil.rmtree(geografia_dir)
    os.remove(geografia_rar)
except OSError as e:
    print(f"Error: {e.strerror}")
finally:
    print_log("Listo!")
