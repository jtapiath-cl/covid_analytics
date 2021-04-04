# imports naturales
import os
import pandas as pd
# import from
from git import Repo
from icecream import ic
from datetime import datetime

# configuraciones previas
def log_timestamp():
    return f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} || "

ic.configureOutput(prefix = log_timestamp)

# parametros
url_covid = "https://github.com/MinCiencia/Datos-COVID19.git"
dir_covid = "source"
dir_target = "target"
path_covid = os.path.join(os.getcwd(), dir_covid)
path_target = os.path.join(os.getcwd(), dir_target)

# obtener repo de MinCiencia
if not os.path.exists(path_covid):
    ic("Clonando repo...")
    ic("Clonando repo:: Creando carpeta...")
    os.mkdir(path_covid)
    ic("Clonando repo:: Clonando repositorio desde URI...")
    Repo.clone_from(url = url_covid, to_path = path_covid)
    ic("Clonando repo:: Listo!")
else:
    ic("Actualizando repo...")
    repo = Repo(dir_covid)
    o = repo.remotes.origin
    o.pull()
    ic("Actualizando repo:: Listo!")

# extraer productos para datos de casos
if not os.path.exists(path_target):
    ic("Extraxendo datos objetivo...")
