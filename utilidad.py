import os
from os.path import join, abspath, dirname, exists
from os import mkdir, makedirs
from abc import ABC, abstractmethod
from datetime import datetime
from sys import stdout, exit
from unicodedata import normalize
from pathlib import Path
import re
from logging import Formatter, getLogger, StreamHandler
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from dateutil.relativedelta import relativedelta
url_base = 'https://www.perupetro.com.pe'

url = 'https://www.perupetro.com.pe/wps/portal/corporativo/PerupetroSite/estadisticas/producción%20hidrocarburos/producción%20diaria/'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'es-ES,es;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection':  'keep-alive',
    'Referer': 'https://www.perupetro.com.pe/wps/portal/corporativo/PerupetroSite/estadisticas/estadística%20petrolera/!ut/p/z1/04_Sj9CPykssy0xPLMnMz0vMAfIjo8zi_YxcTTw8TAy93AN8LQwCTUJcvEKADEMfE_1wsAIDHMDRQD8Kt34nXyOofhwKAo3MKNIPcmAUfufh1Q-yIIqQ_6MIWRGFzwwzf1P8CsBhgEeBu7mRsX5BbmhoaIRBpme6oiIAoRD3mw!!/dz/d5/L2dBISEvZ0FBIS9nQSEh/'
}
fecha_actual = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")
wsresult = f'/home/WSResult/IAE170_8218_PeruPetroDiario/{fecha_actual}01/' #f'/home/practicante.ws2@bcrp.gob.pe/IAE170_8218_PeruPetroDiario/data/{fecha_actual}01/' #'/home/WSResult/IAE170_8145_PeruPetro/' #f'/home/WSResult/IAE170_8218_PeruPetroDiario/{fecha_actual}01/'

path = os.getcwd()
pdf_folder_path = f'{path}/pdf/'
csv_path_for_zone_lote = 'zone_lote.csv'
csv_path_for_faltantes = 'lista_operador.csv'


csv_result_path = f'{path}/data/{fecha_actual}01/'

current_words_to_delete = ['DEL', 'SUR', 'SAVIA', 'UNIENERGIA', 'UNNA', 'ENERGIA', 'VIGO', 'ENERGY', 'SUB', 'CEPSA', 'CNPC', 'PERU', 'CORPORACIO', 'PETROLEO', 'CONDENSADO', 'LIQUIDOS', 'DEL', 'GAS', 'NATURAL', 'LGN', 'TOTAL', 'MONTERRICO', 'OIG', 'OLYMPIC', 'PERENCO', 'PETROPERÚ', 'PETROTAL', 'PLUSPETROL', 'AGUAYTIA', 'UNNA', 'ENERGIA', 'UNIENERGIA', 'PERENCO', 'CEPSA', 'CNPC', 'PERU', 'N', 'Y', 'REPSOL']

xpath_to_bls_data = '//table//tbody//tr//td[position() = 2]//a//@href'
xpath_to_mpc_data = '//table//tbody//tr//td[position() = 3]//a//@href'
xpath_elemnt_to_extract = '//table//tbody//tr//td//span//text()'

meses = {
    'ENE': 1,
    'FEB': 2,
    'MAR': 3,
    'ABR': 4,
    'MAY': 5,
    'JUN': 6,
    'JUL': 7,
    'AGO': 8,
    'SEP': 9,
    'OCT': 10,
    'NOV': 11,
    'DIC': 12
}


class Base(ABC):
    def __init__(self):
        self.pytnon_ubicacion = abspath(dirname(__file__)) 
        self.ruta_data = join(self.pytnon_ubicacion, "data")
        self.ruta_logs = join(self.pytnon_ubicacion, "logs")
        crear_carpeta(self.ruta_data) 
        self.hoy = datetime.today()
        self.log = obtener_logger(self.__class__.__name__, self.ruta_logs)

def crear_carpeta(ruta):
    if not exists(ruta):
        mkdir(ruta)

def obtener_logger(nombre, ruta=None, max_dias=14):
    """Obtiene logger con el nombre establecido.

    Parameters
    ----------
    nombre : str
        Nombre del logger.
    ruta : str or None, default None
        Ruta de carpeta en donde se almacenan los logs.
    max_dias : int, default 14
        Cantidad máxima de días que se almacenan los logs.

    -------
    Logger
        Logger con el nombre establecido.
    """
    logger = getLogger(nombre)
    if logger.hasHandlers():
        return logger

    logger.setLevel(10) #DEBUG

    log_format = '%(asctime)s %(levelname)s: %(message)s [en %(filename)s:%(funcName)s %(lineno)d]'
    
    stream_handler = StreamHandler(stdout) #31-01-25
    stream_handler.setLevel(20) #INFO
    stream_handler.setFormatter(Formatter(log_format))
    logger.addHandler(stream_handler)

    fecha_actual = datetime.now().strftime("%Y%m%d")
    #ruta_logs_fecha = join(ruta or Path.cwd(), fecha_actual)

    #makedirs(ruta_logs_fecha, exist_ok=True)

    #ruta_archivo_log = join(ruta_logs_fecha, f'{nombre}_{fecha_actual}.log')
    
    
    #------------------------- ---
    ruta_logs = str(Path(ruta or Path.cwd(), f'{nombre}_{fecha_actual}.log'))
    
    # Verificar si la ruta existe, y crear los directorios si es necesario
    makedirs(dirname(ruta_logs), exist_ok=True)
    if not exists(ruta_logs):
        open(ruta_logs, 'a+').close()
    #----------------------------
    

    file_handler = TimedRotatingFileHandler(ruta_logs, when='midnight', backupCount=max_dias, encoding='utf-8')
    file_handler.setLevel(10)
    file_handler.setFormatter(Formatter(log_format))
    logger.addHandler(file_handler)

    return logger

def agregar_logger(funcion):  
    def wrapper(*args, **kwargs):
        nombre_funcion = funcion.__name__
        try:
            resultado = funcion(*args, **kwargs)
        except Exception as e:
            args[0].log.debug(nombre_funcion)
            args[0].log.debug(str(e))
            exit() 
        return resultado
    return wrapper

def generate_file_name_bls(month):
    file_name = f'peru_petro_hidrocarburoslíquidos_d_{month[:3].lower()}_{month[3:]}.csv'
    return file_name

def generate_file_name_mpc(month):
    file_name = f'peru_petro_gasnatural_d_{month[:3].lower()}_{month[3:]}.csv'
    return file_name

def file_name(type_data): 
    if type_data == 'bls':
        return 'peru_petro_hidrocarburoslíquidos_d_'
    else:
        return 'peru_petro_gasnatural_d_'

def obtener_iniciales_mes(numero_mes):
    meses_invertido = {v: k for k, v in meses.items()}
    return meses_invertido.get(numero_mes, "Mes no válido")
    
def verify_folder_content():
    year_actual = datetime.now().year
    fecha_actual = datetime.now()
    mes_inicial = datetime.now().month - 1
    mes = obtener_iniciales_mes(mes_inicial).lower()
    file_liquidos_data = f'{csv_result_path}peru_petro_hidrocarburoslíquidos_d_{str(mes).lower()}_{year_actual}.csv'
    file_gas_data = f'{csv_result_path}peru_petro_gasnatural_d_{str(mes).lower()}_{year_actual}.csv'
    
    file_liquidos_ws = f'{wsresult}peru_petro_hidrocarburoslíquidos_d_{str(mes).lower()}_{year_actual}.csv'
    file_gas_ws = f'{wsresult}peru_petro_gasnatural_d_{str(mes).lower()}_{year_actual}.csv'
    
    
    if not exists(wsresult) and not os.path.exists(file_liquidos_ws) and not os.path.exists(file_gas_ws):
        
        if not exists(csv_result_path):
            print(f'No existe, se ha creado la carpeta:{csv_result_path}')
            os.mkdir(csv_result_path)
            return True
        else:
            if os.path.exists(file_liquidos_data) and os.path.exists(file_gas_data):
                return False
            else:
                return True
    else:
        if exists(wsresult) and not os.path.exists(file_liquidos_ws) and not os.path.exists(file_gas_ws):
            if not exists(csv_result_path):
                print(f'No existe, se ha creado la carpeta:{csv_result_path}')
                os.mkdir(csv_result_path)
                return True
            elif exists(csv_result_path) and not os.path.exists(file_liquidos_data) and not os.path.exists(file_gas_data):
                return True 
            elif exists(csv_result_path) and os.path.exists(file_liquidos_data) and os.path.exists(file_gas_data):
                return False
        return False
