import logging
import sys
import os
from urllib.parse import quote
import sqlalchemy as sa
from sqlalchemy import text, Engine, Connection, Table, DATE
import yaml
import pandas as pd
from pandas import DataFrame
from pandas.io.sql import SQLTable
import paramiko
from datetime import datetime
from sources import *


logging.basicConfig(
    level=logging.INFO,
    filename=(os.path.join(proyect_dir := os.path.dirname(
        os.path.abspath(__file__)), '..', 'log', 'logs_main.log')),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)

def conexion_sftp(host, username, password, port):
    client = paramiko.SSHClient()

    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(hostname=host, port=port,
                   username=username, password=password)

    sftp = client.open_sftp()
    return sftp

def load_base():

    with conexion_sftp(**source5) as sftp:
        with sftp.open('/Base EPSA Reporte/Epsa.xlsx') as file:
            df_base = pd.read_excel(file, header=0)
            with engine_6() as con:
                df_base.columns = df_base.columns.str.replace(' ', '_')
                df_base.to_sql('tb_base_epsa_bigdata',con=con, if_exists='replace', index=False) 