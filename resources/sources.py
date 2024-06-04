#!/usr/bin/python

import sys

sys.path.append('/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/resources')

from urllib.parse import quote
import sqlalchemy as sa
import logging_config as log
import yaml


def get_engine(username: str, password: str, host: str, database: str, port: str = 3306, *_):
    return sa.create_engine(f"mysql+pymysql://{username}:{quote(password)}@{host}:{port}/{database}")

with open('/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/config_files/config.yml','r') as f:

    try:
        config = yaml.safe_load(f)
        source1, source2, source3, source4 = config['source1'], config['source2'], config['source3'], config['source4']
        source5, source6 = config['source5'], config['source6']
    except yaml.YAMLError as e:
        log.logging.error(str(e), exc_info=True)

def engine_1():
    return get_engine(**source1).connect()

def engine_2():
    return get_engine(**source2).connect()

def engine_3():
    return get_engine(**source3).connect()

def engine_4():
    return get_engine(**source4).connect()

def engine_6():
    return get_engine(**source6).connect()