#!/usr/bin/python

import sys

sys.path.append('/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/resources')

from resources import etl, etl_2, epsa_base

if __name__=="__main__":

    #epsa_base.load_base()
    etl.etl()
    etl_2.etl_2()
    