import sys
import numpy as np

sys.path.append('/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/resources')

import pandas as pd
import logging_config as log
import sources
from datetime import datetime
from sqlalchemy import text, Engine, Connection, Table, DATE
from pandas.io.sql import SQLTable
import re

def eliminar_caracteres_especiales(texto):
    patron = re.compile(r'[^a-zA-Z0-9\s.,;!?¡¿\-\'\"():\[\]{}\u00E1\u00E9\u00ED\u00F3\u00FA\u00C1\u00C9\u00CD\u00D3\u00DA]')
    return patron.sub('', texto)

def to_sql_replace(table: SQLTable, con: Engine | Connection, keys: list[str], data_iter):

    satable: Table = table.table
    ckeys = list(map(lambda s: s.replace(' ', '_'), keys))
    data = [dict(zip(ckeys, row)) for row in data_iter]
    values = ', '.join(f':{nm}' for nm in ckeys)
    stmt = f"REPLACE INTO {satable.name} VALUES ({values})"
    con.execute(text(stmt), data)

def asign(value):
     cos = ['17450','17449','15839','44275','15870','39681','40661','15826','44856','36116',
            '25111','15838','26450','15857','44857','40660','45582']
     sbs = ['45314','43382','29930','34961','31965','17910','17883']
     saf = ['45596','45505','45506']
     if str(value) in cos:
          return 'COS'
     elif str(value) in sbs:
          return 'SBS'
     elif str(value) in saf:
          return 'SAF'
     else: 
          return str(value)

def etl_2():
    with sources.engine_6() as con:
        df_base_epsa = pd.read_sql("SELECT distinct CUE, ID_CUENTA, ID_TERCERO FROM bbdd_cos_bog_sbs.tb_base_epsa_bigdata;",con)
    with sources.engine_1() as con:
        df_usuarios=pd.read_sql("(SELECT * FROM crm_masterclaro.usuarios)",con
                                    ).rename(columns={"id_usuario":"id_vanti",
                                                      "nombre_usuario":"nombre_usuario_cerro",
                                                      "apellido_usuario":"apellido_usuario_cerro"})
        
        df_retencion=pd.read_sql("(select* from inbound_pqrs_retenciones)",con
                                 ).rename(columns={"id_pqr":"id_pqr_ret",
                                                  "created_at": "created_at_retencion",
                                                  "cert_willis": "CER WILLIS",
                                                  "cert_sbs": "CER_AIG",
                                                  "newcuentanic": "CUE_NUE",
                                                  "newdireccion": "DIR",
                                                  "id_usuario": "id_usuarior"})
        
        df_pqrs=pd.read_sql('(select* from appscosbs.inbound_pqrs)',con)
        df_pqrs["fecha_reporte"]=df_pqrs['created_at'
                                         ].astype(str).apply(lambda x:x.split(' ')[0] if x!= None else None)

        df_pqrs["fecha_reporte"]=pd.to_datetime(df_pqrs['created_at']).dt.strftime('%d/%m/%Y')

        df_pqrs=df_pqrs.rename(columns={"id_tipollamada": "id_tipollamada_pqr",
                                                "created_at": "fecha1"})
           
        df_estados=pd.read_sql("select* from appscosbs.estados",con
                               ).rename(columns={"id":"id_estado1",
                                                    "descripcion":"descripcion_estado"})
        
        df_inbound_pqrs_otros=pd.read_sql('(select* from appscosbs.inbound_pqrs_otros)',con
                                          ).rename(columns={"nic": "CUENT"})
        
        df_asegurado=pd.read_sql('select* from appscosbs.inbound_pqrs_asegurados',con
                                 ).rename(columns={"id_pqr":"id_pqr1",
                                                   "identificacion":"CEDULA",
                                                   "telefono1": "TEL",
                                                   "celular1": "CEL"})
        
        df_departamentos=pd.read_sql('(select* from appscosbs.departamentos)',con
                                     ).rename(columns={"descripcion":"CIUDAD",
                                                       "id_departamento":"id_dpto"})
        
        df_solicitante=pd.read_sql('(select* from appscosbs.inbound_pqrs_solicitantes)',con
                                   ).rename(columns={"id":"id_solicit",
                                                     "nombres":"nombres_solic",
                                                     "id_pqr":"id_pqr_solic"})
        
        df_tipi=pd.read_sql('(select* from appscosbs.tipificaciones)',con
                            ).rename(columns={"id":"id_tipi",
                                              "estado":"estado_tipi",
                                              "descripcion":"descripcion_tipi",
                                              "id_principal":"id_principal_tipi",
                                              "id_secundario":"id_secundario_tipi",
                                              "opcion1":"opcion1_tipi",
                                              "opcion2":"opcion2_tipi"})
        
        df_tipi_sponsor=pd.read_sql('(select* from appscosbs.tipificaciones)',con
                            ).rename(columns={"descripcion":"ENT",
                                                      "id":"id_sponsor",
                                                      "estado":"estado_sponsor",
                                                      "id_principal":"id_principal_sponsor",
                                                      "id_secundario":"id_secundario_sponsor",
                                                      "opcion1":"opcion1_sponsor",
                                                      "opcion2":"opcion2_sponsor"})
        
        df_tipi_medio=pd.read_sql('(select* from appscosbs.tipificaciones)',con
                            ).rename(columns={"descripcion":"MEDIO",
                                                    "id":"id_medio",
                                                    "estado":"estado_medio",
                                                    "id_principal":"id_principal_medio",
                                                    "id_secundario":"id_secundario_medio",
                                                    "opcion1":"opcion1_medio",
                                                    "opcion2":"opcion2_medio"})
        
        df_tipi_producto=pd.read_sql('(select* from appscosbs.tipificaciones)',con
                            ).rename(columns={"descripcion":"PRO1",
                                                    "id":"id_producto",
                                                    "estado":"estado_producto",
                                                    "id_principal":"id_principal_producto",
                                                    "id_secundario":"id_secundario_producto",
                                                    "opcion1":"opcion1_producto",
                                                    "opcion2":"opcion2_producto"})
        
        df_tipi_causal=pd.read_sql('(select* from appscosbs.tipificaciones)',con
                            ).rename(columns={"descripcion":"MOT_CAN",
                                                    "id":"id_causal",
                                                    "estado":"estado_causal",
                                                    "id_principal":"id_principal_causal",
                                                    "id_secundario":"id_secundario_causal",
                                                    "opcion1":"opcion1_causal",
                                                    "opcion2":"opcion2_causal"})


        df_tipi_subcausal=pd.read_sql('(select* from appscosbs.tipificaciones)',con
                            ).rename(columns={"descripcion":"CAUSA",
                                                    "id":"id_subcausal",
                                                    "estado":"estado_subcausal",
                                                    "id_principal":"id_principal_subcausal",
                                                    "id_secundario":"id_secundario_subcausal",
                                                    "opcion1":"opcion1_subcausal",
                                                    "opcion2":"opcion2_subcausal"})

        df_tipi_ret_estado=pd.read_sql('(select* from appscosbs.tipificaciones)',con
                            ).rename(columns={"descripcion":"ret_estado1",
                                                    "id":"id_retencion_estado",
                                                    "estado":"retencionestado",
                                                    "id_principal":"id_principal_retencionestado",
                                                    "id_secundario":"id_secundario_retencionestado",
                                                    "opcion1":"opcion1_retencionestado",
                                                    "opcion2":"opcion2_retencionestado"})
        
    with sources.engine_2() as con:

        df_proyectos=pd.read_sql('select* from appoutbound.proyectos',con
                                     ).rename(columns={"id":"id_proy",
                                                       "descripcion":"descripcion_proy"}) 

        df_usuarios_permisos=pd.read_sql('select* from appoutbound.usuarios_permisos',con
                                     ).rename(columns={"id_usuario": "id_usuariot",
                                                       "url": "url_usuario"})  
        
        df_join = pd.merge(df_retencion, df_pqrs, left_on='id_pqr_ret', right_on='id', suffixes=('', '_A'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_proyectos, left_on='oldid_proyecto', right_on='id_proy', suffixes=('', '_B'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_asegurado, left_on='id_pqr_ret', right_on='id_pqr1', suffixes=('', '_C'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_estados, left_on='id_estado', right_on='id_estado1', suffixes=('', '_D'), how='left')    
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_departamentos, left_on='id_departamento', right_on='id_dpto', suffixes=('', '_E'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_inbound_pqrs_otros, left_on='id_pqr_ret', right_on='id_pqr', suffixes=('', '_F'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_tipi_sponsor, left_on='id_sponsor', right_on='id_sponsor', suffixes=('', '_G'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_tipi_medio, left_on='pqrmedio', right_on='id_medio', suffixes=('', '_H'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_tipi_producto, left_on='id_producto', right_on='id_producto', suffixes=('', '_I'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_tipi_causal, left_on='pqrcausal', right_on='id_causal', suffixes=('', '_I'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_tipi_subcausal, left_on='pqrsubcausal', right_on='id_subcausal', suffixes=('', '_I'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_usuarios, left_on='id_usuario_cerro', right_on='id_vanti', suffixes=('', '_I'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_tipi_ret_estado, left_on='retenido', right_on='id_retencion_estado', suffixes=('', '_I'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join = df_join.merge(df_usuarios_permisos, left_on='id_usuarior', right_on='id_usuariot', suffixes=('', '_J'), how='left')
        print(len(df_join), df_join.shape[1])
        df_join['CUENT'] = df_join['CUENT'].astype(str)
        df_base_epsa['CUE'] = df_base_epsa['CUE'].astype(str)
        df_join = df_join.merge(df_base_epsa, left_on='CUENT', right_on='CUE', suffixes=('', '_K'), how='left')

        df_join['descripcion'] = df_join['descripcion'].str.strip()

        df_join['descripcion'] = df_join['MEDIO'] + ' - ' +df_join['descripcion']

        df_join = df_join.rename(columns={"id_pqr_ret": "No PQR",
                                          "opcion1_producto": "POL",
                                          "fecha_reporte": "FECHA",
                                          "descripcion":"OBSERVACIONES"})
        
        df_join['OBSERVACIONES'] = df_join['OBSERVACIONES'].apply(
            eliminar_caracteres_especiales)

        df_join['asignacion_area'] = df_join['id_usuario_cerro'].apply(asign)

        def asignar_gestion(valor_ent):
            if valor_ent == 'AFINIA - EL':
                    return 'report_1'
            elif valor_ent == 'AIRE - EL':
                    return 'report_1'
            elif valor_ent == 'CELSIA EPSA':
                    return 'report_2'
            else:
                    return 'Not report'

        df_join['ENT_1'] = df_join['ENT'].apply(asignar_gestion)

        df_join['NOMBr'] = df_join['nombres'] + ' ' + df_join['apellido1']

        df_join['NOMBRE'] = df_join['NOMBr'] + ' ' + df_join['apellido2']

        df_join['NOMBRE'] = df_join['NOMBRE'].str.upper()

        df_join['FACTURA'] = '0'
        df_join['VR_CTA'] = '0'
        df_join['CANCELAR'] = 'S'
        df_join['DEVOLVER'] = 'N/A'

        df_join['MOTIVO'] = df_join['CAUSA'] + ' - PQR'+' '+  df_join['No PQR'].astype(str)

        df_join['SOLICITA'] = 'Asegurado'

        df_join['CED_NUE'] = ''
        df_join['NOMB'] = ''
        df_join['AP1'] = ''
        df_join['AP2'] = ''
        df_join['OBS'] = ''
        df_join['Proveedor'] = ''
        df_join['Barrio'] = ''
        df_join['Vivienda'] = ''
        df_join['Uso'] = ''
        df_join['NACE'] = ''
        df_join['CONTRO'] = 'COS-'

        #df_join['PRO1'] = df_join['id_producto'].astype(str) + ' - ' +df_join['PRO1']

        df_join = df_join.rename(columns={"CER WILLIS": "CERT",
                                          "CER_AIG": "CERT_SBS",
                                          "PRO1": "CONCEPTO_DE_POLIZA"})

        currentdate = datetime.now().strftime("%Y-%m-%d") 

        df_CANCELACION_ECAo = df_join[df_join['ENT_1'] == 'report_1']

        df_CANCELACION_ECAo['CONTROL_'] = df_CANCELACION_ECAo['asignacion_area']+ ' - '  + df_CANCELACION_ECAo[
             'nombre_usuario_cerro'] + ' ' + df_CANCELACION_ECAo['apellido_usuario_cerro']
        
        df_CANCELACION_ECA = df_CANCELACION_ECAo[df_CANCELACION_ECAo["MOT_CAN"] == 'CANCELACIÓN DE LA PÓLIZA']
        
        #df_CANCELACION_ECA['CONTROL_'] = df_CANCELACION_ECA['asignacion_area'] + df_CANCELACION_ECA['usuario_red_id']

        df_retencion_eca = df_CANCELACION_ECA[['ENT','CERT','CERT_SBS','CUENT','CEDULA','NOMBRE','POL',
                                               'FACTURA', 'VR_CTA','CANCELAR','DEVOLVER','FECHA','MOTIVO',
                                               'CONTROL_','CONCEPTO_DE_POLIZA','OBSERVACIONES', 'MOT_CAN']]
        
        df_CAMBIO_ECA = df_CANCELACION_ECAo[['ENT','POL','CUENT','CUE_NUE','CEDULA','CERT','OBSERVACIONES',
                                             'CERT_SBS','NOMBRE','SOLICITA','No PQR',"CONTROL_",'ret_estado1']]
        
        df_CAMBIO_ECA = df_CAMBIO_ECA[df_CAMBIO_ECA['ret_estado1'] == 'TRASLADO DE CUENTA']

        df_CAMBIO_ECA = df_CAMBIO_ECA.rename(columns = {"CUENT": "CUENT_ANT",
                                                        "CEDULA": "CED",
                                                        "OBSERVACIONES": "OBS_",
                                                        "CERT_SBS": "CERT_AIG",
                                                        "NOMBRE": "ASEGURADO",
                                                        "No PQR": "PQR"})
        
        df_CAMBIO_ECA = df_CAMBIO_ECA[['ENT','POL','CUENT_ANT','CUE_NUE','CED','CERT','OBS_','CERT_AIG','ASEGURADO',
                                       'SOLICITA','PQR',"CONTROL_"]]
        
        df_MOD_DATOS_ECA = df_CANCELACION_ECAo[['ENT','POL','CUE_NUE','CEDULA','CERT','DIR','TEL','CEL',
                                                'NACE','CIUDAD','CED_NUE','NOMB','AP1','AP2','OBS','MEDIO',
                                                'Proveedor','Barrio','Vivienda','Uso',"CONTROL_"]]
        
        df_MOD_DATOS_ECA = df_MOD_DATOS_ECA.rename(columns = {"CEDULA": "CED",
                                                              "MEDIO": "CANAL"})
        
        df_MOD_DATOS_ECA = df_MOD_DATOS_ECA [['ENT','POL','CUE_NUE','CED','CERT','DIR','TEL','CEL','NACE',
                                              'CIUDAD','CED_NUE','NOMB','AP1','AP2','OBS','CANAL',
                                              'Proveedor','Barrio','Vivienda','Uso',"CONTROL_"]]
        
        ruta_archivo = "/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/data/eca/retencion_eca-" + currentdate + ".xlsx"
        file_path = "/home/usr-dwh/Escritorio/Compartida_L/OPERACIONES/SBS\ INBOUND/ASESORES/Bases/BASES\ SPONSOR/BASES_VANTI/eca/retencion_eca-" + currentdate + ".xlsx"

        df_retencion_eca.to_excel(ruta_archivo, index=False, sheet_name='CANCELACION_ECA', header=True)    
        
        with sources.engine_6() as conn:
            df_retencion_eca['FECHA'] = pd.to_datetime(df_retencion_eca['FECHA'], format='%d/%m/%Y')

            df_retencion_eca.to_sql('tb_reporte_retencion_cancelacion_eca', con=conn,
                                                if_exists='append', index=False, method=to_sql_replace)  
        print(f"retencion_eca {len(df_retencion_eca)}")

        with pd.ExcelWriter(ruta_archivo, mode='a', engine='openpyxl') as writer:
            df_CAMBIO_ECA.to_excel(writer, sheet_name='CAMBIO_ECA', index=False, header=True)
            print(f"CAMBIO_ECA {len(df_CAMBIO_ECA)}")
            with sources.engine_6() as conn:
                df_CAMBIO_ECA.to_sql('tb_reporte_retencion_cambio_eca', con=conn,
                                                if_exists='replace', index=False)

        with pd.ExcelWriter(ruta_archivo, mode='a', engine='openpyxl') as writer:
            df_MOD_DATOS_ECA.to_excel(writer, sheet_name='MOD_DATOS_ECA', index=False, header=True)
            print(f"MOD_DATOS_ECA {len(df_MOD_DATOS_ECA)}")  
            with sources.engine_6() as conn:
                df_MOD_DATOS_ECA.to_sql('tb_reporte_retencion_mod_datos_eca', con=conn,
                                                if_exists='append', index=False, method=to_sql_replace)  

        """df_retencion_eca.to_excel(file_path, index=False, sheet_name='CANCELACION_ECA', header=True)      
        print(f"retencion_eca {len(df_retencion_eca)}")

        with pd.ExcelWriter(file_path, mode='a', engine='openpyxl') as writer:
            df_CAMBIO_ECA.to_excel(writer, sheet_name='CAMBIO_ECA', index=False, header=True)
            print(f"CAMBIO_ECA {len(df_CAMBIO_ECA)}")

        with pd.ExcelWriter(file_path, mode='a', engine='openpyxl') as writer:
            df_MOD_DATOS_ECA.to_excel(writer, sheet_name='MOD_DATOS_ECA', index=False, header=True)
            print(f"MOD_DATOS_ECA {len(df_MOD_DATOS_ECA)}") """    

        df_CANCELACION_EPSAo = df_join[df_join['ENT_1'] == 'report_2'] 

        df_CANCELACION_EPSAo ['ENT'] = 'CELSIA EPSA'

        df_CANCELACION_EPSAo['CONTROL_'] = df_CANCELACION_EPSAo['asignacion_area']+ ' - '  + df_CANCELACION_EPSAo[
             'nombre_usuario_cerro'] + ' ' + df_CANCELACION_EPSAo['apellido_usuario_cerro']

        df_CANCELACION_EPSA = df_CANCELACION_EPSAo[df_CANCELACION_EPSAo['MOT_CAN'] == 'CANCELACIÓN DE LA PÓLIZA'] 

        #df_CANCELACION_EPSA['CONTROL_'] = df_CANCELACION_EPSA['asignacion_area'] + df_CANCELACION_EPSA['usuario_red_id']

        df_retencion_epsa = df_CANCELACION_EPSA[['ENT','CERT','CERT_SBS','CUENT','CEDULA','NOMBRE','POL',
                                                 'FACTURA', 'VR_CTA','CANCELAR','DEVOLVER','FECHA',
                                                 'MOTIVO','CONTROL_','CONCEPTO_DE_POLIZA','OBSERVACIONES',
                                                 'MOT_CAN', 'ID_CUENTA', 'ID_TERCERO']]
        
        df_CAMBIO_EPSA = df_CANCELACION_EPSAo[['ENT','POL','CUENT','CUE_NUE','CEDULA','CERT','OBSERVACIONES',
                                               'CERT_SBS','NOMBRE','SOLICITA','No PQR',
                                               "CONTROL_", 'ret_estado1']]
        
        df_CAMBIO_EPSA = df_CAMBIO_EPSA[df_CAMBIO_EPSA['ret_estado1'] == 'TRASLADO DE CUENTA']

        df_CAMBIO_EPSA = df_CAMBIO_EPSA.rename(columns = {"CUENT": "CUENT_ANT",
                                                          "CEDULA": "CED",
                                                          "OBSERVACIONES": "OBS_",
                                                          "CERT_SBS": "CERT_AIG",
                                                          "NOMBRE": "ASEGURADO",
                                                          "No PQR": "PQR"}) 
        
        df_CAMBIO_EPSA = df_CAMBIO_EPSA [['ENT','POL','CUENT_ANT','CUE_NUE','CED','CERT',
                                          'OBS_','CERT_AIG','ASEGURADO','SOLICITA','PQR'
                                          ,"CONTROL_",'ret_estado1']]
        
        df_MOD_DATOS_EPSA = df_CANCELACION_EPSAo[['ENT','POL','CUE_NUE','CEDULA','CERT','DIR','TEL',
                                                  'CEL','NACE','CIUDAD','CED_NUE','NOMB','AP1',
                                                  'AP2','OBS','MEDIO','Proveedor','Barrio','Vivienda'
                                                  ,'Uso',"CONTROL_"]]
        
        df_MOD_DATOS_EPSA = df_MOD_DATOS_EPSA.rename(columns = {"CEDULA": "CED",
                                                                "MEDIO": "CANAL"})
        
        df_MOD_DATOS_EPSA = df_MOD_DATOS_EPSA[['ENT','POL','CUE_NUE','CED','CERT','DIR','TEL',
                                               'CEL','NACE','CIUDAD','CED_NUE','NOMB','AP1','AP2',
                                               'OBS','CANAL','Proveedor','Barrio','Vivienda','Uso'
                                               ,"CONTROL_"]]
        

        ruta_archivo_2 = "/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/data/epsa/retencion_epsa-" + currentdate + ".xlsx"
        file_path_2 = "/home/usr-dwh/Escritorio/Compartida_L/OPERACIONES/SBS\ INBOUND/ASESORES/Bases/BASES\ SPONSOR/BASES_VANTI/epsa/retencion_epsa-" + currentdate + ".xlsx"
        
        df_retencion_epsa.to_excel(ruta_archivo_2, index=False, sheet_name='CANCELACION_EPSA', header=True)      
        print(f"retencion_epsa {len(df_retencion_epsa)}")
        
        with sources.engine_6() as conn:
                df_retencion_epsa['FECHA'] = pd.to_datetime(df_retencion_epsa['FECHA'], format='%d/%m/%Y')
                df_retencion_epsa.to_sql('tb_reporte_retencion_cancelacion_epsa', con=conn,
                                                if_exists='append', index=False, method=to_sql_replace)

        with pd.ExcelWriter(ruta_archivo_2, mode='a', engine='openpyxl') as writer:
            df_CAMBIO_EPSA.to_excel(writer, sheet_name='CAMBIO_EPSA', index=False, header=True)
            print(f"CAMBIO_EPSA {len(df_CAMBIO_EPSA)}")

        with pd.ExcelWriter(ruta_archivo_2, mode='a', engine='openpyxl') as writer:
            df_MOD_DATOS_EPSA.to_excel(writer, sheet_name='MOD_DATOS_EPSA', index=False, header=True)
            print(f"MOD_DATOS_EPSA {len(df_MOD_DATOS_EPSA)}") 
            with sources.engine_6() as conn:
                df_MOD_DATOS_EPSA.to_sql('tb_reporte_retencion_mod_datos_epsa', con=conn,
                                                if_exists='append', index=False, method=to_sql_replace)

        """df_retencion_epsa.to_excel(file_path_2, index=False, sheet_name='CANCELACION_EPSA', header=True)      
        print(f"retencion_epsa {len(df_retencion_epsa)}")

        with pd.ExcelWriter(file_path_2, mode='a', engine='openpyxl') as writer:
            df_CAMBIO_EPSA.to_excel(writer, sheet_name='CAMBIO_EPSA', index=False, header=True)
            print(f"CAMBIO_EPSA {len(df_CAMBIO_EPSA)}")

        with pd.ExcelWriter(file_path_2, mode='a', engine='openpyxl') as writer:
            df_MOD_DATOS_EPSA.to_excel(writer, sheet_name='MOD_DATOS_EPSA', index=False, header=True)
            print(f"MOD_DATOS_EPSA {len(df_MOD_DATOS_EPSA)}") """   

        
log.logging.info('\n\n')
        
        