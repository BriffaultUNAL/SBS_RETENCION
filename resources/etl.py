#!/usr/bin/python

import sys
import numpy as np
from pandas.io.sql import SQLTable
from sqlalchemy import text, Engine, Connection, Table, DATE
import re

sys.path.append('/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/resources')

import pandas as pd
import logging_config as log
import sources
from datetime import datetime

interval=1

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

def asignar_gestion(valor_ent):
        if valor_ent == 'GAS NATURAL - C1':
                return 'C1'
        elif valor_ent == 'GAS NACER - N1':
                return 'N1'
        elif valor_ent == 'GAS ORIENTE - O1':
                return 'O1'
        elif valor_ent == 'GAS CUNDIBOYACENSE - Y1':
                return 'Y1'
        else:
                return 'Not report'


def etl():

    with sources.engine_1() as con:
        #read inbound_pqrs_retenciones
        df_retencion=pd.read_sql("(select* from inbound_pqrs_retenciones)",con
                                 ).rename(columns={"id_pqr":"id_pqr_ret",
                                                  "created_at": "created_at_retencion",
                                                  "cert_willis": "CER WILLIS",
                                                  "cert_sbs": "CER_AIG",
                                                  "newcuentanic": "CUE_NUE"})
            
        df_usuarios=pd.read_sql("(SELECT * FROM crm_masterclaro.usuarios)",con
                                    ).rename(columns={"id_usuario":"id_vanti",
                                                      "nombre_usuario":"nombre_usuario_cerro",
                                                      "apellido_usuario":"apellido_usuario_cerro"})    
        
        #read inbound_pqrs
        df_pqrs=pd.read_sql('(select* from appscosbs.inbound_pqrs)',con)
        #extaer fecha y hora del calldate
        df_pqrs["fecha_reporte"]=df_pqrs['created_at'
                                         ].astype(str).apply(lambda x:x.split(' ')[0] if x!= None else None)

        df_pqrs["fecha_reporte"]=pd.to_datetime(df_pqrs['created_at']).dt.strftime('%d/%m/%Y')

        df_pqrs=df_pqrs.rename(columns={"id_tipollamada": "id_tipollamada_pqr",
                                                "created_at": "fecha1"})
        
        #read estados
        df_estados=pd.read_sql("select* from appscosbs.estados",con
                               ).rename(columns={"id":"id_estado1",
                                                    "descripcion":"descripcion_estado"})
        
        #read tipificaciones
        df_inbound_pqrs_otros=pd.read_sql('(select* from appscosbs.inbound_pqrs_otros)',con
                                          ).rename(columns={"nic": "CUENT"})
        
        #read tipificaciones
        df_asegurado=pd.read_sql('select* from appscosbs.inbound_pqrs_asegurados',con
                                 ).rename(columns={"id_pqr":"id_pqr1",
                                                   "identificacion":"CEDULA"})
        
        df_asegurado['NOMBRE DEL ASEGURADO'] = df_asegurado['nombres']+ ' ' +df_asegurado['apellido1'
                                                                                    ]+df_asegurado['apellido2']
        
        #read tipificaciones
        df_departamentos=pd.read_sql('(select* from appscosbs.departamentos)',con
                                     ).rename(columns={"descripcion":"CIUDAD",
                                                       "id_departamento":"id_dpto"})
        
        #read tipificaciones
        df_solicitante=pd.read_sql('(select* from appscosbs.inbound_pqrs_solicitantes)',con
                                   ).rename(columns={"id":"id_solicit",
                                                     "nombres":"nombres_solic",
                                                     "id_pqr":"id_pqr_solic"})

        #read tipificaciones
        df_tipi=pd.read_sql('(select* from appscosbs.tipificaciones)',con
                            ).rename(columns={"id":"id_tipi",
                                              "estado":"estado_tipi",
                                              "descripcion":"descripcion_tipi",
                                              "id_principal":"id_principal_tipi",
                                              "id_secundario":"id_secundario_tipi",
                                              "opcion1":"opcion1_tipi",
                                              "opcion2":"opcion2_tipi"})
                
        #read tipificaciones
        df_tipi_sponsor=pd.read_sql('(select* from appscosbs.tipificaciones)',con
                            ).rename(columns={"descripcion":"ENT",
                                                      "id":"id_sponsor",
                                                      "estado":"estado_sponsor",
                                                      "id_principal":"id_principal_sponsor",
                                                      "id_secundario":"id_secundario_sponsor",
                                                      "opcion1":"opcion1_sponsor",
                                                      "opcion2":"opcion2_sponsor"})
        
        #read tipificaciones
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
        
        df_join['FACTURA'] = '0'
        df_join['VR_CTA'] = '0'
        df_join['CANCELAR'] = 'S'
        df_join['DEVOLVER'] = 'N/A'

        print(f"pereira {len(df_join[df_join['ENT']=='ENERGÍA DE PEREIRA'])}")

        df_join['ENT_1'] = df_join['ENT']
        df_join['ENT'] = df_join['ENT'].apply(asignar_gestion)

        df_join['descripcion'] = df_join['descripcion'].str.strip()

        df_join['descripcion'] = df_join['MEDIO'] + ' - ' +df_join['descripcion']
          
        df_join = df_join.rename(columns={"id_pqr_ret": "No PQR",
                                          "opcion1_producto": "PRO",
                                          "fecha_reporte": "FECHA",
                                          "PRO1":"CONCEPTO DE POLIZA",
                                          "descripcion":"OBSERVACIONES"})
        
        df_join['OBSERVACIONES'] = df_join['OBSERVACIONES'].apply(
            eliminar_caracteres_especiales)
        
        df_join['fecha1'] = pd.to_datetime(df_join['fecha1'])

        df_join = df_join[df_join['fecha1'] >= '2021-04-01']
            
        df_join = df_join[df_join['descripcion_estado'] != 'ANULADO']

        df_join['asignacion_area'] = df_join['id_usuario_cerro'].apply(asign)

        df_join['CONTROL'] = df_join['asignacion_area']+ ' - '  + df_join[
             'nombre_usuario_cerro'] + ' ' + df_join['apellido_usuario_cerro']
        
        df_join['MOTIVO'] = df_join['CAUSA'] + ' - PQR'+' '+  df_join['No PQR'].astype(str)

        df_pre_cierre_vanti = df_join[df_join["ENT"] != 'Not report']
        
        df_pre_cierre_vanti = df_pre_cierre_vanti[["ENT","CER WILLIS","CUENT","PRO","CEDULA","FECHA","No PQR","CONTROL","CER_AIG","MOT_CAN","CAUSA","MEDIO"]]
             
        df_pre_cierre_vanti = df_pre_cierre_vanti[df_pre_cierre_vanti["MOT_CAN"] == 'CANCELACIÓN DE LA PÓLIZA']

        df_pre_cierre_vanti['CAUSA'] = df_pre_cierre_vanti['CAUSA'].fillna('NO DESEA EL PRODUCTO') \
                                       .replace({'NO ESPECIFICA MOTIVO DE CANCELACION - NO CONTACTO OUTBOUND': 'NO DESEA EL PRODUCTO'})

        currentdate = datetime.now().strftime("%Y-%m-%d")    

        path1 = "/home/usr-dwh/Escritorio/Compartida_L/OPERACIONES/SBS\ INBOUND/ASESORES/Bases/BASES\ SPONSOR/BASES_VANTI/vanti/"
        ruta1 = "/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/data/vanti/"
        archivo1 = f"retencion_pre-{currentdate}.xlsx"
        ruta_archivo1 = ruta1+archivo1
        file_path1= path1+archivo1
        df_pre_cierre_vanti.to_excel(ruta_archivo1, index=False, header=True)
        
        with sources.engine_6() as conn:
                df_pre_cierre_vanti['FECHA'] = pd.to_datetime(df_pre_cierre_vanti['FECHA'], format='%d/%m/%Y')

                df_pre_cierre_vanti.to_sql('tb_reporte_retencion_pre_vanti', con=conn,
                                                if_exists='append', index=False, method=to_sql_replace)
                
        #df_pre_cierre_vanti.to_excel(file_path1, index=False, header=True)
        print(f"retencion_pre {len(df_pre_cierre_vanti)}")

        def asignar_gestion_2(valor_ent):
                if valor_ent == 'EDEQ':
                        return 'C1'
                elif valor_ent == 'EMCALI':
                        return 'C1'
                elif valor_ent == 'ENERGÍA DE PEREIRA':
                        return 'C1'
                else:
                        return 'Not report'

        df_join['ENT_']= df_join['ENT_1'].apply(asignar_gestion_2)                
                
        df_pre_cierre_vanti_edeq = df_join[df_join["ENT_1"] == 'EDEQ']  

        df_pre_cierre_vanti_edeq = df_pre_cierre_vanti_edeq[df_pre_cierre_vanti_edeq['MOT_CAN'] == 'CANCELACIÓN DE LA PÓLIZA']  

        df_pre_cierre_vanti_edeq = df_pre_cierre_vanti_edeq[["ENT_", "CER WILLIS", "CER_AIG",  "CUENT", "CEDULA", "NOMBRE DEL ASEGURADO",
                                                              "PRO", "FECHA", 'FACTURA', 'VR_CTA','CANCELAR','DEVOLVER', "MOTIVO",
                                                               "MOT_CAN", "CONTROL", "CONCEPTO DE POLIZA", 'OBSERVACIONES']]   
        
        df_pre_cierre_vanti_edeq ['ENT_'] = 'EDEQ'
        df_pre_cierre_vanti_edeq = df_pre_cierre_vanti_edeq.rename(columns={"MOTIVO":"CAUSA"})
          
        path2 = "/home/usr-dwh/Escritorio/Compartida_L/OPERACIONES/SBS\ INBOUND/ASESORES/Bases/BASES\ SPONSOR/BASES_VANTI/edeq/"        
        ruta2 = "/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/data/edeq/"
        archivo2 = f"retencion_pre_edeq-{currentdate}.xlsx"
        ruta_archivo2 = ruta2+archivo2
        file_path2 = path2+archivo2
        df_pre_cierre_vanti_edeq.to_excel(ruta_archivo2, index=False, header=True)
        with sources.engine_6() as conn:
                df_pre_cierre_vanti_edeq['FECHA'] = pd.to_datetime(df_pre_cierre_vanti_edeq['FECHA'], format='%d/%m/%Y')
                df_pre_cierre_vanti_edeq.to_sql('tb_reporte_retencion_pre_edeq', con=conn,
                                                if_exists='append', index=False, method=to_sql_replace)
        #df_pre_cierre_vanti_edeq.to_excel(file_path2, index=False, header=True)
        print(f"retencion_pre_edeq {len(df_pre_cierre_vanti_edeq)}")    

        df_pre_cierre_vanti_emcali = df_join[df_join["ENT_1"] == 'EMCALI']

        df_pre_cierre_vanti_emcali = df_pre_cierre_vanti_emcali[df_pre_cierre_vanti_emcali['MOT_CAN'] == 'CANCELACIÓN DE LA PÓLIZA'] 

        df_pre_cierre_vanti_emcali = df_pre_cierre_vanti_emcali[["ENT_", "CER WILLIS", "CUENT", "PRO", "CEDULA", "FECHA",
                                        "No PQR", "CONTROL", "CER_AIG", "MOT_CAN", "CAUSA", "MEDIO"]]
        
        path3 = '/home/usr-dwh/Escritorio/Compartida_L/OPERACIONES/SBS\ INBOUND/ASESORES/Bases/BASES\ SPONSOR/BASES_VANTI/emcali/'
        ruta3 = "/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/data/emcali/"
        archivo_3 = f"retencion_pre_emcali-{currentdate}.xlsx"
        ruta_archivo3 = ruta3+archivo_3
        file_path3 = path3+archivo_3
        df_pre_cierre_vanti_emcali.to_excel(ruta_archivo3, index=False, header=True)
        
        with sources.engine_6() as conn:
                df_pre_cierre_vanti_emcali['FECHA'] = pd.to_datetime(df_pre_cierre_vanti_emcali['FECHA'], format='%d/%m/%Y')

                df_pre_cierre_vanti_emcali.to_sql('tb_reporte_retencion_pre_emcali', con=conn,
                                                if_exists='append', index=False, method=to_sql_replace)
                
        #df_pre_cierre_vanti_emcali.to_excel(file_path3, index=False, header=True)
        print(f"retencion_pre_emcali {len(df_pre_cierre_vanti_emcali)}")

        df_pre_cierre_vanti_pereira = df_join[df_join["ENT_1"] == 'ENERGÍA DE PEREIRA']

        df_pre_cierre_vanti_pereira = df_pre_cierre_vanti_pereira[df_pre_cierre_vanti_pereira['MOT_CAN'] == 'CANCELACIÓN DE LA PÓLIZA']
        
        df_pre_cierre_vanti_pereira = df_pre_cierre_vanti_pereira[["ENT_", "CER WILLIS", "CER_AIG", "CUENT", "CEDULA", "NOMBRE DEL ASEGURADO",
                                                                   "PRO", 'FACTURA', 'VR_CTA','CANCELAR','DEVOLVER', "FECHA", "MOTIVO",
                                                                   "CONTROL", "CONCEPTO DE POLIZA", "MOT_CAN", "CAUSA", "MEDIO", "OBSERVACIONES"]]
        
        path4 = '/home/usr-dwh/Escritorio/Compartida_L/OPERACIONES/SBS\ INBOUND/ASESORES/Bases/BASES\ SPONSOR/BASES_VANTI/pereira/'
        ruta4 = "/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/data/pereira/"
        archivo_4 = f"retencion_pre_pereira-{currentdate}.xlsx"
        ruta_archivo4 = ruta4+archivo_4
        file_path4 = path4+archivo_4
        df_pre_cierre_vanti_pereira.to_excel(ruta_archivo4, index=False, header=True)
        
        with sources.engine_6() as conn:
                df_pre_cierre_vanti_pereira['FECHA'] = pd.to_datetime(df_pre_cierre_vanti_pereira['FECHA'], format='%d/%m/%Y')
                df_pre_cierre_vanti_pereira.to_sql('tb_reporte_retencion_pre_pereira', con=conn,
                                                if_exists='append', index=False, method=to_sql_replace)
        #df_pre_cierre_vanti_pereira.to_excel(file_path4, index=False, header=True)
        print(f"retencion_pre_pereira {len(df_pre_cierre_vanti_pereira)}")

        df_join = df_join.rename(columns = {"PRO": "POL",
                                            "CUENT": "CUENT_ANT",
                                            "CEDULA": "CED"})     

        df_modifica_cuenta_vanti = df_join[["ENT", "POL", "CUENT_ANT", "CUE_NUE", "CED", "CER WILLIS",
                                    "No PQR", "CONTROL", "CER_AIG", "FECHA", "ret_estado1"]]
        
        df_modifica_cuenta_vanti = df_modifica_cuenta_vanti[df_modifica_cuenta_vanti["ENT"] != 'Not report']

        df_modifica_cuenta_vanti = df_modifica_cuenta_vanti[df_modifica_cuenta_vanti['ret_estado1'] == 'TRASLADO DE CUENTA']

        df_modifica_cuenta_vanti = df_modifica_cuenta_vanti[["ENT", "POL", "CUENT_ANT", "CUE_NUE", "CED", "CER WILLIS", "No PQR",
                                                              "CONTROL", "CER_AIG", "FECHA"]]
        
        archivo_5 = f"retencion_mod-{currentdate}.xlsx"
        ruta_archivo5 = ruta1+archivo_5
        file_path5 = path1+archivo_5
        df_modifica_cuenta_vanti.to_excel(ruta_archivo5, index=False, header=True)
        
        with sources.engine_6() as conn:
                df_modifica_cuenta_vanti['FECHA'] = pd.to_datetime(df_modifica_cuenta_vanti['FECHA'], format='%d/%m/%Y')

                df_modifica_cuenta_vanti.to_sql('tb_reporte_retencion_mod_vanti', con=conn,
                                                if_exists='append', index=False, method=to_sql_replace)
                
        #df_modifica_cuenta_vanti.to_excel(file_path5, index=False, header=True)
        print(f"retencion_mod {len(df_modifica_cuenta_vanti)}")

        df_modifica_cuenta_vanti_edeq = df_join[df_join["ENT_1"] == 'EDEQ']

        df_modifica_cuenta_vanti_edeq = df_modifica_cuenta_vanti_edeq[df_modifica_cuenta_vanti_edeq['ret_estado1'] == 'TRASLADO DE CUENTA']

        df_modifica_cuenta_vanti_edeq = df_modifica_cuenta_vanti_edeq[["ENT_", "POL", "CUENT_ANT", "CUE_NUE", "CED", 
                                                                       "CER WILLIS", "No PQR", "CER_AIG", "FECHA"]]
        
        archivo_6 = f"retencion_mod_edeq-{currentdate}.xlsx"
        ruta_archivo6 = ruta2+archivo_6
        file_path6= path2+archivo_6
        df_modifica_cuenta_vanti_edeq.to_excel(ruta_archivo6, index=False, header=True)
        #df_modifica_cuenta_vanti_edeq.to_excel(file_path6, index=False, header=True)
        print(f"retencion_mod_edeq {len(df_modifica_cuenta_vanti_edeq)}")

        df_modifica_cuenta_vanti_emcali = df_join[df_join["ENT_1"] == 'EMCALI']   

        df_modifica_cuenta_vanti_emcali = df_modifica_cuenta_vanti_emcali[df_modifica_cuenta_vanti_emcali['ret_estado1'] == 'TRASLADO DE CUENTA']

        df_modifica_cuenta_vanti_emcali = df_modifica_cuenta_vanti_emcali [["ENT_", "POL", "CUENT_ANT", "CUE_NUE", "CED",
                                             "CER WILLIS", "No PQR", "CONTROL", "CER_AIG", "FECHA"]]

        archivo_7 = f"retencion_mod_emcali-{currentdate}.xlsx"
        ruta_archivo7 = ruta3+archivo_7
        file_path7 = path3+archivo_7
        df_modifica_cuenta_vanti_emcali.to_excel(ruta_archivo7, index=False, header=True)
        #df_modifica_cuenta_vanti_emcali.to_excel(file_path7, index=False, header=True)
        print(f"retencion_mod_emcali {len(df_modifica_cuenta_vanti_emcali)}")

        df_modifica_cuenta_vanti_pereira = df_join[df_join["ENT_1"] == 'ENERGÍA DE PEREIRA'] 

        df_modifica_cuenta_vanti_pereira = df_modifica_cuenta_vanti_pereira[df_modifica_cuenta_vanti_pereira['ret_estado1'] == 'TRASLADO DE CUENTA']

        df_modifica_cuenta_vanti_pereira = df_modifica_cuenta_vanti_pereira[["ENT_", "POL", "CUENT_ANT", "CUE_NUE", "CED",
                                              "CER WILLIS", "No PQR", "CONTROL", "CER_AIG", "FECHA"]]
        
        archivo_8 = f"retencion_mod_pereira-{currentdate}.xlsx"
        ruta_archivo8 = ruta4+archivo_8
        file_path8 = path4+archivo_8
        df_modifica_cuenta_vanti_pereira.to_excel(ruta_archivo8, index=False, header=True)
        #df_modifica_cuenta_vanti_pereira.to_excel(file_path8, index=False, header=True)
        print(f"retencion_mod_pereira {len(df_modifica_cuenta_vanti_pereira)}")

        df_join = df_join.rename(columns = {"PRO": "POL",
                                            "CUENT_ANT": "CUENT",
                                            "newdireccion": "DIR",
                                            "celular2": "TEL",
                                            "celular1": "CEL"})  
        
        df_join["SEX"] = ''
        df_join["NACE"] = ''
        df_join["CED_NUE"] = ''
        df_join["NOMB"] = ''
        df_join["AP1"] = ''
        df_join["AP2"] = ''

        df_actual_datos_vanti = df_join[df_join['ret_estado1'] == 'TRASLADO DE CUENTA'] 

        df_actual_datos_vanti = df_actual_datos_vanti[["ENT", "POL", "CUENT", "CUE_NUE", "CED", "CER WILLIS", "DIR", "TEL", "CEL", "SEX",
                                               "NACE", "CIUDAD", "CED_NUE", "NOMB", "AP1", "AP2", "No PQR", "CONTROL", "MEDIO", "FECHA"]]

        archivo_9 = f"retencion_actual-{currentdate}.xlsx"
        ruta_archivo9 = ruta1+archivo_9
        file_path9 = path1 + archivo_9
        df_actual_datos_vanti.to_excel(ruta_archivo9, index=False, header=True)
        
        with sources.engine_6() as conn:
                df_actual_datos_vanti['FECHA'] = pd.to_datetime(df_actual_datos_vanti['FECHA'], format='%d/%m/%Y')
                df_actual_datos_vanti.to_sql('tb_reporte_retencion_actual_vanti', con=conn,
                                                if_exists='append', index=False, method=to_sql_replace)
                
        #df_actual_datos_vanti.to_excel(file_path9, index=False, header=True)
        print(f"retencion_actual {len(df_actual_datos_vanti)}")

        df_actual_datos_vanti_edeq = df_join[df_join['ENT_1'] == 'EDEQ'] 

        df_actual_datos_vanti_edeq = df_actual_datos_vanti_edeq[df_actual_datos_vanti_edeq['ret_estado1'] == 'TRASLADO DE CUENTA']

        df_actual_datos_vanti_edeq = df_actual_datos_vanti_edeq[["ENT_", "POL", "CUENT", "CUE_NUE", "CED", "CER WILLIS", "DIR", "TEL", "CEL", "SEX", "NACE",
                                        "CIUDAD", "CED_NUE", "NOMB", "AP1", "AP2", "No PQR", "CONTROL", "MEDIO", "FECHA"]]
        
        archivo_10 = f"retencion_actual_edeq-{currentdate}.xlsx"
        ruta_archivo10 = ruta2+archivo_10
        file_path10 = path2 + archivo_10
        df_actual_datos_vanti_edeq.to_excel(ruta_archivo10, index=False, header=True)
        #df_actual_datos_vanti_edeq.to_excel(file_path10, index=False, header=True)
        print(f"retencion_actual_edeq {len(df_actual_datos_vanti_edeq)}")

        df_actual_datos_vanti_emcali = df_join[df_join['ENT_1'] == 'EMCALI'] 

        df_actual_datos_vanti_emcali = df_actual_datos_vanti_emcali[df_actual_datos_vanti_emcali['ret_estado1'] == 'TRASLADO DE CUENTA']

        df_actual_datos_vanti_emcali = df_actual_datos_vanti_emcali[["ENT_", "POL", "CUENT", "CUE_NUE", "CED", "CER WILLIS", "DIR", "TEL", "CEL", "SEX",
                                          "NACE", "CIUDAD", "CED_NUE", "NOMB", "AP1", "AP2", "No PQR", "CONTROL", "MEDIO", "FECHA"]]
        
        archivo_11 = f"retencion_actual_emcali-{currentdate}.xlsx"
        ruta_archivo11 = ruta3+archivo_11
        file_path11 = path3+archivo_11
        df_actual_datos_vanti_emcali.to_excel(ruta_archivo11, index=False, header=True)
        #df_actual_datos_vanti_emcali.to_excel(file_path11, index=False, header=True)
        print(f"retencion_actual_emcali {len(df_actual_datos_vanti_emcali)}")

        df_actual_datos_vanti_pereira = df_join[df_join['ENT_1'] == 'ENERGÍA DE PEREIRA'] 

        df_actual_datos_vanti_pereira = df_actual_datos_vanti_pereira[df_actual_datos_vanti_pereira['ret_estado1'] == 'TRASLADO DE CUENTA']

        df_actual_datos_vanti_pereira = df_actual_datos_vanti_pereira[["ENT_", "POL", "CUENT", "CUE_NUE", "CED", "CER WILLIS", "DIR", "TEL", "CEL", "SEX",
                                           "NACE", "CIUDAD", "CED_NUE", "NOMB", "AP1", "AP2", "No PQR", "CONTROL", "MEDIO", "FECHA"]]
        
        archivo_12 = f"retencion_actual_pereira-{currentdate}.xlsx"
        ruta_archivo12 = ruta4+archivo_12
        file_path12 = path4+archivo_12
        df_actual_datos_vanti_pereira.to_excel(ruta_archivo12, index=False, header=True)
        #df_actual_datos_vanti_pereira.to_excel(file_path12, index=False, header=True)
        print(f"retencion_actual_pereira {len(df_actual_datos_vanti_pereira)}")
log.logging.info('\n\n')
    