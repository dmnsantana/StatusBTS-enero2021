import warnings
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
import os

from utils import *


def CreateDataframe(aps, df_generator_script, df_bts, aps_revisar):
    aps = aps.reset_index()
    print(aps.columns)
    df_generator_script['ID_BENEFICIARIO'] = df_generator_script['ID_BENEFICIARIO'].astype(str)
    aps['ID_BENEFICIARIO'] = aps['ID_BENEFICIARIO'].astype(str)

    filtro = df_generator_script[df_generator_script.ID_BENEFICIARIO.isin(aps.ID_BENEFICIARIO)]
    filtro = filtro[["ID_MINTIC", "ID_BENEFICIARIO"]]
    # print(filtro)

    df_bts['ID_MINTIC'] = df_bts['ID_MINTIC'].astype(int)
    df_bts['ID_MINTIC'] = df_bts['ID_MINTIC'].astype(str)
    filtro['ID_MINTIC'] = filtro['ID_MINTIC'].astype(str)

    filtro2 = pd.merge(df_bts, filtro, on='ID_MINTIC', how="left")

    Pag = filtro2[['ID_MINTIC', "BTS", "FASE_INSTALACION", "REGIONAL_O&M"]]

    Pag = pd.merge(Pag, filtro, on='ID_MINTIC', how='left')

    Pag = pd.merge(Pag, aps_revisar, on='ID_BENEFICIARIO', how='left')

    Pag = pd.merge(Pag, aps, on='ID_BENEFICIARIO', how='left')

    Pag["DEVICE_NAME"] = Pag["DEVICE_NAME"].astype(str)

    return Pag


def CheckPost(Post, df_assign):
    f = datetime.today().date().__str__()
    f = f.replace(":", "-")
    f = f.replace(".", "-")
    path = os.getcwd()
    print(path + "\ExcelsGenerados\BTS_Status_PRE-" + f + ".xlsx")

    Post['ID_MINTIC'] = Post['ID_MINTIC'].astype(int)
    Post['offline'] = Post['offline'].astype(int)
    Post['online'] = Post['online'].astype(int)
    d_Post = Post[['ID_MINTIC', 'offline', 'online']]

    name = "BTS_Status_PRE-" + f + ".xlsx"
    result_path = return_path(name)
    print("ruta data archivo PRE", result_path)
    d_statusbts_pre = pd.read_excel(result_path)
    # d_statusbts_pre = d_statusbts_pre.reset_index()
    d_statusbts_pre = d_statusbts_pre.rename(columns={"Offline": "offline"})
    d_statusbts_pre = d_statusbts_pre.rename(columns={"Online": "online"})
    # d_statusbts_pre = d_statusbts_pre[['ID_MINTIC', 'ID_BENEFICIARIO', 'DEVICE_NAME', 'offline', 'online']]
    d_statusbts_pre['ID_MINTIC'] = d_statusbts_pre['ID_MINTIC'].astype(int)

    d_statusbts_pre = pd.merge(d_statusbts_pre, d_Post, on='ID_MINTIC', how='left')

    print("PRE")
    print(d_statusbts_pre)
    print(d_statusbts_pre.columns)
    print("POST")
    print(Post)
    print(Post.columns)

    # d_statusbts_pre = d_statusbts_pre.groupby(by=['REGIONAL_O&M', 'BTS', 'ID_MINTIC', 'ID_BENEFICIARIO',
    #                            'FASE_INSTALACION', 'SOPORTE', 'DEVICE_NAME']).mean()
    print("POST reporte")
    to_excel_sheet(d_statusbts_pre, df_assign)


def to_excel_sheet(df_pre, df_assign):
    print("Entro funcion excel")
    f = datetime.today().date().__str__()
    f = f.replace(":", "-")
    f = f.replace(".", "-")
    path = os.getcwd()
    print("ruta os", path)
    print(path + "\BTS_Status_PRE-" + f + ".xlsx")

    with pd.ExcelWriter(path + "\ExcelsGenerados\BTS_Status_PRE-" + f + ".xlsx") as writer:
        df_pre.to_excel(writer, sheet_name="StatusBTS_PRE")
        df_assign.to_excel(writer, sheet_name="Asignados hoy")

        print("Mensaje", "Excel creado con exito")


if __name__ == '__main__':
    # warnings.filterwarnings('ignore')

    # Lectura de datos
    df_bts = Bts()
    df_bts['BTS'] = df_bts['BTS'].astype(str)
    df_bts['BTS'] = df_bts['BTS'].str.upper()
    df_bts['ID_MINTIC'] = df_bts['ID_MINTIC'].astype(str)
    df_generator_script = GeneratorScript()
    # -------------------------------------------------------------------------------------------------------------------
    # Lectura archivo asignados diarios de implementacion-
    name_excel_assign = "asignados_diarios.xlsx"
    result_path = return_path(name_excel_assign)
    df_assign = pd.read_excel(result_path)
    df_assign = DataClean(df_assign)

    df_assign_salas = pd.DataFrame()
    df_assign_salas["ID"] = df_assign['ID_SALAS']
    df_assign_salas = df_assign_salas[df_assign_salas['ID'].notna()]
    df_assign_salas['ID'] = df_assign_salas['ID'].astype(int)
    df_assign_salas['ID'] = df_assign_salas['ID'].astype(str)

    df_assign_oym = pd.DataFrame()
    df_assign_oym["ID"] = df_assign['ID_OYM']
    df_assign_oym = df_assign_oym[df_assign_oym['ID'].notna()]
    df_assign_oym['ID'] = df_assign_oym['ID'].astype(int)
    df_assign_oym['ID'] = df_assign_oym['ID'].astype(str)

    df_bts_assign_salas = df_bts[df_bts.ID_MINTIC.isin(df_assign_salas.ID)]
    df_bts_assign_salas["SOPORTE"] = "Soporte en Sala"
    df_bts_assign_salas = df_bts_assign_salas[['BTS', 'SOPORTE']]

    df_bts_assign_oym = df_bts[df_bts.ID_MINTIC.isin(df_assign_oym.ID)]
    df_bts_assign_oym["SOPORTE"] = "O&M"
    df_bts_assign_oym = df_bts_assign_oym[['BTS', 'SOPORTE']]

    df_bts_assign = pd.concat([df_bts_assign_salas, df_bts_assign_oym], axis=0)
    print(df_bts_assign)
    values = ["FASE 1A", "FASE 1B"]
    df_bts = df_bts[df_bts.FASE_INSTALACION.isin(values)]
    # -------------------------------------------------------------------------------------------------------------------
    df_cnmaestro = cnMaestroAPs()
    df_cnmaestro = cnMaestro(df_cnmaestro)
    # ------------------------------------------------------------------------------------------------------------------
    # Agrupar datos de cn maestro y extraer nombre de aps caidos 1 o 2
    df_aps = df_cnmaestro.groupby(['ID_BENEFICIARIO', "DEVICE_NAME"])['STATUS'].value_counts().unstack().fillna(0)
    df_aps_1 = df_aps
    df_aps = df_cnmaestro.groupby(['ID_BENEFICIARIO'])['STATUS'].value_counts().unstack().fillna(0)

    aps_on_off = df_aps.loc[(df_aps['online'] > 0.0)]
    aps_on_off = aps_on_off.loc[(aps_on_off['online'] < 3.0)]
    aps_revisar = aps_on_off.reset_index()
    aps_revisar = df_cnmaestro[df_cnmaestro.ID_BENEFICIARIO.isin(aps_revisar.ID_BENEFICIARIO)]
    aps_revisar = aps_revisar.groupby(['ID_BENEFICIARIO', "DEVICE_NAME"])['STATUS'].value_counts().unstack().fillna(0)
    aps_revisar = aps_revisar.reset_index()
    aps_revisar = aps_revisar.loc[aps_revisar['offline'] > 0.0]
    aps_revisar = aps_revisar.groupby(['ID_BENEFICIARIO'])['DEVICE_NAME'].apply(list)

    # ------------------------------------------------------------------------------------------------------------------
    # Crear dataframe con todos los cruces
    df_bts_status = CreateDataframe(df_aps, df_generator_script, df_bts, aps_revisar)
    df_bts_status['ID_MINTIC'] = df_bts_status['ID_MINTIC'].astype(str)

    # filtrar por los ids asignados
    df_pre = df_bts_status[df_bts_status.BTS.isin(df_bts_assign.BTS)]
    df_pre = pd.merge(df_pre, df_bts_assign, on='BTS', how='left')
    df_pre = df_pre.drop_duplicates()

    ##==================================================================================================================
    # Valida si es post o pre report
    hora = datetime.today().hour.__str__()
    print("hora:", hora, type(hora))

    if int(hora) > 12:
        df_post = CheckPost(df_pre, df_assign)
        # agrupar para presentar

    else:
        df_pre = df_pre.groupby(by=['REGIONAL_O&M', 'BTS', 'ID_MINTIC', 'ID_BENEFICIARIO',
                                    'FASE_INSTALACION', 'SOPORTE', 'DEVICE_NAME']).mean()
        print("primer reporte")
        to_excel_sheet(df_pre, df_assign)
