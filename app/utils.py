import os
import json
import pandas as pd
import requests
import ssl
import sys

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

path = os.getcwd()


def return_path(name):
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)


# path = "C:\Users\ec4975k\Documents\ZTE\Programas\StatusBTS\data"

def DataClean(dataframe):
    dataframe.columns = map(str.upper, dataframe.columns)
    dataframe.columns = dataframe.columns.astype(str).str.replace("[()]", "")
    dataframe.columns = dataframe.columns.astype(str).str.replace(" ", "_")
    dataframe.columns = dataframe.columns.astype(str).str.replace("/", "_")
    try:
        dataframe['ID_MINTIC'] = dataframe['ID_MINTIC'].astype(str)
    except:
        pass

    return dataframe


def cnMaestro(dataframe):
    array_delete = dataframe.loc[dataframe['Site'] == '777-PILOTO'].index.to_numpy()
    dataframe = dataframe.drop(array_delete)
    dataframe["ID_BENEFICIARIO"] = dataframe["Site"].str.extract(r"(\d{5})")
    dataframe = DataClean(dataframe)
    return dataframe


def GeneratorScript():
    name = "Generador de Script Mintic.xlsm"
    result_path = return_path(name)
    dataframe = pd.read_excel(result_path)
    dataframe = dataframe.drop(dataframe.index[[0, 1, 2]])
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe.drop(dataframe.index[[0]])
    dataframe.columns = dataframe.columns.astype(str)
    dataframe = DataClean(dataframe)

    return dataframe


def Bts():
    name = "BTS.xlsx"
    result_path = return_path(name)
    dataframe = pd.read_excel(result_path)
    dataframe = DataClean(dataframe)
    return dataframe


def ServiceManager():
    name = "IM_SD_TAREAS.xlsx"
    result_path = return_path(name)
    d_SM = pd.read_excel(result_path)
    d_SM = DataClean(d_SM)
    d_SM = d_SM.rename(columns={"ID_DE_INCIDENTE": "IM"})
    d_SM = d_SM[d_SM.ID_BENEFICIARIO.notna()]
    d_SM['FECHA_HORA_DE_APERTURA'] = pd.to_datetime(d_SM['FECHA_HORA_DE_APERTURA'], infer_datetime_format=True)
    d_SM = d_SM.drop_duplicates()
    d_SM['IM'] = d_SM['IM'].astype(str)
    d_SM['ID_MINTIC'] = d_SM['ID_MINTIC'].astype(str)
    d_SM['ID_BENEFICIARIO'] = d_SM['ID_BENEFICIARIO'].astype(str)
    d_SM = d_SM[d_SM['ASIGNADO_A'] == 'Carlos Albeiro. Diaz Tangarife']
    d_SM['ID_MINTIC'] = d_SM['ID_MINTIC'].str.replace("\t", "")
    d_SM = d_SM[d_SM['ID_MINTIC'].str.len() <= 8]
    d_SM = d_SM[d_SM['FECHA_HORA_DE_APERTURA'] >= '2021-09-25']

    return d_SM


def FaseDDA():
    name = "Fase1A_1B_conDDA.xlsx"
    result_path = return_path(name)
    d_FaseDDA = pd.read_excel(result_path)
    d_FaseDDA = DataClean(d_FaseDDA
                          )
    return d_FaseDDA


def cnMaestroAPs():
    def GetDataAPI(api_url, api_call_headers):
        temp_url = api_url
        offset = 0

        read = True
        df_total = pd.DataFrame()

        while read:

            api_url = api_url.format(str(offset))

            api_call_response = requests.get(api_url, headers=api_call_headers, verify=False)
            # print(api_url)

            a = json.loads(api_call_response.text)

            df = pd.json_normalize(a, record_path=['data'])

            offset = offset + 100

            # print(df.shape[0])

            df_total = pd.concat([df, df_total], axis=0)

            api_url = temp_url

            if df.shape[0] < 100:
                offset = offset + int(df.shape[0])
                read = False
            # print(offset)

        df_total = df_total.reset_index(drop=True)
        # print(df_total.columns)
        df_total = df_total[["mac", "network", "site", "name", 'ip', 'status', 'location.coordinates']]
        df_total = df_total.rename(columns={"mac": "Mac"})
        df_total = df_total.rename(columns={"network": "Network"})
        df_total = df_total.rename(columns={"site": "Site"})
        df_total = df_total.rename(columns={"name": "Device Name"})
        df_total = df_total.rename(columns={"ip": "IP Address"})
        df_total = df_total.rename(columns={"status": "Status"})

        return df_total

    def API_CnMaestro(id, secret, serverAP, access_token_url):
        access_token_url = access_token_url.format(str(serverAP))

        client = BackendApplicationClient(client_id=id)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(token_url=access_token_url, client_id=id, client_secret=secret, verify=ssl.CERT_NONE)
        access_token = token['access_token']
        # print(access_token)

        api_call_headers = {'Authorization': 'Bearer ' + access_token}

        api_url_devices_offline = "https://prycnmap{}.claro.net.co/api/v2/devices?offset={}&status=offline&type=wifi-enterprise".format(
            str(serverAP), "{}")
        api_url_devices_online = "https://prycnmap{}.claro.net.co/api/v2/devices?offset={}&status=online&type=wifi-enterprise".format(
            str(serverAP), "{}")

        df_offline = GetDataAPI(api_url_devices_offline, api_call_headers)
        df_online = GetDataAPI(api_url_devices_online, api_call_headers)

        df_cnmaestro = pd.concat([df_offline, df_online], axis=0)

        return df_cnmaestro

    def API_Aps():
        if not sys.warnoptions:
            import warnings

            warnings.simplefilter("ignore")

        id_ap1 = 'oTizXBG8MTa6gSgz'
        secret_ap1 = 'ISgtGUtabnKBMRA6RRLGpAMrNAFDXa'

        id_ap2 = "amx79B3FtBa70D4L"
        secret_ap2 = "b0UTB0KzT3YRn0xID8cCkdoreBD6H9"

        access_token_url = "https://prycnmap{}.claro.net.co/api/v2/access/token"

        df_server1 = API_CnMaestro(id_ap1, secret_ap1, 1, access_token_url)
        print("Termino aps 1")
        df_server2 = API_CnMaestro(id_ap2, secret_ap2, 2, access_token_url)
        print("Termino aps 2")

        df_cnmaestro = pd.concat([df_server1, df_server2], axis=0)

        # path = path.replace(chr(92), '/')

        # to_excel_sheet(path, df_cnmaestro)

        return df_cnmaestro

    df_cnmaestro = API_Aps()
    return df_cnmaestro
