import numpy as np
import pandas as pd

date_format = "%b %d, %Y @ %H:%M:%S.%f"

def build_devicce_conn_df(datafile: str):
    def map_conn_mode(value):
        if "app_conn" in value or "station_conn" in value:
           return "conn"
        elif "app_disconn" or "station_disconn" in value:
            return "disconn"
        return np.nan

    def map_client_type(value):
        if "device-ap" in value:
            return "ap"
        elif "device-icx" in value:
            return "icx"
        elif "se_" in value:
            return "edge"
        else:
            return "app"

    def map_disconn_reason(value):
        if "app_disconn" in value and "Client Closed" in value:
            return "Client Closed"
        if "app_disconn" in value and "Stale Connection" in value:
            return "Stale Connection"
        if "app_disconn" in value and "Write Deadline" in value:
            return "Slow Consumer"
        if "app_disconn" in value and "Read Error" in value:
            return "Read Error"
        if "station_disconn" in value and "Write Deadline" in value:
            return "Slow Consumer"
        return np.nan

    def map_conn_mode_client_type(value):
        if "app_conn" in value and "device-ap" in value:
            return "ap-conn"
        if "app_conn" in value and "device-icx" in value:
            return "icx-conn"
        if "app_disconn" in value and "device-ap" in value:
            return "ap-disconn"
        if "app_disconn" in value and "device-icx" in value:
            return "icx-disconn"
        if "station_conn" in value:
            return "station-conn"
        if "station_disconn" in value:
            return "station-disconn"
        return "other"

    def map_client_name(value):
        str_split = value.split(")(")
        if len(str_split) > 1:
            str_split = str_split[0].split("(")
            return str_split[1]
        return np.nan

    def map_nats_server(value):
        str_split = value.split("thirdparty-nats-")
        if len(str_split) > 1:
            str_split = str_split[1].split(")")
            if len(str_split[0]) > 1:
                str_split = str_split[0].split(".")
            return "nats-" + str_split[0][:1]
        return "N/A"

    df = pd.read_csv(datafile)
    df["timestamp"] = pd.to_datetime(df["@timestamp"], format=date_format) #.dt.floor('s')
    df = df.rename(columns={'kubernetes.pod_name': 'server'})
    df['conn_mode'] = df['message'].map(map_conn_mode)
    df['client_type'] = df['message'].map(map_client_type)
    df['disconn_reason'] = df['message'].map(map_disconn_reason)
    df['client_name'] = df['message'].map(map_client_name)
    df['nats'] = df['message'].map(map_nats_server)
    replacements = {'nats-0': 'n0', 'nats-1': 'n1', 'nats-2': 'n2', 'nats-3': 'n3', 'nats-4': 'n4'}
    df['nats'] = df['nats'].map(replacements).fillna(df['nats'])
    df = df[['timestamp', 'message', 'conn_mode', 'client_type', 'disconn_reason', 'client_name', 'nats', 'server']]

    return df
