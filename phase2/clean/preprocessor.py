import socket
import numpy as np
import pandas as pd
import time
import os
import xarray as xr
import pygrib as pg
import datetime as dt
from datetime import datetime, timedelta

forecast_horizon = 180
history = 240
regions = 36

forecast_horizon = forecast_horizon // 15
history = history // 15

if not os.path.exists("./preprocessor/"):
    os.mkdir("./preprocessor/")

aws_array_save_path = "./preprocessor/aws_array.npy"
aws_array1_save_path = "./preprocessor/aws_array1.npy"
aws_regions_save_path = "./preprocessor/aws_regions.npy"
gfs_array_save_path = "./preprocessor/gfs_array.npy"
encoding_features_save_path = "./preprocessor/encoding_features.npy"

aws_array = np.zeros((history, regions))
np.save(aws_array_save_path, aws_array)

PREPROCESSOR_IP_ADDR = "127.0.0.1"
PREPROCESSOR_PORT = 6005

collector_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
collector_sock.close()
collector_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
collector_sock.bind((PREPROCESSOR_IP_ADDR, PREPROCESSOR_PORT))
collector_sock.listen()

MODEL_IP_ADDR = "127.0.0.1"
MODEL_PORT = 6004

model_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# model_sock.close()
# exit(0)
# model_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
model_sock.connect((MODEL_IP_ADDR, MODEL_PORT))

print(f"Preprocessor node listening on {PREPROCESSOR_IP_ADDR}:{PREPROCESSOR_PORT}")

while True:
    try:
        collector_conn, collector_addr = collector_sock.accept()
        print(f"Connected to collector node: {collector_addr}")

        while True:
            collector_data = collector_conn.recv(1024).decode().splitlines()
            aws_filename = collector_data[0]
            filesize = int(collector_data[1])

            aws_filepath = "./preprocessor/" + aws_filename
            with open(aws_filepath, "wb") as f:
                bytes_received = 0
                while bytes_received < filesize:
                    bytes_read = collector_conn.recv(1024)
                    if not bytes_read:
                        break
                    f.write(bytes_read)
                    bytes_received += len(bytes_read)
            print("Received from collector node:", collector_data)

            time.sleep(1)

            collector_data = collector_conn.recv(1024).decode().splitlines()
            gfs_filename = collector_data[0]
            filesize = int(collector_data[1])

            gfs_filepath = "./preprocessor/" + gfs_filename
            with open(gfs_filepath, "wb") as f:
                bytes_received = 0
                while bytes_received < filesize:
                    bytes_read = collector_conn.recv(1024)
                    if not bytes_read:
                        break
                    f.write(bytes_read)
                    bytes_received += len(bytes_read)
            print("Received from collector node:", collector_data)

            if not collector_data:
                break
            
            aws_df = pd.read_excel(aws_filepath)
            aws_regions = aws_df["Code"].values
            aws_temp = aws_df["Rain"].values
            aws_temp = np.reshape(aws_temp, (1, aws_temp.shape[0]))
            aws_array = np.load(aws_array_save_path)
            aws_array = np.concatenate((aws_array, aws_temp), axis=0)
            aws_array = aws_array[1:]
            np.save(aws_array_save_path, aws_array)
            aws_array = np.expand_dims(aws_array, axis=0)
            np.save(aws_array1_save_path, aws_array)
            np.save(aws_regions_save_path, aws_regions)
            
            if os.path.exists("./preprocessor/GFS/"):
                os.system("rm -rf ./preprocessor/GFS/")
            os.system(f"unzip -q {gfs_filepath} -d ./preprocessor/")
            base_filename = os.listdir("./preprocessor/GFS/PR/")[0][:-4]
            gfs_pr_forecast_pathnames = ["./preprocessor/GFS/PR/" + base_filename + i for i in ["f003", "f006", "f009", "f012"]]
            gfs_pw_forecast_pathnames = ["./preprocessor/GFS/PW/" + base_filename + i for i in ["f003", "f006", "f009", "f012"]]
            gfs_rh_forecast_pathnames = ["./preprocessor/GFS/RH/" + base_filename + i for i in ["f003", "f006", "f009", "f012"]]
            gfs_tcc_forecast_pathnames = ["./preprocessor/GFS/TCC/" + base_filename + i for i in ["f003", "f006", "f009", "f012"]]

            gfs_features = np.zeros((1, 9, 9, 4, 4))

            gfs_features_temp_pr = np.zeros((9, 9, 1, 1))
            gfs_features_temp_pw = np.zeros((9, 9, 1, 1))
            gfs_features_temp_rh = np.zeros((9, 9, 1, 1))
            gfs_features_temp_tcc = np.zeros((9, 9, 1, 1))
    
            for path in gfs_pr_forecast_pathnames:
                # file = xr.open_dataset(path)
                # df = file["PRATE_L1_Avg_1"].to_dataframe().reset_index()
                # gfs_features_temp_pr = np.concatenate((gfs_features_temp_pr, np.reshape(df["PRATE_L1_Avg_1"].values, (9, 9, 1, 1))), axis=2)
                grib_file = pg.open(path)
                grib_object = grib_file.read(1)[0]
                temp = np.flip(grib_object.values, axis=1)      # since pygrib reverses the order of longitude as compared to xarray
                gfs_features_temp_pr = np.concatenate((gfs_features_temp_pr, np.reshape(temp, (9, 9, 1, 1))), axis=2)

            for path in gfs_pw_forecast_pathnames:
                # file = xr.open_dataset(path)
                # df = file["P_WAT_L200"].to_dataframe().reset_index()
                # gfs_features_temp_pw = np.concatenate((gfs_features_temp_pw, np.reshape(df["P_WAT_L200"].values, (9, 9, 1, 1))), axis=2)
                grib_file = pg.open(path)
                grib_object = grib_file.read(1)[0]
                temp = np.flip(grib_object.values, axis=1)      # since pygrib reverses the order of longitude as compared to xarray
                gfs_features_temp_pw = np.concatenate((gfs_features_temp_pw, np.reshape(temp, (9, 9, 1, 1))), axis=2)

            for path in gfs_rh_forecast_pathnames:
                # file = xr.open_dataset(path)
                # df = file["R_H_L103"].to_dataframe().reset_index()
                # gfs_features_temp_rh = np.concatenate((gfs_features_temp_rh, np.reshape(df["R_H_L103"].values, (9, 9, 1, 1))), axis=2)
                grib_file = pg.open(path)
                grib_object = grib_file.read(1)[0]
                temp = np.flip(grib_object.values, axis=1)      # since pygrib reverses the order of longitude as compared to xarray
                gfs_features_temp_rh = np.concatenate((gfs_features_temp_rh, np.reshape(temp, (9, 9, 1, 1))), axis=2)

            for path in gfs_tcc_forecast_pathnames:
                # file = xr.open_dataset(path)
                # df = file["T_CDC_L211_Avg_1"].to_dataframe().reset_index()
                # gfs_features_temp_tcc = np.concatenate((gfs_features_temp_tcc, np.reshape(df["T_CDC_L211_Avg_1"].values, (9, 9, 1, 1))), axis=2)
                grib_file = pg.open(path)
                grib_object = grib_file.read(1)[0]
                temp = np.flip(grib_object.values, axis=1)      # since pygrib reverses the order of longitude as compared to xarray
                gfs_features_temp_tcc = np.concatenate((gfs_features_temp_tcc, np.reshape(temp, (9, 9, 1, 1))), axis=2)

            gfs_features_temp_pr = gfs_features_temp_pr[:, :, 1:]
            gfs_features_temp_pw = gfs_features_temp_pw[:, :, 1:]
            gfs_features_temp_rh = gfs_features_temp_rh[:, :, 1:]
            gfs_features_temp_tcc = gfs_features_temp_tcc[:, :, 1:]

            gfs_features_temp = np.concatenate((gfs_features_temp_pr, gfs_features_temp_pw, gfs_features_temp_rh, gfs_features_temp_tcc), axis=3)

            gfs_features = np.concatenate((gfs_features, gfs_features_temp.reshape(1, 9, 9, 4, 4)), axis=0)
            gfs_features = gfs_features[1:]
            
            gfs_reduced = gfs_features.reshape(-1, 9 * 9 * 4 * 4)
            gfs_scaling_center = np.load("./preprocessor/gfs_scaling_center.npy")
            gfs_scaling_scale = np.load("./preprocessor/gfs_scaling_scale.npy")
            gfs_reduced = (gfs_reduced - gfs_scaling_center) / gfs_scaling_scale
            gfs_features = gfs_reduced.reshape(-1, 9, 9, 4, 4)

            np.save(gfs_array_save_path, gfs_features)
            
            encoding_features = []
            curr = datetime.now()

            replace_minute = curr.minute + 15 - (curr.minute % 15)
            if replace_minute != 60:
                enc_time = curr.time().replace(minute=replace_minute, second=0, microsecond=0)
            else:
                enc_time = (curr + timedelta(hours=1)).time().replace(minute=0, second=0, microsecond=0)
            encoding_features.append((enc_time.hour * 60 + enc_time.minute) // (24 * 60))
            
            enc_day = 0
            if curr.month > 9:
                enc_day = 1
            elif curr.month <= 9 and curr.month >= 6:
                enc_day = ((curr.month - 6) * 30 + curr.day) // 122
            else:
                enc_day = 0
            encoding_features.append(enc_day)

            encoding_features.append(np.random.rand(1).item())
            # gfs_times = [
            #    curr.replace(hour=5, minute=30, second=0, microsecond=0),
            #    curr.replace(hour=11, minute=30, second=0, microsecond=0),
            #    curr.replace(hour=17, minute=30, second=0, microsecond=0),
            #    curr.replace(hour=23, minute=30, second=0, microsecond=0),
            # ]
            
            encoding_features = np.array(encoding_features)
            encoding_features = np.expand_dims(encoding_features, axis=0)
            np.save(encoding_features_save_path, encoding_features)

            print("AWS regions:")
            print(np.load(aws_regions_save_path, allow_pickle=True).shape)
            print()
            print("AWS array:")
            print(np.load(aws_array1_save_path).shape)
            print()
            print("GFS array:")
            print(np.load(gfs_array_save_path).shape)
            print("Encoding features:")
            print(np.load(encoding_features_save_path).shape)
            print()

            print("Sending preprocessed data to model node")
            print()

            aws_regions_filename = aws_regions_save_path.split("/")[-1]
            aws_regions_filesize = os.path.getsize(aws_regions_save_path)
            model_sock.send(f"{aws_regions_filename}\n{aws_regions_filesize}".encode())

            time.sleep(1)

            with open(aws_regions_save_path, "rb") as f:
                bytes_read = f.read(1024)
                while bytes_read:
                    model_sock.send(bytes_read)
                    bytes_read = f.read(1024)
            print("AWS regions data sent")

            time.sleep(1)

            aws_filename = aws_array1_save_path.split("/")[-1]
            aws_filesize = os.path.getsize(aws_array1_save_path)
            model_sock.send(f"{aws_filename}\n{aws_filesize}".encode())

            time.sleep(1)

            with open(aws_array1_save_path, "rb") as f:
                bytes_read = f.read(1024)
                while bytes_read:
                    model_sock.send(bytes_read)
                    bytes_read = f.read(1024)
            print("AWS processed data sent")

            time.sleep(1)

            gfs_filename = gfs_array_save_path.split("/")[-1]
            gfs_filesize = os.path.getsize(gfs_array_save_path)
            model_sock.send(f"{gfs_filename}\n{gfs_filesize}".encode())

            time.sleep(1)

            with open(gfs_array_save_path, "rb") as f:
                bytes_read = f.read(1024)
                while bytes_read:
                    model_sock.send(bytes_read)
                    bytes_read = f.read(1024)
            print("GFS processed data sent")

            time.sleep(1)

            encoding_filename = encoding_features_save_path.split("/")[-1]
            encoding_filesize = os.path.getsize(encoding_features_save_path)
            model_sock.send(f"{encoding_filename}\n{encoding_filesize}".encode())

            time.sleep(1)

            with open(encoding_features_save_path, "rb") as f:
                bytes_read = f.read(1024)
                while bytes_read:
                    model_sock.send(bytes_read)
                    bytes_read = f.read(1024)
            print("Encoding features sent")
            
            # time.sleep(15 * 60 - 20)

    except KeyboardInterrupt:
        print(f"\nClosing connection with collector node: {collector_addr}")
        collector_sock.close()
        model_sock.close()
        break
