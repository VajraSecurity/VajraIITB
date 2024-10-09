import socket
import numpy as np
import time
import os
import torch
from torch import nn


class GFS_CNN_Regression(nn.Module):
    def _init_(self, num_features):
        super(GFS_CNN_Regression, self)._init_()
        self.conv1 = nn.Conv3d(in_channels=4, out_channels=4, kernel_size=(1, 3, 3), stride=1, padding='valid', bias=True)
        self.conv2 = nn.Conv3d(in_channels=4, out_channels=8, kernel_size=(1, 3, 3), stride=1, padding='valid', bias=True)
        self.conv3 = nn.Conv3d(in_channels=8, out_channels=8, kernel_size=(1, 3, 3), stride=1, padding='valid', bias=True)
        self.fc1 = nn.Linear(288 + num_features, 64)
        self.fc2 = nn.Linear(64, 32)
        self.fc3 = nn.Linear(32, 8)
        self.fc4 = nn.Linear(8, 1)
        self.relu = nn.ReLU()

    def forward(self, x, external_features):
        x = self.relu(self.conv1(x))
        x = self.relu(self.conv2(x))
        x = self.relu(self.conv3(x))
        x = x.view(x.size(0), -1)
        x = torch.cat((x, external_features), dim=1)
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.relu(self.fc3(x))
        x = self.fc4(x)
        return x


def run_model(regions, aws, gfs, encoding):
    predictions = []
    
    for i in range(len(regions)):
        cur_region = regions[i]
        home = "./model/checkpoints/"
        model_path = home + 'best_val-' + cur_region + '.pth'
        model = torch.load(model_path)
        model.eval()
        external_feature = np.concatenate((aws[:, :, i], encoding), axis = 1)
        external_feature = np.asarray(external_feature, dtype = np.float32)                          
        external_feature_torch = torch.from_numpy(external_feature).float()

        gfs = np.asarray(gfs, dtype=np.float32)

        x_input = torch.from_numpy(gfs).float()
        x_input = torch.permute(x_input, (0, 4, 3, 1, 2))

        with torch.no_grad():
            y_output = model(x_input, external_feature_torch).detach().numpy()

        predictions.append(y_output[0][0])

    return predictions


if not os.path.exists("./model/"):
    os.mkdir("./model/")

if not os.path.exists("./model/checkpoints/"):
    os.mkdir("./model/checkpoints/")

regions_save_path = "./model/regions.npy"
aws_features_save_path = "./model/aws_features.npy"
gfs_features_save_path = "./model/gfs_features.npy"
encoding_features_save_path = "./model/encoding_features.npy"
forecasts_save_path = "./model/forecasts.npy"

MODEL_IP_ADDR = "127.0.0.1"
MODEL_PORT = 6004

preprocessor_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
preprocessor_sock.close()
preprocessor_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
preprocessor_sock.bind((MODEL_IP_ADDR, MODEL_PORT))
preprocessor_sock.listen()

POLICY_IP_ADDR = "127.0.0.1"
POLICY_PORT = 6003

policy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# policy_sock.close()
# exit(0)
# policy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
policy_sock.connect((POLICY_IP_ADDR, POLICY_PORT))

print(f"Model node listening on {MODEL_IP_ADDR}:{MODEL_PORT}")

while True:
    try:
        preprocessor_conn, preprocessor_addr = preprocessor_sock.accept()
        print(f"Connected to preprocessor node: {preprocessor_addr}")
        
        while True:
            preprocessor_data = preprocessor_conn.recv(1024).decode().splitlines()
            aws_regions_filename = preprocessor_data[0]
            filesize = int(preprocessor_data[1])

            with open(regions_save_path, "wb") as f:
                bytes_received = 0
                while bytes_received < filesize:
                    bytes_read = preprocessor_conn.recv(1024)
                    if not bytes_read:
                        break
                    f.write(bytes_read)
                    bytes_received += len(bytes_read)
            print("Received from preprocessor node:", preprocessor_data)

            time.sleep(1)

            preprocessor_data = preprocessor_conn.recv(1024).decode().splitlines()
            aws_filename = preprocessor_data[0]
            filesize = int(preprocessor_data[1])

            with open(aws_features_save_path, "wb") as f:
                bytes_received = 0
                while bytes_received < filesize:
                    bytes_read = preprocessor_conn.recv(1024)
                    if not bytes_read:
                        break
                    f.write(bytes_read)
                    bytes_received += len(bytes_read)
            print("Received from preprocessor node:", preprocessor_data)

            time.sleep(1)
            
            preprocessor_data = preprocessor_conn.recv(1024).decode().splitlines()
            gfs_filename = preprocessor_data[0]
            filesize = int(preprocessor_data[1])

            with open(gfs_features_save_path, "wb") as f:
                bytes_received = 0
                while bytes_received < filesize:
                    bytes_read = preprocessor_conn.recv(1024)
                    if not bytes_read:
                        break
                    f.write(bytes_read)
                    bytes_received += len(bytes_read)
            print("Received from preprocessor node:", preprocessor_data)

            time.sleep(1)

            preprocessor_data = preprocessor_conn.recv(1024).decode().splitlines()
            encoding_filename = preprocessor_data[0]
            filesize = int(preprocessor_data[1])

            with open(encoding_features_save_path, "wb") as f:
                bytes_received = 0
                while bytes_received < filesize:
                    bytes_read = preprocessor_conn.recv(1024)
                    if not bytes_read:
                        break
                    f.write(bytes_read)
                    bytes_received += len(bytes_read)
            print("Received from preprocessor node:", preprocessor_data)

            if not preprocessor_data:
                break

            regions = np.load(regions_save_path, allow_pickle=True)
            aws = np.load(aws_features_save_path, allow_pickle=True)
            gfs = np.load(gfs_features_save_path)
            encoding = np.load(encoding_features_save_path)

            forecasts = run_model(regions, aws, gfs, encoding)
            forecasts = np.array(forecasts)
            np.save(forecasts_save_path, forecasts)
            print(forecasts)

            forecasts_filename = forecasts_save_path.split("/")[-1]
            forecasts_filesize = os.path.getsize(forecasts_save_path)
            policy_sock.send(f"{forecasts_filename}\n{forecasts_filesize}".encode())

            time.sleep(1)

            with open(forecasts_save_path, "rb") as f:
                bytes_read = f.read(1024)
                while bytes_read:
                    policy_sock.send(bytes_read)
                    bytes_read = f.read(1024)
            print("Forecast data sent")

            # time.sleep(15)

    except KeyboardInterrupt:
        print(f"\nClosing connection with preprocessor node: {preprocessor_addr}")
        preprocessor_sock.close()
        policy_sock.close()
        break
