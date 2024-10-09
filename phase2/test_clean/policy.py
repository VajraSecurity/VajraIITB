import socket
import numpy as np
import time
import os

region_dict = {'Rawali camp': 15.87301874160767,
 'F North': 19.681729888916035,
 'G South': 18.81007146835328,
 'Memonwada': 15.715972089767462,
 'C ward': 15.740541553497323,
 'B ward': 15.726534080505374,
 'Worli': 17.632006931304947,
 'Bandra': 15.932213068008426,
 'D Ward': 15.845744848251343,
 'K West ward': 18.404749774932863,
 'Colaba': 15.190009212493917,
 'K East ward': 19.49791412353518,
 'N ward': 17.495261192321777,
 'Kandivali': 20.965509986877443,
 'Vikhroli': 18.17284383773805,
 'F South': 17.640174579620364,
 'H West ward': 14.453212451934816,
 'Chembur': 18.708271789550786,
 'Marol': 19.625081253051803,
 'MCGM 1': 18.315613269805908,
 'M West ward': 14.86353030204774,
 'Gowanpada': 17.326474952697758,
 'Andheri': 19.85052766799927,
 'Thakare natya': 17.445701313018798,
 'vileparle W': 15.826990079879765,
 'Mulund': 15.37668972015384,
 'Byculla': 16.90901498794556,
 'SWD Workshop dadar': 19.19415864944458,
 'Malvani': 16.0380997657776,
 'Kurla': 19.456711864471444,
 'Dindoshi': 18.714636993408227,
 'S ward': 18.04791631698609,
 'L ward': 17.87476625442507,
 'Nariman Fire': 14.778929042816165,
 'Dahisar': 18.9022617340088,
 'Chincholi': 17.798383712768565}

keys = region_dict.keys()
thresholds = [region_dict[i] for i in keys]

def decide_policy(forecasts):
    flood_arr = []
    for i in range(len(forecasts)):
        if forecasts[i] > thresholds[i]:
            flood_arr.append(1)
        else:
            flood_arr.append(0)
    flood_arr = np.array(flood_arr)
    return flood_arr


if not os.path.exists("./policy/"):
    os.mkdir("./policy")

forecasts_save_path = "./policy/forecasts.npy"
policy_save_path = "./policy/flood_arr.npy"

POLICY_IP_ADDR = "127.0.0.1"
POLICY_PORT = 6003

model_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
model_sock.close()
model_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
model_sock.bind((POLICY_IP_ADDR, POLICY_PORT))
model_sock.listen()

DISTRIBUTOR_IP_ADDR = "127.0.0.1"
DISTRIBUTOR_PORT = 6002

distributor_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# distributor_sock.close()
# exit(0)
# distributor_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
distributor_sock.connect((DISTRIBUTOR_IP_ADDR, DISTRIBUTOR_PORT))

print(f"Policy node listening on {POLICY_IP_ADDR}:{POLICY_PORT}")

while True:
    try:
        model_conn, model_addr = model_sock.accept()
        print(f"Connected to model node: {model_addr}")

        while True:
            model_data = model_conn.recv(1024).decode().splitlines()
            forecast_filename = model_data[0]
            filesize = int(model_data[1])

            with open(forecasts_save_path, "wb") as f:
                bytes_received = 0
                while bytes_received < filesize:
                    bytes_read = model_conn.recv(1024)
                    if not bytes_read:
                        break
                    f.write(bytes_read)
                    bytes_received += len(bytes_read)
            print("Received from model node:", model_data)

            if not model_data:
                break

            forecasts = np.load(forecasts_save_path)
            flood_arr = decide_policy(forecasts)
            policy = ""
            for i in flood_arr:
                policy += str(i) + " "
            policy = policy[:-1]
        
            print(f"Sending policy to distributor: {policy}")
            distributor_sock.send(policy.encode())

            # time.sleep(15 * 60)
            # time.sleep(15)

    except KeyboardInterrupt:
        print(f"\nClosing connection with model node: {model_addr}")
        model_sock.close()
        distributor_sock.close()
        break
