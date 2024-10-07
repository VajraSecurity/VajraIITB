import socket
import numpy as np
import time
import os


def decide_policy(forecasts):
    threshold = 10
    flood_arr = []
    for i in forecasts:
        if i > threshold:
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
POLICY_PORT = 5003

model_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
model_sock.bind((POLICY_IP_ADDR, POLICY_PORT))
model_sock.listen()

DISTRIBUTOR_IP_ADDR = "127.0.0.1"
DISTRIBUTOR_PORT = 5002

distributor_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# distributor_sock.close()
# exit(0)
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
