import socket
import numpy as np
import time

DISTRIBUTOR_IP_ADDR = "127.0.0.1"
DISTRIBUTOR_PORT = 5002

policy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
policy_sock.bind((DISTRIBUTOR_IP_ADDR, DISTRIBUTOR_PORT))
policy_sock.listen()

XAPP_IP_ADDR = "127.0.0.1"
XAPP_PORT = 5001

xapp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# xapp_sock.close()
# exit(0)
xapp_sock.connect((XAPP_IP_ADDR, XAPP_PORT))

print(f"Distributor node listening on {DISTRIBUTOR_IP_ADDR}:{DISTRIBUTOR_PORT}")

while True:
    try:
        policy_conn, policy_addr = policy_sock.accept()
        print(f"Connected to policy node: {policy_addr}")
        # policy_data = None
        while True:
            policy_data = policy_conn.recv(1024).decode()
            if not policy_data:
                break
            print("Received from policy node:", policy_data)
        
            print("Distributing information to sink (xApp)")
            xapp_sock.send(policy_data.encode())
        
            time.sleep(15)

    except KeyboardInterrupt:
        print(f"\nClosing connection with policy node: {policy_addr}")
        policy_sock.close()
        xapp_sock.close()
        break
