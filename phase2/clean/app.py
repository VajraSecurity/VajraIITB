import socket
import xapp_sdk as ric
import time
import pdb
import json
import os

####################
####  SLICE INDICATION MSG TO JSON
####################

def slice_ind_to_dict_json(ind):

    slice_stats = {
        "RAN" : {
            "dl" : {}
        },
        "UE" : {}
    }

    # RAN - dl
    dl_dict = slice_stats["RAN"]["dl"]
    if ind.slice_stats.dl.len_slices <= 0:
        dl_dict["num_of_slices"] = ind.slice_stats.dl.len_slices
        dl_dict["slice_sched_algo"] = "null"
        dl_dict["ue_sched_algo"] = ind.slice_stats.dl.sched_name[0]
    else:
        dl_dict["num_of_slices"] = ind.slice_stats.dl.len_slices
        dl_dict["slice_sched_algo"] = "null"
        dl_dict["slices"] = []
        slice_algo = ""
        for s in ind.slice_stats.dl.slices:
            if s.params.type == 1: # TODO: convert from int to string, ex: type = 1 -> STATIC
                slice_algo = "STATIC"
            elif s.params.type == 2:
                slice_algo = "NVS"
            elif s.params.type == 4:
                slice_algo = "EDF"
            else:
                slice_algo = "unknown"
            dl_dict.update({"slice_sched_algo" : slice_algo})

            slices_dict = {
                "index" : s.id,
                "label" : s.label[0],
                "ue_sched_algo" : s.sched[0],
            }
            if dl_dict["slice_sched_algo"] == "STATIC":
                slices_dict["slice_algo_params"] = {
                    "pos_low" : s.params.u.sta.pos_low,
                    "pos_high" : s.params.u.sta.pos_high
                }
            elif dl_dict["slice_sched_algo"] == "NVS":
                if s.params.u.nvs.conf == 0:
                    slices_dict["slice_algo_params"] = {
                        "type" : "RATE",
                        "mbps_rsvd" : s.params.u.nvs.u.rate.u1.mbps_required,
                        "mbps_ref" : s.params.u.nvs.u.rate.u2.mbps_reference
                    }
                elif s.params.u.nvs.conf == 1:
                    slices_dict["slice_algo_params"] = {
                        "type" : "CAPACITY",
                        "pct_rsvd" : s.params.u.nvs.u.capacity.u.pct_reserved
                    }
                else:
                    slices_dict["slice_algo_params"] = {"type" : "unknown"}
            elif dl_dict["slice_sched_algo"] == "EDF":
                slices_dict["slice_algo_params"] = {
                    "deadline" : s.params.u.edf.deadline,
                    "guaranteed_prbs" : s.params.u.edf.guaranteed_prbs,
                    "max_replenish" : s.params.u.edf.max_replenish
                }
            else:
                print("unknown slice algorithm, cannot handle params")
            dl_dict["slices"].append(slices_dict)

    # UE
    global assoc_rnti
    ue_dict = slice_stats["UE"]
    if ind.ue_slice_stats.len_ue_slice <= 0:
        ue_dict["num_of_ues"] = ind.ue_slice_stats.len_ue_slice
    else:
        ue_dict["num_of_ues"] = ind.ue_slice_stats.len_ue_slice
        ue_dict["ues"] = []
        for u in ind.ue_slice_stats.ues:
            ues_dict = {}
            dl_id = "null"
            if u.dl_id >= 0 and dl_dict["num_of_slices"] > 0:
                dl_id = u.dl_id
            ues_dict = {
                "rnti" : hex(u.rnti),
                "assoc_dl_slice_id" : dl_id
            }
            ue_dict["ues"].append(ues_dict)
            assoc_rnti = u.rnti

    ind_dict = slice_stats
    ind_json = json.dumps(ind_dict)

    with open("rt_slice_stats.json", "w") as outfile:
        outfile.write(ind_json)
    # print(ind_dict)

    return json

####################
#### SLICE INDICATION CALLBACK
####################

class SLICECallback(ric.slice_cb):
    # Define Python class 'constructor'
    def __init__(self):
        # Call C++ base class constructor
        ric.slice_cb.__init__(self)
    # Override C++ method: virtual void handle(swig_slice_ind_msg_t a) = 0;
    def handle(self, ind):
        # Print swig_slice_ind_msg_t
        #if (ind.slice_stats.dl.len_slices > 0):
        #     print('SLICE Indication tstamp = ' + str(ind.tstamp))
        #     print('SLICE STATE: len_slices = ' + str(ind.slice_stats.dl.len_slices))
        #     print('SLICE STATE: sched_name = ' + str(ind.slice_stats.dl.sched_name[0]))
        #if (ind.ue_slice_stats.len_ue_slice > 0):
        #    print('UE ASSOC SLICE STATE: len_ue_slice = ' + str(ind.ue_slice_stats.len_ue_slice))
        slice_ind_to_dict_json(ind)

####################
####  SLICE CONTROL FUNCS
####################
def create_slice(slice_params, slice_sched_algo):
    s = ric.fr_slice_t()
    s.id = slice_params["id"]
    s.label = slice_params["label"]
    s.len_label = len(slice_params["label"])
    s.sched = slice_params["ue_sched_algo"]
    s.len_sched = len(slice_params["ue_sched_algo"])
    if slice_sched_algo == "STATIC":
        s.params.type = ric.SLICE_ALG_SM_V0_STATIC
        s.params.u.sta.pos_low = slice_params["slice_algo_params"]["pos_low"]
        s.params.u.sta.pos_high = slice_params["slice_algo_params"]["pos_high"]
    elif slice_sched_algo == "NVS":
        s.params.type = ric.SLICE_ALG_SM_V0_NVS
        if slice_params["type"] == "SLICE_SM_NVS_V0_RATE":
            s.params.u.nvs.conf = ric.SLICE_SM_NVS_V0_RATE
            s.params.u.nvs.u.rate.u1.mbps_required = slice_params["slice_algo_params"]["mbps_rsvd"]
            s.params.u.nvs.u.rate.u2.mbps_reference = slice_params["slice_algo_params"]["mbps_ref"]
            # print("ADD NVS DL SLCIE: id", s.id,
            # ", conf", s.params.u.nvs.conf,
            # ", mbps_rsrv", s.params.u.nvs.u.rate.u1.mbps_required,
            # ", mbps_ref", s.params.u.nvs.u.rate.u2.mbps_reference)
        elif slice_params["type"] == "SLICE_SM_NVS_V0_CAPACITY":
            s.params.u.nvs.conf = ric.SLICE_SM_NVS_V0_CAPACITY
            s.params.u.nvs.u.capacity.u.pct_reserved = slice_params["slice_algo_params"]["pct_rsvd"]
            # print("ADD NVS DL SLCIE: id", s.id,
            # ", conf", s.params.u.nvs.conf,
            # ", pct_rsvd", s.params.u.nvs.u.capacity.u.pct_reserved)
        else:
            print("Unkown NVS conf")
    elif slice_sched_algo == "EDF":
        s.params.type = ric.SLICE_ALG_SM_V0_EDF
        s.params.u.edf.deadline = slice_params["slice_algo_params"]["deadline"]
        s.params.u.edf.guaranteed_prbs = slice_params["slice_algo_params"]["guaranteed_prbs"]
        s.params.u.edf.max_replenish = slice_params["slice_algo_params"]["max_replenish"]
    else:
        print("Unkown slice algo type")

    return s


def fill_slice_ctrl_msg(ctrl_type, ctrl_msg):
    msg = ric.slice_ctrl_msg_t()
    if (ctrl_type == "ADDMOD"):
        msg.type = ric.SLICE_CTRL_SM_V0_ADD
        dl = ric.ul_dl_slice_conf_t()
        # ue_sched_algo can be "RR"(round-robin), "PF"(proportional fair) or "MT"(maximum throughput) and it has to be set in any len_slices
        ue_sched_algo = "PF"
        dl.sched_name = ue_sched_algo
        dl.len_sched_name = len(ue_sched_algo)

        dl.len_slices = ctrl_msg["num_slices"]
        slices = ric.slice_array(ctrl_msg["num_slices"])
        for i in range(0, ctrl_msg["num_slices"]):
            slices[i] = create_slice(ctrl_msg["slices"][i], ctrl_msg["slice_sched_algo"])

        dl.slices = slices
        msg.u.add_mod_slice.dl = dl
    elif (ctrl_type == "DEL"):
        msg.type = ric.SLICE_CTRL_SM_V0_DEL

        msg.u.del_slice.len_dl = ctrl_msg["num_dl_slices"]
        del_dl_id = ric.del_dl_array(ctrl_msg["num_dl_slices"])
        for i in range(ctrl_msg["num_dl_slices"]):
            del_dl_id[i] = ctrl_msg["delete_dl_slice_id"][i]
        # print("DEL DL SLICE: id", del_dl_id)

        msg.u.del_slice.dl = del_dl_id
    elif (ctrl_type == "ASSOC_UE_SLICE"):
        msg.type = ric.SLICE_CTRL_SM_V0_UE_SLICE_ASSOC

        msg.u.ue_slice.len_ue_slice = ctrl_msg["num_ues"]
        assoc = ric.ue_slice_assoc_array(ctrl_msg["num_ues"])
        for i in range(ctrl_msg["num_ues"]):
            a = ric.ue_slice_assoc_t()
            a.rnti = assoc_rnti # TODO: assign the rnti after get the indication msg from slice_ind_to_dict_json()
            a.dl_id = ctrl_msg["ues"][i]["assoc_dl_slice_id"]
            assoc[i] = a
        msg.u.ue_slice.ues = assoc

    return msg


add_static_slices = {
    "num_slices" : 3,
    "slice_sched_algo" : "STATIC",
    "slices" : [
        {
            "id" : 0,
            "label" : "s1",
            "ue_sched_algo" : "PF",
            "slice_algo_params" : {"pos_low" : 0, "pos_high" : 2},
        },
        {
            "id" : 1,
            "label" : "s2",
            "ue_sched_algo" : "PF",
            "slice_algo_params" : {"pos_low" : 3, "pos_high" : 7},
        },
        {
            "id" : 2,
            "label" : "s3",
            "ue_sched_algo" : "PF",
            "slice_algo_params" : {"pos_low" : 8, "pos_high" : 10},
        }
    ]
}

add_emergency_slice = {
    "num_slices": 1,
    "slice_sched_algo": "STATIC",
    "slices" : [
        {
            "id": 3,
            "label": "s4",
            "ue_sched_algo": "PF",
            "slice_algo_params" : {"pos_low": 11, "pos_high": 13},
        }
    ]
}

delete_emergency_slice = {
    "num_dl_slices" : 1,
    "delete_dl_slice_id" : [3],
}

assoc_emergency_ue_slice = {
    "num_ues" : 1,
    "ues" : [
        {
            "rnti" : 0,
            "assoc_dl_slice_id" : 3
        }
    ]
}

ric.init()

conn = ric.conn_e2_nodes()
assert(len(conn) > 0)
# print(len(conn))

node_idx = 0


cmds = {
    "HIGH_PRIORITY_RESOURCE_ALLOCATION": "HIGH_PRIORITY",
    "NORMAL_RESOURCE_ALLOCATION": "NORMAL_PRIORITY",
    "BROADCAST_MESSAGE": "BROADCAST_ALERT",
}

regions = ['Andheri', 'B ward', 'Bandra', 'Byculla', 'C ward', 'Chembur', 
           'Chincholi', 'Colaba', 'D Ward', 'Dahisar', 'Dindoshi', 'F North',
           'F South', 'G South', 'Gowanpada', 'H West ward', 'K East ward',
           'K West ward', 'Kandivali', 'Kurla', 'L ward', 'M West ward', 'MCGM 1',
           'Malvani', 'Marol', 'Memonwada', 'Mulund', 'N ward', 'Nariman Fire',
           'Rawali camp', 'S ward', 'SWD Workshop dadar', 'Thakare natya',
           'Vikhroli', 'Worli', 'vileparle W']

XAPP_IP_ADDR = "127.0.0.1"
XAPP_PORT = 6001

distributor_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
distributor_sock.close()
distributor_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
distributor_sock.bind((XAPP_IP_ADDR, XAPP_PORT))
distributor_sock.listen()

print(f"xApp listening on {XAPP_IP_ADDR}:{XAPP_PORT}")

# Receive input from distributor node about the to-be-affected regions
# We are observing 36 regions, so input is a 36-dimensional vector
# An entry of 1 in the vector denotes that the region is likely to be flooded, whereas 0 denotes that the region is unlikely to be flooded
while True:
    try:
        distributor_conn, distributor_addr = distributor_sock.accept()
        print(f"Connected to distributor node: {distributor_addr}")
        slice_cb = SLICECallback()
        hndlr = ric.report_slice_sm(conn[node_idx].id, ric.Interval_ms_5, slice_cb)
        time.sleep(5)
        msg = fill_slice_ctrl_msg("ADDMOD", add_static_slices)
        ric.control_slice_sm(conn[node_idx].id, msg)
        time.sleep(5)
        while True:
            data = distributor_conn.recv(1024).decode()
            if not data:
                break
            print("Received from distributor node:", data)
            # print("Each number (0/1) corresponds to a particular region")
            # print("0 denotes normal rainfall forecasted in next 15 minutes")
            # print("1 denotes heavy rainfall forecasted in next 15 minutes")
            status = data.split(" ")
            print()
            print("Decoding and sending commands to respective base stations")
            print()
            assert len(status) == len(regions)
            for i in range(len(regions)):
                region = regions[i]
                # status = ["1"]
                if status[i] == "1":
                    # add emergency slice
                    msg = fill_slice_ctrl_msg("ADDMOD", add_emergency_slice)
                    ric.control_slice_sm(conn[node_idx].id, msg)
                    time.sleep(5)
                    # associate emergency ues
                    msg = fill_slice_ctrl_msg("ASSOC_UE_SLICE", assoc_emergency_ue_slice)
                    ric.control_slice_sm(conn[node_idx].id, msg)
                    time.sleep(5)
                    # Send alert
                    os.system("ping -c 2 192.168.70.135")
                else:
                    # delete emergency slice
                    msg = fill_slice_ctrl_msg("DEL", delete_emergency_slice)
                    ric.control_slice_sm(conn[node_idx].id, msg)
                    time.sleep(5)
                break


    except KeyboardInterrupt:
        print("\nClosing connection with distributor node: {distributor_addr}")
        distributor_sock.close()
        break
