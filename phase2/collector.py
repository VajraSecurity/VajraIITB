import socket
import requests
import shutil
import datetime as dt
from datetime import datetime, timedelta
import os
import time
import pandas as pd
import concurrent.futures
import warnings


def download_gfs_data(year, month, day, hour):
    pdate = datetime(year, month, day, 11, 00, 00).strftime("%Y%m%d")
    # direc = os.path.join('./GFS', datetime.now().strftime("%d-%m-%Y-%H-00-00"))
    direc = "./collector/GFS/"
    
    if os.path.exists("./collector/GFS"):
        os.system("rm -rf ./collector/GFS")
    # direc_prev = os.path.join('./GFS', (datetime.now() - timedelta(hours=6)).strftime("%d-%m-%Y-%H-00-00-00"))
    
    # delete old files
    # if os.path.exists(direc_prev):
    #     os.system("rm -rf " + direc_prev)
    
    # create new mkdir
    os.system(f"mkdir -p {direc}")
    os.chdir(direc)

    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl"
    # forecast_hours = [f"{hour:03d}" for hour in range(3, 163, 3)]
    forecast_hours = [f"{hour:03d}" for hour in range(3, 27, 3)]

    # Iterate over the forecast hours and download the files
    os.mkdir("./PR/")
    os.chdir("./PR/")
    for fhour in forecast_hours:
        url = (f"{base_url}?dir=%2Fgfs.{pdate}%2F{str(hour).zfill(2)}%2Fatmos"
               f"&file=gfs.t{str(hour).zfill(2)}z.pgrb2.0p25.f{fhour}"
               "&var_PRATE=on&lev_surface=on"
               "&subregion=&toplat=20&leftlon=72&rightlon=74&bottomlat=18")
        # print(url)
        
        r = requests.get(url, verify=False, stream=True)
        r.raw.decode_content = True
        
        with open(f"gfs.t0{hour}z.pgrb2.0p25.f{fhour}", 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    os.chdir("../")
    os.mkdir("./PW/")
    os.chdir("./PW/")
    for fhour in forecast_hours:
        url = (f"{base_url}?dir=%2Fgfs.{pdate}%2F{str(hour).zfill(2)}%2Fatmos"
               f"&file=gfs.t{str(hour).zfill(2)}z.pgrb2.0p25.f{fhour}"
               "&var_PWAT=on&lev_entire_atmosphere_(considered_as_a_single_layer)=on"
               "&subregion=&toplat=20&leftlon=72&rightlon=74&bottomlat=18")
        # print(url)
        
        r = requests.get(url, verify=False, stream=True)
        r.raw.decode_content = True
        
        with open(f"gfs.t0{hour}z.pgrb2.0p25.f{fhour}", 'wb') as f:
            shutil.copyfileobj(r.raw, f)
    
    os.chdir("../")
    os.mkdir("./RH/")
    os.chdir("./RH/")
    for fhour in forecast_hours:
        url = (f"{base_url}?dir=%2Fgfs.{pdate}%2F{str(hour).zfill(2)}%2Fatmos"
               f"&file=gfs.t{str(hour).zfill(2)}z.pgrb2.0p25.f{fhour}"
               "&var_RH=on&lev_2_m_above_ground=on"
               "&subregion=&toplat=20&leftlon=72&rightlon=74&bottomlat=18")
        # print(url)
        
        r = requests.get(url, verify=False, stream=True)
        r.raw.decode_content = True
        
        with open(f"gfs.t0{hour}z.pgrb2.0p25.f{fhour}", 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    os.chdir("../")
    os.mkdir("./TCC/")
    os.chdir("./TCC/")
    for fhour in forecast_hours:
        url = (f"{base_url}?dir=%2Fgfs.{pdate}%2F{str(hour).zfill(2)}%2Fatmos"
               f"&file=gfs.t{str(hour).zfill(2)}z.pgrb2.0p25.f{fhour}"
               "&var_TCDC=on&lev_boundary_layer_cloud_layer=on"
               "&subregion=&toplat=20&leftlon=72&rightlon=74&bottomlat=18")
        # print(url)
        
        r = requests.get(url, verify=False, stream=True)
        r.raw.decode_content = True
        
        with open(f"gfs.t0{hour}z.pgrb2.0p25.f{fhour}", 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    os.chdir("../../../")
    return


def fetch(id):
    payload = {"id": id}
    response = requests.post(url, json=payload, headers=headers)
    return id, response


def download_aws_data(ids):
    # os.system("rm -rf AWS_data*")
    os.system("rm -rf ./collector/AWS_data.xlsx")
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        results = executor.map(fetch, ids)

    data_list = []
    for id, response in results:
        try:
            if response.status_code == 200:
                # print(f"Data for ID {id}:")
                json_response = response.json()
                data_list.append(json_response)
                # print(response.json())
            else:
                # Request failed
                print(f"POST request for ID {id} failed with status code {response.status_code}")
        except:
            print("error------------------------------------")

    # Initialize empty lists to store extracted data
    ids = []
    codes = []
    descriptions = []
    zone_ids = []
    latitudes = []
    longitudes = []
    status = []
    tempOuts = []
    highTemps = []
    lowTemps = []
    outHumidities = []
    dewPoints = []
    windSpeeds = []
    windDirs = []
    windRuns = []
    hiSpeeds = []
    hiDirs = []
    windChills = []
    heatIndexes = []
    thwIndexes = []
    bars = []
    rains = []
    rainRates = []
    headDds = []
    coolDds = []
    inTemps = []
    inHumidities = []

    # Loop through the list of dictionaries and extract data
    for data in data_list:
        location = data['locationList']
        dummy_data = data['dummyTestRaingaugeDataDetails']
        
        ids.append(location['id'])
        codes.append(location['code'])
        descriptions.append(location['description'])
        zone_ids.append(location['zoneid'])
        latitudes.append(location['lati'])
        longitudes.append(location['longi'])
        status.append(location['status'])
        
        tempOuts.append(dummy_data['tempOut'])
        highTemps.append(dummy_data['highTemp'])
        lowTemps.append(dummy_data['lowTemp'])
        outHumidities.append(dummy_data['outHumidity'])
        dewPoints.append(dummy_data['dewPoint'])
        windSpeeds.append(dummy_data['windSpeed'])
        windDirs.append(dummy_data['windDir'])
        windRuns.append(dummy_data['windRun'])
        hiSpeeds.append(dummy_data['hiSpeed'])
        hiDirs.append(dummy_data['hiDir'])
        windChills.append(dummy_data['windChill'])
        heatIndexes.append(dummy_data['heatIndex'])
        thwIndexes.append(dummy_data['thwIndex'])
        bars.append(dummy_data['bar'])
        rains.append(dummy_data['rain'])
        rainRates.append(dummy_data['rainRate'])
        headDds.append(dummy_data['headDd'])
        coolDds.append(dummy_data['coolDd'])
        inTemps.append(dummy_data['inTemp'])
        inHumidities.append(dummy_data['inHumidity'])

    # Create a Pandas DataFrame
    df = pd.DataFrame({
        'ID': ids,
        'Code': codes,
        'Description': descriptions,
        'Zone_ID': zone_ids,
        'Latitude': latitudes,
        'Longitude': longitudes,
        'Status': status,
        'Temp_Out': tempOuts,
        'High_Temp': highTemps,
        'Low_Temp': lowTemps,
        'Out_Humidity': outHumidities,
        'Dew_Point': dewPoints,
        'Wind_Speed': windSpeeds,
        'Wind_Dir': windDirs,
        'Wind_Run': windRuns,
        'Hi_Speed': hiSpeeds,
        'Hi_Dir': hiDirs,
        'Wind_Chill': windChills,
        'Heat_Index': heatIndexes,
        'THW_Index': thwIndexes,
        'Bar': bars,
        'Rain': rains,
        'Rain_Rate': rainRates,
        'Head_Dd': headDds,
        'Cool_Dd': coolDds,
        'In_Temp': inTemps,
        'In_Humidity': inHumidities
    })

    # df = pd.read_excel("/Users/kpmac/Documents/IITB/DS593/code/xapp/mcgm_data_2024_10_05_16_24_43.xlsx")
    code_dict = {
        "Andheri": "Andheri",
        "BWard": "B ward",
        "BandraNe": "Bandra",
        "Byculla": "Byculla",
        "CWard": "C ward",
        "Chembur": "Chembur",
        "Chinchol": "Chincholi",
        "Colaba": "Colaba",
        "DWardNan": "D Ward",
        "Dahisarn": "Dahisar",
        "Dindoshi": "Dindoshi",
        "FNorthWa": "F North",
        "FSouthWa": "F South",
        "GSouthWa": "G South",
        "Gawanpad": "Gowanpada",
        "HWestWar": "H West ward",
        "KEastWar": "K East ward",
        "KWestWar": "K West ward",
        "Kandivali": "Kandivali",
        "Kurla": "Kurla",
        "LWard": "L ward",
        "MWestWar": "M West ward",
        "MCGM1": "MCGM 1",
        "MalvaniF": "Malvani",
        "MAROL": "Marol",
        "Memonwad": "Memonwada",
        "Mulund": "Mulund",
        "NWard": "N ward",
        "NarimanF": "Nariman Fire",
        "RawaliCa": "Rawali camp",
        "SWard": "S ward",
        "SWDWorkS": "SWD Workshop dadar",
        "PrThackerayNatyaMandir": "Thakare natya",
        "Vikhroli": "Vikhroli",
        "Worli": "Worli",
        "Villewir": "vileparle W"
    }
    df = df[df["Code"].isin(code_dict.keys())]
    df = df[["Code", "Rain"]]
    df.replace(code_dict, inplace=True)
    df.fillna(0, inplace=True)
    # df.to_excel("/Users/kpmac/Documents/IITB/DS593/code/xapp/mcgm_data_2024_10_05_16_24_43.xlsx")
    # Print the DataFrame
    # print(df)

    current_datetime = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    # df.to_excel('AWS_data' + current_datetime + '.xlsx')
    if not os.path.exists("./collector/"):
        os.mkdir("./collector/")
    df.to_excel("./collector/AWS_data.xlsx")
    return


if __name__ == "__main__":
    PREPROCESSOR_IP_ADDR = "127.0.0.1"
    PREPROCESSOR_PORT = 5005

    preprocessor_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # preprocessor_sock.close()
    # exit(0)
    preprocessor_sock.connect((PREPROCESSOR_IP_ADDR, PREPROCESSOR_PORT))

    warnings.filterwarnings("ignore")

    df = pd.read_csv('station_data.csv', index_col='id')
    ids = list(set(df['locationid']))

    url = "https://dmwebtwo.mcgm.gov.in/api/tabWeatherForecastData/loadById"
    headers = {
        "Authorization": "REDACTED"
    }

    while True:
        curr = dt.datetime.now()

        gfs_times = [
            curr.replace(hour=9, minute=15, second=30),
            curr.replace(hour=15, minute=15, second=30),
            curr.replace(hour=21, minute=15, second=30),
            curr.replace(hour=3, minute=15, second=30) + timedelta(days=1),
            curr.replace(hour=9, minute=15, second=30) + timedelta(days=1)
        ]

        print("Current date and time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print()

        print("Collecting AWS data")
        download_aws_data(ids)
        print("AWS data collected")
        print()

        print("Collecting GFS data")

        if curr >= gfs_times[0] and curr < gfs_times[1]:
            download_gfs_data(year=curr.year, month=curr.month, day=curr.day, hour=0)
        elif curr >= gfs_times[1] and curr < gfs_times[2]:
            download_gfs_data(year=curr.year, month=curr.month, day=curr.day, hour=6)
        elif curr >= gfs_times[2] and curr < gfs_times[3]:
            download_gfs_data(year=curr.year, month=curr.month, day=curr.day, hour=12)
        elif curr >= gfs_times[3] and curr < gfs_times[4]:
            download_gfs_data(year=curr.year, month=curr.month, day=curr.day, hour=18)

        print("GFS data collected")
        print()

        os.chdir("./collector/")
        os.system("zip -q -r GFS.zip GFS/")
        os.system("rm -rf GFS/")
        os.chdir("../")

        try:
            print("Sending AWS and GFS data to preprocessor node")
            print()
            
            aws_filepath = "./collector/AWS_data.xlsx"
            aws_filename = aws_filepath.split("/")[-1]
            aws_filesize = os.path.getsize(aws_filepath)
            preprocessor_sock.send(f"{aws_filename}\n{aws_filesize}".encode())
            
            time.sleep(1)

            with open(aws_filepath, "rb") as f:
                bytes_read = f.read(1024)
                while bytes_read:
                    preprocessor_sock.send(bytes_read)
                    bytes_read = f.read(1024)
            print("AWS data sent")

            time.sleep(1)

            gfs_filepath = "./collector/GFS.zip"
            gfs_filename = gfs_filepath.split("/")[-1]
            gfs_filesize = os.path.getsize(gfs_filepath)
            preprocessor_sock.send(f"{gfs_filename}\n{gfs_filesize}".encode())
            
            time.sleep(1)

            with open(gfs_filepath, "rb") as f:
                bytes_read = f.read(1024)
                while bytes_read:
                    preprocessor_sock.send(bytes_read)
                    bytes_read = f.read(1024)
            print("GFS data sent")
            
            # time.sleep(15 * 60 - 3)
            time.sleep(10)

        except KeyboardInterrupt:
            preprocessor_sock.close()
            break
