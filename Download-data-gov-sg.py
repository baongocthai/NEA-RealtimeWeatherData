# =============================================================================
# This script is used to download data from specified periods (start_date, end_date) and specified parameter 
# from data.gov.sg, for all stations
# =============================================================================
import requests
import datetime as dt
import pandas as pd
import os

#Choose directory
directory = r'S:\01_PROJECTS\H2I-C2020-016_PUB-OMS\5_WorkingDocuments\PartC\LowerSeletar_EM_WAQ_D3D_D3DWAQ\DataDownload'
os.chdir(directory)

#Choose period of interest
start_date = dt.datetime(2019, 1, 1, 0, 0, 0)
end_date = dt.datetime(2020, 1, 1, 0, 0, 0)

#Choose parameter of interest & change output file name
parameter = 'wind-direction' #'air-temperature', 'rainfall', 'relative-humidity', 'wind-direction'
output_file = 'WindDirection_2019-2020.csv'

total_days = (end_date - start_date).days + 1
neadatasum = []

#Download data
for day_number in range(total_days):
    for day_time in range(0, 1440, 1):
        current_date = (start_date + dt.timedelta(days = day_number)).date()
        current_time = (start_date + dt.timedelta(hours = day_time)).time()
        current_time = (start_date + dt.timedelta(minutes = day_time)).time()
        url = 'https://api.data.gov.sg/v1/environment/'+ parameter +'?date_time=' + str(current_date) + 'T' + \
              str(current_time)
        headers = {"api-key": "ViSZINAmeMJNZgKlk66CwIl3lyrHgsu8"}
        data = requests.get(url, headers=headers).json()
        #Get values at 1 time stamp
        new_value = []
        new_value_station = []
        for i in range(len(data['items'][0]['readings'])):
            new_value.append(data['items'][0]['readings'][i]['value'])
            new_value_station.append(data['items'][0]['readings'][i]['station_id'])
            values = pd.DataFrame(list(zip(new_value_station, new_value)))
        values = values.transpose()
        #Get time stamp of data
        actualtime = []
        actualtime.append(str(current_date) + 'T' + str(current_time) + '+08:00')
        neadata_temp = [actualtime, values]
        neadatasum.append(neadata_temp)
        print(actualtime)

# Extract timestamp
timestamp = []
for i in range(len(neadatasum)):
    timestamp.append(neadatasum[i][0][0])
    
# Extract data
df = []
for i in range(len(neadatasum)):
    df_temp = neadatasum[i][1]
    df_temp.columns = df_temp.iloc[0]
    df_temp = df_temp[1:2]
    df.append(df_temp)

# Merge time to data
df_new = pd.concat(df, axis=0, ignore_index=False)
df_new.index = timestamp
df_new.index = pd.to_datetime(df_new.index)
df_new.to_csv(output_file)
