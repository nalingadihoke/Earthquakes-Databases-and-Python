import requests
import pandas as pd
import json

def api_call(sd, ed):
    #url
    url = ('https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson' + '&starttime=' + sd + '&endtime='+ ed)

    #extract data
    req = requests.get(url)
    eq_data = req.json()

    #record count
    rec_cnt = eq_data['metadata']['count']

    #create df
    eq_data = eq_data['features']
    eq_list = []  #initializing list of dictionaries
    for x in (list(range(0, rec_cnt))):
        #getting event properties in a dictionary format
        eq_dict = {'id': eq_data[x]['id'],
                  'time': eq_data[x]['properties']['time'],
                  'updated': eq_data[x]['properties']['updated'],
                  'title': eq_data[x]['properties']['title'],
                  'mag': eq_data[x]['properties']['mag'],
                  'magType': eq_data[x]['properties']['magType'],
                  'lon': eq_data[x]['geometry']['coordinates'][0],
                  'lat': eq_data[x]['geometry']['coordinates'][1],
                  'depth': eq_data[x]['geometry']['coordinates'][2],
                  'sources': eq_data[x]['properties']['sources'],
                   'url': eq_data[x]['properties']['url'],
                   'tsunami': eq_data[x]['properties']['tsunami']}
        eq_list.append(eq_dict)
    #save data into dataframe
    df = pd.DataFrame(eq_list)

    #data cleaning

    #update time from unix timestamps to datetime
    df['date_time'] = pd.to_datetime(pd.to_datetime(df.time, unit='ms',origin='unix').apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S")))
    df['updated_datetime'] = pd.to_datetime(pd.to_datetime(df.updated, unit='ms',origin='unix').apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S")))

    #magtype normalizing
    df.magType = df.magType.str.upper()

    #convert mag from str to numeric
    df.mag = pd.to_numeric(df.mag)

    #removing unnecessary string columns
    df.drop(columns = ['time','updated'], inplace = True)
    return df