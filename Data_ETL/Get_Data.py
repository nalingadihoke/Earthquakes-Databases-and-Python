from Data_ETL.api_call import api_call
import pandas as pd

# Get data incrementally since a non-incremental request results in a 400 error
def get_2017_data():
    #date ranges
    sd = ['2017-01-01', '2017-02-01', '2017-03-01', '2017-04-01', '2017-05-01', '2017-06-01', '2017-07-01', '2017-08-01', '2017-09-01', '2017-10-01', '2017-11-01', '2017-12-01']
    ed = ['2017-01-31', '2017-02-28', '2017-03-31', '2017-04-30', '2017-05-31', '2017-06-30', '2017-07-31', '2017-08-31', '2017-09-30', '2017-10-31', '2017-11-30', '2017-12-31']

    df = api_call(sd[0], ed[0])

    count = 0
    for (i, j) in zip(sd, ed):
        count += 1
        print('-------------------------------------')
        print('Fetching data for month '+str(count))
        if count > 1:
            df2 = api_call(i, j)
            df = pd.concat([df,df2])
            print("total number of records: " + str(len(df.index)))

    #df = df.mask(df.eq('None')).dropna()
    quake_location = df[['id', 'title', 'lon', 'lat', 'depth', 'sources']].copy(deep=True)
    quake_location['title_ID'] = pd.factorize(quake_location['title'])[0]
    quake_location['sources_ID'] = pd.factorize(quake_location['sources'])[0]
    quake_title = quake_location[["title", "title_ID"]].drop_duplicates()
    quake_sources = quake_location[["sources", "sources_ID"]].drop_duplicates()

    quake_magnitude = df[['id', 'mag', 'magType', 'tsunami']].copy(deep=True)
    quake_magnitude['magType_ID'] = pd.factorize(quake_magnitude['magType'])[0]
    quake_magType = quake_magnitude[["magType", "magType_ID"]].drop_duplicates()

    quake_time = df[['id', 'date_time', 'updated_datetime']].copy(deep=True)

    return quake_location, quake_title, quake_sources, quake_magnitude, quake_magType, quake_time

'''
test = get_2017_data()
# Adding data to csvs as a back up data source
test[0].to_csv("/Users/nalingadihoke/Desktop/Github_Nalin/Tesla-Technical_Assesment/Data/quake_location.csv")
test[1].to_csv("/Users/nalingadihoke/Desktop/Github_Nalin/Tesla-Technical_Assesment/Data/quake_title.csv")
test[2].to_csv("/Users/nalingadihoke/Desktop/Github_Nalin/Tesla-Technical_Assesment/Data/quake_sources.csv")
test[3].to_csv("/Users/nalingadihoke/Desktop/Github_Nalin/Tesla-Technical_Assesment/Data/quake_magnitude.csv")
test[4].to_csv("/Users/nalingadihoke/Desktop/Github_Nalin/Tesla-Technical_Assesment/Data/quake_magType.csv")
test[5].to_csv("/Users/nalingadihoke/Desktop/Github_Nalin/Tesla-Technical_Assesment/Data/quake_time.csv")
'''
