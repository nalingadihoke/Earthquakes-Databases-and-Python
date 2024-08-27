from Data_Analysis.Read_Data_Frm_DB import Get_Tables
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import sys

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)

DATABASE = ''
HOSTNAME = ''
USERNAME = ''
PASSWORD = ''


def main():
    dfs = Get_Tables(HOSTNAME, USERNAME, PASSWORD, DATABASE)

    location, magnitude, time = dfs[0], dfs[1], dfs[2]

    master_df = location.merge(magnitude, on='id').merge(time, on='id')

    # Largest Earthquake of 2017

    largest_quake_row = master_df.iloc[master_df['mag'].idxmax()]
    # print(largest_quake_row)

    master_df['datetime_dt'] = pd.to_datetime(master_df['datetime'])
    master_df['datehour'] = master_df['datetime_dt'].dt.hour
    df2 = master_df[["id", "mag", "datetime_dt", "datehour"]].copy(deep=True)

    bins = [0, 0.99, 1.99, 2.99, 3.99, 4.99, 5.99, 100]
    category = ['0-1', '1-2', '2-3', '3-4', '4-5', '5-6', '>6']

    df2['mag_binned'] = pd.cut(df2['mag'], bins, include_lowest=True, labels=category)

    # print(df2.head(100))

    for cat in category:
        hour_mode = int(df2[df2['mag_binned'] == cat]['datehour'].mode())
        hour_mode_counts = max(list(df2[df2['mag_binned'] == cat]['datehour'].value_counts()))
        print(
            'For magnitude category {} the most probable hour of the day for an earthquake is {} with {} recorded events.'.format(
                cat, hour_mode, hour_mode_counts))


if __name__ == '__main__':
    # if len(sys.argv) < 4:
    # print("Usage: python Main_DB_Creation.py <DB name> <hostname> <username> <password>")
    # exit(1)
    # See expected input file format in README
    DATABASE = sys.argv[1]
    HOSTNAME = sys.argv[2]
    USERNAME = sys.argv[3]
    PASSWORD = sys.argv[4]
    main()
