import pandas as pd
import sqlalchemy as sql
import secrets
import glob
from os.path import basename
import re
import shutil

def fix_col_nm(df):
    col_nm_list = list(df.columns.values)
    for c_index, col in enumerate(col_nm_list):
        # remove spaces in header
        new_col = re.sub(r'\s+', '', col)

        # find first paren and truncate
        paren_loc = new_col.find('(')
        if paren_loc != -1:
            new_col = new_col[:paren_loc]

        # truncate columns to only 64 characters for mysql db
        new_col = new_col[:64]

        rename_dict = {col: new_col}
        df = df.rename(columns=rename_dict)
    return df


password = secrets.db_pass()
engine = sql.create_engine('mysql+pymysql://loader:'+password+'@gmidata4good.cloudapp.net:3306/data4good')


def load_csv(folder):
    in_dir = '/Users/dreyco676/Desktop/SDS/' + folder + '/'
    for filename in glob.glob(in_dir + '*.csv'):
        print(filename)
        short_nm = basename(filename).replace('-', '').replace('.csv', '')
        short_nm = re.sub(r'\s+', '', short_nm)
        # shorten file name
        sheet_loc = short_nm.find('Sheet')
        if sheet_loc != -1:
            short_nm = short_nm[:sheet_loc]
        table_name = short_nm[:64].lower()

        try:
            csv_df = pd.read_csv(filename)
        except:
            csv_df = pd.read_csv(filename, header=3)
        csv_df = fix_col_nm(csv_df)

        csv_df.to_sql(table_name, engine, schema='data4good', if_exists='replace')
        # move file to completed dir
        shutil.move(filename, './complete/')
    return 0


enroll_data = load_csv('staff')
