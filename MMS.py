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

my_df = pd.read_excel('/Users/dreyco676/Desktop/SDS/MMS/MMS2012.xlsx', header=2, sheetname='1. Results')
my_df = fix_col_nm(my_df)
my_df.to_sql('MMS2012_Results', engine, schema='data4good')

my_df = pd.read_excel('/Users/dreyco676/Desktop/SDS/MMS/MMS2013.xlsx', header=2, sheetname='1. Results')
my_df = fix_col_nm(my_df)
my_df.to_sql('MMS2013_Results', engine, schema='data4good')

my_df = pd.read_excel('/Users/dreyco676/Desktop/SDS/MMS/MMS2014.xlsx', header=2, sheetname='1. Results')
my_df = fix_col_nm(my_df)
my_df.to_sql('MMS2014_Results', engine, schema='data4good')

my_df = pd.read_excel('/Users/dreyco676/Desktop/SDS/MMS/MMS2015.xlsx', header=2, sheetname='1. Results')
my_df = fix_col_nm(my_df)
my_df.to_sql('MMS2015_Results', engine, schema='data4good')



my_df = pd.read_excel('/Users/dreyco676/Desktop/SDS/MMS/MMS2012.xlsx', header=2, sheetname='2. Summary')
my_df = fix_col_nm(my_df)
my_df.to_sql('MMS2012_Summary', engine, schema='data4good')

my_df = pd.read_excel('/Users/dreyco676/Desktop/SDS/MMS/MMS2013.xlsx', header=2, sheetname='2. Summary')
my_df = fix_col_nm(my_df)
my_df.to_sql('MMS2013_Summary', engine, schema='data4good')

my_df = pd.read_excel('/Users/dreyco676/Desktop/SDS/MMS/MMS2014.xlsx', header=2, sheetname='2. Summary')
my_df = fix_col_nm(my_df)
my_df.to_sql('MMS2014_Summary', engine, schema='data4good')

my_df = pd.read_excel('/Users/dreyco676/Desktop/SDS/MMS/MMS2015.xlsx', header=2, sheetname='2. Summary')
my_df = fix_col_nm(my_df)
my_df.to_sql('MMS2015_Summary', engine, schema='data4good')