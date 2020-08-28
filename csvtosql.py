import numpy as np
import mysql.connector
from tqdm import tqdm
import pandas as pd
from dateutil.parser import parse
import sys
import re 
from tabulate import tabulate
import argparse


replace_aps_from_string = lambda x: str(x).replace("'",'"').replace('(', '[').replace(')', ']')

   

def data_clean(csv,date_col,time_col, value_encoder_template, string = ''):
    for i in range(len(csv)):
        string += '(%s), '
    csv = csv.fillna('NULL')
    for i in time_col:
        csv[i] = csv[i].apply(convert_time_to_sql_format)
    for i in date_col:
        csv[i] = csv[i].apply(convert_date_to_sql_format)
    for i in csv.columns:
        csv[i]  = csv[i].apply(replace_aps_from_string)
    values = tuple(csv.T.apply(lambda x:value_encoder_template%tuple(x.values)))    
    return string%values
    
def convert_date_to_sql_format(str_stamp):
    if str_stamp:
        return parse(str_stamp).strftime('%Y-%m-%d')

def convert_time_to_sql_format(str_stamp):
    if str_stamp:
        return parse(str_stamp).strftime('%H:%M:%S')
    

def is_date(string, fuzzy=False):
    try: 
        date = parse(string, fuzzy=fuzzy)
        if date.hour == 0 and date.minute == 0:
            return 'DATE'
        return 'TIME'

    except ValueError:
        return 'NONE'


def finding_datatype(sample):
    if 'int' in str(type(sample)).lower() or 'float' in str(type(sample)).lower():
        return 'FLOAT', '%s'
    if re.compile("[a-z]").match(sample.lower()):
        return 'varchar(1000)',"'%s'"
    result = is_date(sample)
    if result != 'NONE':
        return result, "'%s'"
    
    else:
        return 'varchar(1000)',"'%s'"

    
def drop_table(TABLE_NAME, mycursor):
    try:
        mycursor.execute(f'DROP TABLE {TABLE_NAME}')
    except:
        print(f"No Table found with name {TABLE_NAME}")
        pass
def creating_table( FILE_PATH, TABLE_NAME, execute):
    df = pd.read_csv(FILE_PATH, iterator=True, chunksize=100)
    datatype_list, datatype_string_for_table_creation, col_name_string, value_encoder_template, date_col, time_col = [],'', '', '', [], []
    for csv in df:    
        # print('Printing sample values from every coloumn with their datatype\n')
        for col_name in csv:
            datatype, value_encoder = finding_datatype(csv[col_name].dropna().sample(1).values[0])
            # print(col_name , csv[col_name].dropna().sample(1).values[0],datatype, sep = ' -> ')
            col_name = col_name.replace(' ', '_').replace(':', "_").replace('.','_')
            datatype_string_for_table_creation += f"{col_name} {datatype}" + ','
            col_name_string += '{}, '.format(col_name)
            value_encoder_template += f'{value_encoder}, '
            if datatype == 'DATE':
                date_col.append(col_name)
            if datatype == 'TIME':
                time_col.append(col_name)
            datatype_list.append([col_name, datatype])
        break
            
    command_for_table_creation = f'CREATE TABLE {TABLE_NAME} ({datatype_string_for_table_creation[:-1]});'
    tableAdditionPrefix = "INSERT INTO {} ({}) VALUES ".format(TABLE_NAME, col_name_string[:-2])
    #print("\nPrinting Command for Table creation", command_for_table_creation, sep = '\n')
    print(tabulate(datatype_list, headers=['Name', 'DataType']))
    execute(command_for_table_creation)
    return tableAdditionPrefix,date_col, time_col, value_encoder_template[:-2]


def csv_to_sql(

        TABLE_NAME='Order_report',
        FILE_PATH=r'C:\Users\Avish\Desktop\Order Report.xlsx',
        CH=10000,
        HOST='localhost',
        USER='root',
        PASS='root',
        DATABASE='analytics'):
    
    
    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASS,
        database = DATABASE
    )
    def Insertion(TableInsertion, counter, csv):
        try:
            execute(TableInsertion)
            mydb.commit()
            counter+=len(csv)
        except Exception as e:
            print(e,'Error occured while running command ', mycursor.statement)
            raise
        return counter
        
    counter = 0
    mycursor = mydb.cursor()
    drop_table(TABLE_NAME, mycursor)
    execute = lambda cmd: mycursor.execute(cmd)
    chunk = pd.read_csv(FILE_PATH, iterator=True, chunksize=CH)
    TableAdditionCmd, dcol, tcol, v_e_t = creating_table( FILE_PATH, TABLE_NAME, execute)
    

    for csv in tqdm(chunk):
        value_list = data_clean(csv,dcol,tcol,v_e_t)
        TableInsertion = TableAdditionCmd + value_list[:-2]+';'
        counter = Insertion(TableInsertion, counter, csv)

    print('Number of Rows iterrated {}'.format(counter))
if __name__ == '__main__':
   parser = argparse.ArgumentParser()
   parser.add_argument("CSV", help="Path to csv")
   parser.add_argument('-H', "--HOST", help="Host of SQL", default='localhost',)
   parser.add_argument('-u', "--USER", help="USER  SQL", default='root')
   parser.add_argument('-p', "--PASS", help="PASS of SQL", default='root')
   parser.add_argument('-d', "--DATABASE", help="DATABASE in SQL", default='mydatabase')
   parser.add_argument('-t', "--TABLE_NAME", help="TABLE_NAME in SQL", default='My_table')
   args = parser.parse_args()
   csv_to_sql(HOST=args.HOST,USER=args.USER,PASS=args.PASS,DATABASE=args.DATABASE,TABLE_NAME=args.TABLE_NAME,FILE_PATH=args.CSV,CH=10000)
