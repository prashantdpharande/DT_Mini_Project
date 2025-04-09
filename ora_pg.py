#------------------------------------------------------------------------------
# File name       : pg_ora_extract.py
# Parameters      :
#
# Description     : THIS SCRIPT LOADS DATA FROM PG TO ORACLE
#==============================================================================
# Modification History:
#
# Date 01/07/2024         Created by  Amol Mahadik
#==============================================================================


import cx_Oracle
from datetime import  datetime,timedelta
import psycopg2
import pandas as pd
import sys
import csv
import warnings
import numpy as np
import math
import subprocess
import configparser
import logging
import pandas.io.sql as psql
from pandarallel import pandarallel
import time
import numpy as np
import random
import string
import hashlib
import os
import zipfile
import shutil
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import configparser
warnings.filterwarnings('ignore')



def get_column_names(cur, table_name):

    #Retrieve all column names for the table
    cur.execute(f"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = UPPER('{tgt_table_name}') and OWNER=UPPER('{or_db}')")
    columns = [row[0] for row in cur.fetchall()]
    return columns

def get_primary_key_columns(cur, table_name):

    #Retrieve the primary key columns dynamically for the table."""
    cur.execute(f"""
        SELECT cols.column_name
        FROM all_constraints cons, all_cons_columns cols
        WHERE cons.constraint_type = 'P'
        AND cons.constraint_name = cols.constraint_name
        AND cons.table_name = UPPER('{tgt_table_name}')
        AND cons.owner = UPPER('{or_db}')
    """)
    key_columns = [row[0] for row in cur.fetchall()]
    return key_columns

def get_table_columns_and_types(cur, table_name):

    #Retrieve column names and data types from Oracle metadata for the specified table

    cur.execute(f"""
    SELECT column_name, data_type
    FROM all_tab_columns
    WHERE table_name = UPPER('{tgt_table_name}')
    """)
    columns = cur.fetchall()
    #return columns
    return {row[0]: row[1] for row in columns}


def preprocess_row(row, headers, columns_types):
    """
    Preprocess a single row of data based on column types.
    """
    processed_row = []
    for header, value in zip(headers, row):
        col_type = columns_types.get(header.upper())
        if col_type in ['DATE', 'TIMESTAMP', 'TIMESTAMP(6)']:
            processed_row.append(preprocess_timestamp(value))
        elif col_type == 'NUMBER':
            processed_row.append(int(value) if value.isdigit() else 0)  # Default to 0 for invalid numbers
        else:
            processed_row.append(value if value != '' else None)  # Handle empty strings as NULL
    return processed_row


def preprocess_timestamp(value):
    """
    Preprocess timestamp values to handle ISO, standard, and NULL values.
    Convert all formats to 'YYYY-MM-DD HH24:MI:SS'.
    """
    if value is None or value == '':
        #return '1970-01-01 00:00:00'  # Default for NULL or empty values
        return None
    try:
        # Handle ISO format
        if 'T' in value:
            return datetime.fromisoformat(value.replace('Z', '')).strftime('%Y-%m-%d %H:%M:%S')
        # Assume standard format
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        # Fallback to default if parsing fails
        return '1970-01-01 00:00:00'


def generate_upsert_query(table_name, columns, key_columns, columns_types):
    """
    Generate an upsert (MERGE) query for the given table and column details.
    """
    if isinstance(columns, str):
        columns = [col.strip().upper() for col in columns.split(',')]
    else:
        columns = [col.upper() for col in columns]

    select_clause = []
    update_clause = []

    for i, col in enumerate(columns, start=1):
        dtype = columns_types.get(col.upper())

        # Construct SELECT clause
        if dtype in ['DATE', 'TIMESTAMP', 'TIMESTAMP(6)']:
            select_clause.append(f"TO_TIMESTAMP(:{i}, 'YYYY-MM-DD HH24:MI:SS') AS {col}")
        elif dtype in ['CHAR', 'VARCHAR2']:
            select_clause.append(f"NVL(:{i}, NULL) AS {col}")
        elif dtype == 'NUMBER':
            select_clause.append(f"TO_NUMBER(NVL(:{i}, '0')) AS {col}")
        else:
            select_clause.append(f"NVL(:{i}, NULL) AS {col}")

        # Construct UPDATE clause
        if col not in key_columns:
            update_clause.append(f"{col} = src.{col}")

    insert_columns_str = ", ".join(columns)
    select_clause_str = ", ".join(select_clause)
    update_clause_str = ", ".join(update_clause)
    on_clause_str = " AND ".join([f"tgt.{col} = src.{col}" for col in key_columns])

    # Construct the MERGE statement
    merge_statement = f"""
    MERGE INTO {table_name} tgt
    USING (
        SELECT {select_clause_str}
        FROM dual
    ) src
    ON ({on_clause_str})
    WHEN MATCHED THEN
        UPDATE SET {update_clause_str}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns_str})
        VALUES ({', '.join([f'src.{col}' for col in columns])})
    """

    return merge_statement


def send_email(message, load_id, tgt_table_name, logfile, recipients):
    # Email Configuration
    emailp=config['email']['email_sec']
    cmd = f"sh {path}/bin/main.env {emailp}"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pwd = result.stdout.strip()
    emailpwd=pwd.decode('utf-8')

    host=config['email']['e_host']
    smtp_host=config['email']['e_smtp_host']
    smtp_server=config['email']['e_smtp_server']
    smtp_port=config['email']['e_smtp_port']
    sender_email=config['email']['e_smtp_username']
    smtp_password=emailpwd
    recipients = config['email']['recp']
    env=config['email']['env_name']
    if isinstance(recipients, str):
        recipients = [email.strip() for email in recipients.split(',')]


    current_date = datetime.now().strftime('%d-%b-%Y')

    # Create message
    msg = MIMEMultipart()
    msg['Subject'] = f"{env} | Error | {current_date} | {src_table_name} "
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipients)

    body = f"""/mnt/ref-prod/refdb/bin/pg_ora_data_extract.py has Failed with below with Run ID {load_id} :-

+++++++++++++++++++++++++++++++++++++++++++++
Error Text : {message}

+++++++++++++++++++++++++++++++++++++++++++++

Detailed job logs are present at : {logfile}
+++++++++++++++++++++++++++++++++++++++++++++"""
    msg.attach(MIMEText(body, 'plain'))

    try:
        # Connect to SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, smtp_password)

        # Send email
        server.sendmail(sender_email, recipients, msg.as_string())
        print("Email notification sent successfully.")
    except Exception as e:
        print("Error sending email notification:", str(e))
    finally:
        # Close connection
        server.quit()



def error_handling():
    postgre_conn = psycopg2.connect(database=pg_db,host=pg_host,user=pg_usn,password=pg_pwd,port=pg_port)
    postgre_conn.autocommit = True
    cursor=postgre_conn.cursor()
    fail_audit=f"UPDATE {pg_sch}.LAST_LOAD_STATUS SET STATUS='FAILED',end_time=start_time  WHERE TABLE_NAME ='{src_table_name}'"
    cursor.execute(fail_audit)

def extract_query():
    try:
        pg_conn=psycopg2.connect(database=pg_db, user=pg_usn, password=pg_pwd, host=pg_host, port=pg_port)
        pg_cur = pg_conn.cursor()
        logger.info(f"PG CONNECTION ESTABLISHED SUCCESSFULLY FOR DATA EXTRACTION")
        query =f"SELECT * FROM  {pg_sch}.{src_table_name} {load_condition} ;"
        logger.info(f"EXTRACTION QUERY : {query}")
        df=pd.read_sql(query,pg_conn)
        pg_status=f"INSERT INTO {pg_sch}.load_status_ora  (load_id,load_date,start_time,end_time,status,table_name,row_count,execution_time) VALUES ('{unique_id}',current_date,current_timestamp,null,'RUNNING','{tgt_table_name}',null,null);"
        cursor.execute(pg_status)
        logger.info(f"check table {pg_sch}.LOAD_STATUS_ORA for status")
        df.to_csv(datap + "/" + src_table_name + timestamp+".csv",index=False)
        logger.info(f"DATA EXTRACTED SUCCESSFULLY")
        count_q=f"select count(*) from {pg_sch}.{src_table_name} {load_condition} ;"
        pg_cur.execute(count_q)
        row_c=pg_cur.fetchone()[0]
        logger.info(f"ROWS TO INSERT: {row_c}")
        pg_conn.close()
    except Exception as e:
        logger.info(f"An unexpected error occurred: {e}")
        failq=f"UPDATE {pg_sch}.load_status_ora set status ='FAILED', end_time =current_timestamp,execution_time=(current_timestamp-{start_exec})  where table_name ='{tgt_table_name}'   and status='RUNNING';"
        cursor.execute(failq)
        pg_conn.close()

        #ORACLE CONNECTIOION
    try:

        ip ,port,SID = or_host,or_port,or_sid
        dsn_tns = cx_Oracle.makedsn(ip, port, SID)
        oracle_conn = cx_Oracle.connect(or_usn, or_pwd, dsn_tns)
        insert=f"{datap}/{src_table_name}{timestamp}.csv"
        cur = oracle_conn.cursor()
        with open(insert, 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Get the header row

            # Retrieve column details
            columns = get_column_names(cur, tgt_table_name)
            columns_types = get_table_columns_and_types(cur, tgt_table_name)
            key_columns = get_primary_key_columns(cur, tgt_table_name)

            # Generate upsert query
            sql = generate_upsert_query(tgt_table_name,columns,key_columns,columns_types)
            #sql = generate_upsert_query(tgt_table_name, columns, key_columns, columns_types)
            #logger.info(f"INSERT QUERY: {sql}")

            batch_size = 10000
            batch_data = []

            for row in reader:
                processed_row = preprocess_row(row, headers, columns_types)
                batch_data.append(processed_row)
                if len(batch_data) == batch_size:
                    try:
                        cur.executemany(sql, batch_data)
                        oracle_conn.commit()
                    except Exception as e:
                        print(f"Error occurred during batch insert: {e}")
                        raise
                    finally:
                        batch_data.clear()
            if batch_data:
                try:
                    cur.executemany(sql, batch_data)
                    oracle_conn.commit()
                except Exception as e:
                    print(f"Error occurred during final batch insert: {e}")
                    raise
                finally:
                    batch_data.clear()




    except Exception as e:
        # Handle any other exceptions
        logger.info(f"AN UNEXPECTED ERROR OCCURED: {e}")
        error_handling()
        send_email(f"FOLLOWING ERROR HAS BEEN OCCURED :\n \n \n {e}",unique_id,tgt_table_name,logfile,recipients)
        sys.exit()

    else:
        # Code that runs if no exceptions occur
        logger.info(f"DATA INSERTED SUCCESSFULLY IN ORACLE STAGE TABLE.")
        current_timestamp=datetime.now()
        tot_exe_time=current_timestamp-start_exec
        update_load=f"UPDATE {pg_sch}.load_status_ora set status ='COMPLETE', end_time =current_timestamp,row_count={row_c},execution_time='{tot_exe_time}'  where table_name ='{tgt_table_name}'   and status='RUNNING' "
        cursor.execute(update_load)
        update_status=f"UPDATE {pg_sch}.LAST_LOAD_STATUS SET status='COMPLETE' where table_name ='{src_table_name}'"
        cursor.execute(update_status)
        pg_q=f"select count(*) from {pg_sch}.{src_table_name}"
        cursor.execute(pg_q)
        pg_rc=cursor.fetchone()[0]
        ora_q=f"select count(*) from {or_db}.{tgt_table_name}"
        cur.execute(ora_q)
        ora_rc=cur.fetchone()[0]
        diff=pg_rc-ora_rc
        diff_per=diff/100
        table_compare=f"insert into {pg_sch}.table_compare values('{src_table_name}','{ora_rc}','{pg_rc}','{diff}',current_date,291291,'{diff_per}',current_timestamp,'{unique_id}')"
        cursor.execute(table_compare)
        logger.info(f"DATA COMPARED SUCCESSFULLY AND LOGGED IN TABLE_COMPARE")
        logger.info(f"AUDIT TABLES UPDATED SUCCESSFULLY")
        logger.info(f"NUMBER OF ROWS LOADED IN STAGE TABLE FROM CURRENT DELTA: {row_c}")
        logger.info(f"TOTAL NUMBER OF ROWS IN STAGE TABLE: {ora_rc}")



        # Commit the transaction
        oracle_conn.commit()

        # Close the cursor and connection
        cur.close()
        oracle_conn.close()

    return 0






#CONNECTION TO PG

if __name__ == "__main__":

    #Read file path
    script_path = os.path.abspath(__file__)
    paths = script_path.rsplit('/', 2)
    path=paths[0]

    # ARGUMENTS TO BE PASSED
    src_table_name=sys.argv[1]
    tgt_table_name=sys.argv[2]
    tgt_table_name=tgt_table_name.upper()
    db=sys.argv[3]
    load_type=sys.argv[4]
    sp_name=sys.argv[5]

    # PARALLELISATION
    pandarallel.initialize(progress_bar=False)

    datap=f"{path}/data"
    env  =f"{path}/env/"
    log  =f"{path}/log"

    # Read INI file
    config = configparser.ConfigParser()
    config.read(f"{path}/env/db_cred_prd.ini")


    # read oracle credentials
    or_host=config[db]['or_host']
    or_sid =config[db]['or_sid']
    or_port=config[db]['or_port']
    or_usn =config[db]['or_usn']
    or_db=config[db]['or_db']
    recipients = config['email']['recp']


        # read timezone
    or_timezone=config['TIMEZONE']['hours']

    # EXTRACT ORACLE PASSWORD

    ora_secret_name=config[db]['or_sec']
    cmd = f"sh {path}/bin/main.env {ora_secret_name}"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode == 0:
        pwd = result.stdout.strip()
        or_pwd=pwd.decode('utf-8')

    else:
        logger.info(f"Failed to execute the script. Error message: {result.stderr}")
        sys.exit()

    ## read PG  credentials
    pg_host = config['PG']['pg_host']
    pg_usn  = config['PG']['pg_usn']
    pg_db   = config['PG']['pg_db']
    pg_port = config['PG']['pg_port']
    pg_sch =  config['PG']['pg_sch']

    # EXTRACT PG PASSWORD
    pg_secret_name=config['PG']['pg_sec']
    cmd = f"sh {path}/bin/main.env {pg_secret_name}"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if result.returncode == 0:
        pwd = result.stdout.strip()
        pg_pwd=pwd.decode('utf-8')

    else:
        logger.info(f"Failed to execute the script. Error message: {result.stderr}")
        sys.exit()

        #creating unique identifier
    hashed_name = hashlib.sha256(tgt_table_name.encode()).hexdigest()
    load_id = int(hashed_name[:5], 16) % 10000

    #LOG FILE
    now = datetime.now()
    unique_id = str(now.strftime("%m%d%Y%H%M%S"))+"_"+str(load_id)
    timestamp = "_EXTRACT_"+unique_id

    logfile = f"{log}/{src_table_name}_{unique_id}.log"
    # Create a logger object
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Create a file handler to store logs in a file
    file_handler = logging.FileHandler(logfile)
    file_handler.setLevel(logging.DEBUG)

    # Create a console handler to print logs to the console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)

    # Create a formatter to add timestamp to the logs
    formatter = logging.Formatter('%(asctime)s -  %(levelname)s - %(message)s')

    # Add the formatter to the handlers
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    if load_type=='N':
        print('THIS IS NRT')
        start_exec=datetime.now()
        try:
            postgre_conn = psycopg2.connect(database=pg_db,host=pg_host,user=pg_usn,password=pg_pwd,port=pg_port)
        except Exception as e:
            logger.info(f"CONNECTION ERROR OCCURED: {e}", send_email(f"FOLLOWING ERROR HAS BEEN OCCURED :\n \n \n {e}",unique_id,tgt_table_name,logfile,recipients))
            sys.exit()

        postgre_conn.autocommit = True
        cursor=postgre_conn.cursor()
        status_query=(f"SELECT count(*) from {pg_sch}.last_load_status where table_name='{src_table_name}'")
        cursor.execute(status_query)
        row=cursor.fetchone()
        load_status_count=row[0]
        logger.info(f"LOAD STATUS COUNT: {load_status_count}")
        if load_status_count==0:
            logger.info(f"THIS IS INITIAL LOAD")
            load_condition=f"where 1=1"
            logger.info(f"THE CONDITION USED : {load_condition}")
            s_time='1990-02-17 10:14:59'
            current_timestamp=datetime.now()
            new_time_end=current_timestamp-timedelta(hours=int(or_timezone))
            e_time=new_time_end.strftime('%Y-%m-%d %H:%M:%S')
            ins_status=f"INSERT INTO {pg_sch}.last_load_status values('{unique_id}','{s_time}','{e_time}','RUNNING','{src_table_name}')"
            cursor.execute(ins_status)
            result=extract_query()
            if result !=0:
                del_q=f("delete from {pg_sch}.LAST_LOAD_STATUS WHERE TABLE_NAME='{src_table_name}'")
                cursor.execute(del_q)



        else:
            logger.info(f"THIS IS DELTA")
            status_query=(f"SELECT * FROM {pg_sch}.last_load_status where table_name='{src_table_name}'")
            cursor.execute(status_query)
            row=cursor.fetchone()
            start_time=row[2]
            current_timestamp=datetime.now()
            new_time_end=current_timestamp-timedelta(hours=int(or_timezone))
            e_time=new_time_end.strftime('%Y-%m-%d %H:%M:%S')
            ins_status=f"UPDATE {pg_sch}.last_load_status SET LOAD_ID='{unique_id}',START_TIME='{start_time}',END_TIME='{e_time}',STATUS='RUNNING' WHERE TABLE_NAME='{src_table_name}'"
            cursor.execute(ins_status)
            logger.info(f"PREVIOUS PG LOAD DATE:{start_time}")
            load_condition=f"WHERE PG_LOAD_DT >='{start_time}' OR PG_MODIFY_DT>='{start_time}'"
            logger.info(f"THE CONDITION USED : {load_condition}")
            result=extract_query()
            if result !=0:
                up_q=f("update {pg_sch}.LAST_LOAD_STATUS SET END_TIME='{start_time}',STATUS='FAILED' where TABLE_NAME='{src_table_name}'")
                cursor.execute(up_q)
        postgre_conn.close()
