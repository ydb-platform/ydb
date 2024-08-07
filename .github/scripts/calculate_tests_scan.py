#!/usr/bin/env python3

import configparser
import os
import ydb
import traceback
from collections import Counter
import posixpath




dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../config/ydb_qa_db.ini"
config.read(config_file_path)

build_preset = os.environ.get("build_preset")
branch = os.environ.get("branch_to_compare")

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]





def create_tables(pool,  table_path):
    def callee(session):
        # Creating Series table
        session.execute_scheme(f"""
            CREATE table `{table_path}` (
                `test_name` Utf8 NOT NULL,
                `suite_folder` Utf8 NOT NULL,
                `full_name` Utf8 NOT NULL,
                `date_window` Date NOT NULL,
                `history` String,
                `history_class` String,
                `pass_count` Uint64,
                `mute_count` Uint64,
                `fail_count` Uint64,
                `skip_count` Uint64,
                PRIMARY KEY (`test_name`, `suite_folder`, `full_name`,date_window)
            )
                PARTITION BY HASH(`full_name`)
                WITH (STORE = COLUMN)
            """)

    return pool.retry_operation_sync(callee)


def bulk_upsert(table_client, table_path,rows):
    print("\n> bulk upsert: episodes")
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("date_window", ydb.OptionalType(ydb.PrimitiveType.Date))
        .add_column("history", ydb.OptionalType(ydb.PrimitiveType.String))
        .add_column("history_class", ydb.OptionalType(ydb.PrimitiveType.String))
        .add_column("pass_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("mute_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("fail_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("skip_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
)
    #rows = basic_example_data.get_episodes_data_for_bulk_upsert()
    table_client.bulk_upsert(table_path, rows, column_types)

   
def main():
    print(1)
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print(
            "Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping"
        )
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]="/home/kirrysin/fork/ydb/.github/scripts/my-robot-key.json"
        #return 0
    else:
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

 
    

    page_size = 1000
    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        #result = get_records_with_pagination(session,200)
       

        last_key = '1'
        
        # Создание запроса SELECT с фильтрацией по ключам
        query1=f"""

select full_name,date_base,history_list,dist_hist,suite_folder,test_name  
  from (
    select full_name,
    date_base,
    AGG_LIST(status) as history_list ,
    String::JoinFromList( AGG_LIST_DISTINCT(status) ,',') as dist_hist,
    suite_folder,
    test_name
   
    
    from (
        select * from (
            select * from (select 
                distinct suite_folder || test_name as full_name,suite_folder,test_name

                from  `test_results/test_runs_results`
                where status in ('failure','mute')
                and job_name in ('Nightly-run', 'Postcommit_relwithdebinfo')
                and build_type = 'relwithdebinfo'
            ) as a 
            cross join
                (select  DISTINCT 
                  DateTime::MakeDate(run_timestamp) as date_base
                from  `test_results/test_runs_results`
                where status in ('failure','mute')
                and job_name in ('Nightly-run', 'Postcommit_relwithdebinfo')
                and build_type = 'relwithdebinfo' 
                
                ) as b
            ) as name
        left JOIN (
            select * from (
                select 
                suite_folder || test_name as full_name,
                run_timestamp,status,
                ROW_NUMBER() OVER (PARTITION BY test_name ORDER BY run_timestamp DESC) AS rn
                from  `test_results/test_runs_results`
                where  
                --run_timestamp >= run_timestamp -5*Interval("P1D") and
                
                 job_name in ('Nightly-run', 'Postcommit_relwithdebinfo')
                and build_type = 'relwithdebinfo'
            ) where rn<=20
        ) as hist
        ON name.full_name=hist.full_name 
        where  
        hist.run_timestamp>= name.date_base -5*Interval("P1D") AND 
        hist.run_timestamp<= name.date_base 

    )
 GROUP BY full_name,suite_folder,test_name,date_base
 
)
where dist_hist not in ('mute','failure','skipped','passed')

        """
        query = ydb.ScanQuery(query1, {})

        it = driver.table_client.scan_query(query)
        results=[]
        prepared_for_update_rows=[]
        while True:
            try:
                result = next(it)
                results= results + result.result_set.rows
            except StopIteration:
                break
        for row in results:
            row['count']=dict(zip(list(row['history_list']),[list(row['history_list']).count(i) for i in list(row['history_list'])]))
            prepared_for_update_rows.append({
                'suite_folder':row['suite_folder'],
                'test_name':row['test_name'],
                'full_name':row['full_name'],
                'date_window':row['date_base'],
                'history':','.join(row['history_list']).encode('utf8'),
                'history_class': row['dist_hist'],
                'pass_count': row['count'].get('passed',0),
                'mute_count':row['count'].get('mute',0),
                'fail_count':row['count'].get('failure',0),
                'skip_count':row['count'].get('skipped',0),

            }
            )
        with ydb.SessionPool(driver) as pool:
            table_path='test_results/test_runs_history'
            table_name='test_runs_history'
            
            create_tables(pool, table_path)
            full_path = posixpath.join(DATABASE_PATH, table_path)
            bulk_upsert(driver.table_client, full_path,prepared_for_update_rows)
        

        print('done')    
   
   


if __name__ == "__main__":
    main()

