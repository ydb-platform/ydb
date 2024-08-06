#!/usr/bin/env python3

import configparser
import os
import ydb
import traceback


dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../config/ydb_qa_db.ini"
config.read(config_file_path)

build_preset = os.environ.get("build_preset")
branch = os.environ.get("branch_to_compare")

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]



   
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

    records=[]
    page_n=0
    count = 4
    while count > 0:
        print('123')
        count -= 1

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
        while True:
            query = f"""
            
            DECLARE $page_size AS Int32;
            DECLARE $last_key AS Utf8;

            $test = (
            SELECT  suite_folder, test_name,test_id
            FROM `test_results/test_runs_results`  
            where test_id > $last_key and 
                (job_name ='Nightly-run' or job_name = 'Postcommit_relwithdebinfo')  and
            build_type = 'relwithdebinfo'  and
            run_timestamp >= DateTime::FromSeconds(1722522137)
            order by test_id
            LIMIT $page_size
            );
            select DISTINCT suite_folder, test_name,test_id
             from $test
            --ORDER by test_id
    
            """
            prepared_query = session.prepare(query)
            params = {
                "$last_key":last_key,
                "$page_size": page_size
            }
            
            result_set = session.transaction().execute(prepared_query, params,commit_tx=True)

            records = records + list(result_set[0].rows)
            last_key = records[-1]['test_id']
            page_n+=1
            print(page_n*page_size)
            print(len(result_set[0].rows))
            if not result_set[0].rows:
               break
        print(records)
        
       #if not records:
        #    break
        
        #for record in records:
        #    yield record

        
        
        

        print('done')    
   
   


if __name__ == "__main__":
    main()

