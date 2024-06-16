#!/usr/bin/env python3

import os
import configparser
import ydb


dir=os.path.dirname(__file__)
config = configparser.ConfigParser() 
config_file_path=f'{dir}/../config/ydb_qa_db.ini'
config.read(config_file_path)  

build_preset = os.environ.get("build_preset", "relwithdebinfo")

DATABASE_ENDPOINT=config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH=config["QA_DB"]["DATABASE_PATH"]



def main():

   # if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
     #   print("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
     #   return 1
    
    # Do not set up 'real' variable from gh workflows because it interfere with ydb tests 
    # So, set up it locally
    os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ["CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]
    #os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]= "/home/kirrysin/.ydb/my-robot-key.json"

    sql = f"""
        --!syntax_v1
    select git_commit_time,github_sha,size_bytes,size_stripped_bytes,build_preset from binary_size 
    where github_workflow="Postcommit_{build_preset}" and build_preset="{build_preset}"
    order by git_commit_time desc
    limit 1;    
    """
    #select git_commit_time,size_bytes,size_stripped_bytes,build_preset from binary_size 
    #where github_workflow="PR-check" and github_sha="{github_sha}" and build_preset="{build_preset}"
    #order by git_commit_time desc
    #limit 1;

    with ydb.Driver(
         
            endpoint=DATABASE_ENDPOINT,
            database=DATABASE_PATH,
            credentials=ydb.credentials_from_env_variables()

        ) as driver:
            driver.wait(timeout=10, fail_fast=True)
            session = ydb.retry_operation_sync(
                lambda: driver.table_client.session().create()
            )
            with session.transaction() as transaction:   
                result = transaction.execute(sql, commit_tx=True)
                for row in result[0].rows:
                    main_data={}
                    for field in row:
                        main_data[field]=row[field] if type(row[field])!=bytes else row[field].decode("utf-8") 
                 
                try:
                    main_data
                except NameError :
                    print(f'Cant get binary size in var for main (undefined)')
                    return 1

            print( f'sizes:{main_data["github_sha"]}:{str(main_data["git_commit_time"])}:{str(main_data["size_bytes"])}:{str(main_data["size_stripped_bytes"])}')
            return 0


if __name__ == "__main__":
    main()
