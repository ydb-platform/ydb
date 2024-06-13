import os
import configparser
import ydb


dir=os.path.dirname(__file__)
config = configparser.ConfigParser() 
config_file_path=f'{dir}/../config/ydb_qa_db.ini'
config.read(config_file_path)  


github_sha = os.environ.get("GITHUB_SHA", None)


with ydb.Driver(
        endpoint=config["QA_DB"]["DATABASE_ENDPOINT"],
        database=config["QA_DB"]["DATABASE_PATH"],
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        with session.transaction() as transaction:
                
            #tx.execute(prepared_query, parameters, commit_tx=True)
            transaction(sql, commit_tx=True)