#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from sqlalchemy.testing.provision import set_default_schema_on_connection


# This is only for test purpose required by Requirement "default_schema_name_switch"
@set_default_schema_on_connection.for_db("snowflake")
def _snowflake_set_default_schema_on_connection(cfg, dbapi_connection, schema_name):
    cursor = dbapi_connection.cursor()
    cursor.execute(f"USE SCHEMA {dbapi_connection.database}.{schema_name};")
    cursor.close()
