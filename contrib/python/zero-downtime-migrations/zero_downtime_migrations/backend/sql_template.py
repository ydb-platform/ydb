# coding: utf-8

from __future__ import unicode_literals


SQL_ESTIMATE_COUNT_IN_TABLE = "SELECT reltuples::BIGINT FROM pg_class WHERE relname = '%(table)s';"

SQL_COUNT_IN_TABLE = "SELECT COUNT(*) FROM %(table)s;"

SQL_COUNT_IN_TABLE_WITH_NULL = "SELECT COUNT(*) FROM %(table)s WHERE %(column)s is NULL;"

SQL_UPDATE_BATCH = ("WITH cte AS ( "
                    "SELECT %(pk_column_name)s as pk "
                    "FROM %(table)s "
                    "WHERE  %(column)s is null "
                    "LIMIT  %(batch_size)s "
                    ") "
                    "UPDATE %(table)s table_ "
                    "SET %(column)s = %(value)s "
                    "FROM   cte "
                    "WHERE  table_.%(pk_column_name)s = cte.pk"
                    )

SQL_CHECK_COLUMN_STATUS = ("SELECT IS_NULLABLE, DATA_TYPE, COLUMN_DEFAULT from information_schema.columns "
                           "where table_name = '%(table)s' and column_name = '%(column)s';")


SQL_CREATE_UNIQUE_INDEX = "CREATE UNIQUE INDEX CONCURRENTLY %(name)s ON %(table)s (%(columns)s)%(extra)s"
SQL_ADD_UNIQUE_CONSTRAINT_FROM_INDEX = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s UNIQUE USING INDEX %(index_name)s"
SQL_CHECK_INDEX_STATUS = ("SELECT 1 FROM pg_class, pg_index WHERE pg_index.indisvalid = false "
                          "AND pg_index.indexrelid = pg_class.oid and pg_class.relname = '%(index_name)s'")
