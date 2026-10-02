#!/bin/bash
set -ex

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.primitive_types_non_nullable;
    CREATE TABLE db.primitive_types_non_nullable (
        col_00_id Int32,
        col_01_boolean Boolean,
        col_02_int8 Int8,
        col_03_uint8 UInt8,
        col_04_int16 Int16,
        col_05_uint16 UInt16,
        col_06_int32 Int32,
        col_07_uint32 UInt32,
        col_08_int64 Int64,
        col_09_uint64 UInt64,
        col_10_float32 Float32,
        col_11_float64 Float64,
        col_12_string String,
        col_13_fixed_string FixedString(13),
        col_14_date Date,
        col_15_date32 Date32,
        col_16_datetime DateTime,
        col_17_datetime64 DateTime64(3)
    ) ENGINE = MergeTree ORDER BY col_00_id;
    INSERT INTO db.primitive_types_non_nullable (*) VALUES
        (1, False, 2, 3, 4, 5, 6, 7, 8, 9, 10.10, 11.11, 'az', 'az', '1988-11-20', '1988-11-20', '1988-11-20 12:55:28', '1988-11-20 12:55:28.123') \
        (2, True, -2, 3, -4, 5, -6, 7, -8, 9, -10.10, -11.11, 'буки', 'буки', '2023-03-21', '2023-03-21', '2023-03-21 11:21:31', '2023-03-21 11:21:31.456');
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.primitive_types_nullable;
    CREATE TABLE db.primitive_types_nullable (
        col_00_id Int32,
        col_01_boolean Nullable(Boolean),
        col_02_int8 Nullable(Int8),
        col_03_uint8 Nullable(UInt8),
        col_04_int16 Nullable(Int16),
        col_05_uint16 Nullable(UInt16),
        col_06_int32 Nullable(Int32),
        col_07_uint32 Nullable(UInt32),
        col_08_int64 Nullable(Int64),
        col_09_uint64 Nullable(UInt64),
        col_10_float32 Nullable(Float32),
        col_11_float64 Nullable(Float64),
        col_12_string Nullable(String),
        col_13_fixed_string Nullable(FixedString(13)),
        col_14_date Nullable(Date),
        col_15_date32 Nullable(Date32),
        col_16_datetime Nullable(DateTime('UTC')),
        col_17_datetime64 Nullable(DateTime64(6, 'UTC'))
    ) ENGINE = MergeTree ORDER BY col_00_id;
    INSERT INTO db.primitive_types_nullable (*) VALUES
        (1, False, 2, 3, 4, 5, 6, 7, 8, 9, 10.10, 11.11, 'az', 'az', '1988-11-20', '1988-11-20', '1988-11-20 12:55:28', '1988-11-20 12:55:28.123') \
        (2, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL) \
        (3, True, -2, 3, -4, 5, -6, 7, -8, 9, -10.10, -11.11, 'буки', 'буки', '2023-03-21', '2023-03-21', '2023-03-21 11:21:31', '2023-03-21 11:21:31.456');
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.datetime_string;
    CREATE TABLE db.datetime_string (
        col_00_id Int32,
        col_01_date Date,
        col_02_date32 Date32,
        col_03_datetime DateTime,
        col_04_datetime64 DateTime64(8)
    ) ENGINE = MergeTree ORDER BY col_00_id;
/*
    Value is too early for both CH and YQL
    In this case ClickHouse behaviour is undefined
    For Datetime Clickhouse returns bottom bound and
    cuts off only date part of value along ClickHouse bottom bound for other types
*/
    INSERT INTO db.datetime_string (*) VALUES
        (1, '1950-01-10', '1850-01-10', '1950-01-10 12:23:45', '1950-01-10 12:23:45.678910');

    /* Value is OK for CH, but can be too early for YQL */
    INSERT INTO db.datetime_string (*) VALUES
        (2, '1970-01-10', '1950-01-10', '1980-01-10 12:23:45', '1950-01-10 12:23:45.678910');

    /* Value is OK for both CH and YQL */
    INSERT INTO db.datetime_string (*) VALUES
        (3, '2004-01-10', '2004-01-10', '2004-01-10 12:23:45', '2004-01-10 12:23:45.678910');

    /* Value is OK for CH, but too late for YQL */
    INSERT INTO db.datetime_string (*) VALUES
        (4, '2110-01-10', '2110-01-10', '2106-01-10 12:23:45', '2110-01-10 12:23:45.678910');
       
    /*
    Value is too late for both YQL and CH.
    In this case ClickHouse behaviour is undefined.
    */
    INSERT INTO db.datetime_string (*) VALUES
        (5, '2150-01-10', '2300-01-10', '2107-01-10 12:23:45', '2300-01-10 12:23:45.678910');
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.datetime_YQL;
    CREATE TABLE db.datetime_YQL (
        col_00_id Int32,
        col_01_date Date,
        col_02_date32 Date32,
        col_03_datetime DateTime,
        col_04_datetime64 DateTime64(8)
    ) ENGINE = MergeTree ORDER BY col_00_id;
    INSERT INTO db.datetime_YQL (*) VALUES
        (1, '1950-01-10', '1850-01-10', '1950-01-10 12:23:45', '1950-01-10 12:23:45.678910') \
        (2, '1970-01-10', '1950-01-10', '1980-01-10 12:23:45', '1950-01-10 12:23:45.678910') \
        (3, '2004-01-10', '2004-01-10', '2004-01-10 12:23:45', '2004-01-10 12:23:45.678910') \
        (4, '2110-01-10', '2110-01-10', '2106-01-10 12:23:45', '2110-01-10 12:23:45.678910') \
        (5, '2150-01-10', '2300-01-10', '2107-01-10 12:23:45', '2300-01-10 12:23:45.678910');
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.constant;
    CREATE TABLE db.constant (
        id Int32,
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.constant (*) VALUES
        (1) \
        (2) \
        (3);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.counts;
    CREATE TABLE db.counts (
        col Float64,
    ) ENGINE = MergeTree ORDER BY col;
    INSERT INTO db.counts (*) VALUES
        (3.14) \
        (1.0) \
        (2.718) \
        (-0.0);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.pushdown;
    CREATE TABLE db.pushdown (
        col_00_int32 Int32,
        col_01_string Nullable(String)
    ) ENGINE = MergeTree ORDER BY col_00_int32;
    INSERT INTO db.pushdown (*) VALUES
        (1, 'one') \
        (2, 'two') \
        (3, 'three') \
        (4, NULL);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.large;
    CREATE TABLE db.large (
        col_00_int32 Int32,
        col_01_string Nullable(String)
    ) ENGINE = MergeTree ORDER BY col_00_int32;

    INSERT INTO db.large
    SELECT
        number AS col_00_int32,
        substring(randomPrintableASCII(32), 1, 32) AS col_01_string
    FROM
        numbers(1000000);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.column_selection_A_b_C_d_E;
    CREATE TABLE db.column_selection_A_b_C_d_E (COL1 Int32, col2 Int32) 
        ENGINE = MergeTree ORDER BY COL1;
    INSERT INTO db.column_selection_A_b_C_d_E (*) VALUES
        (1, 2) \
        (10, 20);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.column_selection_COL1;
    CREATE TABLE db.column_selection_COL1 (COL1 Int32, col2 Int32) 
        ENGINE = MergeTree ORDER BY COL1;
    INSERT INTO db.column_selection_COL1 (*) VALUES
        (1, 2) \
        (10, 20);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.column_selection_asterisk;
    CREATE TABLE db.column_selection_asterisk (COL1 Int32, col2 Int32) 
        ENGINE = MergeTree ORDER BY COL1;
    INSERT INTO db.column_selection_asterisk (*) VALUES
        (1, 2) \
        (10, 20);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.column_selection_col2_COL1;
    CREATE TABLE db.column_selection_col2_COL1 (COL1 Int32, col2 Int32) 
        ENGINE = MergeTree ORDER BY COL1;
    INSERT INTO db.column_selection_col2_COL1 (*) VALUES
        (1, 2) \
        (10, 20);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.column_selection_col2;
    CREATE TABLE db.column_selection_col2 (COL1 Int32, col2 Int32) 
        ENGINE = MergeTree ORDER BY COL1;
    INSERT INTO db.column_selection_col2 (*) VALUES
        (1, 2) \
        (10, 20);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.column_selection_col3;
    CREATE TABLE db.column_selection_col3 (COL1 Int32, col2 Int32) 
        ENGINE = MergeTree ORDER BY COL1;
    INSERT INTO db.column_selection_col3 (*) VALUES
        (1, 2) \
        (10, 20);
EOSQL