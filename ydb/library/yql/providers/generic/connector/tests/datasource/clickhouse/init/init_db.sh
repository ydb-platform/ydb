#!/bin/bash
set -ex

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.simple;
    CREATE TABLE db.simple (id Int32, col1 String, col2 Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.simple (*) VALUES
        (1, 'ch_a', 10) \
        (2, 'ch_b', 20) \
        (3, 'ch_c', 30) \
        (4, 'ch_d', 40) \
        (5, 'ch_e', 50);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.primitives;
    CREATE TABLE db.primitives (
        id Int32,
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
        col_13_string FixedString(13),
        col_14_date Date,
        col_15_date32 Date32,
        col_16_datetime DateTime,
        col_17_datetime64 DateTime64(3)
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.primitives (*) VALUES
        (1, False, 2, 3, 4, 5, 6, 7, 8, 9, 10.10, 11.11, 'az', 'az', '1988-11-20', '1988-11-20', '1988-11-20 12:55:28', '1988-11-20 12:55:28.123') \
        (2, True, -2, 3, -4, 5, -6, 7, -8, 9, -10.10, -11.11, 'буки', 'буки', '2023-03-21', '2023-03-21', '2023-03-21 11:21:31', '2023-03-21 11:21:31.456');
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.primitive_types_nullable;
    CREATE TABLE db.primitive_types_nullable (
        id Int32,
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
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.primitive_types_nullable (*) VALUES
        (1, False, 2, 3, 4, 5, 6, 7, 8, 9, 10.10, 11.11, 'az', 'az', '1988-11-20', '1988-11-20', '1988-11-20 12:55:28', '1988-11-20 12:55:28.123') \
        (2, True, -2, 3, -4, 5, -6, 7, -8, 9, -10.10, -11.11, 'буки', 'буки', '2023-03-21', '2023-03-21', '2023-03-21 11:21:31', '2023-03-21 11:21:31.456') \
        (3, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.datetime;
    CREATE TABLE db.datetime (
        id Int32,
        col_01_date Date,
        col_02_date32 Date32,
        col_03_datetime DateTime,
        col_04_datetime64 DateTime64(8)
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.datetime (*) VALUES
        (1, '1950-05-27', '1950-05-27', '1950-05-27 01:02:03', '1950-05-27 01:02:03.1111') \
        (2, '1988-11-20', '1988-11-20', '1988-11-20 12:55:28', '1988-11-20 12:55:28.12345678') \
        (3, '2023-03-21', '2023-03-21', '2023-03-21 11:21:31', '2023-03-21 11:21:31.98765432');
EOSQL

# Feel free to add new columns to this table to test new filters
clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.pushdown;
    CREATE TABLE db.pushdown (
        id Int32,
        col_01_int32 Nullable(Int32),
        col_02_string Nullable(String)
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.pushdown (*) VALUES
        (1, 10, 'a') \
        (2, 20, 'b') \
        (3, 30, 'c') \
        (4, NULL, NULL);
EOSQL


clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.arrays;
    CREATE TABLE db.arrays (
        id Int32,
        col_01_int32 Nullable(Int32),
        col_02_array Array(DateTime)
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.arrays (*) VALUES
        (1, 10, []) \
        (2, 20, ['1950-05-27 01:02:03']) \
        (3, 30, ['1950-05-27 01:02:03', '2023-03-21 11:21:31']);
EOSQL
