#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    DROP TABLE IF EXISTS dqrun.example_1;
    CREATE TABLE dqrun.example_1 (id Int32, col1 String, col2 Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO dqrun.example_1 (*) VALUES (1, 'ch_example_1_a', 10);
    INSERT INTO dqrun.example_1 (*) VALUES (2, 'ch_example_1_b', 20);
    INSERT INTO dqrun.example_1 (*) VALUES (3, 'ch_example_1_c', 30);
    INSERT INTO dqrun.example_1 (*) VALUES (4, 'ch_example_1_d', 40);
    INSERT INTO dqrun.example_1 (*) VALUES (5, 'ch_example_1_e', 50);
EOSQL

clickhouse client -n <<-EOSQL
    DROP TABLE IF EXISTS dqrun.date_time;
    CREATE TABLE dqrun.date_time (
        col_date Date,
        col_datetime DateTime,
        col_datetime_zone DateTime('Europe/Moscow'),
        col_date64 DateTime64(3),
        col_date64_zone DateTime64(3, 'Asia/Istanbul')
    ) ENGINE = TinyLog;
    INSERT INTO dqrun.date_time (*) VALUES (now(), now(), now(), now(), now());
    INSERT INTO dqrun.date_time (*) VALUES ('2004-01-10', '2004-01-10 00:05:00', '2004-01-10 00:05:00', '2004-01-10T00:05:00.321', '2004-01-10T00:05:00.321');
    INSERT INTO dqrun.date_time (*) VALUES ('1950-01-10', '1950-01-10 00:05:00', '1950-01-10 00:05:00', '2100-01-10T00:05:00.321', '2004-01-10T00:05:00.321');
EOSQL

clickhouse client -n <<-EOSQL
    DROP TABLE IF EXISTS dqrun.primitives;
    CREATE TABLE dqrun.primitives (
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
        col_16_datetime DateTime,
        col_17_date64 DateTime64(3)
    ) ENGINE = TinyLog;
    INSERT INTO dqrun.primitives (*) VALUES (True, 2, 3, 4, 5, 6, 7, 8, 9, 10.10, 11.11, 'az', 'az', now(), now(), '1988-11-20 12:55:08.123');
    INSERT INTO dqrun.primitives (*) VALUES (False, -2, 3, -4, 5, -6, 7, -8, 9, -10.10, -11.11, 'буки', 'буки', now(), now(), now());
EOSQL

clickhouse client -n <<-EOSQL
    DROP TABLE IF EXISTS dqrun.optional;
    CREATE TABLE dqrun.optional (
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
        col_13_string Nullable(FixedString(13)),
        col_14_date Nullable(Date),
        col_15_datetime Nullable(DateTime),
        col_16_date64 Nullable(DateTime64(3))
    ) ENGINE = TinyLog;
    INSERT INTO dqrun.optional (*) VALUES (True, 2, 3, 4, 5, 6, 7, 8, 9, 10.10, 11.11, 'az', 'az', now(), now(), now());
    INSERT INTO dqrun.optional (*) VALUES (False, -2, 3, -4, 5, -6, 7, -8, 9, -10.10, -11.11, 'буки', 'буки', now(), now(), now());
    INSERT INTO dqrun.optional (*) VALUES (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
EOSQL

clickhouse client -n <<-EOSQL
    DROP TABLE IF EXISTS dqrun.unsupported;
    CREATE TABLE dqrun.unsupported (id Int32, col1 String, col2 UUID) ENGINE = TinyLog;
    INSERT INTO dqrun.unsupported (*) VALUES (1, 'ch_az', generateUUIDv4());
    INSERT INTO dqrun.unsupported (*) VALUES (2, 'ch_buki', generateUUIDv4());
    INSERT INTO dqrun.unsupported (*) VALUES (3, 'ch_vedi', generateUUIDv4());
    INSERT INTO dqrun.unsupported (*) VALUES (4, 'ch_glagol', generateUUIDv4());
    INSERT INTO dqrun.unsupported (*) VALUES (5, 'ch_dobro', generateUUIDv4());
EOSQL

clickhouse client -n <<-EOSQL
    DROP TABLE IF EXISTS dqrun.benchmark_1g;
    CREATE TABLE dqrun.benchmark_1g (id UInt64, col Text) ENGINE = MergeTree ORDER BY id;
    INSERT INTO dqrun.benchmark_1g SELECT *, randomPrintableASCII(randUniform(1024, 1024)) FROM numbers(1048576);
EOSQL
