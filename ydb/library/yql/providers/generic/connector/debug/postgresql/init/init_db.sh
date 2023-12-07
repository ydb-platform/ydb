#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS example_1;
    CREATE TABLE example_1 (id integer, col1 text, col2 integer);
    INSERT INTO example_1 VALUES (1, 'pg_example_1_a', 10);
    INSERT INTO example_1 VALUES (2, 'pg_example_1_b', 20);
    INSERT INTO example_1 VALUES (3, 'pg_example_1_c', 30);
    INSERT INTO example_1 VALUES (4, 'pg_example_1_d', 40);
    INSERT INTO example_1 VALUES (5, 'pg_example_1_e', 50);
    INSERT INTO example_1 VALUES (6, NULL, 1);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS example_2;
    CREATE TABLE example_2 (col_01_bool bool, col_02_smallint smallint, col_03_int2 int2, col_04_smallserial smallserial, col_05_serial2 serial2, col_06_integer integer, col_07_int int, col_08_int4 int4, col_09_serial serial, col_10_serial4 serial4, col_11_bigint bigint, col_12_int8 int8, col_13_bigserial bigserial, col_14_serial8 serial8, col_15_real real, col_16_float4 float4, col_17_double_precision double precision, col_18_float8 float8, col_19_bytea bytea, col_20_character character (5), col_21_character_varying character varying (5), col_22_text text, col_23_date date, col_24_timestamp timestamp without time zone);
    INSERT INTO example_2 (col_01_bool, col_02_smallint, col_03_int2, col_04_smallserial, col_05_serial2, col_06_integer, col_07_int, col_08_int4, col_09_serial, col_10_serial4, col_11_bigint, col_12_int8, col_13_bigserial, col_14_serial8, col_15_real, col_16_float4, col_17_double_precision, col_18_float8, col_19_bytea, col_20_character, col_21_character_varying, col_22_text, col_23_date, col_24_timestamp) VALUES(False, 2, 3, 1, 1, 6, 7, 8, 1, 1, 11, 12, 1, 1, 15.15, 16.16, 17.17, 18.18, 'az', 'az   ', 'az   ', 'az', '2023-08-09', '2023-08-09 13:19:11');
    INSERT INTO example_2 (col_01_bool, col_02_smallint, col_03_int2, col_04_smallserial, col_05_serial2, col_06_integer, col_07_int, col_08_int4, col_09_serial, col_10_serial4, col_11_bigint, col_12_int8, col_13_bigserial, col_14_serial8, col_15_real, col_16_float4, col_17_double_precision, col_18_float8, col_19_bytea, col_20_character, col_21_character_varying, col_22_text, col_23_date, col_24_timestamp) VALUES(NULL, NULL, NULL, 3, 3, NULL, NULL, NULL, 3, 3, NULL, NULL, 3, 3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS empty;
    CREATE TABLE empty (id integer, col1 text, col2 integer);
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS primitives;
    CREATE TABLE primitives (
        col_01_bool bool,
        col_02_smallint smallint,
        col_03_int2 int2,
        col_04_smallserial smallserial,
        col_05_serial2 serial2,
        col_06_integer integer,
        col_07_int int,
        col_08_int4 int4,
        col_09_serial serial,
        col_10_serial4 serial4,
        col_11_bigint bigint,
        col_12_int8 int8,
        col_13_bigserial bigserial,
        col_14_serial8 serial8,
        col_15_real real,
        col_16_float4 float4,
        col_17_double_precision double precision,
        col_18_float8 float8,
        col_19_bytea bytea,
        col_20_character_n character(20),
        col_21_character_varying_n character varying(21),
        col_22_text text,
        col_23_timestamp timestamp,
        col_24_date date
    );
    INSERT INTO primitives VALUES (
        true, 2, 3, DEFAULT, DEFAULT, 6, 7, 8, DEFAULT, DEFAULT, 11, 12, DEFAULT, DEFAULT,
        15.15, 16.16, 17.17, 18.18, 'az', 'az', 'az', 'az',
        current_timestamp, current_timestamp);
    INSERT INTO primitives VALUES (
        false, -2, -3, DEFAULT, DEFAULT, -6, -7, -8, DEFAULT, DEFAULT, -11, -12, DEFAULT, DEFAULT,
        -15.15, -16.16, -17.17, -18.18, 'буки', 'буки', 'буки', 'буки',
        current_timestamp, current_timestamp);
    INSERT INTO primitives VALUES (
        NULL, NULL, NULL, DEFAULT, DEFAULT, NULL,
        NULL, NULL, DEFAULT, DEFAULT, NULL, NULL,
        DEFAULT, DEFAULT, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL
        );
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS benchmark_1g;
    CREATE TABLE benchmark_1g (id bigserial, col varchar(1024));
    INSERT INTO benchmark_1g SELECT generate_series(1,1048576) AS id, REPEAT(md5(random()::text), 32) AS col;
EOSQL
