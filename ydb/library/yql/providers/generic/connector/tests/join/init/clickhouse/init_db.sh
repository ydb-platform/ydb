#!/bin/bash
set -ex

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.example_1;
    CREATE TABLE db.example_1 (
        id Int32,
        col1 String,
        col2 Int32
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.example_1 (*) VALUES
        (1, 'example_1_a', 10) \
        (2, 'example_1_b', 20) \
        (3, 'example_1_c', 30) \
        (4, 'example_1_d', 40) \
        (5, 'example_1_e', 50);
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.example_2;
    CREATE TABLE db.example_2 (
        id Int32,
        col2 Int32,
        col1 String
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.example_2 (*) VALUES
        (1, 2, 'example_2_a') \
        (2, 4, 'example_2_b') \
        (3, 8, 'example_2_c') \
        (4, 16, 'example_2_d') \
        (5, 32, 'example_2_e');
EOSQL

clickhouse-client -n <<-EOSQL
    DROP TABLE IF EXISTS db.test_1;
    CREATE TABLE db.test_1 (
        id Int32,
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.test_1 (*) VALUES
        (1) \
        (2) \
        (3) \
        (4) \
        (5);
EOSQL