#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username gpadmin --dbname template1 <<-EOSQL
    CREATE TABLE simple_table (number INT);
    INSERT INTO simple_table VALUES ((1)), ((2)), ((3));

    CREATE TABLE join_table (id INT, data bytea);
    INSERT INTO join_table VALUES (1, 'gp10'), (2, 'gp20'), (3, 'gp30');
EOSQL
