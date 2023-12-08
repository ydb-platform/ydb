#!/bin/bash

set -ex

URL='http://crab:qwerty12345@localhost:8123/?database=dqrun'

SCRIPTS=(
"DROP TABLE IF EXISTS date_time;"
"CREATE TABLE date_time (
    col_date Date,
    col_date32 Date32,
    col_datetime DateTime,
    col_datetime64 DateTime64(3)
) ENGINE = TinyLog;"
"INSERT INTO date_time (*) VALUES ('1950-01-10', '1850-01-10', '1950-01-10 12:23:45', '1850-01-10 12:23:45.678');"
"INSERT INTO date_time (*) VALUES ('1970-01-10', '1950-01-10', '1970-01-10 12:23:45', '1950-01-10 12:23:45.678');"
"INSERT INTO date_time (*) VALUES ('2004-01-10', '2004-01-10', '2004-01-10 12:23:45', '2004-01-10 12:23:45.678');"
"INSERT INTO date_time (*) VALUES ('2110-01-10', '2110-01-10', '2106-01-10 12:23:45', '2110-01-10 12:23:45.678');"
"INSERT INTO date_time (*) VALUES ('2150-01-10', '2300-01-10', '2107-01-10 12:23:45', '2300-01-10 12:23:45.678');"
)

for ((i = 0; i < ${#SCRIPTS[@]}; i++))
do
    echo "${SCRIPTS[$i]}" | curl "${URL}" --data-binary @-
done

echo "SELECT * FROM date_time" | curl "${URL}" --data-binary @-
