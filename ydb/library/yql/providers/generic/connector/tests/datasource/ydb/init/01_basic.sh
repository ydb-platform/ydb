#!/bin/bash

set -ex

/ydb -p ${PROFILE} yql -s '
    CREATE TABLE column_selection_A_b_C_d_E (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_A_b_C_d_E (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_COL1 (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_COL1 (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_asterisk (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_asterisk (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_col2_COL1 (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_col2_COL1 (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_col2 (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_col2 (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_col3 (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_col3 (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE primitive_types (
      col_00_id Int32,
      col_01_bool Bool,
      col_02_int8 Int8,
      col_03_int16 Int16,
      col_04_int32 Int32,
      col_05_int64 Int64,
      col_06_uint8 Uint8,
      col_07_uint16 Uint16,
      col_08_uint32 Uint32,
      col_09_uint64 Uint64,
      col_10_float Float,
      col_11_double Double,
      col_12_string String,
      col_13_utf8 Utf8,
      col_14_date Date,
      col_15_datetime Datetime,
      col_16_timestamp Timestamp,
      col_17_json Json,
      PRIMARY KEY (col_00_id)
    );
    COMMIT;
    INSERT INTO
    primitive_types 
                (col_00_id, col_01_bool, col_02_int8, col_03_int16, col_04_int32, col_05_int64, col_06_uint8, 
                col_07_uint16, col_08_uint32, col_09_uint64, col_10_float, col_11_double, col_12_string,
                col_13_utf8, col_14_date, col_15_datetime, col_16_timestamp, col_17_json)
    VALUES (1, false, 2, 3, 4, 5, 6, 7, 8, 9, 10.10f, 11.11f, "аз", "az",
            Date("1988-11-20"), Datetime("1988-11-20T12:55:28Z"), Timestamp("1988-11-20T12:55:28.111Z"),
            @@{ "friends" : [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}@@),
           (2, true, -2, -3, -4, -5, 6, 7, 8, 9, -10.10f, -11.11f, "буки", "buki",
            Date("2024-05-27"), Datetime("2024-05-27T18:43:32Z"), Timestamp("2024-05-27T18:43:32.123456Z"),
            @@{ "TODO": "unicode" }@@);
    COMMIT;

    CREATE TABLE optional_types (
      col_00_id Int32 NOT NULL,
      col_01_bool Bool,
      col_02_int8 Int8,
      col_03_int16 Int16,
      col_04_int32 Int32,
      col_05_int64 Int64,
      col_06_uint8 Uint8,
      col_07_uint16 Uint16,
      col_08_uint32 Uint32,
      col_09_uint64 Uint64,
      col_10_float Float,
      col_11_double Double,
      col_12_string String,
      col_13_utf8 Utf8,
      col_14_date Date,
      col_15_datetime Datetime,
      col_16_timestamp Timestamp,
      col_17_json Json,
      PRIMARY KEY (col_00_id)
    );
    COMMIT;
    INSERT INTO
    optional_types 
                (col_00_id, col_01_bool, col_02_int8, col_03_int16, col_04_int32, col_05_int64, col_06_uint8, 
                col_07_uint16, col_08_uint32, col_09_uint64, col_10_float, col_11_double, col_12_string,
                col_13_utf8, col_14_date, col_15_datetime, col_16_timestamp, col_17_json)
    VALUES (1, false, 2, 3, 4, 5, 6, 7, 8, 9, 10.10f, 11.11f, "аз", "az",
            Date("1988-11-20"), Datetime("1988-11-20T12:55:28Z"), Timestamp("1988-11-20T12:55:28.111Z"),
            CAST(@@{ "friends" : [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}@@ AS Json)),
           (2, true, -2, -3, -4, -5, 6, 7, 8, 9, -10.10f, -11.11f, "буки", "buki",
            Date("2024-05-27"), Datetime("2024-05-27T18:43:32Z"), Timestamp("2024-05-27T18:43:32.123456Z"),
            CAST(@@{ "TODO" : "unicode" }@@ AS Json)),
           (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL);
    COMMIT;

    CREATE TABLE constant (col_00_id Int32 NOT NULL, PRIMARY KEY (col_00_id));
    COMMIT;
    INSERT INTO constant (col_00_id) VALUES
      (1),
      (2),
      (3);
    COMMIT;

    CREATE TABLE count (col_00_id Int32 NOT NULL, PRIMARY KEY (col_00_id));
    COMMIT;
    INSERT INTO count (col_00_id) VALUES
      (1),
      (2),
      (3),
      (4);
    COMMIT;

    CREATE TABLE pushdown (col_00_id Int32 NOT NULL, col_01_string String, PRIMARY KEY (col_00_id));
    COMMIT;
    INSERT INTO pushdown (col_00_id, col_01_string) VALUES
      (1, "one"),
      (2, "two"),
      (3, "three");
    COMMIT;

    -- As of 2024.05.31, INTERVAL type is not supported, so we use it to check behavior of connector
    -- when reading table containing usupported type columns.
    CREATE TABLE unsupported_types (col_00_id Int32 NOT NULL, col_01_interval INTERVAL, PRIMARY KEY (col_00_id));
    COMMIT;
    INSERT INTO unsupported_types (col_00_id, col_01_interval) VALUES
      (1, DATE("2024-01-01") - DATE("2023-01-01")),
      (2, DATE("2022-01-01") - DATE("2023-01-01"));
    COMMIT;

    CREATE TABLE json (col_00_id Int32 NOT NULL, col_01_json Json NOT NULL, col_02_json_nullable Json, PRIMARY KEY (col_00_id));
    COMMIT;
    INSERT INTO json (col_00_id, col_01_json, col_02_json_nullable) VALUES
      (
        1, 
        @@{ "friends" : [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}@@, 
        CAST(@@{ "friends" : [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}@@ AS Json)
      ),
      (2, @@{ "TODO": "unicode" }@@, CAST(@@{ "TODO": "unicode" }@@AS Json)),
      (3, @@{ }@@, NULL);
    COMMIT;
  '

echo $(date +"%T.%6N") "SUCCESS"
