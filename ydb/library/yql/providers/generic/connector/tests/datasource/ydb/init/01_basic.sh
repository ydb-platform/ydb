#!/bin/bash

set -ex

/ydb -p ${PROFILE} yql -s '
    CREATE TABLE column_selection_A_b_C_d_E_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_A_b_C_d_E_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_COL1_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_COL1_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_asterisk_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_asterisk_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_col2_COL1_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_col2_COL1_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_col2_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_col2_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_col3_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_col3_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE primitives (
      id Int32 NOT NULL,
      col_01_bool Bool NOT NULL,
      col_02_int8 Int8 NOT NULL,
      col_03_int16 Int16 NOT NULL,
      col_04_int32 Int32 NOT NULL,
      col_05_int64 Int64 NOT NULL,
      col_06_uint8 Uint8 NOT NULL,
      col_07_uint16 Uint16 NOT NULL,
      col_08_uint32 Uint32 NOT NULL,
      col_09_uint64 Uint64 NOT NULL,
      col_10_float Float NOT NULL,
      col_11_double Double NOT NULL,
      col_12_string String NOT NULL,
      col_13_utf8 Utf8 NOT NULL,
      col_14_date Date NOT NULL,
      col_15_datetime Datetime NOT NULL,
      col_16_timestamp Timestamp NOT NULL,
      PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO
    primitives (id, col_01_bool, col_02_int8, col_03_int16, col_04_int32, col_05_int64, col_06_uint8, col_07_uint16,
                col_08_uint32, col_09_uint64, col_10_float, col_11_double, col_12_string, col_13_utf8,
                col_14_date, col_15_datetime, col_16_timestamp)
    VALUES (1, false, 1, -2, 3, -4, 5, 6, 7, 8, 9.9f, -10.10, "аз", "az",
            Date("1988-11-20"), Datetime("1988-11-20T12:55:28Z"), Timestamp("1988-11-20T12:55:28.123Z")),
           (2, true, 20, -20, 30, -40, 50, 60, 70, 80, 90.90f, -100.100, "буки", "buki",
            Date("2024-05-27"), Datetime("2024-05-27T18:43:32Z"), Timestamp("2024-05-27T18:43:32.123Z"));
    COMMIT;
  '

echo $(date +"%T.%6N") "SUCCESS"
