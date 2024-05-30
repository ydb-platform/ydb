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

    CREATE TABLE primitive_types_NATIVE (
      col_00_id Int32 NOT NULL,
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
      PRIMARY KEY (col_00_id)
    );
    COMMIT;
    INSERT INTO
    primitive_types_NATIVE 
                (col_00_id, col_01_bool, col_02_int8, col_03_int16, col_04_int32, col_05_int64, col_06_uint8, 
                col_07_uint16, col_08_uint32, col_09_uint64, col_10_float, col_11_double, col_12_string,
                col_13_utf8, col_14_date, col_15_datetime, col_16_timestamp)
    VALUES (1, false, 2, 3, 4, 5, 6, 7, 8, 9, 10.10f, 11.11f, "аз", "az",
            Date("1988-11-20"), Datetime("1988-11-20T12:55:28Z"), Timestamp("1988-11-20T12:55:28.111Z")),
           (2, true, -2, -3, -4, -5, 6, 7, 8, 9, -10.10f, -11.11f, "буки", "buki",
            Date("2024-05-27"), Datetime("2024-05-27T18:43:32Z"), Timestamp("2024-05-27T18:43:32.123456Z"));
    COMMIT;
  '

echo $(date +"%T.%6N") "SUCCESS"
