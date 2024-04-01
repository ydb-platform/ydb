#!/bin/bash

/ydb -p tests-ydb-client yql -s '

    CREATE TABLE simple (id Int8 , col1 String , col2 Int32 , PRIMARY KEY (id));
    COMMIT;
    INSERT INTO simple (id, col1, col2) VALUES
      (1, "ydb_a", 10),
      (2, "ydb_b", 20),
      (3, "ydb_c", 30),
      (4, "ydb_d", 40),
      (5, "ydb_e", 50);
    COMMIT;


    CREATE TABLE primitives (
        id Int8 ,
        col_01_bool Bool ,
        col_02_int8 Int8 ,
        col_03_int16 Int16 ,
        col_04_int32 Int32 ,
        col_05_int64 Int64 ,
        col_06_uint8 Uint8 ,
        col_07_uint16 Uint16 ,
        col_08_uint32 Uint32 ,
        col_09_uint64 Uint64 ,
        col_10_float Float ,
        col_11_double Double ,
        col_12_string String ,
        col_13_utf8 Utf8 ,
        col_14_date Date ,
        col_15_datetime Datetime ,
        col_16_timestamp Timestamp ,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO
    primitives (id, col_01_bool, col_02_int8, col_03_int16, col_04_int32, col_05_int64, col_06_uint8, col_07_uint16,
                col_08_uint32, col_09_uint64, col_10_float, col_11_double, col_12_string, col_13_utf8,
                col_14_date, col_15_datetime, col_16_timestamp)
    VALUES (1, false, 1, -2, 3, -4, 5, 6, 7, 8, 9.9f, -10.10, "ая", "az",
            Date("1988-11-20"), Datetime("1988-11-20T12:55:28Z"), Timestamp("1988-11-20T12:55:28.123Z"));
    COMMIT;


    CREATE TABLE optionals (
        id Int8 ,
        col_01_bool Optional<Bool>,
        col_02_int8 Optional<Int8>,
        col_03_int16 Optional<Int16>,
        col_04_int32 Optional<Int32>,
        col_05_int64 Optional<Int64>,
        col_06_uint8 Optional<Uint8>,
        col_07_uint16 Optional<Uint16>,
        col_08_uint32 Optional<Uint32>,
        col_09_uint64 Optional<Uint64>,
        col_10_float Optional<Float>,
        col_11_double Optional<Double>,
        col_12_string Optional<String>,
        col_13_utf8 Optional<Utf8>,
        col_14_date Optional<Date>,
        col_15_datetime Optional<Datetime>,
        col_16_timestamp Optional<Timestamp>,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO
    optionals (id, col_01_bool, col_02_int8, col_03_int16, col_04_int32, col_05_int64, col_06_uint8, col_07_uint16,
               col_08_uint32, col_09_uint64, col_10_float, col_11_double, col_12_string, col_13_utf8,
               col_14_date, col_15_datetime, col_16_timestamp)
    VALUES
      (1, true, 1, -2, 3, -4, 5, 6, 7, 8, 9.9f, -10.10, "ая", "az",
       Date("1988-11-20"), Datetime("1988-11-20T12:55:28Z"), Timestamp("1988-11-20T12:55:28.123Z")),
      (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    COMMIT;


    CREATE TABLE datetime (
        id Int8 ,
        col_01_date Date ,
        col_02_datetime Datetime ,
        col_03_timestamp Timestamp ,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO
    datetime (id, col_01_date, col_02_datetime, col_03_timestamp)
    VALUES (1, Date("1988-11-20"), Datetime("1988-11-20T12:55:28Z"), Timestamp("1988-11-20T12:55:28.123456Z"));
    COMMIT;


    CREATE TABLE pushdown (
        id Int32 ,
        col_01_int Int32,
        col_02_text UTF8,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO pushdown (id, col_01_int, col_02_text) VALUES
        (1, 10, "a"),
        (2, 20, "b"),
        (3, 30, "c"),
        (4, NULL, NULL);
    COMMIT;

    CREATE TABLE `parent/child` (
        id INT8 ,
        col UTF8 ,
        PRIMARY KEY (id)
    );
    COMMIT;
    INSERT INTO `parent/child` (id, col) VALUES
      (1, "a"),
      (2, "b"),
      (3, "c"),
      (4, "d"),
      (5, "e");
    COMMIT;
  '

  echo "SUCCESS"
