#!/bin/bash

export NLS_LANG=AMERICAN_CIS.AL32UTF8


echo creating user
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE USER C##admin IDENTIFIED BY password;

GRANT CREATE SESSION TO C##admin;
grant create any table to C##admin;
alter user C##admin quota unlimited on users;
GRANT resource TO C##admin;

exit;
EOF

echo creating table constant
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE IF NOT EXISTS "C##ADMIN".constant (
    id INTEGER NOT NULL PRIMARY KEY
 );

INSERT INTO "C##ADMIN".constant
VALUES (1),
	(2),
	(3);

exit;
EOF

echo creating table count_rows
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE IF NOT EXISTS "C##ADMIN".count_rows (
    id INTEGER NOT NULL PRIMARY KEY
 );

INSERT INTO "C##ADMIN".count_rows
VALUES (1),
	(2),
	(3);

exit;
EOF

echo creating table json
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE IF NOT EXISTS "C##ADMIN".json (
    id INTEGER NOT NULL PRIMARY KEY,
    col_01_json JSON
 );

INSERT INTO "C##ADMIN".json
VALUES (1, '{ "friends": [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}'),
	(2, '{ "TODO" : "unicode" }'),
	(3, NULL);

exit;
EOF

echo creating table primitives
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;


CREATE TABLE IF NOT EXISTS "C##ADMIN".primitives (
    col_00_id INTEGER NOT NULL PRIMARY KEY,
    col_01_int INTEGER,
    col_02_float FLOAT,
    col_03_int_number NUMBER(3,0),
    col_05_binary_float BINARY_FLOAT,
    col_06_binary_double BINARY_DOUBLE,
	col_07_varchar2 VARCHAR2(7),
	col_08_nvarchar2 NVARCHAR2(12),
	col_09_char_one CHAR,
	col_10_char_small CHAR(3),
	col_11_nchar_one NCHAR,
	col_12_nchar_small NCHAR(3),
	col_13_clob CLOB,
	col_14_nclob NCLOB,
	col_15_raw RAW(8),
	col_16_blob BLOB,
	col_17_date DATE,
	col_18_timestamp TIMESTAMP,
	col_19_timestamp_w_timezone TIMESTAMP WITH TIME ZONE,
	col_20_timestamp_w_local_timezone TIMESTAMP WITH LOCAL TIME ZONE,
    col_21_json JSON
 );

INSERT INTO "C##ADMIN".primitives 
(col_00_id, col_01_int, col_02_float, col_03_int_number,  
col_05_binary_float,  col_06_binary_double,  col_07_varchar2, 
col_08_nvarchar2,  col_09_char_one, col_10_char_small, col_11_nchar_one, 
col_12_nchar_small, col_13_clob, col_14_nclob, col_15_raw, col_16_blob, 
col_17_date, 
col_18_timestamp, col_19_timestamp_w_timezone, col_20_timestamp_w_local_timezone, col_21_json) 
VALUES 
(1, 1, 1.1, 123, 1.1, 1.1, 'varchar', N'варчар', 'c', 'cha', N'ч', N'чар', 'clob', N'клоб', utl_raw.cast_to_raw('ABCD'), utl_raw.cast_to_raw('EF'), 
    TO_DATE('01 01, 1970, 00:00:00', 'mm dd, YYYY, HH24:MI:SS'), 
    TO_TIMESTAMP('1970-01-01 01:01:01.111111', 'YYYY-mm-dd HH24:MI:SS.FF'), 
    TO_TIMESTAMP_TZ('1970-01-01 01:01:01.111111 -1:00', 'YYYY-mm-dd HH24:MI:SS.FF TZH:TZM'), 
    TO_TIMESTAMP_TZ('1970-01-01 01:01:01.111111 -1:11', 'YYYY-mm-dd HH24:MI:SS.FF TZH:TZM'),
    '{ "friends": [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}'
    ),
(2, -1, -1.1, -123, -1.1, -1.1, 'varchar', N'варчар', 'c', 'cha', N'ч', N'чар', 'clob', N'клоб', utl_raw.cast_to_raw('1234'), utl_raw.cast_to_raw('5678'),
            TO_DATE('01 01, 1970, 00:00:00', 'mm dd, YYYY, HH24:MI:SS'),
            TO_TIMESTAMP('1970-01-01 01:01:01.111111', 'YYYY-mm-dd HH24:MI:SS.FF'),
            TO_TIMESTAMP_TZ('1970-01-01 01:01:01.111111 -1:00', 'YYYY-mm-dd HH24:MI:SS.FF TZH:TZM'),
            TO_TIMESTAMP_TZ('1970-01-01 01:01:01.111111 -1:11', 'YYYY-mm-dd HH24:MI:SS.FF TZH:TZM'),
            '{ "TODO" : "unicode" }'
            ),
(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
exit;
EOF


echo creating table long_table
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE IF NOT EXISTS "C##ADMIN".long_table (
    col_00_id INTEGER NOT NULL PRIMARY KEY,
	col_01_long LONG
 );

INSERT INTO "C##ADMIN".long_table
VALUES (1, 'long'),
	(2, ''),
	(3, NULL);

exit;
EOF

echo creating table longraw
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE IF NOT EXISTS "C##ADMIN".longraw (
    col_00_id INTEGER NOT NULL PRIMARY KEY,
	col_01_long_raw LONG RAW
 );

INSERT INTO "C##ADMIN".longraw
    VALUES (1, utl_raw.cast_to_raw('12'));
INSERT INTO "C##ADMIN".longraw
    VALUES	(2, utl_raw.cast_to_raw(''));
INSERT INTO "C##ADMIN".longraw
    VALUES	(3, NULL);

exit;
EOF

echo creating table datetimes
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE "C##ADMIN".datetimes (
    col_00_id INTEGER,
    col_01_date DATE,
    col_02_timestamp TIMESTAMP(6)
);

INSERT INTO "C##ADMIN".datetimes 
VALUES 
(1, 
	TO_DATE('05 27, 1950, 01:02:03', 'mm dd, YYYY, HH24:MI:SS'),
	TO_TIMESTAMP('1950-05-27 01:02:03.111111', 'YYYY-mm-dd HH24:MI:SS.FF')
),
(2, 
	TO_DATE('11 20, 1988, 12:55:28', 'mm dd, YYYY, HH24:MI:SS'),
	TO_TIMESTAMP('1988-11-20 12:55:28.123000', 'YYYY-mm-dd HH24:MI:SS.FF')
),
(3, 
	TO_DATE('01 19, 2038, 3:14:07', 'mm dd, YYYY, HH24:MI:SS'),
	TO_TIMESTAMP('2038-01-19 3:14:07.0', 'YYYY-mm-dd HH24:MI:SS.FF')
),
(4, 
	TO_DATE('12 31, 9999, 23:59:59', 'mm dd, YYYY, HH24:MI:SS'),
	TO_TIMESTAMP('9999-12-31 23:59:59.999999', 'YYYY-mm-dd HH24:MI:SS.FF')
);

exit;
EOF


echo creating table pushdown
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE "C##ADMIN".pushdown (
    col_00_id INTEGER NOT NULL,
    col_01_integer INTEGER,
    col_02_text VARCHAR(255)
);

INSERT INTO "C##ADMIN".pushdown VALUES
                     (1, 10, 'a'),
                     (2, 2, 'b'),
                     (3, 30, 'c'),
                     (4, NULL, NULL);

exit;
EOF


echo creating table column_selection_A_b_C_d_E
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE "C##ADMIN"."column_selection_A_b_C_d_E" ("COL1" INTEGER, "col2" INTEGER);

INSERT INTO "C##ADMIN"."column_selection_A_b_C_d_E" ("COL1", "col2") VALUES
    (1, 2),
    (10, 20);

exit;
EOF

echo creating table column_selection_COL1
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE "C##ADMIN"."column_selection_COL1" ("COL1" INTEGER, "col2" INTEGER);
INSERT INTO "C##ADMIN"."column_selection_COL1" ("COL1", "col2") VALUES
    (1, 2),
    (10, 20);
    exit;
EOF

echo creating table column_selection_col1
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE "C##ADMIN"."column_selection_col1" ("COL1" INTEGER, "col2" INTEGER);
INSERT INTO "C##ADMIN"."column_selection_col1" ("COL1", "col2") VALUES
    (1, 2),
    (10, 20);
    exit;
EOF

echo creating table column_selection_asterisk
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE "C##ADMIN"."column_selection_asterisk" ("COL1" INTEGER, "col2" INTEGER);
INSERT INTO "C##ADMIN"."column_selection_asterisk" ("COL1", "col2") VALUES
    (1, 2),
    (10, 20);
    exit;
EOF

echo creating table column_selection_col2_COL1
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE "C##ADMIN"."column_selection_col2_COL1" ("COL1" INTEGER, "col2" INTEGER);
INSERT INTO "C##ADMIN"."column_selection_col2_COL1" ("COL1", "col2") VALUES
    (1, 2),
    (10, 20);
    exit;
EOF

echo creating table column_selection_col2_col1
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE "C##ADMIN"."column_selection_col2_col1" ("COL1" INTEGER, "col2" INTEGER);
INSERT INTO "C##ADMIN"."column_selection_col2_col1" ("COL1", "col2") VALUES
    (1, 2),
    (10, 20);
    exit;
EOF

echo creating table column_selection_col2
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE "C##ADMIN"."column_selection_col2" ("COL1" INTEGER, "col2" INTEGER);
INSERT INTO "C##ADMIN"."column_selection_col2" ("COL1", "col2") VALUES
    (1, 2),
    (10, 20);
    exit;
EOF

echo creating table column_selection_col3
"$ORACLE_HOME"/bin/sqlplus -s system/password << EOF
whenever sqlerror exit sql.sqlcode;

CREATE TABLE "C##ADMIN"."column_selection_col3" ("COL1" INTEGER, "col2" INTEGER);
INSERT INTO "C##ADMIN"."column_selection_col3" ("COL1", "col2") VALUES
    (1, 2),
    (10, 20);
    exit;
EOF