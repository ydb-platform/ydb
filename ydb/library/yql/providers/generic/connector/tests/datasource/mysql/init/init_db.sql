DROP TABLE IF EXISTS primitives;

CREATE TABLE primitives (
    col_00_id INT NOT NULL, 
    col_01_tinyint TINYINT,
    col_02_tinyint_unsigned TINYINT UNSIGNED,
    col_03_smallint SMALLINT ,
    col_04_smallint_unsigned SMALLINT UNSIGNED,
    col_05_mediumint MEDIUMINT,
    col_06_mediumint_unsigned MEDIUMINT UNSIGNED,
    col_07_integer INTEGER,
    col_08_integer_unsigned INTEGER UNSIGNED,
    col_09_bigint BIGINT,
    col_10_bigint_unsigned BIGINT UNSIGNED,
    col_11_float FLOAT,
    col_12_double DOUBLE,
    col_13_date DATE,
    col_14_datetime DATETIME(6),
    col_15_timestamp TIMESTAMP(6),
    col_16_char CHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
    col_17_varchar VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
    col_18_tinytext TINYTEXT,
    col_19_text TEXT,
    col_20_mediumtext MEDIUMTEXT,
    col_21_longtext LONGTEXT,
    col_22_binary BINARY(8),
    col_23_varbinary VARBINARY(10),
    col_24_tinyblob TINYBLOB,
    col_25_blob BLOB,
    col_26_mediumblob MEDIUMBLOB,
    col_27_longblob LONGBLOB,
    col_28_bool BOOL,
    col_29_json JSON,
    PRIMARY KEY (col_00_id)
);

INSERT INTO primitives VALUES 
       (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11.11, 12.12, 
       '1988-11-20', '1988-11-20T12:34:56.777777', '1988-11-20T12:34:56.777777',
       'az', 'az', 'az', 'az', 'az', 'az', 'az', 'az', 'az', 'az', 'az', 'az',
       true,
       '{ "friends": [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}'
       ),
       (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
       NULL, NULL, NULL,
       NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
       NULL,
       NULL
       ),
       (2, -10, 20, -30, 40, -50, 60, -70, 80, -90, 100, -1111.1111, -1212.1212, 
       '2024-07-01', '2024-07-01T01:02:03.444444', '2024-07-01T01:02:03.444444',
       'буки', 'буки', 'буки', 'буки', 'буки', 'буки', 'буки', 'буки', 'буки', 'буки', 'буки', 'буки',
       false,
       '{ "TODO" : "unicode" }'
       )
       ;


DROP TABLE IF EXISTS constant;
CREATE TABLE constant (
    col_01 INTEGER NOT NULL,
    PRIMARY KEY (col_01)
);

INSERT INTO constant VALUES (0), (1), (2);

DROP TABLE IF EXISTS count_rows;
CREATE TABLE count_rows (
    col_01 INTEGER NOT NULL,
    PRIMARY KEY (col_01)
);

INSERT INTO count_rows VALUES (0), (1), (2);

DROP TABLE IF EXISTS pushdown;
CREATE TABLE pushdown (
    col_00_id INT NOT NULL,
    col_01_integer INT,
    col_02_text VARCHAR(255)
);

INSERT INTO pushdown VALUES
                     (1, 10, 'a'),
                     (2, 2, 'b'),
                     (3, 30, 'c'),
                     (4, NULL, NULL);

DROP TABLE IF EXISTS json;
CREATE TABLE json (
    col_00_id INT NOT NULL,
    col_01_json JSON,
    PRIMARY KEY (col_00_id)
);

INSERT INTO json VALUES 
    (0, '{ "friends": [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}'), 
    (1, '{ "TODO" : "unicode" }'),
    (2, NULL);

DROP TABLE IF EXISTS datetimes;
CREATE TABLE datetimes (
    id int,
    col_01_date DATE,
    col_02_datetime DATETIME(6),
    col_03_timestamp TIMESTAMP(6),
    PRIMARY KEY (id)
);

INSERT INTO datetimes VALUES (1, '1950-05-27', '1950-05-27 01:02:03.111111', NULL);
INSERT INTO datetimes VALUES (2, '1988-11-20', '1988-11-20 12:55:28.123000', '1988-11-20 12:55:28.123000');
INSERT INTO datetimes VALUES (3, '2023-03-21', '2023-03-21 11:21:31', '2023-03-21 11:21:31');
