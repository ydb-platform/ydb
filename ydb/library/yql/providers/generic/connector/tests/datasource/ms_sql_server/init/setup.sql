CREATE TABLE column_selection_A_b_C_d_E (COL1 INTEGER, col2 INTEGER);
INSERT INTO column_selection_A_b_C_d_E (COL1, col2) VALUES
    (1, 2),
    (10, 20);

-- TODO: Cannot create table with this name, otherwise duplicate occurs
--
-- CREATE TABLE column_selection_COL1 (COL1 INTEGER, col2 INTEGER);
-- INSERT INTO column_selection_COL1 (COL1, col2) VALUES
--     (1, 2),
--     (10, 20);

CREATE TABLE column_selection_col1 (COL1 INTEGER, col2 INTEGER);
INSERT INTO column_selection_col1 (COL1, col2) VALUES
    (1, 2),
    (10, 20);

CREATE TABLE column_selection_asterisk (COL1 INTEGER, col2 INTEGER);
INSERT INTO column_selection_asterisk (COL1, col2) VALUES
    (1, 2),
    (10, 20);

-- TODO: Cannot create table with this name, otherwise duplicate occurs
--
-- CREATE TABLE column_selection_col2_COL1 (COL1 INTEGER, col2 INTEGER);
-- INSERT INTO column_selection_col2_COL1 (COL1, col2) VALUES
--     (1, 2),
--     (10, 20);

CREATE TABLE column_selection_col2_col1 (COL1 INTEGER, col2 INTEGER);
INSERT INTO column_selection_col2_col1 (COL1, col2) VALUES
    (1, 2),
    (10, 20);

CREATE TABLE column_selection_col2 (COL1 INTEGER, col2 INTEGER);
INSERT INTO column_selection_col2 (COL1, col2) VALUES
    (1, 2),
    (10, 20);

CREATE TABLE column_selection_col3 (COL1 INTEGER, col2 INTEGER);
INSERT INTO column_selection_col3 (COL1, col2) VALUES
    (1, 2),
    (10, 20);

CREATE TABLE primitives (
    id INTEGER PRIMARY KEY,
    col_01_bit BIT,
    col_02_tinyint TINYINT,
    col_03_smallint SMALLINT,
    col_04_int INT,
    col_05_bigint BIGINT,
    col_06_float FLOAT,
    col_07_real REAL,
    col_08_char CHAR(8),
    col_09_varchar VARCHAR(8),
    col_10_text TEXT,
    col_11_nchar NCHAR(8),
    col_12_nvarchar NVARCHAR(8),
    col_13_ntext NTEXT,
    col_14_binary BINARY(8),
    col_15_varbinary VARBINARY(8),
    col_16_image IMAGE,
    col_17_date DATE,
    col_18_smalldatetime SMALLDATETIME,
    col_19_datetime DATETIME,
    col_20_datetime2 DATETIME2(7)
);

INSERT INTO primitives VALUES
    (0, 1, 2, 3, 4, 5, 6.6, 7.7, 'az', 'az', 'az', 'az', 'az', 'az', 0x1234567890ABCDEF, 0x1234567890ABCDEF, 0x1234567890ABCDEF,
    '1988-11-20', '1988-11-20 12:55:00', '1988-11-20 12:55:28.123', '1988-11-20 12:55:28.1231231'),
    (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, NULL, NULL),
    (2, 0, 2, -3, -4, -5, -6.6, -7.7, N'буки', N'буки', N'буки', N'буки', N'буки', N'буки', 0x1234567890ABCDEF, 0x1234567890ABCDEF, 0x1234567890ABCDEF,
    '2023-03-21', '2023-03-21 11:21:00', '2023-03-21 11:21:31', '2023-03-21 11:21:31');

CREATE TABLE constant (
    col_00_id INTEGER NOT NULL,
    PRIMARY KEY (col_00_id)
);

INSERT INTO constant VALUES (0), (1), (2);


CREATE TABLE count_rows (
    col_00_id INTEGER NOT NULL,
    PRIMARY KEY (col_00_id)
);

INSERT INTO count_rows VALUES (0), (1), (2);


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

DROP TABLE IF EXISTS datetimes;
CREATE TABLE datetimes (
    id INTEGER PRIMARY KEY,
    col_01_date DATE,
    col_02_smalldatetime SMALLDATETIME,
    col_03_datetime DATETIME,
    col_04_datetime2 DATETIME2(7)
);

INSERT INTO datetimes VALUES 
    (1, '1950-05-27', '1950-05-27 01:02:00', '1950-05-27 01:02:03.110', '1950-05-27 01:02:03.1111111'),
    (2, '1988-11-20', '1988-11-20 12:55:00', '1988-11-20 12:55:28.123', '1988-11-20 12:55:28.1231231'),
    (3, '2023-03-21', '2023-03-21 11:21:00', '2023-03-21 11:21:31', '2023-03-21 11:21:31');

SELECT * FROM datetimes;
