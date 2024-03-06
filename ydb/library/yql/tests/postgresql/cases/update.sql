--
-- UPDATE syntax tests
--
CREATE TABLE update_test (
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
);
CREATE TABLE upsert_test (
    a   INT PRIMARY KEY,
    b   TEXT
);
INSERT INTO update_test VALUES (5, 10, 'foo');
INSERT INTO update_test(b, a) VALUES (15, 10);
SELECT * FROM update_test;
--
-- Test multiple-set-clause syntax
--
INSERT INTO update_test SELECT a,b+1,c FROM update_test;
-- Test ON CONFLICT DO UPDATE
INSERT INTO upsert_test VALUES(1, 'Boo'), (3, 'Zoo');
DROP TABLE update_test;
DROP TABLE upsert_test;
CREATE TABLE upsert_test_2 (b TEXT, a INT PRIMARY KEY);
-- Create partitions intentionally in descending bound order, so as to test
-- that update-row-movement works with the leaf partitions not in bound order.
CREATE TABLE part_b_20_b_30 (e varchar, c numeric, a text, b bigint, d int);
CREATE TABLE part_c_1_100 (e varchar, d int, c numeric, b bigint, a text);
\set init_range_parted 'truncate range_parted; insert into range_parted VALUES (''a'', 1, 1, 1), (''a'', 10, 200, 1), (''b'', 12, 96, 1), (''b'', 13, 97, 2), (''b'', 15, 105, 16), (''b'', 17, 105, 19)'
\set show_data 'select tableoid::regclass::text COLLATE "C" partname, * from range_parted ORDER BY 1, 2, 3, 4, 5, 6'
-- Common table needed for multiple test scenarios.
CREATE TABLE mintab(c1 int);
INSERT into mintab VALUES (120);
DROP TABLE mintab;
CREATE TABLE sub_part1(b int, c int8, a numeric);
CREATE TABLE sub_part2(b int, c int8, a numeric);
CREATE TABLE list_part1(a numeric, b int, c int8);
-- UPDATE partition-key with FROM clause. If join produces multiple output
-- rows for the same row to be modified, we should tuple-route the row only
-- once. There should not be any rows inserted.
CREATE TABLE non_parted (id int);
INSERT into non_parted VALUES (1), (1), (1), (2), (2), (2), (3), (3), (3);
DROP TABLE non_parted;
