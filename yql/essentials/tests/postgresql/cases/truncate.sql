-- Test basic TRUNCATE functionality.
CREATE TABLE truncate_a (col1 integer primary key);
INSERT INTO truncate_a VALUES (1);
INSERT INTO truncate_a VALUES (2);
SELECT * FROM truncate_a;
-- Roll truncate back
BEGIN;
ROLLBACK;
SELECT * FROM truncate_a;
-- Commit the truncate this time
BEGIN;
COMMIT;
CREATE TABLE trunc_c (a serial PRIMARY KEY);
-- Add some data to verify that truncating actually works ...
INSERT INTO trunc_c VALUES (1);
INSERT INTO truncate_a VALUES (1);
-- Add data again to test TRUNCATE ... CASCADE
INSERT INTO trunc_c VALUES (1);
INSERT INTO truncate_a VALUES (1);
-- Test TRUNCATE with inheritance
CREATE TABLE trunc_f (col1 integer primary key);
INSERT INTO trunc_f VALUES (1);
INSERT INTO trunc_f VALUES (2);
BEGIN;
ROLLBACK;
BEGIN;
ROLLBACK;
BEGIN;
ROLLBACK;
BEGIN;
SELECT * FROM trunc_f;
ROLLBACK;
-- Test ON TRUNCATE triggers
CREATE TABLE trunc_trigger_test (f1 int, f2 text, f3 text);
CREATE TABLE trunc_trigger_log (tgop text, tglevel text, tgwhen text,
        tgargv text, tgtable name, rowcount bigint);
-- basic before trigger
INSERT INTO trunc_trigger_test VALUES(1, 'foo', 'bar'), (2, 'baz', 'quux');
SELECT count(*) as "Row count in test table" FROM trunc_trigger_test;
SELECT * FROM trunc_trigger_log;
-- same test with an after trigger
INSERT INTO trunc_trigger_test VALUES(1, 'foo', 'bar'), (2, 'baz', 'quux');
SELECT * FROM trunc_trigger_log;
DROP TABLE trunc_trigger_test;
DROP TABLE trunc_trigger_log;
CREATE TABLE truncate_a (id serial,
                         id1 integer default nextval('truncate_a_id1'));
-- check rollback of a RESTART IDENTITY operation
BEGIN;
ROLLBACK;
DROP TABLE truncate_a;
SELECT nextval('truncate_a_id1'); -- fail, seq should have been dropped
CREATE TABLE truncprim (a int PRIMARY KEY);
