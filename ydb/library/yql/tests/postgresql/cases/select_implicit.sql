--
-- SELECT_IMPLICIT
-- Test cases for queries with ordering terms missing from the target list.
-- This used to be called "junkfilter.sql".
-- The parser uses the term "resjunk" to handle these cases.
-- - thomas 1998-07-09
--
-- load test data
CREATE TABLE test_missing_target (a int, b int, c char(8), d char);
INSERT INTO test_missing_target VALUES (0, 1, 'XXXX', 'A');
INSERT INTO test_missing_target VALUES (1, 2, 'ABAB', 'b');
INSERT INTO test_missing_target VALUES (2, 2, 'ABAB', 'c');
INSERT INTO test_missing_target VALUES (3, 3, 'BBBB', 'D');
INSERT INTO test_missing_target VALUES (4, 3, 'BBBB', 'e');
INSERT INTO test_missing_target VALUES (5, 3, 'bbbb', 'F');
INSERT INTO test_missing_target VALUES (6, 4, 'cccc', 'g');
INSERT INTO test_missing_target VALUES (7, 4, 'cccc', 'h');
INSERT INTO test_missing_target VALUES (8, 4, 'CCCC', 'I');
INSERT INTO test_missing_target VALUES (9, 4, 'CCCC', 'j');
--   group using reference number
SELECT count(*) FROM test_missing_target ORDER BY 1 desc;
--   Cleanup
DROP TABLE test_missing_target;
