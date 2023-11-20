--
-- NUMEROLOGY
-- Test various combinations of numeric types and functions.
--
--
-- Test implicit type conversions
-- This fails for Postgres v6.1 (and earlier?)
--  so let's try explicit conversions for now - tgl 97/05/07
--
CREATE TABLE TEMP_FLOAT (f1 FLOAT8);
-- int4
CREATE TABLE TEMP_INT4 (f1 INT4);
-- int2
CREATE TABLE TEMP_INT2 (f1 INT2);
--
-- Group-by combinations
--
CREATE TABLE TEMP_GROUP (f1 INT4, f2 INT4, f3 FLOAT8);
DROP TABLE TEMP_INT2;
DROP TABLE TEMP_INT4;
DROP TABLE TEMP_FLOAT;
DROP TABLE TEMP_GROUP;
