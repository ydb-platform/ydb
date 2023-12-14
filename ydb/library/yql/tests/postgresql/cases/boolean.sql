--
-- BOOLEAN
--
--
-- sanity check - if this fails go insane!
--
SELECT 1 AS one;
-- ******************testing built-in type bool********************
-- check bool input syntax
SELECT true AS true;
SELECT false AS false;
SELECT bool 't' AS true;
SELECT bool '   f           ' AS false;
SELECT bool 'true' AS true;
SELECT bool 'test' AS error;
SELECT bool 'false' AS false;
SELECT bool 'foo' AS error;
SELECT bool 'y' AS true;
SELECT bool 'yes' AS true;
SELECT bool 'yeah' AS error;
SELECT bool 'n' AS false;
SELECT bool 'no' AS false;
SELECT bool 'nay' AS error;
SELECT bool 'on' AS true;
SELECT bool 'off' AS false;
SELECT bool 'of' AS false;
SELECT bool 'o' AS error;
SELECT bool 'on_' AS error;
SELECT bool 'off_' AS error;
SELECT bool '1' AS true;
SELECT bool '11' AS error;
SELECT bool '0' AS false;
SELECT bool '000' AS error;
SELECT bool '' AS error;
-- and, or, not in qualifications
SELECT bool 't' or bool 'f' AS true;
SELECT bool 't' and bool 'f' AS false;
SELECT not bool 'f' AS true;
SELECT bool 't' = bool 'f' AS false;
SELECT bool 't' <> bool 'f' AS true;
SELECT bool 't' > bool 'f' AS true;
SELECT bool 't' >= bool 'f' AS true;
SELECT bool 'f' < bool 't' AS true;
SELECT bool 'f' <= bool 't' AS true;
-- explicit casts to/from text
SELECT 'TrUe'::text::boolean AS true, 'fAlse'::text::boolean AS false;
SELECT '    true   '::text::boolean AS true,
       '     FALSE'::text::boolean AS false;
SELECT true::boolean::text AS true, false::boolean::text AS false;
SELECT '  tru e '::text::boolean AS invalid;    -- error
SELECT ''::text::boolean AS invalid;            -- error
CREATE TABLE BOOLTBL1 (f1 bool);
INSERT INTO BOOLTBL1 (f1) VALUES (bool 't');
INSERT INTO BOOLTBL1 (f1) VALUES (bool 'True');
INSERT INTO BOOLTBL1 (f1) VALUES (bool 'true');
-- BOOLTBL1 should be full of true's at this point
SELECT BOOLTBL1.* FROM BOOLTBL1;
SELECT BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = bool 'true';
SELECT BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 <> bool 'false';
SELECT BOOLTBL1.*
   FROM BOOLTBL1
   WHERE booleq(bool 'false', f1);
INSERT INTO BOOLTBL1 (f1) VALUES (bool 'f');
SELECT BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = bool 'false';
CREATE TABLE BOOLTBL2 (f1 bool);
INSERT INTO BOOLTBL2 (f1) VALUES (bool 'f');
INSERT INTO BOOLTBL2 (f1) VALUES (bool 'false');
INSERT INTO BOOLTBL2 (f1) VALUES (bool 'False');
INSERT INTO BOOLTBL2 (f1) VALUES (bool 'FALSE');
-- This is now an invalid expression
-- For pre-v6.3 this evaluated to false - thomas 1997-10-23
INSERT INTO BOOLTBL2 (f1)
   VALUES (bool 'XXX');
-- BOOLTBL2 should be full of false's at this point
SELECT BOOLTBL2.* FROM BOOLTBL2;
--
-- Tests for BooleanTest
--
CREATE TABLE BOOLTBL3 (d text, b bool, o int);
INSERT INTO BOOLTBL3 (d, b, o) VALUES ('true', true, 1);
INSERT INTO BOOLTBL3 (d, b, o) VALUES ('false', false, 2);
INSERT INTO BOOLTBL3 (d, b, o) VALUES ('null', null, 3);
-- Test to make sure short-circuiting and NULL handling is
-- correct. Use a table as source to prevent constant simplification
-- to interfer.
CREATE TABLE booltbl4(isfalse bool, istrue bool, isnul bool);
INSERT INTO booltbl4 VALUES (false, true, null);
\pset null '(null)'
--
-- Clean up
-- Many tables are retained by the regression test, but these do not seem
--  particularly useful so just get rid of them for now.
--  - thomas 1997-11-30
--
DROP TABLE  BOOLTBL1;
DROP TABLE  BOOLTBL2;
DROP TABLE  BOOLTBL3;
