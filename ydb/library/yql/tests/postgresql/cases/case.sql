--
-- CASE
-- Test the case statement
--
CREATE TABLE CASE_TBL (
  i integer,
  f double precision
);
CREATE TABLE CASE2_TBL (
  i integer,
  j integer
);
INSERT INTO CASE_TBL VALUES (1, 10.1);
INSERT INTO CASE_TBL VALUES (2, 20.2);
INSERT INTO CASE_TBL VALUES (3, -30.3);
INSERT INTO CASE_TBL VALUES (4, NULL);
INSERT INTO CASE2_TBL VALUES (1, -1);
INSERT INTO CASE2_TBL VALUES (2, -2);
INSERT INTO CASE2_TBL VALUES (3, -3);
INSERT INTO CASE2_TBL VALUES (2, -4);
INSERT INTO CASE2_TBL VALUES (1, NULL);
INSERT INTO CASE2_TBL VALUES (NULL, -6);
--
-- Simplest examples without tables
--
SELECT '3' AS "One",
  CASE
    WHEN 1 < 2 THEN 3
  END AS "Simple WHEN";
SELECT '<NULL>' AS "One",
  CASE
    WHEN 1 > 2 THEN 3
  END AS "Simple default";
SELECT '3' AS "One",
  CASE
    WHEN 1 < 2 THEN 3
    ELSE 4
  END AS "Simple ELSE";
SELECT '4' AS "One",
  CASE
    WHEN 1 > 2 THEN 3
    ELSE 4
  END AS "ELSE default";
SELECT '6' AS "One",
  CASE
    WHEN 1 > 2 THEN 3
    WHEN 4 < 5 THEN 6
    ELSE 7
  END AS "Two WHEN with default";
SELECT '7' AS "None",
   CASE WHEN random() < 0 THEN 1
   END AS "NULL on no matches";
-- Constant-expression folding shouldn't evaluate unreachable subexpressions
SELECT CASE WHEN 1=0 THEN 1/0 WHEN 1=1 THEN 1 ELSE 2/0 END;
SELECT CASE 1 WHEN 0 THEN 1/0 WHEN 1 THEN 1 ELSE 2/0 END;
-- Test for cases involving untyped literals in test expression
SELECT CASE 'a' WHEN 'a' THEN 1 ELSE 2 END;
--
-- Examples of targets involving tables
--
SELECT
  CASE
    WHEN i >= 3 THEN i
  END AS ">= 3 or Null"
  FROM CASE_TBL;
SELECT
  CASE WHEN i >= 3 THEN (i + i)
       ELSE i
  END AS "Simplest Math"
  FROM CASE_TBL;
SELECT i AS "Value",
  CASE WHEN (i < 0) THEN 'small'
       WHEN (i = 0) THEN 'zero'
       WHEN (i = 1) THEN 'one'
       WHEN (i = 2) THEN 'two'
       ELSE 'big'
  END AS "Category"
  FROM CASE_TBL;
SELECT
  CASE WHEN ((i < 0) or (i < 0)) THEN 'small'
       WHEN ((i = 0) or (i = 0)) THEN 'zero'
       WHEN ((i = 1) or (i = 1)) THEN 'one'
       WHEN ((i = 2) or (i = 2)) THEN 'two'
       ELSE 'big'
  END AS "Category"
  FROM CASE_TBL;
--
-- Examples of qualifications involving tables
--
--
-- NULLIF() and COALESCE()
-- Shorthand forms for typical CASE constructs
--  defined in the SQL standard.
--
SELECT * FROM CASE_TBL WHERE COALESCE(f,i) = 4;
SELECT * FROM CASE_TBL WHERE NULLIF(f,i) = 2;
SELECT COALESCE(a.f, b.i, b.j)
  FROM CASE_TBL a, CASE2_TBL b;
SELECT NULLIF(a.i,b.i) AS "NULLIF(a.i,b.i)",
  NULLIF(b.i, 4) AS "NULLIF(b.i,4)"
  FROM CASE_TBL a, CASE2_TBL b;
--
-- Nested CASE expressions
--
-- This test exercises a bug caused by aliasing econtext->caseValue_isNull
-- with the isNull argument of the inner CASE's CaseExpr evaluation.  After
-- evaluating the vol(null) expression in the inner CASE's second WHEN-clause,
-- the isNull flag for the case test value incorrectly became true, causing
-- the third WHEN-clause not to match.  The volatile function calls are needed
-- to prevent constant-folding in the planner, which would hide the bug.
-- Wrap this in a single transaction so the transient '=' operator doesn't
-- cause problems in concurrent sessions
BEGIN;
ROLLBACK;
-- Test multiple evaluation of a CASE arg that is a read/write object (#14472)
-- Wrap this in a single transaction so the transient '=' operator doesn't
-- cause problems in concurrent sessions
BEGIN;
ROLLBACK;
-- Test interaction of CASE with ArrayCoerceExpr (bug #15471)
BEGIN;
ROLLBACK;
--
-- Clean up
--
DROP TABLE CASE_TBL;
DROP TABLE CASE2_TBL;
