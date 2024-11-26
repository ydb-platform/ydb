--
-- UNION (also INTERSECT, EXCEPT)
--
-- Simple UNION constructs
SELECT 1 AS two UNION SELECT 2 ORDER BY 1;
SELECT 1 AS one UNION SELECT 1 ORDER BY 1;
SELECT 1 AS two UNION ALL SELECT 2;
SELECT 1 AS two UNION ALL SELECT 1;
SELECT 1 AS three UNION SELECT 2 UNION SELECT 3 ORDER BY 1;
SELECT 1 AS two UNION SELECT 2 UNION SELECT 2 ORDER BY 1;
SELECT 1 AS three UNION SELECT 2 UNION ALL SELECT 2 ORDER BY 1;
SELECT 1.1 AS two UNION SELECT 2.2 ORDER BY 1;
-- Mixed types
SELECT 1.1 AS two UNION SELECT 2 ORDER BY 1;
SELECT 1 AS two UNION SELECT 2.2 ORDER BY 1;
SELECT 1 AS one UNION SELECT 1.0::float8 ORDER BY 1;
SELECT 1.1 AS two UNION ALL SELECT 2 ORDER BY 1;
SELECT 1.0::float8 AS two UNION ALL SELECT 1 ORDER BY 1;
SELECT 1.1 AS three UNION SELECT 2 UNION SELECT 3 ORDER BY 1;
SELECT 1.1::float8 AS two UNION SELECT 2 UNION SELECT 2.0::float8 ORDER BY 1;
SELECT 1.1 AS three UNION SELECT 2 UNION ALL SELECT 2 ORDER BY 1;
SELECT 1.1 AS two UNION (SELECT 2 UNION ALL SELECT 2) ORDER BY 1;
SELECT f1 AS three FROM VARCHAR_TBL
UNION
SELECT CAST(f1 AS varchar) FROM CHAR_TBL
ORDER BY 1;
SELECT f1 AS eight FROM VARCHAR_TBL
UNION ALL
SELECT f1 FROM CHAR_TBL;
SELECT f1 AS five FROM TEXT_TBL
UNION
SELECT f1 FROM VARCHAR_TBL
UNION
SELECT TRIM(TRAILING FROM f1) FROM CHAR_TBL
ORDER BY 1;
--
-- INTERSECT and EXCEPT
--
SELECT q2 FROM int8_tbl INTERSECT SELECT q1 FROM int8_tbl ORDER BY 1;
SELECT q2 FROM int8_tbl INTERSECT ALL SELECT q1 FROM int8_tbl ORDER BY 1;
SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1;
SELECT q2 FROM int8_tbl EXCEPT ALL SELECT q1 FROM int8_tbl ORDER BY 1;
SELECT q2 FROM int8_tbl EXCEPT ALL SELECT DISTINCT q1 FROM int8_tbl ORDER BY 1;
SELECT q1 FROM int8_tbl EXCEPT ALL SELECT q2 FROM int8_tbl ORDER BY 1;
SELECT q1 FROM int8_tbl EXCEPT ALL SELECT DISTINCT q2 FROM int8_tbl ORDER BY 1;
-- nested cases
(SELECT 1,2,3 UNION SELECT 4,5,6) INTERSECT SELECT 4,5,6;
(SELECT 1,2,3 UNION SELECT 4,5,6 ORDER BY 1,2) INTERSECT SELECT 4,5,6;
(SELECT 1,2,3 UNION SELECT 4,5,6) EXCEPT SELECT 4,5,6;
(SELECT 1,2,3 UNION SELECT 4,5,6 ORDER BY 1,2) EXCEPT SELECT 4,5,6;
select count(*) from
  ( select unique1 from tenk1 union select fivethous from tenk1 ) ss;
select count(*) from
  ( select unique1 from tenk1 intersect select fivethous from tenk1 ) ss;
select unique1 from tenk1 except select unique2 from tenk1 where unique2 != 10;
select count(*) from
  ( select unique1 from tenk1 union select fivethous from tenk1 ) ss;
select count(*) from
  ( select unique1 from tenk1 intersect select fivethous from tenk1 ) ss;
select unique1 from tenk1 except select unique2 from tenk1 where unique2 != 10;
select x from (values (array[1, 2]), (array[1, 3])) _(x) intersect select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) except select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) intersect select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) except select x from (values (array[1, 2]), (array[1, 4])) _(x);
--
-- Operator precedence and (((((extra))))) parentheses
--
SELECT q1 FROM int8_tbl INTERSECT SELECT q2 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl  ORDER BY 1;
SELECT q1 FROM int8_tbl INTERSECT (((SELECT q2 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl))) ORDER BY 1;
(((SELECT q1 FROM int8_tbl INTERSECT SELECT q2 FROM int8_tbl ORDER BY 1))) UNION ALL SELECT q2 FROM int8_tbl;
SELECT q1 FROM int8_tbl UNION ALL (((SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1)));
--
-- Subqueries with ORDER BY & LIMIT clauses
--
-- In this syntax, ORDER BY/LIMIT apply to the result of the EXCEPT
SELECT q1,q2 FROM int8_tbl EXCEPT SELECT q2,q1 FROM int8_tbl
ORDER BY q2,q1;
--
-- New syntaxes (7.1) permit new tests
--
(((((select * from int8_tbl)))));
--
-- Check handling of a case with unknown constants.  We don't guarantee
-- an undecorated constant will work in all cases, but historically this
-- usage has worked, so test we don't break it.
--
SELECT a.f1 FROM (SELECT 'test' AS f1 FROM varchar_tbl) a
UNION
SELECT b.f1 FROM (SELECT f1 FROM varchar_tbl) b
ORDER BY 1;
-- This should fail, but it should produce an error cursor
SELECT '3.4'::numeric UNION SELECT 'foo';
--
-- Test that expression-index constraints can be pushed down through
-- UNION or UNION ALL
--
CREATE TEMP TABLE t1 (a text, b text);
CREATE TEMP TABLE t2 (ab text primary key);
INSERT INTO t1 VALUES ('a', 'b'), ('x', 'y');
INSERT INTO t2 VALUES ('ab'), ('xy');
--
-- Test that ORDER BY for UNION ALL can be pushed down to inheritance
-- children.
--
CREATE TEMP TABLE t1c (b text, a text);
INSERT INTO t1c VALUES ('v', 'w'), ('c', 'd'), ('m', 'n'), ('e', 'f');
-- This simpler variant of the above test has been observed to fail differently
create table events (event_id int primary key);
create table other_events (event_id int primary key);
drop table events_child, events, other_events;
SELECT * FROM
  (SELECT 1 AS t, 2 AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x < 4
ORDER BY x;
SELECT * FROM
  (SELECT 1 AS t, (random()*3)::int AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x > 3
ORDER BY x;
select distinct q1 from
  (select distinct * from int8_tbl i81
   union all
   select distinct * from int8_tbl i82) ss
where q2 = q2;
select distinct q1 from
  (select distinct * from int8_tbl i81
   union all
   select distinct * from int8_tbl i82) ss
where -q1 = q2;
