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
-- nested cases
(SELECT 1,2,3 UNION SELECT 4,5,6) INTERSECT SELECT 4,5,6;
(SELECT 1,2,3 UNION SELECT 4,5,6 ORDER BY 1,2) INTERSECT SELECT 4,5,6;
(SELECT 1,2,3 UNION SELECT 4,5,6) EXCEPT SELECT 4,5,6;
(SELECT 1,2,3 UNION SELECT 4,5,6 ORDER BY 1,2) EXCEPT SELECT 4,5,6;
select x from (values (array[1, 2]), (array[1, 3])) _(x) intersect select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) except select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) intersect select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) except select x from (values (array[1, 2]), (array[1, 4])) _(x);
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
