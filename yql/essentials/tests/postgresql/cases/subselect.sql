--
-- SUBSELECT
--
SELECT 1 AS one WHERE 1 IN (SELECT 1);
SELECT 1 AS zero WHERE 1 NOT IN (SELECT 1);
SELECT 1 AS zero WHERE 1 IN (SELECT 2);
-- Check grammar's handling of extra parens in assorted contexts
SELECT * FROM (SELECT 1 AS x) ss;
SELECT * FROM ((SELECT 1 AS x)) ss;
(SELECT 2) UNION SELECT 2;
((SELECT 2)) UNION SELECT 2;
SELECT ((SELECT 2) UNION SELECT 2);
SELECT (((SELECT 2)) UNION SELECT 2);
-- Set up some simple test tables
CREATE TABLE SUBSELECT_TBL (
  f1 integer,
  f2 integer,
  f3 float
);
INSERT INTO SUBSELECT_TBL VALUES (1, 2, 3);
INSERT INTO SUBSELECT_TBL VALUES (2, 3, 4);
INSERT INTO SUBSELECT_TBL VALUES (3, 4, 5);
INSERT INTO SUBSELECT_TBL VALUES (1, 1, 1);
INSERT INTO SUBSELECT_TBL VALUES (2, 2, 2);
INSERT INTO SUBSELECT_TBL VALUES (3, 3, 3);
INSERT INTO SUBSELECT_TBL VALUES (6, 7, 8);
INSERT INTO SUBSELECT_TBL VALUES (8, 9, NULL);
SELECT * FROM SUBSELECT_TBL;
-- Uncorrelated subselects
SELECT f1 AS "Constant Select" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT 1);
select 1 = all (select (select 1));
--
-- Test cases to catch unpleasant interactions between IN-join processing
-- and subquery pullup.
--
select count(*) from
  (select 1 from tenk1 a
   where unique1 IN (select hundred from tenk1 b)) ss;
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select hundred from tenk1 b)) ss;
select count(*) from
  (select 1 from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;
--
-- Test cases to check for overenthusiastic optimization of
-- "IN (SELECT DISTINCT ...)" and related cases.  Per example from
-- Luca Pireddu and Michael Fuhr.
--
CREATE TEMP TABLE foo (id integer);
CREATE TEMP TABLE bar (id1 integer, id2 integer);
INSERT INTO foo VALUES (1);
INSERT INTO bar VALUES (1, 1);
INSERT INTO bar VALUES (2, 2);
INSERT INTO bar VALUES (3, 1);
-- These cases require an extra level of distinct-ing above subquery s
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT id1, id2 FROM bar) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id1,id2 FROM bar GROUP BY id1,id2) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id1, id2 FROM bar UNION
                      SELECT id1, id2 FROM bar) AS s);
-- These cases do not
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT ON (id2) id1, id2 FROM bar) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM bar GROUP BY id2) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM bar UNION
                      SELECT id2 FROM bar) AS s);
--
-- Test case to catch problems with multiply nested sub-SELECTs not getting
-- recalculated properly.  Per bug report from Didier Moens.
--
CREATE TABLE orderstest (
    approver_ref integer,
    po_ref integer,
    ordercanceled boolean
);
INSERT INTO orderstest VALUES (1, 1, false);
INSERT INTO orderstest VALUES (66, 5, false);
INSERT INTO orderstest VALUES (66, 6, false);
INSERT INTO orderstest VALUES (66, 7, false);
INSERT INTO orderstest VALUES (66, 1, true);
INSERT INTO orderstest VALUES (66, 8, false);
INSERT INTO orderstest VALUES (66, 1, false);
INSERT INTO orderstest VALUES (77, 1, false);
INSERT INTO orderstest VALUES (1, 1, false);
INSERT INTO orderstest VALUES (66, 1, false);
INSERT INTO orderstest VALUES (1, 1, false);
--
-- Test cases to catch situations where rule rewriter fails to propagate
-- hasSubLinks flag correctly.  Per example from Kyle Bateman.
--
create temp table parts (
    partnum     text,
    cost        float8
);
create temp table shipped (
    ttype       char(2),
    ordnum      int4,
    partnum     text,
    value       float8
);
insert into parts (partnum, cost) values (1, 1234.56);
--
-- Test cases involving PARAM_EXEC parameters and min/max index optimizations.
-- Per bug report from David Sanchez i Gregori.
--
select * from (
  select max(unique1) from tenk1 as a
  where exists (select 1 from tenk1 as b where b.thousand = a.unique2)
) ss;
select * from (
  select min(unique1) from tenk1 as a
  where not exists (select 1 from tenk1 as b where b.unique2 = 10000)
) ss;
--
-- Test that an IN implemented using a UniquePath does unique-ification
-- with the right semantics, as per bug #4113.  (Unfortunately we have
-- no simple way to ensure that this test case actually chooses that type
-- of plan, but it does in releases 7.4-8.3.  Note that an ordering difference
-- here might mean that some other plan type is being used, rendering the test
-- pointless.)
--
create temp table numeric_table (num_col numeric);
insert into numeric_table values (1), (1.000000000000000000001), (2), (3);
create temp table float_table (float_col float8);
insert into float_table values (1), (2), (3);
select * from float_table
  where float_col in (select num_col from numeric_table);
--
-- Test case for bug #4290: bogus calculation of subplan param sets
--
create temp table ta (id int primary key, val int);
insert into ta values(1,1);
insert into ta values(2,2);
create temp table tb (id int primary key, aval int);
insert into tb values(1,1);
insert into tb values(2,1);
insert into tb values(3,2);
insert into tb values(4,2);
create temp table tc (id int primary key, aid int);
insert into tc values(1,1);
insert into tc values(2,2);
--
-- Test case for 8.3 "failed to locate grouping columns" bug
--
create temp table t1 (f1 numeric(14,0), f2 varchar(30));
select * from
  (select distinct f1, f2, (select f2 from t1 x where x.f1 = up.f1) as fs
   from t1 up) ss
group by f1,f2,fs;
--
-- Test case for bug #5514 (mishandling of whole-row Vars in subselects)
--
create temp table table_a(id integer);
insert into table_a values (42);
--
-- Test case for sublinks pulled up into joinaliasvars lists in an
-- inherited update/delete query
--
begin;  --  this shouldn't delete anything, but be safe
rollback;
--
-- Test case for subselect within UPDATE of INSERT...ON CONFLICT DO UPDATE
--
create temp table upsert(key int4 primary key, val text);
--
-- Test case for cross-type partial matching in hashed subplan (bug #7597)
--
create temp table outer_7597 (f1 int4, f2 int4);
insert into outer_7597 values (0, 0);
insert into outer_7597 values (1, 0);
insert into outer_7597 values (0, null);
insert into outer_7597 values (1, null);
create temp table inner_7597(c1 int8, c2 int8);
insert into inner_7597 values(0, null);
--
-- Similar test case using text that verifies that collation
-- information is passed through by execTuplesEqual() in nodeSubplan.c
-- (otherwise it would error in texteq())
--
create temp table outer_text (f1 text, f2 text);
insert into outer_text values ('a', 'a');
insert into outer_text values ('b', 'a');
insert into outer_text values ('a', null);
insert into outer_text values ('b', null);
create temp table inner_text (c1 text, c2 text);
insert into inner_text values ('a', null);
insert into inner_text values ('123', '456');
begin;
rollback;  -- to get rid of the bogus operator
select count(*) from tenk1 t
where (exists(select 1 from tenk1 k where k.unique1 = t.unique2) or ten < 0);
select count(*) from tenk1 t
where (exists(select 1 from tenk1 k where k.unique1 = t.unique2) or ten < 0)
  and thousand = 1;
--
-- Check we don't misoptimize a NOT IN where the subquery returns no rows.
--
create temp table notinouter (a int);
create temp table notininner (b int not null);
insert into notinouter values (null), (1);
--
-- Check we behave sanely in corner case of empty SELECT list (bug #8648)
--
create temp table nocolumns();
select exists(select * from nocolumns);
--
-- Test that LIMIT can be pushed to SORT through a subquery that just projects
-- columns.  We check for that having happened by looking to see if EXPLAIN
-- ANALYZE shows that a top-N sort was used.  We must suppress or filter away
-- all the non-invariant parts of the EXPLAIN ANALYZE output.
--
create table sq_limit (pk int primary key, c1 int, c2 int);
insert into sq_limit values
    (1, 1, 1),
    (2, 2, 2),
    (3, 3, 3),
    (4, 4, 4),
    (5, 1, 1),
    (6, 2, 2),
    (7, 3, 3),
    (8, 4, 4);
select * from (select pk,c2 from sq_limit order by c1,pk) as x limit 3;
drop table sq_limit;
--
-- Ensure that backward scan direction isn't propagated into
-- expression subqueries (bug #15336)
--
begin;
commit;
