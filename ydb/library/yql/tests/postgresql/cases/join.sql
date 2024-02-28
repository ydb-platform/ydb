--
-- JOIN
-- Test JOIN clauses
--
CREATE TABLE J1_TBL (
  i integer,
  j integer,
  t text
);
CREATE TABLE J2_TBL (
  i integer,
  k integer
);
INSERT INTO J1_TBL VALUES (1, 4, 'one');
INSERT INTO J1_TBL VALUES (2, 3, 'two');
INSERT INTO J1_TBL VALUES (3, 2, 'three');
INSERT INTO J1_TBL VALUES (4, 1, 'four');
INSERT INTO J1_TBL VALUES (5, 0, 'five');
INSERT INTO J1_TBL VALUES (6, 6, 'six');
INSERT INTO J1_TBL VALUES (7, 7, 'seven');
INSERT INTO J1_TBL VALUES (8, 8, 'eight');
INSERT INTO J1_TBL VALUES (0, NULL, 'zero');
INSERT INTO J1_TBL VALUES (NULL, NULL, 'null');
INSERT INTO J1_TBL VALUES (NULL, 0, 'zero');
INSERT INTO J2_TBL VALUES (1, -1);
INSERT INTO J2_TBL VALUES (2, 2);
INSERT INTO J2_TBL VALUES (3, -3);
INSERT INTO J2_TBL VALUES (2, 4);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (0, NULL);
INSERT INTO J2_TBL VALUES (NULL, NULL);
INSERT INTO J2_TBL VALUES (NULL, 0);
-- useful in some tests below
create temp table onerow();
--
-- CORRELATION NAMES
-- Make sure that table/column aliases are supported
-- before diving into more complex join syntax.
--
SELECT *
  FROM J1_TBL AS tx;
SELECT *
  FROM J1_TBL tx;
SELECT *
  FROM J1_TBL AS t1 (a, b, c);
SELECT *
  FROM J1_TBL t1 (a, b, c);
--
-- More complicated constructs
--
--
-- Multiway full join
--
CREATE TABLE t1 (name TEXT, n INTEGER);
CREATE TABLE t2 (name TEXT, n INTEGER);
CREATE TABLE t3 (name TEXT, n INTEGER);
INSERT INTO t1 VALUES ( 'bb', 11 );
INSERT INTO t2 VALUES ( 'bb', 12 );
INSERT INTO t2 VALUES ( 'cc', 22 );
INSERT INTO t2 VALUES ( 'ee', 42 );
INSERT INTO t3 VALUES ( 'bb', 13 );
INSERT INTO t3 VALUES ( 'cc', 23 );
INSERT INTO t3 VALUES ( 'dd', 33 );
-- Test for propagation of nullability constraints into sub-joins
create temp table x (x1 int, x2 int);
insert into x values (1,11);
insert into x values (2,22);
insert into x values (3,null);
insert into x values (4,44);
insert into x values (5,null);
create temp table y (y1 int, y2 int);
insert into y values (1,111);
insert into y values (2,222);
insert into y values (3,333);
insert into y values (4,null);
select * from x;
select * from y;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and xx2 is not null);
-- these should NOT give the same answers as above
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (x2 is not null);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (y2 is not null);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (xx2 is not null);
-- try that with GEQO too
begin;
rollback;
--
-- Clean up
--
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE J1_TBL;
DROP TABLE J2_TBL;
-- Both DELETE and UPDATE allow the specification of additional tables
-- to "join" against to determine which rows should be modified.
CREATE TEMP TABLE t1 (a int, b int);
CREATE TEMP TABLE t2 (a int, b int);
CREATE TEMP TABLE t3 (x int, y int);
INSERT INTO t1 VALUES (5, 10);
INSERT INTO t1 VALUES (15, 20);
INSERT INTO t1 VALUES (100, 100);
INSERT INTO t1 VALUES (200, 1000);
INSERT INTO t2 VALUES (200, 2000);
INSERT INTO t3 VALUES (5, 20);
INSERT INTO t3 VALUES (6, 7);
INSERT INTO t3 VALUES (7, 8);
INSERT INTO t3 VALUES (500, 100);
--
-- regression test for 8.1 merge right join bug
--
CREATE TEMP TABLE tt1 ( tt1_id int4, joincol int4 );
INSERT INTO tt1 VALUES (1, 11);
INSERT INTO tt1 VALUES (2, NULL);
CREATE TEMP TABLE tt2 ( tt2_id int4, joincol int4 );
INSERT INTO tt2 VALUES (21, 11);
INSERT INTO tt2 VALUES (22, 11);
--
-- regression test for 8.2 bug with improper re-ordering of left joins
--
create temp table tt3(f1 int, f2 text);
insert into tt3 select x, repeat('xyzzy', 100) from generate_series(1,10000) x;
create index tt3i on tt3(f1);
create temp table tt4(f1 int);
insert into tt4 values (0),(1),(9999);
--
-- regression test for proper handling of outer joins within antijoins
--
create temp table tt4x(c1 int, c2 int, c3 int);
--
-- regression test for problems of the sort depicted in bug #3494
--
create temp table tt5(f1 int, f2 int);
create temp table tt6(f1 int, f2 int);
insert into tt5 values(1, 10);
insert into tt5 values(1, 11);
insert into tt6 values(1, 9);
insert into tt6 values(1, 2);
insert into tt6 values(2, 9);
--
-- regression test for problems of the sort depicted in bug #3588
--
create temp table xx (pkxx int);
create temp table yy (pkyy int, pkxx int);
insert into xx values (1);
insert into xx values (2);
insert into xx values (3);
insert into yy values (101, 1);
insert into yy values (201, 2);
insert into yy values (301, NULL);
--
-- regression test for improper pushing of constants across outer-join clauses
-- (as seen in early 8.2.x releases)
--
create temp table zt1 (f1 int primary key);
create temp table zt2 (f2 int primary key);
create temp table zt3 (f3 int primary key);
insert into zt1 values(53);
insert into zt2 values(53);
select * from
  zt2 left join zt3 on (f2 = f3)
      left join zt1 on (f3 = f1)
where f2 = 53;
--
-- test for sane behavior with noncanonical merge clauses, per bug #4926
--
begin;
create temp table a (i integer);
create temp table b (x integer, y integer);
select * from a left join b on i = x and i = y and x = i;
rollback;
--
-- test handling of merge clauses using record_ops
--
begin;
create temp table tidv (idv mycomptype);
create index on tidv (idv);
rollback;
--
-- test incorrect failure to NULL pulled-up subexpressions
--
begin;
create temp table a (
     code char not null,
     constraint a_pk primary key (code)
);
create temp table b (
     a char not null,
     num integer not null,
     constraint b_pk primary key (a, num)
);
create temp table c (
     name char not null,
     a char,
     constraint c_pk primary key (name)
);
insert into a (code) values ('p');
insert into a (code) values ('q');
insert into b (a, num) values ('p', 1);
insert into b (a, num) values ('p', 2);
insert into c (name, a) values ('A', 'p');
insert into c (name, a) values ('B', 'q');
insert into c (name, a) values ('C', null);
rollback;
--
-- test incorrect handling of placeholders that only appear in targetlists,
-- per bug #6154
--
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, sub4.value2, COALESCE(sub4.value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;
-- test the path using join aliases, too
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, value2, COALESCE(value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;
--
-- nested nestloops can require nested PlaceHolderVars
--
create temp table nt1 (
  id int primary key,
  a1 boolean,
  a2 boolean
);
insert into nt1 values (1,true,true);
insert into nt1 values (2,true,false);
insert into nt1 values (3,false,false);
select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;
