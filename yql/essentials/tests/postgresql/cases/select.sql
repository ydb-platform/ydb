--
-- SELECT
--
-- btree index
-- awk '{if($1<10){print;}else{next;}}' onek.data | sort +0n -1
--
SELECT * FROM onek
   WHERE onek.unique1 < 10
   ORDER BY onek.unique1;
--
-- Test some cases involving whole-row Var referencing a subquery
--
select foo from (select 1 offset 0) as foo;
select foo from (select null offset 0) as foo;
select foo from (select 'xyzzy',1,null offset 0) as foo;
-- VALUES is also legal as a standalone query or a set-operation member
VALUES (1,2), (3,4+4), (7,77.7);
VALUES (1,2), (3,4+4), (7,77.7)
UNION ALL
SELECT 2+2, 57
UNION ALL
TABLE int8_tbl;
-- corner case: VALUES with no columns
CREATE TEMP TABLE nocols();
--
-- Test ORDER BY options
--
CREATE TEMP TABLE foo (f1 int);
INSERT INTO foo VALUES (42),(3),(10),(7),(null),(null),(1);
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
-- check if indexscans do the right things
CREATE INDEX fooi ON foo (f1);
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
CREATE INDEX fooi ON foo (f1 DESC);
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
CREATE INDEX fooi ON foo (f1 DESC NULLS LAST);
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
select * from onek2 where unique2 = 11 and stringu1 < 'B';
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;
select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
select unique1, unique2 from onek2
  where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';
select unique1, unique2 from onek2
  where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;
-- X = X isn't a no-op, it's effectively X IS NOT NULL assuming = is strict
-- (see bug #5084)
select * from (values (2),(null),(1)) v(k) where k = k order by k;
select * from (values (2),(null),(1)) v(k) where k = k;
