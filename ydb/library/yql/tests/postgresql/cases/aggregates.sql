--
-- AGGREGATES
--
-- avoid bit-exact output here because operations may not be bit-exact.
SET extra_float_digits = 0;
-- population variance is defined for a single tuple, sample variance
-- is not
SELECT var_pop(1.0::float8), var_samp(2.0::float8);
SELECT stddev_pop(3.0::float8), stddev_samp(4.0::float8);
SELECT var_pop(1.0::float4), var_samp(2.0::float4);
SELECT stddev_pop(3.0::float4), stddev_samp(4.0::float4);
SELECT var_pop('inf'::numeric), var_samp('inf'::numeric);
SELECT stddev_pop('inf'::numeric), stddev_samp('inf'::numeric);
SELECT var_pop('nan'::numeric), var_samp('nan'::numeric);
SELECT stddev_pop('nan'::numeric), stddev_samp('nan'::numeric);
SELECT sum(x::numeric), avg(x::numeric), var_pop(x::numeric)
FROM (VALUES ('1'), ('infinity')) v(x);
SELECT sum(x::numeric), avg(x::numeric), var_pop(x::numeric)
FROM (VALUES ('infinity'), ('1')) v(x);
SELECT sum(x::numeric), avg(x::numeric), var_pop(x::numeric)
FROM (VALUES ('infinity'), ('infinity')) v(x);
SELECT sum(x::numeric), avg(x::numeric), var_pop(x::numeric)
FROM (VALUES ('-infinity'), ('infinity')) v(x);
SELECT sum(x::numeric), avg(x::numeric), var_pop(x::numeric)
FROM (VALUES ('-infinity'), ('-infinity')) v(x);
-- test accuracy with a large input offset
SELECT avg(x::float8), var_pop(x::float8)
FROM (VALUES (100000003), (100000004), (100000006), (100000007)) v(x);
SELECT avg(x::float8), var_pop(x::float8)
FROM (VALUES (7000000000005), (7000000000007)) v(x);
-- check single-tuple behavior
SELECT covar_pop(1::float8,2::float8), covar_samp(3::float8,4::float8);
-- test accum and combine functions directly
CREATE TABLE regr_test (x float8, y float8);
INSERT INTO regr_test VALUES (10,150),(20,250),(30,350),(80,540),(100,200);
SELECT float8_accum('{4,140,2900}'::float8[], 100);
SELECT float8_regr_accum('{4,140,2900,1290,83075,15050}'::float8[], 200, 100);
SELECT float8_combine('{3,60,200}'::float8[], '{0,0,0}'::float8[]);
SELECT float8_combine('{0,0,0}'::float8[], '{2,180,200}'::float8[]);
SELECT float8_combine('{3,60,200}'::float8[], '{2,180,200}'::float8[]);
SELECT float8_regr_combine('{3,60,200,750,20000,2000}'::float8[],
                           '{0,0,0,0,0,0}'::float8[]);
SELECT float8_regr_combine('{0,0,0,0,0,0}'::float8[],
                           '{2,180,200,740,57800,-3400}'::float8[]);
SELECT float8_regr_combine('{3,60,200,750,20000,2000}'::float8[],
                           '{2,180,200,740,57800,-3400}'::float8[]);
DROP TABLE regr_test;
--
-- test for bitwise integer aggregates
--
CREATE TEMPORARY TABLE bitwise_test(
  i2 INT2,
  i4 INT4,
  i8 INT8,
  i INTEGER,
  x INT2,
  y BIT(4)
);
CREATE TEMPORARY TABLE bool_test(
  b1 BOOL,
  b2 BOOL,
  b3 BOOL,
  b4 BOOL);
-- the planner may choose a generic aggregate here if parallel query is
-- enabled, since that plan will be parallel safe and the "optimized"
-- plan, which has almost identical cost, will not be.  we want to test
-- the optimized plan, so temporarily disable parallel query.
begin;
rollback;
-- try it on an inheritance tree
create table minmaxtest(f1 int);
create index minmaxtesti on minmaxtest(f1);
create index minmaxtest1i on minmaxtest1(f1);
create index minmaxtest2i on minmaxtest2(f1 desc);
insert into minmaxtest values(11), (12);
--
-- Test removal of redundant GROUP BY columns
--
create temp table t1 (a int, b int, c int, d int, primary key (a, b));
create temp table t2 (x int, y int, z int, primary key (x, y));
drop table t2;
