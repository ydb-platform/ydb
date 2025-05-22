--
-- AGGREGATES
--
-- avoid bit-exact output here because operations may not be bit-exact.
SET extra_float_digits = 0;
SELECT avg(four) AS avg_1 FROM onek;
SELECT avg(a) AS avg_32 FROM aggtest WHERE a < 100;
-- In 7.1, avg(float4) is computed using float8 arithmetic.
-- Round the result to 3 digits to avoid platform-specific results.
SELECT avg(b)::numeric(10,3) AS avg_107_943 FROM aggtest;
SELECT avg(gpa) AS avg_3_4 FROM ONLY student;
SELECT sum(four) AS sum_1500 FROM onek;
SELECT sum(a) AS sum_198 FROM aggtest;
SELECT sum(gpa) AS avg_6_8 FROM ONLY student;
SELECT max(four) AS max_3 FROM onek;
SELECT max(a) AS max_100 FROM aggtest;
SELECT max(aggtest.b) AS max_324_78 FROM aggtest;
SELECT max(student.gpa) AS max_3_7 FROM student;
SELECT stddev_pop(b) FROM aggtest;
SELECT stddev_samp(b) FROM aggtest;
SELECT var_pop(b) FROM aggtest;
SELECT var_samp(b) FROM aggtest;
SELECT stddev_pop(b::numeric) FROM aggtest;
SELECT stddev_samp(b::numeric) FROM aggtest;
SELECT var_pop(b::numeric) FROM aggtest;
SELECT var_samp(b::numeric) FROM aggtest;
-- population variance is defined for a single tuple, sample variance
-- is not
SELECT var_pop(1.0::float8), var_samp(2.0::float8);
SELECT stddev_pop(3.0::float8), stddev_samp(4.0::float8);
SELECT var_pop('inf'::float8), var_samp('inf'::float8);
SELECT stddev_pop('inf'::float8), stddev_samp('inf'::float8);
SELECT var_pop('nan'::float8), var_samp('nan'::float8);
SELECT stddev_pop('nan'::float8), stddev_samp('nan'::float8);
SELECT var_pop(1.0::float4), var_samp(2.0::float4);
SELECT stddev_pop(3.0::float4), stddev_samp(4.0::float4);
SELECT var_pop('inf'::float4), var_samp('inf'::float4);
SELECT stddev_pop('inf'::float4), stddev_samp('inf'::float4);
SELECT var_pop('nan'::float4), var_samp('nan'::float4);
SELECT stddev_pop('nan'::float4), stddev_samp('nan'::float4);
SELECT var_pop('inf'::numeric), var_samp('inf'::numeric);
SELECT stddev_pop('inf'::numeric), stddev_samp('inf'::numeric);
SELECT var_pop('nan'::numeric), var_samp('nan'::numeric);
SELECT stddev_pop('nan'::numeric), stddev_samp('nan'::numeric);
-- verify correct results for null and NaN inputs
select sum(null::int4) from generate_series(1,3);
select sum(null::int8) from generate_series(1,3);
select sum(null::numeric) from generate_series(1,3);
select sum(null::float8) from generate_series(1,3);
select avg(null::int4) from generate_series(1,3);
select avg(null::int8) from generate_series(1,3);
select avg(null::numeric) from generate_series(1,3);
select avg(null::float8) from generate_series(1,3);
select sum('NaN'::numeric) from generate_series(1,3);
select avg('NaN'::numeric) from generate_series(1,3);
-- verify correct results for infinite inputs
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
FROM (VALUES ('1'), ('infinity')) v(x);
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
FROM (VALUES ('infinity'), ('1')) v(x);
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
FROM (VALUES ('infinity'), ('infinity')) v(x);
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
FROM (VALUES ('-infinity'), ('infinity')) v(x);
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
FROM (VALUES ('-infinity'), ('-infinity')) v(x);
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
-- SQL2003 binary aggregates
SELECT regr_count(b, a) FROM aggtest;
SELECT regr_sxx(b, a) FROM aggtest;
SELECT regr_syy(b, a) FROM aggtest;
SELECT regr_sxy(b, a) FROM aggtest;
SELECT regr_avgx(b, a), regr_avgy(b, a) FROM aggtest;
SELECT regr_r2(b, a) FROM aggtest;
SELECT regr_slope(b, a), regr_intercept(b, a) FROM aggtest;
SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest;
SELECT corr(b, a) FROM aggtest;
-- check single-tuple behavior
SELECT covar_pop(1::float8,2::float8), covar_samp(3::float8,4::float8);
SELECT covar_pop(1::float8,'inf'::float8), covar_samp(3::float8,'inf'::float8);
SELECT covar_pop(1::float8,'nan'::float8), covar_samp(3::float8,'nan'::float8);
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
-- test count, distinct
SELECT count(four) AS cnt_1000 FROM onek;
SELECT count(DISTINCT four) AS cnt_4 FROM onek;
select ten, count(*), sum(four) from onek
group by ten order by ten;
select ten, count(four), sum(DISTINCT four) from onek
group by ten order by ten;
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
select min(unique1) from tenk1;
select max(unique1) from tenk1;
select max(unique1) from tenk1 where unique1 < 42;
select max(unique1) from tenk1 where unique1 > 42;
-- the planner may choose a generic aggregate here if parallel query is
-- enabled, since that plan will be parallel safe and the "optimized"
-- plan, which has almost identical cost, will not be.  we want to test
-- the optimized plan, so temporarily disable parallel query.
begin;
select max(unique1) from tenk1 where unique1 > 42000;
rollback;
select max(tenthous) from tenk1 where thousand = 33;
select min(tenthous) from tenk1 where thousand = 33;
select distinct max(unique2) from tenk1;
select max(unique2) from tenk1 order by 1;
select max(unique2) from tenk1 order by max(unique2);
select max(unique2) from tenk1 order by max(unique2)+1;
select max(100) from tenk1;
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
--
-- Test GROUP BY matching of join columns that are type-coerced due to USING
--
create temp table t1(f1 int, f2 bigint);
create temp table t2(f1 bigint, f22 bigint);
drop table t1, t2;
select array_agg(distinct a)
  from (values (1),(2),(1),(3),(null),(2)) v(a);
-- string_agg tests
select string_agg(a,',') from (values('aaaa'),('bbbb'),('cccc')) g(a);
select string_agg(a,',') from (values('aaaa'),(null),('bbbb'),('cccc')) g(a);
select string_agg(a,'AB') from (values(null),(null),('bbbb'),('cccc')) g(a);
select string_agg(a,',') from (values(null),(null)) g(a);
-- string_agg bytea tests
create table bytea_test_table(v bytea);
select string_agg(v, '') from bytea_test_table;
insert into bytea_test_table values(decode('ff','hex'));
select string_agg(v, '') from bytea_test_table;
insert into bytea_test_table values(decode('aa','hex'));
select string_agg(v, '') from bytea_test_table;
select string_agg(v, NULL) from bytea_test_table;
select string_agg(v, decode('ee', 'hex')) from bytea_test_table;
drop table bytea_test_table;
-- outer reference in FILTER (PostgreSQL extension)
select (select count(*)
        from (values (1)) t0(inner_c))
from (values (2),(3)) t1(outer_c); -- inner query is aggregation query
select p, percentile_cont(p order by p) within group (order by x)  -- error
from generate_series(1,5) x,
     (values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1)) v(p)
group by p order by p;
-- test aggregates with common transition functions share the same states
begin work;
