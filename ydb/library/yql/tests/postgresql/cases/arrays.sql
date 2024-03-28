--
-- ARRAYS
--
CREATE TABLE arrtest (
	a 			int2[],
	b 			int4[][][],
	c 			name[],
	d			text[][],
	e 			float8[],
	f			char(5)[],
	g			varchar(5)[]
);
-- test mixed slice/scalar subscripting
select '{{1,2,3},{4,5,6},{7,8,9}}'::int[];
select '[0:2][0:2]={{1,2,3},{4,5,6},{7,8,9}}'::int[];
-- test slices with empty lower and/or upper index
CREATE TEMP TABLE arrtest_s (
  a       int2[],
  b       int2[][]
);
INSERT INTO arrtest_s VALUES(NULL, NULL);
--
-- test array extension
--
CREATE TEMP TABLE arrtest1 (i int[], t text[]);
--
-- array expressions and operators
--
-- table creation and INSERTs
CREATE TEMP TABLE arrtest2 (i integer ARRAY[4], f float8[], n numeric[], t text[], d timestamp[]);
-- some more test data
CREATE TEMP TABLE arrtest_f (f0 int, f1 text, f2 float8);
insert into arrtest_f values(1,'cat1',1.21);
insert into arrtest_f values(2,'cat1',1.24);
insert into arrtest_f values(3,'cat1',1.18);
insert into arrtest_f values(4,'cat1',1.26);
insert into arrtest_f values(5,'cat1',1.15);
insert into arrtest_f values(6,'cat2',1.15);
insert into arrtest_f values(7,'cat2',1.26);
insert into arrtest_f values(8,'cat2',1.32);
insert into arrtest_f values(9,'cat2',1.30);
CREATE TEMP TABLE arrtest_i (f0 int, f1 text, f2 int);
insert into arrtest_i values(1,'cat1',21);
insert into arrtest_i values(2,'cat1',24);
insert into arrtest_i values(3,'cat1',18);
insert into arrtest_i values(4,'cat1',26);
insert into arrtest_i values(5,'cat1',15);
insert into arrtest_i values(6,'cat2',15);
insert into arrtest_i values(7,'cat2',26);
insert into arrtest_i values(8,'cat2',32);
insert into arrtest_i values(9,'cat2',30);
SELECT ARRAY[[[[[['hello'],['world']]]]]];
SELECT ARRAY[ARRAY['hello'],ARRAY['world']];
-- with nulls
SELECT '{1,null,3}'::int[];
SELECT ARRAY[1,NULL,3];
SELECT NOT ARRAY[1.1,1.2,1.3] = ARRAY[1.1,1.2,1.3] AS "FALSE";
-- array casts
SELECT ARRAY[1,2,3]::text[]::int[]::float8[] AS "{1,2,3}";
SELECT pg_typeof(ARRAY[1,2,3]::text[]::int[]::float8[]) AS "double precision[]";
SELECT ARRAY[['a','bc'],['def','hijk']]::text[]::varchar[] AS "{{a,bc},{def,hijk}}";
SELECT pg_typeof(ARRAY[['a','bc'],['def','hijk']]::text[]::varchar[]) AS "character varying[]";
SELECT CAST(ARRAY[[[[[['a','bb','ccc']]]]]] as text[]) as "{{{{{{a,bb,ccc}}}}}}";
SELECT NULL::text[]::int[] AS "NULL";
-- scalar op any/all (array)
select 33 = any ('{1,2,3}');
select 33 = any ('{1,2,33}');
select 33 = all ('{1,2,33}');
select 33 >= all ('{1,2,33}');
-- boundary cases
select null::int >= all ('{1,2,33}');
select null::int >= all ('{}');
select null::int >= any ('{}');
