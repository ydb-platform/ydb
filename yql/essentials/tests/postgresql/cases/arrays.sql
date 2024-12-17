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
-- cross-datatype
select 33.4 = any (array[1,2,3]);
select 33.4 > all (array[1,2,3]);
-- nulls
select 33 = any (null::int[]);
select null::int = any ('{1,2,3}');
select 33 = any ('{1,null,3}');
select 33 = any ('{1,null,33}');
select 33 = all (null::int[]);
select null::int = all ('{1,2,3}');
select 33 = all ('{1,null,3}');
select 33 = all ('{33,null,33}');
-- nulls later in the bitmap
SELECT -1 != ALL(ARRAY(SELECT NULLIF(g.i, 900) FROM generate_series(1,1000) g(i)));
-- test indexes on arrays
create temp table arr_tbl (f1 int[] unique);
-- test ON CONFLICT DO UPDATE with arrays
create temp table arr_pk_tbl (pk int4 primary key, f1 int[]);
-- test [not] (like|ilike) (any|all) (...)
select 'foo' like any (array['%a', '%o']); -- t
select 'foo' like any (array['%a', '%b']); -- f
select 'foo' like all (array['f%', '%o']); -- t
select 'foo' like all (array['f%', '%b']); -- f
select 'foo' not like any (array['%a', '%b']); -- t
select 'foo' not like all (array['%a', '%o']); -- f
select 'foo' ilike any (array['%A', '%O']); -- t
select 'foo' ilike all (array['F%', '%O']); -- t
--
-- General array parser tests
--
-- none of the following should be accepted
select '{{1,{2}},{2,3}}'::text[];
select '{{},{}}'::text[];
select E'{{1,2},\\{2,3}}'::text[];
select '{{"1 2" x},{3}}'::text[];
select '{}}'::text[];
select '{ }}'::text[];
-- none of the above should be accepted
-- all of the following should be accepted
select '{}'::text[];
select '{{{1,2,3,4},{2,3,4,5}},{{3,4,5,6},{4,5,6,7}}}'::text[];
select '{ { "," } , { 3 } }'::text[];
select '  {   {  "  0 second  "   ,  0 second  }   }'::text[];
select '[0:1]={1.1,2.2}'::float8[];
-- all of the above should be accepted
-- tests for array aggregates
CREATE TEMP TABLE arraggtest ( f1 INT[], f2 TEXT[][], f3 FLOAT[]);
create table comptable (c1 comptype, c2 comptype[]);
drop table comptable;
select string_to_array('1|2|3', '|');
select string_to_array('1|2|3|', '|');
select string_to_array('1||2|3||', '||');
select string_to_array('1|2|3', '');
select string_to_array('', '|');
select string_to_array('1|2|3', NULL);
select string_to_array(NULL, '|') IS NULL;
select string_to_array('abc', '');
select string_to_array('abc', '', 'abc');
select string_to_array('abc', ',');
select string_to_array('abc', ',', 'abc');
select string_to_array('1,2,3,4,,6', ',');
select string_to_array('1,2,3,4,,6', ',', '');
select string_to_array('1,2,3,4,*,6', ',', '*');
select v, v is null as "is null" from string_to_table('1|2|3', '|') g(v);
select v, v is null as "is null" from string_to_table('1|2|3|', '|') g(v);
select v, v is null as "is null" from string_to_table('1||2|3||', '||') g(v);
select v, v is null as "is null" from string_to_table('1|2|3', '') g(v);
select v, v is null as "is null" from string_to_table('', '|') g(v);
select v, v is null as "is null" from string_to_table('1|2|3', NULL) g(v);
select v, v is null as "is null" from string_to_table(NULL, '|') g(v);
select v, v is null as "is null" from string_to_table('abc', '') g(v);
select v, v is null as "is null" from string_to_table('abc', '', 'abc') g(v);
select v, v is null as "is null" from string_to_table('abc', ',') g(v);
select v, v is null as "is null" from string_to_table('abc', ',', 'abc') g(v);
select v, v is null as "is null" from string_to_table('1,2,3,4,,6', ',') g(v);
select v, v is null as "is null" from string_to_table('1,2,3,4,,6', ',', '') g(v);
select v, v is null as "is null" from string_to_table('1,2,3,4,*,6', ',', '*') g(v);
select array_to_string(NULL::int4[], ',') IS NULL;
select array_to_string('{}'::int4[], ',');
select array_to_string(array[1,2,3,4,NULL,6], ',');
select array_to_string(array[1,2,3,4,NULL,6], ',', '*');
select array_to_string(array[1,2,3,4,NULL,6], NULL);
select array_to_string(array[1,2,3,4,NULL,6], ',', NULL);
select array_to_string(string_to_array('1|2|3', '|'), '|');
select array_length(array[1,2,3], 1);
select array_length(array[[1,2,3], [4,5,6]], 0);
select array_length(array[[1,2,3], [4,5,6]], 1);
select array_length(array[[1,2,3], [4,5,6]], 2);
select array_length(array[[1,2,3], [4,5,6]], 3);
select cardinality(NULL::int[]);
select cardinality('{}'::int[]);
select cardinality(array[1,2,3]);
select cardinality('[2:4]={5,6,7}'::int[]);
select cardinality('{{1,2}}'::int[]);
select cardinality('{{1,2},{3,4},{5,6}}'::int[]);
select cardinality('{{{1,9},{5,6}},{{2,3},{3,4}}}'::int[]);
select array_agg(unique1) from tenk1 where unique1 < -15;
-- array_agg(anyarray)
select array_agg(ar)
  from (values ('{1,2}'::int[]), ('{3,4}'::int[])) v(ar);
select array_agg(ar)
  from (select array_agg(array[i, i+1, i-1])
        from generate_series(1,2) a(i)) b(ar);
select array_agg(array[i+1.2, i+1.3, i+1.4]) from generate_series(1,3) g(i);
select array_agg(array['Hello', i::text]) from generate_series(9,11) g(i);
select array_agg(array[i, nullif(i, 3), i+1]) from generate_series(1,4) g(i);
-- errors
select array_agg('{}'::int[]) from generate_series(1,2);
select array_agg(null::int[]) from generate_series(1,2);
select array_agg(ar)
  from (values ('{1,2}'::int[]), ('{3}'::int[])) v(ar);
select * from unnest(array[1,2,3]);
-- array(select array-value ...)
select array(select array[i,i/2] from generate_series(1,5) i);
select array(select array['Hello', i::text] from generate_series(9,11) i);
-- Insert/update on a column that is array of composite
create temp table t1 (f1 int8_tbl[]);
-- Check that arrays of composites are safely detoasted when needed
create temp table src (f1 text);
insert into src
  select string_agg(random()::text,'') from generate_series(1,10000);
create temp table dest (f1 textandtext[]);
drop table src;
drop table dest;
-- trim_array
SELECT arr, trim_array(arr, 2)
FROM
(VALUES ('{1,2,3,4,5,6}'::bigint[]),
        ('{1,2}'),
        ('[10:16]={1,2,3,4,5,6,7}'),
        ('[-15:-10]={1,2,3,4,5,6}'),
        ('{{1,10},{2,20},{3,30},{4,40}}')) v(arr);
SELECT trim_array(ARRAY[1, 2, 3], -1); -- fail
SELECT trim_array(ARRAY[1, 2, 3], 10); -- fail
