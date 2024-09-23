-- Strings.
SELECT '""'::json;				-- OK.
SELECT $$''$$::json;			-- ERROR, single quotes are not allowed
SELECT '"abc"'::json;			-- OK
SELECT '"abc'::json;			-- ERROR, quotes not closed
SELECT '"abc
def"'::json;					-- ERROR, unescaped newline in string constant
SELECT '"\n\"\\"'::json;		-- OK, legal escapes
SELECT '"\v"'::json;			-- ERROR, not a valid JSON escape
-- see json_encoding test for input with unicode escapes
-- Numbers.
SELECT '1'::json;				-- OK
SELECT '0'::json;				-- OK
SELECT '01'::json;				-- ERROR, not valid according to JSON spec
SELECT '0.1'::json;				-- OK
SELECT '9223372036854775808'::json;	-- OK, even though it's too large for int8
SELECT '1e100'::json;			-- OK
SELECT '1.3e100'::json;			-- OK
SELECT '1f2'::json;				-- ERROR
SELECT '0.x1'::json;			-- ERROR
SELECT '1.3ex100'::json;		-- ERROR
-- Arrays.
SELECT '[]'::json;				-- OK
SELECT '[1,2]'::json;			-- OK
SELECT '[1,2,]'::json;			-- ERROR, trailing comma
SELECT '[1,2'::json;			-- ERROR, no closing bracket
SELECT '[1,[2]'::json;			-- ERROR, no closing bracket
-- Objects.
SELECT '{}'::json;				-- OK
SELECT '{"abc"}'::json;			-- ERROR, no value
SELECT '{"abc":1}'::json;		-- OK
SELECT '{1:"abc"}'::json;		-- ERROR, keys must be strings
SELECT '{"abc",1}'::json;		-- ERROR, wrong separator
SELECT '{"abc"=1}'::json;		-- ERROR, totally wrong separator
SELECT '{"abc"::1}'::json;		-- ERROR, another wrong separator
SELECT '{"abc":1,"def":2,"ghi":[3,4],"hij":{"klm":5,"nop":[6]}}'::json; -- OK
SELECT '{"abc":1:2}'::json;		-- ERROR, colon in wrong spot
SELECT '{"abc":1,3}'::json;		-- ERROR, no value
-- Miscellaneous stuff.
SELECT 'true'::json;			-- OK
SELECT 'false'::json;			-- OK
SELECT 'null'::json;			-- OK
SELECT ' true '::json;			-- OK, even with extra whitespace
SELECT 'true false'::json;		-- ERROR, too many values
SELECT 'true, false'::json;		-- ERROR, too many values
SELECT 'truf'::json;			-- ERROR, not a keyword
SELECT 'trues'::json;			-- ERROR, not a keyword
SELECT ''::json;				-- ERROR, no value
SELECT '    '::json;			-- ERROR, no value
SELECT '{
		"one": 1,
		"two":,"two",  -- ERROR extraneous comma before field "two"
		"three":
		true}'::json;
SELECT '{
		"one": 1,
		"two":"two",
		"averyveryveryveryveryveryveryveryveryverylongfieldname":}'::json;
-- ERROR missing value for last field
--constructors
-- array_to_json
SELECT array_to_json(array(select 1 as a));
SELECT array_to_json(array_agg(x),false) from generate_series(5,10) x;
SELECT array_to_json('{{1,5},{99,100}}'::int[]);
BEGIN;
SET LOCAL TIME ZONE 10.5;
SET LOCAL TIME ZONE -8;
COMMIT;
-- non-numeric output
SELECT row_to_json(q)
FROM (SELECT 'NaN'::float8 AS "float8field") q;
SELECT row_to_json(q)
FROM (SELECT 'Infinity'::float8 AS "float8field") q;
SELECT row_to_json(q)
FROM (SELECT '-Infinity'::float8 AS "float8field") q;
-- json input
SELECT row_to_json(q)
FROM (SELECT '{"a":1,"b": [2,3,4,"d","e","f"],"c":{"p":1,"q":2}}'::json AS "jsonfield") q;
-- json extraction functions
CREATE TEMP TABLE test_json (
       json_type text,
       test_json json
);
INSERT INTO test_json VALUES
('scalar','"a scalar"'),
('array','["zero", "one","two",null,"four","five", [1,2,3],{"f1":9}]'),
('object','{"field1":"val1","field2":"val2","field3":null, "field4": 4, "field5": [1,2,3], "field6": {"f1":9}}');
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'scalar';
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'array';
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'object';
SELECT test_json->'field2'
FROM test_json
WHERE json_type = 'object';
SELECT test_json->>'field2'
FROM test_json
WHERE json_type = 'object';
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'scalar';
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'array';
SELECT test_json -> -1
FROM test_json
WHERE json_type = 'array';
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'object';
SELECT test_json->>2
FROM test_json
WHERE json_type = 'array';
SELECT test_json ->> 6 FROM test_json WHERE json_type = 'array';
SELECT test_json ->> 7 FROM test_json WHERE json_type = 'array';
SELECT test_json ->> 'field4' FROM test_json WHERE json_type = 'object';
SELECT test_json ->> 'field5' FROM test_json WHERE json_type = 'object';
SELECT test_json ->> 'field6' FROM test_json WHERE json_type = 'object';
-- nulls
select (test_json->'field3') is null as expect_false
from test_json
where json_type = 'object';
select (test_json->>'field3') is null as expect_true
from test_json
where json_type = 'object';
select (test_json->3) is null as expect_false
from test_json
where json_type = 'array';
select (test_json->>3) is null as expect_true
from test_json
where json_type = 'array';
-- corner cases
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::json -> null::text;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::json -> null::int;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::json -> 1;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::json -> -1;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::json -> 'z';
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::json -> '';
select '[{"b": "c"}, {"b": "cc"}]'::json -> 1;
select '[{"b": "c"}, {"b": "cc"}]'::json -> 3;
select '[{"b": "c"}, {"b": "cc"}]'::json -> 'z';
select '{"a": "c", "b": null}'::json -> 'b';
select '"foo"'::json -> 1;
select '"foo"'::json -> 'z';
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::json ->> null::text;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::json ->> null::int;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::json ->> 1;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::json ->> 'z';
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::json ->> '';
select '[{"b": "c"}, {"b": "cc"}]'::json ->> 1;
select '[{"b": "c"}, {"b": "cc"}]'::json ->> 3;
select '[{"b": "c"}, {"b": "cc"}]'::json ->> 'z';
select '{"a": "c", "b": null}'::json ->> 'b';
select '"foo"'::json ->> 1;
select '"foo"'::json ->> 'z';
-- array length
SELECT json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]');
SELECT json_array_length('[]');
SELECT json_array_length('{"f1":1,"f2":[5,6]}');
SELECT json_array_length('4');
select * from json_each('{"f1":[1,2,3],"f2":{"f3":1},"f4":null,"f5":99,"f6":"stringy"}') q;
select * from json_each_text('{"f1":[1,2,3],"f2":{"f3":1},"f4":null,"f5":99,"f6":"stringy"}') q;
-- extract_path, extract_path_as_text
select json_extract_path('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f4','f6');
select json_extract_path('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f2');
select json_extract_path('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',0::text);
select json_extract_path('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',1::text);
select json_extract_path_text('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f4','f6');
select json_extract_path_text('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f2');
select json_extract_path_text('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',0::text);
select json_extract_path_text('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',1::text);
-- extract_path nulls
select json_extract_path('{"f2":{"f3":1},"f4":{"f5":null,"f6":"stringy"}}','f4','f5') is null as expect_false;
select json_extract_path_text('{"f2":{"f3":1},"f4":{"f5":null,"f6":"stringy"}}','f4','f5') is null as expect_true;
select json_extract_path('{"f2":{"f3":1},"f4":[0,1,2,null]}','f4','3') is null as expect_false;
select json_extract_path_text('{"f2":{"f3":1},"f4":[0,1,2,null]}','f4','3') is null as expect_true;
select * from json_array_elements('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false,"stringy"]') q;
select * from json_array_elements_text('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false,"stringy"]') q;
-- test type info caching in json_populate_record()
CREATE TEMP TABLE jspoptest (js json);
INSERT INTO jspoptest
SELECT '{
	"jsa": [1, "2", null, 4],
	"rec": {"a": "abc", "c": "01.02.2003", "x": 43.2},
	"reca": [{"a": "abc", "b": 456}, null, {"c": "01.02.2003", "x": 43.2}]
}'::json
FROM generate_series(1, 3);
--json_typeof() function
select value, json_typeof(value)
  from (values (json '123.4'),
               (json '-1'),
               (json '"foo"'),
               (json 'true'),
               (json 'false'),
               (json 'null'),
               (json '[1, 2, 3]'),
               (json '[]'),
               (json '{"x":"foo", "y":123}'),
               (json '{}'),
               (NULL::json))
      as data(value);
-- json_build_array, json_build_object, json_object_agg
SELECT json_build_array('a',1,'b',1.2,'c',true,'d',null,'e',json '{"x": 3, "y": [1,2,3]}');
SELECT json_build_array('a', NULL); -- ok
SELECT json_build_object('a',1,'b',1.2,'c',true,'d',null,'e',json '{"x": 3, "y": [1,2,3]}');
SELECT json_build_object('{a,b,c}'::text[]); -- error
SELECT json_build_object('{a,b,c}'::text[], '{d,e,f}'::text[]); -- error, key cannot be array
SELECT json_build_object('a', 'b', 'c'); -- error
SELECT json_build_object(NULL, 'a'); -- error, key cannot be NULL
SELECT json_build_object('a', NULL); -- ok
-- empty objects/arrays
SELECT json_build_array();
SELECT json_build_object();
-- make sure keys are quoted
SELECT json_build_object(1,2);
-- keys must be scalar and not null
SELECT json_build_object(null,2);
SELECT json_build_object(r,2) FROM (SELECT 1 AS a, 2 AS b) r;
SELECT json_build_object(json '{"a":1,"b":2}', 3);
SELECT json_build_object('{1,2,3}'::int[], 3);
CREATE TEMP TABLE foo (serial_num int, name text, type text);
INSERT INTO foo VALUES (847001,'t15','GE1043');
INSERT INTO foo VALUES (847002,'t16','GE1043');
INSERT INTO foo VALUES (847003,'sub-alpha','GESS90');
SELECT json_build_object('turbines',json_object_agg(serial_num,json_build_object('name',name,'type',type)))
FROM foo;
SELECT json_object_agg(name, type) FROM foo;
INSERT INTO foo VALUES (999999, NULL, 'bar');
SELECT json_object_agg(name, type) FROM foo;
-- json_object
-- empty object, one dimension
SELECT json_object('{}');
-- empty object, two dimensions
SELECT json_object('{}', '{}');
-- one dimension
SELECT json_object('{a,1,b,2,3,NULL,"d e f","a b c"}');
-- same but with two dimensions
SELECT json_object('{{a,1},{b,2},{3,NULL},{"d e f","a b c"}}');
-- odd number error
SELECT json_object('{a,b,c}');
-- one column error
SELECT json_object('{{a},{b}}');
-- too many columns error
SELECT json_object('{{a,b,c},{b,c,d}}');
-- too many dimensions error
SELECT json_object('{{{a,b},{c,d}},{{b,c},{d,e}}}');
--two argument form of json_object
select json_object('{a,b,c,"d e f"}','{1,2,3,"a b c"}');
-- too many dimensions
SELECT json_object('{{a,1},{b,2},{3,NULL},{"d e f","a b c"}}', '{{a,1},{b,2},{3,NULL},{"d e f","a b c"}}');
-- mismatched dimensions
select json_object('{a,b,c,"d e f",g}','{1,2,3,"a b c"}');
select json_object('{a,b,c,"d e f"}','{1,2,3,"a b c",g}');
-- null key error
select json_object('{a,b,NULL,"d e f"}','{1,2,3,"a b c"}');
-- empty key is allowed
select json_object('{a,b,"","d e f"}','{1,2,3,"a b c"}');
-- json_strip_nulls
select json_strip_nulls(null);
select json_strip_nulls('1');
select json_strip_nulls('"a string"');
select json_strip_nulls('null');
select json_strip_nulls('[1,2,null,3,4]');
select json_strip_nulls('{"a":1,"b":null,"c":[2,null,3],"d":{"e":4,"f":null}}');
select json_strip_nulls('[1,{"a":1,"b":null,"c":2},3]');
-- an empty object is not null and should not be stripped
select json_strip_nulls('{"a": {"b": null, "c": null}, "d": {} }');
