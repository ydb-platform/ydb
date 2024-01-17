-- Strings.
SELECT '""'::jsonb;				-- OK.
SELECT $$''$$::jsonb;			-- ERROR, single quotes are not allowed
SELECT '"abc"'::jsonb;			-- OK
SELECT '"abc'::jsonb;			-- ERROR, quotes not closed
SELECT '"abc
def"'::jsonb;					-- ERROR, unescaped newline in string constant
SELECT '"\n\"\\"'::jsonb;		-- OK, legal escapes
SELECT '"\v"'::jsonb;			-- ERROR, not a valid JSON escape
-- see json_encoding test for input with unicode escapes
-- Numbers.
SELECT '1'::jsonb;				-- OK
SELECT '0'::jsonb;				-- OK
SELECT '01'::jsonb;				-- ERROR, not valid according to JSON spec
SELECT '0.1'::jsonb;				-- OK
SELECT '9223372036854775808'::jsonb;	-- OK, even though it's too large for int8
SELECT '1f2'::jsonb;				-- ERROR
SELECT '0.x1'::jsonb;			-- ERROR
SELECT '1.3ex100'::jsonb;		-- ERROR
-- Arrays.
SELECT '[]'::jsonb;				-- OK
SELECT '[1,2]'::jsonb;			-- OK
SELECT '[1,2,]'::jsonb;			-- ERROR, trailing comma
SELECT '[1,2'::jsonb;			-- ERROR, no closing bracket
SELECT '[1,[2]'::jsonb;			-- ERROR, no closing bracket
-- Objects.
SELECT '{}'::jsonb;				-- OK
SELECT '{"abc"}'::jsonb;			-- ERROR, no value
SELECT '{"abc":1}'::jsonb;		-- OK
SELECT '{1:"abc"}'::jsonb;		-- ERROR, keys must be strings
SELECT '{"abc",1}'::jsonb;		-- ERROR, wrong separator
SELECT '{"abc"=1}'::jsonb;		-- ERROR, totally wrong separator
SELECT '{"abc"::1}'::jsonb;		-- ERROR, another wrong separator
SELECT '{"abc":1,"def":2,"ghi":[3,4],"hij":{"klm":5,"nop":[6]}}'::jsonb; -- OK
SELECT '{"abc":1:2}'::jsonb;		-- ERROR, colon in wrong spot
SELECT '{"abc":1,3}'::jsonb;		-- ERROR, no value
-- Miscellaneous stuff.
SELECT 'true'::jsonb;			-- OK
SELECT 'false'::jsonb;			-- OK
SELECT 'null'::jsonb;			-- OK
SELECT ' true '::jsonb;			-- OK, even with extra whitespace
SELECT 'true false'::jsonb;		-- ERROR, too many values
SELECT 'true, false'::jsonb;		-- ERROR, too many values
SELECT 'truf'::jsonb;			-- ERROR, not a keyword
SELECT 'trues'::jsonb;			-- ERROR, not a keyword
SELECT ''::jsonb;				-- ERROR, no value
SELECT '    '::jsonb;			-- ERROR, no value
-- Multi-line JSON input to check ERROR reporting
SELECT '{
		"one": 1,
		"two":"two",
		"three":
		true}'::jsonb; -- OK
SELECT '{
		"one": 1,
		"two":,"two",  -- ERROR extraneous comma before field "two"
		"three":
		true}'::jsonb;
SELECT '{
		"one": 1,
		"two":"two",
		"averyveryveryveryveryveryveryveryveryverylongfieldname":}'::jsonb;
-- ERROR missing value for last field
-- make sure jsonb is passed through json generators without being escaped
SELECT array_to_json(ARRAY [jsonb '{"a":1}', jsonb '{"b":[2,3]}']);
BEGIN;
COMMIT;
-- jsonb extraction functions
CREATE TEMP TABLE test_jsonb (
       json_type text,
       test_json jsonb
);
INSERT INTO test_jsonb VALUES
('scalar','"a scalar"'),
('array','["zero", "one","two",null,"four","five", [1,2,3],{"f1":9}]'),
('object','{"field1":"val1","field2":"val2","field3":null, "field4": 4, "field5": [1,2,3], "field6": {"f1":9}}');
-- corner cases
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::jsonb -> null::text;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::jsonb -> null::int;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::jsonb -> 1;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::jsonb -> 'z';
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::jsonb -> '';
select '[{"b": "c"}, {"b": "cc"}]'::jsonb -> 1;
select '[{"b": "c"}, {"b": "cc"}]'::jsonb -> 3;
select '[{"b": "c"}, {"b": "cc"}]'::jsonb -> 'z';
select '{"a": "c", "b": null}'::jsonb -> 'b';
select '"foo"'::jsonb -> 1;
select '"foo"'::jsonb -> 'z';
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::jsonb ->> null::text;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::jsonb ->> null::int;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::jsonb ->> 1;
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::jsonb ->> 'z';
select '{"a": [{"b": "c"}, {"b": "cc"}]}'::jsonb ->> '';
select '[{"b": "c"}, {"b": "cc"}]'::jsonb ->> 1;
select '[{"b": "c"}, {"b": "cc"}]'::jsonb ->> 3;
select '[{"b": "c"}, {"b": "cc"}]'::jsonb ->> 'z';
select '{"a": "c", "b": null}'::jsonb ->> 'b';
select '"foo"'::jsonb ->> 1;
select '"foo"'::jsonb ->> 'z';
-- equality and inequality
SELECT '{"x":"y"}'::jsonb = '{"x":"y"}'::jsonb;
SELECT '{"x":"y"}'::jsonb = '{"x":"z"}'::jsonb;
SELECT '{"x":"y"}'::jsonb <> '{"x":"y"}'::jsonb;
SELECT '{"x":"y"}'::jsonb <> '{"x":"z"}'::jsonb;
-- containment
SELECT jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"b"}');
SELECT jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"b", "c":null}');
SELECT jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"b", "g":null}');
SELECT jsonb_contains('{"a":"b", "b":1, "c":null}', '{"g":null}');
SELECT jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"c"}');
SELECT jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"b"}');
SELECT jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"b", "c":"q"}');
SELECT '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"b"}';
SELECT '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"b", "c":null}';
SELECT '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"b", "g":null}';
SELECT '{"a":"b", "b":1, "c":null}'::jsonb @> '{"g":null}';
SELECT '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"c"}';
SELECT '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"b"}';
SELECT '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"b", "c":"q"}';
SELECT '[1,2]'::jsonb @> '[1,2,2]'::jsonb;
SELECT '[1,1,2]'::jsonb @> '[1,2,2]'::jsonb;
SELECT '[[1,2]]'::jsonb @> '[[1,2,2]]'::jsonb;
SELECT '[1,2,2]'::jsonb <@ '[1,2]'::jsonb;
SELECT '[1,2,2]'::jsonb <@ '[1,1,2]'::jsonb;
SELECT '[[1,2,2]]'::jsonb <@ '[[1,2]]'::jsonb;
SELECT jsonb_contained('{"a":"b"}', '{"a":"b", "b":1, "c":null}');
SELECT jsonb_contained('{"a":"b", "c":null}', '{"a":"b", "b":1, "c":null}');
SELECT jsonb_contained('{"a":"b", "g":null}', '{"a":"b", "b":1, "c":null}');
SELECT jsonb_contained('{"g":null}', '{"a":"b", "b":1, "c":null}');
SELECT jsonb_contained('{"a":"c"}', '{"a":"b", "b":1, "c":null}');
SELECT jsonb_contained('{"a":"b"}', '{"a":"b", "b":1, "c":null}');
SELECT jsonb_contained('{"a":"b", "c":"q"}', '{"a":"b", "b":1, "c":null}');
SELECT '{"a":"b"}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
SELECT '{"a":"b", "c":null}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
SELECT '{"a":"b", "g":null}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
SELECT '{"g":null}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
SELECT '{"a":"c"}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
SELECT '{"a":"b"}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
SELECT '{"a":"b", "c":"q"}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
-- Raw scalar may contain another raw scalar, array may contain a raw scalar
SELECT '[5]'::jsonb @> '[5]';
SELECT '5'::jsonb @> '5';
SELECT '[5]'::jsonb @> '5';
-- But a raw scalar cannot contain an array
SELECT '5'::jsonb @> '[5]';
-- In general, one thing should always contain itself. Test array containment:
SELECT '["9", ["7", "3"], 1]'::jsonb @> '["9", ["7", "3"], 1]'::jsonb;
SELECT '["9", ["7", "3"], ["1"]]'::jsonb @> '["9", ["7", "3"], ["1"]]'::jsonb;
-- array containment string matching confusion bug
SELECT '{ "name": "Bob", "tags": [ "enim", "qui"]}'::jsonb @> '{"tags":["qu"]}';
-- array length
SELECT jsonb_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]');
SELECT jsonb_array_length('[]');
SELECT jsonb_array_length('{"f1":1,"f2":[5,6]}');
SELECT jsonb_array_length('4');
SELECT * FROM jsonb_each('{"f1":[1,2,3],"f2":{"f3":1},"f4":null,"f5":99,"f6":"stringy"}') q;
SELECT * FROM jsonb_each('{"a":{"b":"c","c":"b","1":"first"},"b":[1,2],"c":"cc","1":"first","n":null}'::jsonb) AS q;
SELECT * FROM jsonb_each_text('{"f1":[1,2,3],"f2":{"f3":1},"f4":null,"f5":99,"f6":"stringy"}') q;
SELECT * FROM jsonb_each_text('{"a":{"b":"c","c":"b","1":"first"},"b":[1,2],"c":"cc","1":"first","n":null}'::jsonb) AS q;
-- exists
SELECT jsonb_exists('{"a":null, "b":"qq"}', 'a');
SELECT jsonb_exists('{"a":null, "b":"qq"}', 'b');
SELECT jsonb_exists('{"a":null, "b":"qq"}', 'c');
SELECT jsonb_exists('{"a":"null", "b":"qq"}', 'a');
SELECT jsonb '{"a":null, "b":"qq"}' ? 'a';
SELECT jsonb '{"a":null, "b":"qq"}' ? 'b';
SELECT jsonb '{"a":null, "b":"qq"}' ? 'c';
SELECT jsonb '{"a":"null", "b":"qq"}' ? 'a';
SELECT jsonb_exists_any('{"a":null, "b":"qq"}', ARRAY['a','b']);
SELECT jsonb_exists_any('{"a":null, "b":"qq"}', ARRAY['b','a']);
SELECT jsonb_exists_any('{"a":null, "b":"qq"}', ARRAY['c','a']);
SELECT jsonb_exists_any('{"a":null, "b":"qq"}', ARRAY['c','d']);
SELECT jsonb_exists_any('{"a":null, "b":"qq"}', '{}'::text[]);
SELECT jsonb '{"a":null, "b":"qq"}' ?| ARRAY['a','b'];
SELECT jsonb '{"a":null, "b":"qq"}' ?| ARRAY['b','a'];
SELECT jsonb '{"a":null, "b":"qq"}' ?| ARRAY['c','a'];
SELECT jsonb '{"a":null, "b":"qq"}' ?| ARRAY['c','d'];
SELECT jsonb '{"a":null, "b":"qq"}' ?| '{}'::text[];
SELECT jsonb_exists_all('{"a":null, "b":"qq"}', ARRAY['a','b']);
SELECT jsonb_exists_all('{"a":null, "b":"qq"}', ARRAY['b','a']);
SELECT jsonb_exists_all('{"a":null, "b":"qq"}', ARRAY['c','a']);
SELECT jsonb_exists_all('{"a":null, "b":"qq"}', ARRAY['c','d']);
SELECT jsonb_exists_all('{"a":null, "b":"qq"}', '{}'::text[]);
SELECT jsonb '{"a":null, "b":"qq"}' ?& ARRAY['a','b'];
SELECT jsonb '{"a":null, "b":"qq"}' ?& ARRAY['b','a'];
SELECT jsonb '{"a":null, "b":"qq"}' ?& ARRAY['c','a'];
SELECT jsonb '{"a":null, "b":"qq"}' ?& ARRAY['c','d'];
SELECT jsonb '{"a":null, "b":"qq"}' ?& ARRAY['a','a', 'b', 'b', 'b'];
SELECT jsonb '{"a":null, "b":"qq"}' ?& '{}'::text[];
-- typeof
SELECT jsonb_typeof('{}') AS object;
SELECT jsonb_typeof('{"c":3,"p":"o"}') AS object;
SELECT jsonb_typeof('[]') AS array;
SELECT jsonb_typeof('["a", 1]') AS array;
SELECT jsonb_typeof('null') AS "null";
SELECT jsonb_typeof('1') AS number;
SELECT jsonb_typeof('-1') AS number;
SELECT jsonb_typeof('1.0') AS number;
SELECT jsonb_typeof('1e2') AS number;
SELECT jsonb_typeof('-1.0') AS number;
SELECT jsonb_typeof('true') AS boolean;
SELECT jsonb_typeof('false') AS boolean;
SELECT jsonb_typeof('"hello"') AS string;
SELECT jsonb_typeof('"true"') AS string;
SELECT jsonb_typeof('"1.0"') AS string;
-- empty objects/arrays
SELECT jsonb_build_array();
SELECT jsonb_build_object();
-- handling of NULL values
SELECT jsonb_object_agg(1, NULL::jsonb);
SELECT jsonb_object_agg(NULL, '{"a":1}');
CREATE TEMP TABLE foo (serial_num int, name text, type text);
INSERT INTO foo VALUES (847001,'t15','GE1043');
INSERT INTO foo VALUES (847002,'t16','GE1043');
INSERT INTO foo VALUES (847003,'sub-alpha','GESS90');
INSERT INTO foo VALUES (999999, NULL, 'bar');
SELECT jsonb_object_agg(name, type) FROM foo;
-- jsonb_object
-- empty object, one dimension
SELECT jsonb_object('{}');
-- empty object, two dimensions
SELECT jsonb_object('{}', '{}');
-- one dimension
SELECT jsonb_object('{a,1,b,2,3,NULL,"d e f","a b c"}');
-- same but with two dimensions
SELECT jsonb_object('{{a,1},{b,2},{3,NULL},{"d e f","a b c"}}');
-- odd number error
SELECT jsonb_object('{a,b,c}');
-- one column error
SELECT jsonb_object('{{a},{b}}');
-- too many columns error
SELECT jsonb_object('{{a,b,c},{b,c,d}}');
-- too many dimensions error
SELECT jsonb_object('{{{a,b},{c,d}},{{b,c},{d,e}}}');
--two argument form of jsonb_object
select jsonb_object('{a,b,c,"d e f"}','{1,2,3,"a b c"}');
-- too many dimensions
SELECT jsonb_object('{{a,1},{b,2},{3,NULL},{"d e f","a b c"}}', '{{a,1},{b,2},{3,NULL},{"d e f","a b c"}}');
-- mismatched dimensions
select jsonb_object('{a,b,c,"d e f",g}','{1,2,3,"a b c"}');
select jsonb_object('{a,b,c,"d e f"}','{1,2,3,"a b c",g}');
-- null key error
select jsonb_object('{a,b,NULL,"d e f"}','{1,2,3,"a b c"}');
-- empty key is allowed
select jsonb_object('{a,b,"","d e f"}','{1,2,3,"a b c"}');
SELECT * FROM jsonb_array_elements('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false]') q;
SELECT * FROM jsonb_array_elements_text('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false,"stringy"]') q;
-- test type info caching in jsonb_populate_record()
CREATE TEMP TABLE jsbpoptest (js jsonb);
INSERT INTO jsbpoptest
SELECT '{
	"jsa": [1, "2", null, 4],
	"rec": {"a": "abc", "c": "01.02.2003", "x": 43.2},
	"reca": [{"a": "abc", "b": 456}, null, {"c": "01.02.2003", "x": 43.2}]
}'::jsonb
FROM generate_series(1, 3);
CREATE INDEX jidx ON testjsonb USING gin (j);
-- btree
CREATE INDEX jidx ON testjsonb USING btree (j);
CREATE INDEX jidx ON testjsonb USING gin (j jsonb_path_ops);
-- nested tests
SELECT '{"ff":{"a":12,"b":16}}'::jsonb;
SELECT '{"ff":{"a":12,"b":16},"qq":123}'::jsonb;
SELECT '{"aa":["a","aaa"],"qq":{"a":12,"b":16,"c":["c1","c2"],"d":{"d1":"d1","d2":"d2","d1":"d3"}}}'::jsonb;
SELECT '{"ff":["a","aaa"]}'::jsonb;
SELECT
  '{"ff":{"a":12,"b":16},"qq":123,"x":[1,2],"Y":null}'::jsonb -> 'ff',
  '{"ff":{"a":12,"b":16},"qq":123,"x":[1,2],"Y":null}'::jsonb -> 'qq',
  ('{"ff":{"a":12,"b":16},"qq":123,"x":[1,2],"Y":null}'::jsonb -> 'Y') IS NULL AS f,
  ('{"ff":{"a":12,"b":16},"qq":123,"x":[1,2],"Y":null}'::jsonb ->> 'Y') IS NULL AS t,
   '{"ff":{"a":12,"b":16},"qq":123,"x":[1,2],"Y":null}'::jsonb -> 'x';
-- nested containment
SELECT '{"a":[1,2],"c":"b"}'::jsonb @> '{"a":[1,2]}';
SELECT '{"a":[2,1],"c":"b"}'::jsonb @> '{"a":[1,2]}';
SELECT '{"a":{"1":2},"c":"b"}'::jsonb @> '{"a":[1,2]}';
SELECT '{"a":{"2":1},"c":"b"}'::jsonb @> '{"a":[1,2]}';
SELECT '{"a":{"1":2},"c":"b"}'::jsonb @> '{"a":{"1":2}}';
SELECT '{"a":{"2":1},"c":"b"}'::jsonb @> '{"a":{"1":2}}';
SELECT '["a","b"]'::jsonb @> '["a","b","c","b"]';
SELECT '["a","b","c","b"]'::jsonb @> '["a","b"]';
SELECT '["a","b","c",[1,2]]'::jsonb @> '["a",[1,2]]';
SELECT '["a","b","c",[1,2]]'::jsonb @> '["b",[1,2]]';
SELECT '{"a":[1,2],"c":"b"}'::jsonb @> '{"a":[1]}';
SELECT '{"a":[1,2],"c":"b"}'::jsonb @> '{"a":[2]}';
SELECT '{"a":[1,2],"c":"b"}'::jsonb @> '{"a":[3]}';
SELECT '{"a":[1,2,{"c":3,"x":4}],"c":"b"}'::jsonb @> '{"a":[{"c":3}]}';
SELECT '{"a":[1,2,{"c":3,"x":4}],"c":"b"}'::jsonb @> '{"a":[{"x":4}]}';
SELECT '{"a":[1,2,{"c":3,"x":4}],"c":"b"}'::jsonb @> '{"a":[{"x":4},3]}';
SELECT '{"a":[1,2,{"c":3,"x":4}],"c":"b"}'::jsonb @> '{"a":[{"x":4},1]}';
-- check some corner cases for indexed nested containment (bug #13756)
create temp table nestjsonb (j jsonb);
insert into nestjsonb (j) values ('{"a":[["b",{"x":1}],["b",{"x":2}]],"c":3}');
insert into nestjsonb (j) values ('[[14,2,3]]');
insert into nestjsonb (j) values ('[1,[14,2,3]]');
create index on nestjsonb using gin(j jsonb_path_ops);
select * from nestjsonb where j @> '{"a":[[{"x":2}]]}'::jsonb;
select * from nestjsonb where j @> '{"c":3}';
select * from nestjsonb where j @> '[[14]]';
select * from nestjsonb where j @> '{"a":[[{"x":2}]]}'::jsonb;
select * from nestjsonb where j @> '{"c":3}';
select * from nestjsonb where j @> '[[14]]';
-- nested object field / array index lookup
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb -> 'n';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb -> 'a';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb -> 'b';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb -> 'c';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb -> 'd';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb -> 'd' -> '1';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb -> 'e';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb -> 0; --expecting error
SELECT '["a","b","c",[1,2],null]'::jsonb -> 0;
SELECT '["a","b","c",[1,2],null]'::jsonb -> 1;
SELECT '["a","b","c",[1,2],null]'::jsonb -> 2;
SELECT '["a","b","c",[1,2],null]'::jsonb -> 3;
SELECT '["a","b","c",[1,2],null]'::jsonb -> 3 -> 1;
SELECT '["a","b","c",[1,2],null]'::jsonb -> 4;
SELECT '["a","b","c",[1,2],null]'::jsonb -> 5;
SELECT '["a","b","c",[1,2],null]'::jsonb -> -1;
SELECT '["a","b","c",[1,2],null]'::jsonb -> -5;
SELECT '["a","b","c",[1,2],null]'::jsonb -> -6;
--nested exists
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb ? 'n';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb ? 'a';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb ? 'b';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb ? 'c';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb ? 'd';
SELECT '{"n":null,"a":1,"b":[1,2],"c":{"1":2},"d":{"1":[2,3]}}'::jsonb ? 'e';
-- jsonb_strip_nulls
select jsonb_strip_nulls(null);
select jsonb_strip_nulls('1');
select jsonb_strip_nulls('"a string"');
select jsonb_strip_nulls('null');
select jsonb_strip_nulls('[1,2,null,3,4]');
select jsonb_strip_nulls('{"a":1,"b":null,"c":[2,null,3],"d":{"e":4,"f":null}}');
select jsonb_strip_nulls('[1,{"a":1,"b":null,"c":2},3]');
-- an empty object is not null and should not be stripped
select jsonb_strip_nulls('{"a": {"b": null, "c": null}, "d": {} }');
select jsonb_concat('{"d": "test", "a": [1, 2]}', '{"g": "test2", "c": {"c1":1, "c2":2}}');
select '{"aa":1 , "b":2, "cq":3}'::jsonb || '{"cq":"l", "b":"g", "fg":false}';
select '{"aa":1 , "b":2, "cq":3}'::jsonb || '{"aq":"l"}';
select '{"aa":1 , "b":2, "cq":3}'::jsonb || '{"aa":"l"}';
select '{"aa":1 , "b":2, "cq":3}'::jsonb || '{}';
select '["a", "b"]'::jsonb || '["c"]';
select '["a", "b"]'::jsonb || '["c", "d"]';
select '["c"]' || '["a", "b"]'::jsonb;
select '["a", "b"]'::jsonb || '"c"';
select '"c"' || '["a", "b"]'::jsonb;
select '[]'::jsonb || '["a"]'::jsonb;
select '[]'::jsonb || '"a"'::jsonb;
select '"b"'::jsonb || '"a"'::jsonb;
select '{}'::jsonb || '{"a":"b"}'::jsonb;
select '[]'::jsonb || '{"a":"b"}'::jsonb;
select '{"a":"b"}'::jsonb || '[]'::jsonb;
select '"a"'::jsonb || '{"a":1}';
select '{"a":1}' || '"a"'::jsonb;
select '[3]'::jsonb || '{}'::jsonb;
select '3'::jsonb || '[]'::jsonb;
select '3'::jsonb || '4'::jsonb;
select '3'::jsonb || '{}'::jsonb;
select '["a", "b"]'::jsonb || '{"c":1}';
select '{"c": 1}'::jsonb || '["a", "b"]';
select '{}'::jsonb || '{"cq":"l", "b":"g", "fg":false}';
select pg_column_size('{}'::jsonb || '{}'::jsonb) = pg_column_size('{}'::jsonb);
select pg_column_size('{"aa":1}'::jsonb || '{"b":2}'::jsonb) = pg_column_size('{"aa":1, "b":2}'::jsonb);
select pg_column_size('{"aa":1, "b":2}'::jsonb || '{}'::jsonb) = pg_column_size('{"aa":1, "b":2}'::jsonb);
select pg_column_size('{}'::jsonb || '{"aa":1, "b":2}'::jsonb) = pg_column_size('{"aa":1, "b":2}'::jsonb);
select jsonb_delete('{"a":1 , "b":2, "c":3}'::jsonb, 'a');
select jsonb_delete('{"a":null , "b":2, "c":3}'::jsonb, 'a');
select jsonb_delete('{"a":1 , "b":2, "c":3}'::jsonb, 'b');
select jsonb_delete('{"a":1 , "b":2, "c":3}'::jsonb, 'c');
select jsonb_delete('{"a":1 , "b":2, "c":3}'::jsonb, 'd');
select '{"a":1 , "b":2, "c":3}'::jsonb - 'a';
select '{"a":null , "b":2, "c":3}'::jsonb - 'a';
select '{"a":1 , "b":2, "c":3}'::jsonb - 'b';
select '{"a":1 , "b":2, "c":3}'::jsonb - 'c';
select '{"a":1 , "b":2, "c":3}'::jsonb - 'd';
select pg_column_size('{"a":1 , "b":2, "c":3}'::jsonb - 'b') = pg_column_size('{"a":1, "b":2}'::jsonb);
select '["a","b","c"]'::jsonb - 3;
select '["a","b","c"]'::jsonb - 2;
select '["a","b","c"]'::jsonb - 1;
select '["a","b","c"]'::jsonb - 0;
select '["a","b","c"]'::jsonb - -1;
select '["a","b","c"]'::jsonb - -2;
select '["a","b","c"]'::jsonb - -3;
select '["a","b","c"]'::jsonb - -4;
select jsonb_delete_path('{"n":null, "a":1, "b":[1,2], "c":{"1":2}, "d":{"1":[2,3]}}', '{n}');
select jsonb_delete_path('{"n":null, "a":1, "b":[1,2], "c":{"1":2}, "d":{"1":[2,3]}}', '{b,-1}');
select jsonb_delete_path('{"n":null, "a":1, "b":[1,2], "c":{"1":2}, "d":{"1":[2,3]}}', '{d,1,0}');
select '{"n":null, "a":1, "b":[1,2], "c":{"1":2}, "d":{"1":[2,3]}}'::jsonb #- '{n}';
select '{"n":null, "a":1, "b":[1,2], "c":{"1":2}, "d":{"1":[2,3]}}'::jsonb #- '{b,-1}';
select '{"n":null, "a":1, "b":[1,2], "c":{"1":2}, "d":{"1":[2,3]}}'::jsonb #- '{b,-1e}'; -- invalid array subscript
select '{"n":null, "a":1, "b":[1,2], "c":{"1":2}, "d":{"1":[2,3]}}'::jsonb #- '{d,1,0}';
-- empty structure and error conditions for delete and replace
select '"a"'::jsonb - 'a'; -- error
select '{}'::jsonb - 'a';
select '[]'::jsonb - 'a';
select '"a"'::jsonb - 1; -- error
select '{}'::jsonb -  1; -- error
select '[]'::jsonb - 1;
select '"a"'::jsonb #- '{a}'; -- error
select '{}'::jsonb #- '{a}';
select '[]'::jsonb #- '{a}';
select jsonb_set('{}','{a}','"b"', false);
select jsonb_set('[]','{1}','"b"', false);
select jsonb_set('[{"f1":1,"f2":null},2,null,3]', '{0}','[2,3,4]', false);
-- jsonb_set_lax
\pset null NULL
-- errors
select jsonb_set_lax('{"a":1,"b":2}', '{b}', null, true, null);
select jsonb_set_lax('{"a":1,"b":2}', '{b}', null, true, 'no_such_treatment');
\pset null ''
select jsonb_insert('{"a": [0,1,2]}', '{a, 1}', '"new_value"', true);
select jsonb_insert('{"a": {"b": {"c": [0, 1, "test1", "test2"]}}}', '{a, b, c, 2}', '"new_value"', true);
select jsonb_insert('{"a": [0,1,2]}', '{a, 0}', '"new_value"', true);
select jsonb_insert('{"a": [0,1,2]}', '{a, 2}', '"new_value"', true);
select jsonb_insert('{"a": [0,1,2]}', '{a, -1}', '"new_value"', true);
select jsonb_insert('[]', '{1}', '"new_value"', true);
select jsonb_insert('{"a": []}', '{a, 1}', '"new_value"', true);
select jsonb_insert('{"a": {"b": "value"}}', '{a, c}', '"new_value"', true);
select jsonb_insert('{"a": {"b": "value"}}', '{a, b}', '"new_value"', true);
create TEMP TABLE test_jsonb_subscript (
       id int,
       test_json jsonb
);
insert into test_jsonb_subscript values
(1, '{}'), -- empty jsonb
(2, '{"key": "value"}'); -- jsonb with data
-- NULL as jsonb source
insert into test_jsonb_subscript values (3, NULL);
insert into test_jsonb_subscript values (1, '[0]');
insert into test_jsonb_subscript values (1, '[]');
insert into test_jsonb_subscript values (1, '{}');
insert into test_jsonb_subscript values (1, '{}');
insert into test_jsonb_subscript values (1, '{"b": 1}');
insert into test_jsonb_subscript values (1, '{}');
insert into test_jsonb_subscript values (1, '[]');
insert into test_jsonb_subscript values (1, '{}');
insert into test_jsonb_subscript values (1, '[]');
insert into test_jsonb_subscript values (1, '{}');
insert into test_jsonb_subscript values (1, '{"a": {}}');
insert into test_jsonb_subscript values (1, '{"a": []}');
insert into test_jsonb_subscript values (1, '{"a": 1}');
insert into test_jsonb_subscript values (1, 'null');
-- casts
select 'true'::jsonb::bool;
select '[]'::jsonb::bool;
select '1.0'::jsonb::float;
select '[1.0]'::jsonb::float;
select '12345'::jsonb::int4;
select '"hello"'::jsonb::int4;
select '12345'::jsonb::numeric;
select '{}'::jsonb::numeric;
select '12345.05'::jsonb::numeric;
select '12345.05'::jsonb::float4;
select '12345.05'::jsonb::float8;
select '12345.05'::jsonb::int2;
select '12345.05'::jsonb::int4;
select '12345.05'::jsonb::int8;
select '12345.0000000000000000000000000000000000000000000005'::jsonb::numeric;
select '12345.0000000000000000000000000000000000000000000005'::jsonb::float4;
select '12345.0000000000000000000000000000000000000000000005'::jsonb::float8;
select '12345.0000000000000000000000000000000000000000000005'::jsonb::int2;
select '12345.0000000000000000000000000000000000000000000005'::jsonb::int4;
select '12345.0000000000000000000000000000000000000000000005'::jsonb::int8;
