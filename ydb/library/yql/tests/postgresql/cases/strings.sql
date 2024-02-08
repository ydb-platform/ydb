--
-- STRINGS
-- Test various data entry syntaxes.
--
-- SQL string continuation syntax
-- E021-03 character string literals
SELECT 'first line'
' - next line'
	' - third line'
	AS "Three lines to one";
-- illegal string continuation syntax
SELECT 'first line'
' - next line' /* this comment is not allowed here */
' - third line'
	AS "Illegal comment within continuation";
-- Unicode escapes
SET standard_conforming_strings TO on;
SELECT U&'d\0061t\+000061' AS U&"d\0061t\+000061";
SELECT U&'d!0061t\+000061' UESCAPE '!' AS U&"d*0061t\+000061" UESCAPE '*';
SELECT U&'a\\b' AS "a\b";
SELECT U&' \' UESCAPE '!' AS "tricky";
SELECT 'tricky' AS U&"\" UESCAPE '!';
SELECT U&'wrong: \061';
SELECT U&'wrong: \+0061';
SELECT U&'wrong: +0061' UESCAPE +;
SELECT U&'wrong: +0061' UESCAPE '+';
SELECT U&'wrong: \db99';
SELECT U&'wrong: \db99xy';
SELECT U&'wrong: \db99\\';
SELECT U&'wrong: \db99\0061';
SELECT U&'wrong: \+00db99\+000061';
SELECT U&'wrong: \+2FFFFF';
-- while we're here, check the same cases in E-style literals
SELECT E'd\u0061t\U00000061' AS "data";
SELECT E'a\\b' AS "a\b";
SELECT E'wrong: \u061';
SELECT E'wrong: \U0061';
SELECT E'wrong: \udb99';
SELECT E'wrong: \udb99xy';
SELECT E'wrong: \udb99\\';
SELECT E'wrong: \udb99\u0061';
SELECT E'wrong: \U0000db99\U00000061';
SELECT E'wrong: \U002FFFFF';
SET standard_conforming_strings TO off;
SELECT 'tricky' AS U&"\" UESCAPE '!';
RESET standard_conforming_strings;
-- bytea
SET bytea_output TO hex;
SELECT E'\\xDeAdBeEf'::bytea;
SELECT E'\\x De Ad Be Ef '::bytea;
SELECT E'\\xDeAdBeE'::bytea;
SELECT E'\\xDeAdBeEx'::bytea;
SELECT E'\\xDe00BeEf'::bytea;
SELECT E'DeAdBeEf'::bytea;
SELECT E'De\\000dBeEf'::bytea;
SELECT E'De\123dBeEf'::bytea;
SELECT E'De\\123dBeEf'::bytea;
SELECT E'De\\678dBeEf'::bytea;
SET bytea_output TO escape;
SELECT E'\\xDeAdBeEf'::bytea;
SELECT E'\\x De Ad Be Ef '::bytea;
SELECT E'\\xDe00BeEf'::bytea;
SELECT E'DeAdBeEf'::bytea;
SELECT E'De\\000dBeEf'::bytea;
SELECT E'De\\123dBeEf'::bytea;
SELECT CAST(name 'namefield' AS text) AS "text(name)";
SELECT CAST(name 'namefield' AS char(10)) AS "char(name)";
SELECT CAST(name 'namefield' AS varchar) AS "varchar(name)";
--
-- test SQL string functions
-- E### and T### are feature reference numbers from SQL99
--
-- E021-09 trim function
SELECT TRIM(BOTH FROM '  bunch o blanks  ') = 'bunch o blanks' AS "bunch o blanks";
SELECT TRIM(LEADING FROM '  bunch o blanks  ') = 'bunch o blanks  ' AS "bunch o blanks  ";
SELECT TRIM(TRAILING FROM '  bunch o blanks  ') = '  bunch o blanks' AS "  bunch o blanks";
SELECT TRIM(BOTH 'x' FROM 'xxxxxsome Xsxxxxx') = 'some Xs' AS "some Xs";
-- E021-06 substring expression
SELECT SUBSTRING('1234567890' FROM 3) = '34567890' AS "34567890";
SELECT SUBSTRING('1234567890' FROM 4 FOR 3) = '456' AS "456";
-- test overflow cases
SELECT SUBSTRING('string' FROM 2 FOR 2147483646) AS "tring";
SELECT SUBSTRING('string' FROM -10 FOR 2147483646) AS "string";
SELECT SUBSTRING('string' FROM -10 FOR -2147483646) AS "error";
SELECT SUBSTRING(NULL SIMILAR '%' ESCAPE '#') IS NULL AS "True";
SELECT SUBSTRING('abcdefg' SIMILAR NULL ESCAPE '#') IS NULL AS "True";
-- substring() with just two arguments is not allowed by SQL spec;
-- we accept it, but we interpret the pattern as a POSIX regexp not SQL
SELECT SUBSTRING('abcdefg' FROM 'c.e') AS "cde";
-- With a parenthesized subexpression, return only what matches the subexpr
SELECT SUBSTRING('abcdefg' FROM 'b(.*)f') AS "cde";
-- Test back reference in regexp_replace
SELECT regexp_replace('1112223333', E'(\\d{3})(\\d{3})(\\d{4})', E'(\\1) \\2-\\3');
SELECT regexp_replace('AAA   BBB   CCC   ', E'\\s+', ' ', 'g');
SELECT regexp_replace('AAA', '^|$', 'Z', 'g');
SELECT regexp_replace('AAA aaa', 'A+', 'Z', 'gi');
-- invalid regexp option
SELECT regexp_replace('AAA aaa', 'A+', 'Z', 'z');
-- set so we can tell NULL from empty string
\pset null '\\N'
-- split string on regexp
SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', $re$\s+$re$) AS foo;
SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', $re$\s+$re$);
SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', $re$\s*$re$) AS foo;
SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', $re$\s*$re$);
SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', '') AS foo;
-- case insensitive
SELECT foo, length(foo) FROM regexp_split_to_table('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'i') AS foo;
SELECT regexp_split_to_array('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'i');
-- no match of pattern
SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', 'nomatch') AS foo;
SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', 'nomatch');
-- some corner cases
SELECT regexp_split_to_array('123456','1');
SELECT regexp_split_to_array('123456','6');
SELECT regexp_split_to_array('123456','.');
SELECT regexp_split_to_array('123456','');
SELECT regexp_split_to_array('123456','(?:)');
SELECT regexp_split_to_array('1','');
-- errors
SELECT foo, length(foo) FROM regexp_split_to_table('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'zippy') AS foo;
SELECT regexp_split_to_array('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'iz');
-- global option meaningless for regexp_split
SELECT foo, length(foo) FROM regexp_split_to_table('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'g') AS foo;
SELECT regexp_split_to_array('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'g');
-- change NULL-display back
\pset null ''
-- E021-11 position expression
SELECT POSITION('4' IN '1234567890') = '4' AS "4";
SELECT POSITION('5' IN '1234567890') = '5' AS "5";
-- T312 character overlay function
SELECT OVERLAY('abcdef' PLACING '45' FROM 4) AS "abc45f";
SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5) AS "yabadaba";
SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5 FOR 0) AS "yabadabadoo";
SELECT OVERLAY('babosa' PLACING 'ubb' FROM 2 FOR 4) AS "bubba";
--
-- test LIKE
-- Be sure to form every test as a LIKE/NOT LIKE pair.
--
-- simplest examples
-- E061-04 like predicate
SELECT 'hawkeye' LIKE 'h%' AS "true";
SELECT 'hawkeye' NOT LIKE 'h%' AS "false";
SELECT 'hawkeye' LIKE 'H%' AS "false";
SELECT 'hawkeye' NOT LIKE 'H%' AS "true";
SELECT 'hawkeye' LIKE 'indio%' AS "false";
SELECT 'hawkeye' NOT LIKE 'indio%' AS "true";
