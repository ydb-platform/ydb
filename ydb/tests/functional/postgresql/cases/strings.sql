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
--
-- test conversions between various string types
-- E021-10 implicit casting among the character data types
--

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

-- substring() with just two arguments is not allowed by SQL spec;
-- we accept it, but we interpret the pattern as a POSIX regexp not SQL
SELECT SUBSTRING('abcdefg' FROM 'c.e') AS "cde";

-- With a parenthesized subexpression, return only what matches the subexpr
SELECT SUBSTRING('abcdefg' FROM 'b(.*)f') AS "cde";

-- T312 character overlay function
SELECT OVERLAY('abcdef' PLACING '45' FROM 4) AS "abc45f";

SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5) AS "yabadaba";

SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5 FOR 0) AS "yabadabadoo";

SELECT OVERLAY('babosa' PLACING 'ubb' FROM 2 FOR 4) AS "bubba";

--
-- test implicit type conversion
--

-- E021-07 character concatenation
SELECT 'unknown' || ' and unknown' AS "Concat unknown types";

SELECT text 'text' || ' and unknown' AS "Concat text to unknown type";

--
-- test length
--

SELECT length('abcdef') AS "length_6";

--
-- test strpos
--

SELECT strpos('abcdef', 'cd') AS "pos_3";

SELECT strpos('abcdef', 'xy') AS "pos_0";

SELECT strpos('abcdef', '') AS "pos_1";

SELECT strpos('', 'xy') AS "pos_0";

SELECT strpos('', '') AS "pos_1";

--
-- test replace
--
SELECT replace('abcdef', 'de', '45') AS "abc45f";

SELECT replace('yabadabadoo', 'ba', '123') AS "ya123da123doo";

SELECT replace('yabadoo', 'bad', '') AS "yaoo";

--
-- test split_part
--
select split_part('','@',1) AS "empty string";

select split_part('','@',-1) AS "empty string";

select split_part('joeuser@mydatabase','',1) AS "joeuser@mydatabase";

select split_part('joeuser@mydatabase','',2) AS "empty string";

select split_part('joeuser@mydatabase','',-1) AS "joeuser@mydatabase";

select split_part('joeuser@mydatabase','',-2) AS "empty string";

select split_part('joeuser@mydatabase','@',0) AS "an error";

select split_part('joeuser@mydatabase','@@',1) AS "joeuser@mydatabase";

select split_part('joeuser@mydatabase','@@',2) AS "empty string";

select split_part('joeuser@mydatabase','@',1) AS "joeuser";

select split_part('joeuser@mydatabase','@',2) AS "mydatabase";

select split_part('joeuser@mydatabase','@',3) AS "empty string";

select split_part('@joeuser@mydatabase@','@',2) AS "joeuser";

select split_part('joeuser@mydatabase','@',-1) AS "mydatabase";

select split_part('joeuser@mydatabase','@',-2) AS "joeuser";

select split_part('joeuser@mydatabase','@',-3) AS "empty string";

select split_part('@joeuser@mydatabase@','@',-2) AS "mydatabase";

--
-- test to_hex
--
select to_hex(256*256*256 - 1) AS "ffffff";

select to_hex(256::bigint*256::bigint*256::bigint*256::bigint - 1) AS "ffffffff";

--
-- MD5 test suite - from IETF RFC 1321
-- (see: ftp://ftp.rfc-editor.org/in-notes/rfc1321.txt)
--
select md5('') = 'd41d8cd98f00b204e9800998ecf8427e' AS "TRUE";

select md5('a') = '0cc175b9c0f1b6a831c399e269772661' AS "TRUE";

select md5('abc') = '900150983cd24fb0d6963f7d28e17f72' AS "TRUE";

select md5('message digest') = 'f96b697d7cb7938d525a2f31aaf161d0' AS "TRUE";

select md5('abcdefghijklmnopqrstuvwxyz') = 'c3fcd3d76192e4007dfb496cca67e13b' AS "TRUE";

select md5('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789') = 'd174ab98d277d9f5a5611c2c9f419d9f' AS "TRUE";

select md5('12345678901234567890123456789012345678901234567890123456789012345678901234567890') = '57edf4a22be3c955ac49da2e2107b67a' AS "TRUE";

select md5(''::bytea) = 'd41d8cd98f00b204e9800998ecf8427e' AS "TRUE";

select md5('a'::bytea) = '0cc175b9c0f1b6a831c399e269772661' AS "TRUE";

select md5('abc'::bytea) = '900150983cd24fb0d6963f7d28e17f72' AS "TRUE";

select md5('message digest'::bytea) = 'f96b697d7cb7938d525a2f31aaf161d0' AS "TRUE";

select md5('abcdefghijklmnopqrstuvwxyz'::bytea) = 'c3fcd3d76192e4007dfb496cca67e13b' AS "TRUE";

select md5('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'::bytea) = 'd174ab98d277d9f5a5611c2c9f419d9f' AS "TRUE";

select md5('12345678901234567890123456789012345678901234567890123456789012345678901234567890'::bytea) = '57edf4a22be3c955ac49da2e2107b67a' AS "TRUE";
