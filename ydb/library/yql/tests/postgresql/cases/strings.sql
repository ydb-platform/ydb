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
