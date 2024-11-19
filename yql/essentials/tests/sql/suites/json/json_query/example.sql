/* syntax version 1 */
/* postgres can not */
USE plato;

-- These examples are taken from [ISO/IEC TR 19075-6:2017] standard (https://www.iso.org/standard/67367.html)
SELECT T.K,
    JSON_VALUE (T.J, 'lax $.who') AS Who,
    JSON_VALUE (T.J, 'lax $.where' NULL ON EMPTY) AS Nali,
    JSON_QUERY (T.J, 'lax $.friends') AS Friends
FROM T
WHERE JSON_EXISTS (T.J, 'lax $.friends');

SELECT T.K,
    JSON_VALUE (T.J, 'lax $.who') AS Who,
    JSON_VALUE (T.J, 'lax $.where' NULL ON EMPTY) AS Nali,
    JSON_QUERY (T.J, 'lax $.friends' NULL ON EMPTY) AS Friends
FROM T;

SELECT T.K,
    JSON_VALUE (T.J, 'lax $.who') AS Who,
    JSON_VALUE (T.J, 'lax $.where' NULL ON EMPTY) AS Nali,
    JSON_QUERY (T.J, 'lax $.friends.name' WITH ARRAY WRAPPER) AS FriendsNames
FROM T;

-- In standard this example is demonstrated using PDF table, without any queries.
-- We represent it as set of queries, using NULL as error value.
-- Each SELECT is a row in source PDF table
$J2 = CAST(@@{ "a": "[1,2]", "b": [1,2], "c": "hi"}@@ as Json);
SELECT
    JSON_VALUE($J2, "strict $.a"),
    JSON_VALUE($J2, "strict $.b" NULL ON ERROR),
    JSON_VALUE($J2, "strict $.c");

SELECT
    JSON_QUERY($J2, "strict $.a" WITHOUT ARRAY WRAPPER NULL ON ERROR),
    JSON_QUERY($J2, "strict $.b" WITHOUT ARRAY WRAPPER),
    JSON_QUERY($J2, "strict $.c" WITHOUT ARRAY WRAPPER NULL ON ERROR);

SELECT
    JSON_QUERY($J2, "strict $.a" WITH UNCONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($J2, "strict $.b" WITH UNCONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($J2, "strict $.c" WITH UNCONDITIONAL ARRAY WRAPPER);

SELECT
    JSON_QUERY($J2, "strict $.a" WITH CONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($J2, "strict $.b" WITH CONDITIONAL ARRAY WRAPPER),
    JSON_QUERY($J2, "strict $.c" WITH CONDITIONAL ARRAY WRAPPER);
