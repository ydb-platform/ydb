/* syntax version 1 */
/* postgres can not */
USE plato;

$t = [<|'key1': '1', 'subkey1': 'a', 'key': '1', 'subkey': 'a', 'value': 'value2_1'|>, <|'key1': '4', 'subkey1': 'd', 'key': '4', 'subkey': 'd', 'value': 'value2_4'|>, <|'key1': '-5', 'subkey1': 'e', 'key': '-5', 'subkey': 'e', 'value': 'value2_5'|>];

SELECT
    *
FROM
    Input AS A
LEFT SEMI JOIN
    AS_TABLE($t) AS B
ON
    AsTuple(CAST(A.key AS uint64), CAST(A.subkey AS String)) == AsTuple(CAST(B.key AS int64), CAST(B.subkey AS Utf8))
    AND AsTuple(CAST(A.key1 AS uint64), CAST(A.subkey1 AS String)) == AsTuple(CAST(B.key1 AS int64), CAST(B.subkey1 AS Utf8))
ORDER BY
    `key`
;
