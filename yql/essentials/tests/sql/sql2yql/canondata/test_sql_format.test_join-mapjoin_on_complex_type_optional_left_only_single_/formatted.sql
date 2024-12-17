/* syntax version 1 */
/* postgres can not */
USE plato;

$t = [<|'key': '1', 'subkey': 'a', 'value': 'value2_1'|>, <|'key': '4', 'subkey': 'd', 'value': 'value2_4'|>, <|'key': '-5', 'subkey': 'e', 'value': 'value2_5'|>];

SELECT
    *
FROM
    Input AS A
LEFT ONLY JOIN
    AS_TABLE($t) AS B
ON
    AsTuple(CAST(A.key AS uint64), CAST(A.subkey AS String)) == AsTuple(CAST(B.key AS int64), CAST(B.subkey AS Utf8))
ORDER BY
    `key`
;
