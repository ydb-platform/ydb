/* postgres can not */
PRAGMA DisableSimpleColumns;

$a = AsList(
    AsStruct(255ut AS K, 1 AS V),
    AsStruct(127ut AS K, 2 AS V),
    AsStruct(0ut AS K, 3 AS V)
);

$b = AsList(
    AsStruct(Int8('-1') AS K, 1 AS V),
    AsStruct(Int8('127') AS K, 2 AS V),
    AsStruct(Int8('0') AS K, 3 AS V)
);

SELECT
    a.K,
    b.V
FROM
    as_table($a) AS a
JOIN
    as_table($b) AS b
ON
    a.K == b.K AND a.V == b.V
ORDER BY
    a.K,
    b.V
;
