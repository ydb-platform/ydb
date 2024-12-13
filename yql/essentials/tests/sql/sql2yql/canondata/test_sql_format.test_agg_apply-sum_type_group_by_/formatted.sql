/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

$p =
    SELECT
        key,
        sum(value) AS a
    FROM
        AS_TABLE([<|key: 1, value: 2|>])
    GROUP BY
        key
;

$p =
    PROCESS $p
;

SELECT
    FormatType(TypeOf($p))
;
