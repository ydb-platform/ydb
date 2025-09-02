/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

$p = (
    SELECT
        sum(value) AS a
    FROM
        AS_TABLE([<|key: 1, value: 2|>])
    LIMIT 0
);

$p = (
    PROCESS $p
);

SELECT
    FormatType(TypeOf($p))
;
