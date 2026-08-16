PRAGMA EmitAggApply;

$p = (
    SELECT
        sum(value) AS a
    FROM
        AS_TABLE([<|key: 1, value: 2|>])
);

$p = (
    PROCESS $p
);

SELECT
    FormatType(TypeOf($p))
;
