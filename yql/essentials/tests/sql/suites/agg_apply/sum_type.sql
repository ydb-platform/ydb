/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

$p =
SELECT
    sum(value) as a
FROM AS_TABLE([<|key: 1, value: 2|>])
;

$p = PROCESS $p;
select FormatType(TypeOf($p));