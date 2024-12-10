/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

$a =
    SELECT
        CurrentUtcDate() AS _date,
        Just(Interval("P1W")) AS parsed_lag
    FROM
        plato.Input
;

SELECT
    AVG(parsed_lag)
FROM
    $a
;
