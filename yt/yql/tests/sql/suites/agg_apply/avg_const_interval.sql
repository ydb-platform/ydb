/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

$a = select CurrentUtcDate() as _date, Just(Interval("P1W")) as parsed_lag from plato.Input;
SELECT AVG(parsed_lag) FROM  $a;
