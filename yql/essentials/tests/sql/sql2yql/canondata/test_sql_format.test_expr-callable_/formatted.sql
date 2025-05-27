/* postgres can not */
/* syntax version 1 */
$lambda = ($x) -> {
    RETURN CAST($x AS String);
};

$callables = AsTuple(
    Callable(Callable<(Int32) -> String>, $lambda),
    Callable(Callable<(Bool) -> String>, $lambda),
);

SELECT
    $callables.0(10),
    $callables.1(TRUE)
;
