/* postgres can not */
/* syntax version 1 */
$lambda = ($x) -> {
    return cast($x as String)
};

$callables = AsTuple(
  Callable(Callable<(Int32)->String>, $lambda),
  Callable(Callable<(Bool)->String>, $lambda),
);

select $callables.0(10), $callables.1(true);
