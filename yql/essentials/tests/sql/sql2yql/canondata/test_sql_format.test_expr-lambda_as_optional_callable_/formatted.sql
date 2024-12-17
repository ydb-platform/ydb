$callbackType = Callable<(Int32) -> Int32>;
$defaultFunc = CALLABLE ($callbackType, ($x) -> ($x));
$outerType = CallableType(1, Int32, Int32, Optional<$callbackType>);
$outer = CALLABLE ($outerType, ($x, $f) -> (($f ?? $defaultFunc)($x)));

SELECT
    $outer(1, ($y) -> ($y + 1)),
    $outer(1)
;
