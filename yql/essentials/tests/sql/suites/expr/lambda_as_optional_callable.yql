$callbackType = Callable<(Int32)->Int32>;
$defaultFunc = Callable($callbackType, ($x)->($x));
$outerType = CallableType(1, Int32, Int32, Optional<$callbackType>);
$outer = Callable($outerType, ($x,$f)->(($f ?? $defaultFunc)($x)));

select $outer(1,($y)->($y+1)), $outer(1);
