$identity = ($x)-> { return $x };
$idDate32 = Callable(Callable<(date32)->date32>, $identity);
$idDatetime64 = Callable(Callable<(datetime64)->datetime64>, $identity);
$idTimestamp64 = Callable(Callable<(timestamp64)->timestamp64>, $identity);
$idInterval64 = Callable(Callable<(interval64)->interval64>, $identity);

$valDate = cast(1 as date);
$valDate32 = cast(-1 as date32);
select $idDate32($valDate), $idDate32($valDate32);
-- todo
