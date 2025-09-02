/* postgres can not */
/* custom error: Error: Implicit decimal cast would narrow the range */
$lambda = ($small_dec) -> { return $small_dec; };

$func_3_1 = Callable(Callable<(Decimal(3, 1))->Decimal(3, 1)>, $lambda);
$dec_5_1 = Decimal("22.2", 5, 1);

SELECT $func_3_1($dec_5_1) AS try_downcast;
