/* postgres can not */
/* custom error: Error: Implicit decimal cast would lose precision */
$lambda = ($small_dec) -> { return $small_dec; };

$func_3_1 = Callable(Callable<(Decimal(3, 1))->Decimal(3, 1)>, $lambda);
$dec_4_2 = Decimal("11.1", 4, 2);

SELECT $func_3_1($dec_4_2) AS try_round;
