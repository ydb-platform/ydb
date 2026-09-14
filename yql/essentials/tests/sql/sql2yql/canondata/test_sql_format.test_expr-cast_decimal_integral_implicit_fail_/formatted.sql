/* custom error: Mismatch type argument #1, type diff: Decimal(9,0)!=Int32 */
$identity = ($value) -> {
    RETURN $value;
};

$accept_decimal = Callable(Callable<(Decimal (9, 0)) -> Decimal (9, 0)>, $identity);
$integral = Int32('1');

SELECT
    $accept_decimal($integral)
;
