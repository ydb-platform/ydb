$identity = ($value) -> {
    RETURN $value;
};

$accept_decimal = Callable(Callable<(Decimal (11, 0)) -> Decimal (11, 0)>, $identity);
$integral = Int32('1');

SELECT
    $accept_decimal($integral) AS converted
;
