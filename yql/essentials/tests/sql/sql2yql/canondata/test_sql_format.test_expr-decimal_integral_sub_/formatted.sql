$decimal_int8 = Decimal('12.34', 6, 2);
$decimal_int64 = Decimal('0.0001', 35, 4);
$int8 = Int8('-5');
$int64 = Int64('9223372036854775807');

SELECT
    $decimal_int8 - $int8 AS sub_int8,
    $decimal_int64 - $int64 AS sub_int64,
    Just($decimal_int8) - Just($int8) AS both_optional_sub_int8,
    Just($decimal_int64) - Just($int64) AS both_optional_sub_int64,
    $decimal_int64 - Nothing(Optional<Int64>) AS empty_int64
;
