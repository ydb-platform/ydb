$decimal_int8 = Decimal('12.34', 6, 2);
$decimal_int64 = Decimal('0.0001', 35, 4);
$int8 = Int8('-5');
$int64 = Int64('9223372036854775807');

SELECT
    $decimal_int8 + $int8 AS add_int8,
    $int8 + $decimal_int8 AS add_int8_reversed,
    $decimal_int64 + $int64 AS add_int64,
    $int64 + $decimal_int64 AS add_int64_reversed,
    Just($decimal_int8) + $int8 AS optional_decimal_add_int8,
    $decimal_int8 + Just($int8) AS decimal_add_optional_int8,
    Just($decimal_int64) + $int64 AS optional_decimal_add_int64,
    $decimal_int64 + Just($int64) AS decimal_add_optional_int64,
    Nothing(Optional<Decimal (6, 2)>) + $int8 AS empty_decimal
;
