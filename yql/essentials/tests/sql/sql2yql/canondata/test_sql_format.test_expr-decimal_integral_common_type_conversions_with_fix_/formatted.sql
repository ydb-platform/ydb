PRAGMA config.flags('DecimalCommonTypeConversionMode', 'with_common_type_fixup');

$integral = Int32('1');
$decimal = Decimal('2.5', 3, 1);

SELECT
    [$integral, $decimal] AS integral_decimal,
    [$decimal, $integral] AS decimal_integral,
    [Just($integral), $decimal] AS optional_integral_decimal,
    [$integral, Just($decimal)] AS integral_optional_decimal,
    [Just($integral), Just($decimal)] AS optional_integral_optional_decimal
;
