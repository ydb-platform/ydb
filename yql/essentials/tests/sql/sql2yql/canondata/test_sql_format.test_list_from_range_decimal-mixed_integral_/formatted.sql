PRAGMA config.flags('DecimalCommonTypeConversionMode', 'with_common_type_fixup');

$begin = Int8('1');
$end = Decimal('4.0', 2, 1);
$step = Int8('1');

SELECT
    ListFromRange($begin, $end, $step) AS plain,
    ListFromRange(Just($begin), $end, $step) AS optional_begin,
    ListFromRange($begin, Just($end), $step) AS optional_end,
    ListFromRange($begin, $end, Just($step)) AS optional_step,
    ListFromRange(Just($begin), Just($end), Just($step)) AS all_optional,
    ListFromRange(Nothing(Optional<Int8>), $end, $step) AS empty_optional_begin,
    ListFromRange($begin, Nothing(Optional<Decimal (2, 1)>), $step) AS empty_optional_end,
    ListFromRange($begin, $end, Nothing(Optional<Int8>)) AS empty_optional_step
;
