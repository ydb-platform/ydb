PRAGMA config.flags('DecimalCommonTypeConversionMode', 'with_common_type_fixup');

$fractional_start = Decimal('0', 1, 0);
$fractional_end = Decimal('3', 1, 0);

SELECT
    ListFromRange(
        Decimal('1.00', 5, 2),
        Decimal('2.0', 4, 1),
        Decimal('0.25', 5, 2)
    ) AS different_end_type,
    ListFromRange(
        Decimal('1.00', 5, 2),
        Decimal('2.00', 5, 2),
        Decimal('0.250', 6, 3)
    ) AS different_step_type,
    ListFromRange(
        Decimal('0', 2, 0),
        Just(Decimal('0.3', 2, 1)),
        Decimal('0.10', 3, 2)
    ) AS optional_type,
    ListFromRange(
        Decimal('0', 1, 0),
        Decimal('3.00', 3, 2)
    ) AS default_step_type,
    ListFromRange(
        $fractional_start,
        $fractional_end,
        Decimal('0.23', 3, 2)
    ) AS fractional_step_23,
    ListFromRange(
        $fractional_start,
        $fractional_end,
        Decimal('0.26', 3, 2)
    ) AS fractional_step_26
;
