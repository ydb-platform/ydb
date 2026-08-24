PRAGMA config.flags('DecimalCommonTypeConversionMode', 'with_common_type_fixup');

/* custom error: Cannot add different decimals. */
SELECT
    Decimal('10', 10, 3) + Decimal('10', 10, 2)
;
