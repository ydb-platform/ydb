PRAGMA config.flags('DecimalCommonTypeConversionMode', 'with_common_type_fixup');

/* custom error: Cannot infer common type for Decimal(35,35) and Int32: common precision is 45, that is greater than 35 */
SELECT
    ListFromRange(Decimal('0.1', 35, 35), Int32('1'))
;
