PRAGMA config.flags('DecimalCommonTypeConversionMode', 'with_common_type_fixup');

/* custom error: Cannot infer common type for Decimal(34,16) and Decimal(33,18): common precision is 36, that is greater than 35 */
SELECT
    min_of(CAST(1 AS Decimal (34, 16)), CAST(2 AS Decimal (33, 18)))
;
