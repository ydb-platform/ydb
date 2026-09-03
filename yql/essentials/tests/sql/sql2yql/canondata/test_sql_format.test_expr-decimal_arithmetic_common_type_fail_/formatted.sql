PRAGMA config.flags('DecimalCommonTypeConversionMode', 'with_common_type_fixup');

/* custom error: Cannot infer common Decimal type for '+' operation. */
/* custom error: Cannot infer common Decimal type for '-' operation. */
/* custom error: Cannot infer common Decimal type for '*' operation. */
/* custom error: Cannot infer common Decimal type for '/' operation. */
/* custom error: Cannot infer common Decimal type for '%' operation. */
/* custom error: Cannot infer common type for Decimal(35,1) and Decimal(35,34): common precision is 68, that is greater than 35 */
SELECT
    Decimal('1', 35, 1) + Decimal('1', 35, 34) AS add,
    Decimal('1', 35, 1) - Decimal('1', 35, 34) AS sub,
    Decimal('1', 35, 1) * Decimal('1', 35, 34) AS mul,
    Decimal('1', 35, 1) / Decimal('1', 35, 34) AS div,
    Decimal('1', 35, 1) % Decimal('1', 35, 34) AS mod
;
