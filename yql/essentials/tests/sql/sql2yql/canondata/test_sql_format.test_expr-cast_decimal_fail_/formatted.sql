/* custom error: Cannot infer common type for Decimal and Decimal: */
SELECT
    min_of(CAST(1 AS Decimal (20, 8)), CAST(2 AS Decimal (35, 6)))

-- Decimal(37, 8) :(
;
