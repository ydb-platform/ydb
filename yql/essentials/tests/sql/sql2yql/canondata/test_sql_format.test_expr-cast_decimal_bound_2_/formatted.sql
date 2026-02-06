/* custom error: Cannot infer common type for Decimal and Decimal: */
SELECT
    min_of(CAST(1 AS Decimal (34, 16)), CAST(2 AS Decimal (33, 18)))

-- Decimal(36, 18) :(
;
