/* custom error: Decimal common type fix must be enabled for arithmetic operations on different Decimal types */
SELECT
    Decimal('10', 10, 3) + Decimal('10', 10, 2)
;
