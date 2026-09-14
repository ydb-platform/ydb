/* custom error: Cannot substract type Int8 and Decimal(6,2) */
SELECT
    Int8('1') - Decimal('12.34', 6, 2) AS value
;
