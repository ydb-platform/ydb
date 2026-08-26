/* custom error: ListFromRange over Decimal requires Start, End, and Step with the same precision and scale */
SELECT
    ListFromRange(
        Decimal('1.00', 5, 2),
        Decimal('2.00', 5, 2),
        Decimal('0.250', 6, 3)
    )
;
