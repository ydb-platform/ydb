$data = [
    <|x: Decimal('3', 5, 1), y: Decimal('2', 5, 1)|>,
];

SELECT
    x * y,
    x / y,
    x % y
FROM
    as_table($data)
;
