USE plato;

SELECT
    sum(x),
    avg(x)
FROM (
    VALUES
        (Decimal("99999999999999999999999999999999999", 35, 0)),
        (Decimal("1", 35, 0))
) AS a (
    x
);
