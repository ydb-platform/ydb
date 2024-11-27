USE plato;

SELECT
    key,
    ob1 ?? b1,
    ob1 ?? FALSE,
    ob1 ?? (1 / 0 > 0),
    (1 / 2 > 0) ?? ob1,
    (1 / 2 == 0) ?? b1,
    (key / 0 >= 0) ?? TRUE,
    (key / 0 >= 0) ?? b1,
    (key / 2 >= 0) ?? FALSE,
FROM Input
ORDER BY
    key;
