SELECT
    x IS NOT NULL
    OR (x IS NOT NULL AND y)
FROM (
    SELECT
        Opaque(just(1)) AS x,
        Opaque(just(TRUE)) AS y
);
