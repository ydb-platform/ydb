USE plato;

SELECT
    *
FROM (
    SELECT
        *
    FROM Input
)
    SAMPLE (0.5)
ORDER BY
    key;
