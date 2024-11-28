/* postgres can not */
SELECT
    Math::Round(median(val), -3) AS med,
    Math::Round(stddev(val), -3) AS dev
FROM (
    SELECT
        CAST(value AS int) AS val
    FROM plato.Input
);
