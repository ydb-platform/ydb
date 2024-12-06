/* syntax version 1 */
SELECT
    HISTOGRAM(DISTINCT key) AS key_histogram,
    Histogram::Print(HISTOGRAM(key)) <> Histogram::Print(HISTOGRAM(DISTINCT key)) AS is_different
FROM (
    SELECT
        CAST(key AS Double) AS key
    FROM Input
);
