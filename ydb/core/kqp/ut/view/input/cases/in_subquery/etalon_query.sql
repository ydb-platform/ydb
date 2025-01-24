SELECT
    *
FROM (
    SELECT
        *
    FROM series
    WHERE series_id IN (
        SELECT
            series_id
        FROM series
    )
);
