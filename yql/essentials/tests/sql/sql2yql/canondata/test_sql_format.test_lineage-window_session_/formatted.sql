INSERT INTO plato.Output
SELECT
    key,
    row_number() OVER w AS r,
    session_start() OVER w AS s
FROM plato.Input
WINDOW
    w AS (
        PARTITION BY
            key,
            SessionWindow(CAST(subkey AS Datetime), DateTime::IntervalFromMinutes(15))
    );
