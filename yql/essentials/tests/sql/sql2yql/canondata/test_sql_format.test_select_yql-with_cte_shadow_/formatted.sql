PRAGMA YqlSelect = 'force';

WITH
    x AS (
        SELECT
            111 AS a
    ),
    y AS (
        WITH x AS (
            SELECT
                0 + 1 AS a
        )
        SELECT
            a + 1 AS a
        FROM
            x
    )
SELECT
    a + 1 AS a
FROM
    y
;
