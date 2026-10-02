PRAGMA YqlSelect = 'force';

SELECT
    (
        SELECT
            *
        FROM (
            SELECT
                1 AS a
        )
    )
;
