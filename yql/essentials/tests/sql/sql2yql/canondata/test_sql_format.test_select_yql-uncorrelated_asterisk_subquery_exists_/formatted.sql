PRAGMA YqlSelect = 'force';

SELECT
    EXISTS (
        SELECT
            *
        FROM (
            SELECT
                1 AS a
        )
    )
;
