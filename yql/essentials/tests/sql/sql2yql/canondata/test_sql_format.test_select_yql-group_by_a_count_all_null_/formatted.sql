PRAGMA YqlSelect = 'force';

SELECT
    a,
    Count(*)
FROM (
    VALUES
        (1, 11),
        (2, NULL),
        (2, 22),
        (3, NULL),
        (3, NULL),
        (3, 33)
) AS x (
    a,
    b
)
GROUP BY
    a
;
