PRAGMA YqlSelect = 'force';

SELECT
    max(x.b) AS maxb,
    min(x.a)
FROM (
    VALUES
        (1, 2)
) AS x (
    a,
    b
)
ORDER BY
    maxb
;
