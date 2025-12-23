PRAGMA YqlSelect = 'force';

SELECT
    a,
    b IN ('MAIL', 'SHIP')
FROM (
    VALUES
        (1, 'MAIL'),
        (2, 'TEST')
) AS x (
    a,
    b
);
