PRAGMA YqlSelect = 'force';

SELECT
    a,
    b
FROM (
    VALUES
        (1, '1'),
        (2, '2'),
        (3, '3')
) AS x (
    a,
    b
);
