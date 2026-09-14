PRAGMA YqlSelect = 'force';

SELECT
    a,
    b
FROM (
    VALUES
        (1, '1', 'Ignored'),
        (2, '2', 'Ignored'),
        (3, '3', 'Ignored')
) AS x (
    a,
    b
);
