PRAGMA YqlSelect = 'force';

$x = (
    SELECT
        1 AS a
    UNION
    SELECT
        2 AS a
    ORDER BY
        a
    LIMIT 1
);

SELECT
    $x
;
