/* custom error: :6:9: Error: Column reference can't be used without FROM */
PRAGMA YqlSelect = 'force';

$x = (
    SELECT
        a
);

SELECT
    $x
FROM (
    VALUES
        (1)
) AS x (
    a
);
