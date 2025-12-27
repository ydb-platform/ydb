PRAGMA YqlSelect = 'force';

-- FIXME(YQL-20436): bad test.
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
