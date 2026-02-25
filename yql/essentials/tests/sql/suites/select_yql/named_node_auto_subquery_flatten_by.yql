PRAGMA YqlSelect = 'auto';

$data = (
    SELECT
        'a,b,c,d' AS a,
        'e,f,g,h' AS b,
        'c' AS c
);

SELECT
    a,
    b,
    c
FROM
    $data
    FLATTEN BY (
        String::SplitToList(a, ',') AS a,
        String::SplitToList(b, ',') AS b
    )
ORDER BY
    a,
    b
;
