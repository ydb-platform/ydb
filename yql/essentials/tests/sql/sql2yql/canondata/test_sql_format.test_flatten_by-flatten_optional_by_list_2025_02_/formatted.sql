$data4 = (
    SELECT
        [1, 2, 3] AS a,
        'e,f,g,h' AS b,
        'x' AS c
);

/* custom error: Expected optional type in field of struct */
SELECT
    a,
    b,
    c
FROM
    $data4
    FLATTEN OPTIONAL BY a
;
