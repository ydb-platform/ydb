$data1 = (
    SELECT
        NULL AS a,
        'e,f,g,h' AS b,
        'x' AS c
);

/* custom error: Expected optional type in field of struct */
SELECT
    a,
    b,
    c
FROM
    $data1
    FLATTEN OPTIONAL BY a
;
