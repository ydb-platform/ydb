$pgint4_ty = Typeof(1p);

$data3 = (
    SELECT
        12345p AS a,
        'e,f,g,h' AS b,
        'x' AS c
    UNION
    SELECT
        CAST(NULL AS $pgint4_ty) AS a,
        'should be' AS b,
        'omitted' AS c
);

/* custom error: Expected optional type in field of struct */
SELECT
    a,
    b,
    c
FROM
    $data3
    FLATTEN OPTIONAL BY a
;
