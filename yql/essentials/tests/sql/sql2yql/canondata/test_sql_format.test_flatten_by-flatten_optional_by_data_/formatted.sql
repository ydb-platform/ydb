$data1 = (
    SELECT
        NULL AS a,
        'e,f,g,h' AS b,
        'x' AS c
);

SELECT
    a,
    b,
    c
FROM
    $data1
    FLATTEN OPTIONAL BY a
;

$data2 = (
    SELECT
        12345 AS a,
        'e,f,g,h' AS b,
        'x' AS c
);

SELECT
    a,
    b,
    c
FROM
    $data2
    FLATTEN OPTIONAL BY a
;

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

SELECT
    a,
    b,
    c
FROM
    $data3
    FLATTEN OPTIONAL BY a
;

$data4 = (
    SELECT
        [1, 2, 3] AS a,
        'e,f,g,h' AS b,
        'x' AS c
);

SELECT
    a,
    b,
    c
FROM
    $data4
    FLATTEN OPTIONAL BY a
;

$data5 = (
    SELECT
        {'a': 1, 'b': 2} AS a,
        'e,f,g,h' AS b,
        'x' AS c
);

SELECT
    a,
    b,
    c
FROM
    $data5
    FLATTEN OPTIONAL BY a
;
