/* syntax version 1 */
/* postgres can not */
USE plato;

$data =
    SELECT
        1 AS n,
        AsList(4, 5, 6) AS l,
        AsStruct(10 AS n, AsList(1, 2, 3) AS l) AS s
    UNION ALL
    SELECT
        2 AS n,
        AsList(4, 5) AS l,
        AsStruct(20 AS n, AsList(1, 2) AS l) AS s;

SELECT
    n,
    l
FROM $data
    FLATTEN BY
        s.l AS l
ORDER BY
    n,
    l;

SELECT
    n,
    l
FROM $data
    FLATTEN BY (
        s.l AS l
    )
ORDER BY
    n,
    l;

SELECT
    n,
    l
FROM $data
    FLATTEN BY (
        ListExtend(s.l, AsList(100)) AS l
    )
ORDER BY
    n,
    l;

SELECT
    n,
    l,
    sl
FROM $data
    FLATTEN BY (
        l,
        s.l AS sl
    )
ORDER BY
    n,
    l,
    sl;
