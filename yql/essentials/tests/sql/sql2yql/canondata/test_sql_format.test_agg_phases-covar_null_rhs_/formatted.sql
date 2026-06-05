/* syntax version 1 */
/* postgres can not */
/* custom error: Expected data or optional of data, but got: Null */
/* FIXME(YQL-20234): support Null type */
$t = (
    SELECT
        *
    FROM
        AS_TABLE([
            <|key: 1, a: 1.0, b: NULL|>,
            <|key: 1, a: 2.0, b: NULL|>
        ])
);

$p = (
    SELECT
        key,
        Covar(a, b) AS a
    FROM
        $t
    GROUP BY
        key
        WITH combine
);

$p = (
    PROCESS $p
);

--select FormatType(TypeOf($p));
SELECT
    *
FROM
    $p
;

$p = (
    SELECT
        key,
        Covar(a) AS a
    FROM
        $p
    GROUP BY
        key
        WITH combinestate
);

$p = (
    PROCESS $p
);

--select FormatType(TypeOf($p));
SELECT
    *
FROM
    $p
;

$p = (
    SELECT
        key,
        Covar(a) AS a
    FROM
        $p
    GROUP BY
        key
        WITH mergestate
);

$p = (
    PROCESS $p
);

--select FormatType(TypeOf($p));
SELECT
    *
FROM
    $p
;

$p1 = (
    SELECT
        key,
        Covar(a) AS a
    FROM
        $p
    GROUP BY
        key
        WITH mergefinalize
);

$p1 = (
    PROCESS $p1
);

--select FormatType(TypeOf($p1));
SELECT
    *
FROM
    $p1
;

$p2 = (
    SELECT
        key,
        Covar(a) AS a
    FROM (
        SELECT
            key,
            just(a) AS a
        FROM
            $p
    )
    GROUP BY
        key
        WITH mergemanyfinalize
);

$p2 = (
    PROCESS $p2
);

--select FormatType(TypeOf($p2));
SELECT
    *
FROM
    $p2
;

$p3 = (
    SELECT
        key,
        Covar(a, b) AS a
    FROM
        $t
    GROUP BY
        key
        WITH finalize
);

$p3 = (
    PROCESS $p3
);

--select FormatType(TypeOf($p));
SELECT
    *
FROM
    $p3
;
