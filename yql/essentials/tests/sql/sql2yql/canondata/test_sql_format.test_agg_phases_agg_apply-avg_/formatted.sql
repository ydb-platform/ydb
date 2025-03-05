/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

$t = (
    SELECT
        *
    FROM
        AS_TABLE([<|key: 1, value: 2|>, <|key: 1, value: 3|>])
);

$p = (
    SELECT
        key,
        avg(value) AS a
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
        avg(a) AS a
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
        avg(a) AS a
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
        avg(a) AS a
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
        avg(a) AS a
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
        avg(value) AS a
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
