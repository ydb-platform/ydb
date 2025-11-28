/* syntax version 1 */
/* postgres can not */
$t = (
    SELECT
        *
    FROM
        AS_TABLE([<|key: 1, value: just(2)|>, <|key: 1, value: just(3)|>])
);

$p = (
    SELECT
        key,
        CountIf(value % 2 == 1) AS a
    FROM
        $t
    GROUP BY
        key
        WITH combine
);

$p = (
    PROCESS $p
);

SELECT
    *
FROM
    $p
;

$p = (
    SELECT
        key,
        CountIf(a) AS a
    FROM
        $p
    GROUP BY
        key
        WITH combinestate
);

$p = (
    PROCESS $p
);

SELECT
    *
FROM
    $p
;

$p = (
    SELECT
        key,
        CountIf(a) AS a
    FROM
        $p
    GROUP BY
        key
        WITH mergestate
);

$p = (
    PROCESS $p
);

SELECT
    *
FROM
    $p
;

$p1 = (
    SELECT
        key,
        CountIf(a) AS a
    FROM
        $p
    GROUP BY
        key
        WITH mergefinalize
);

$p1 = (
    PROCESS $p1
);

SELECT
    *
FROM
    $p1
;

$p2 = (
    SELECT
        key,
        CountIf(a) AS a
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

SELECT
    *
FROM
    $p2
;

$p3 = (
    SELECT
        key,
        CountIf(value % 2 == 1) AS a
    FROM
        $t
    GROUP BY
        key
        WITH finalize
);

$p3 = (
    PROCESS $p3
);

SELECT
    *
FROM
    $p3
;
