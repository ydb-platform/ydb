PRAGMA config.flags('OptimizerFlags', 'EmitPruneKeys');

$a = (
    SELECT
        *
    FROM
        as_table([
            <|x: 1, t: 1|>,
            <|x: 1, t: 1|>,
            <|x: 1, t: 2|>,
            <|x: 3, t: 1|>,
            <|x: 3, t: 4|>,
            <|x: 3, t: 2|>,
        ])
);

$b = (
    SELECT
        *
    FROM
        as_table([
            <|x: 1, y: 1|>,
            <|x: 1, y: 2|>,
            <|x: 1, y: 3|>,
            <|x: 1, y: 3|>,
            <|x: 2, y: 3|>,
            <|x: 2, y: 4|>,
        ])
);

$c = (
    SELECT
        *
    FROM
        as_table([
            <|x: 1|>,
            <|x: 1|>,
            <|x: 1|>,
            <|x: 1|>,
            <|x: 2|>,
            <|x: 2|>,
        ])
);

-- PruneKeys
SELECT
    a.*
FROM
    $a AS a
WHERE
    a.x IN (
        SELECT
            x
        FROM
            $b
    )
; -- PruneKeys

SELECT
    a.*
FROM
    $a AS a
WHERE
    a.x IN (
        SELECT
            /*+ distinct(x) */ x
        FROM
            $b
    )
; -- nothing

SELECT
    a.*
FROM
    $a AS a
WHERE
    a.x IN (
        SELECT
            x
        FROM
            $c
    )
; -- PruneKeys

SELECT
    a.*
FROM
    $a AS a
LEFT SEMI JOIN
    $b AS b
ON
    a.x == b.x
; -- PruneKeys(b)

SELECT
    a.*
FROM
    $b AS b
RIGHT SEMI JOIN
    $a AS a
ON
    b.x == a.x
; -- PruneKeys(b)

SELECT
    a.x,
    a.t,
    b.x
FROM ANY
    $a AS a
JOIN
    $b AS b
ON
    a.x == b.x
; -- PruneKeys(a)

SELECT
    a.x,
    a.t,
    b.x
FROM
    $a AS a
JOIN ANY
    $b AS b
ON
    a.x == b.x
; -- PruneKeys(b)

$a_sorted = (
    SELECT
        *
    FROM
        $a
    ASSUME ORDER BY
        x
);

$b_sorted = (
    SELECT
        *
    FROM
        $b
    ASSUME ORDER BY
        x
);

$c_sorted = (
    SELECT
        *
    FROM
        $c
    ASSUME ORDER BY
        x
);

-- PruneAdjacentKeys
SELECT
    a.*
FROM
    $a AS a
WHERE
    a.x IN (
        SELECT
            x
        FROM
            $b_sorted
    )
; -- PruneAdjacentKeys

SELECT
    a.*
FROM
    $a AS a
WHERE
    a.x IN (
        SELECT
            /*+ distinct(x) */ x
        FROM
            $b_sorted
    )
; -- nothing

SELECT
    a.*
FROM
    $a AS a
WHERE
    a.x IN (
        SELECT
            x
        FROM
            $c_sorted
    )
; -- PruneAdjacentKeys

SELECT
    a.*
FROM
    $a AS a
LEFT SEMI JOIN
    $b_sorted AS b
ON
    a.x == b.x
; -- PruneAdjacentKeys(b_sorted)

SELECT
    a.*
FROM
    $b_sorted AS b
RIGHT SEMI JOIN
    $a AS a
ON
    b.x == a.x
; -- PruneAdjacentKeys(b_sorted)

SELECT
    a.x,
    a.t,
    b.x
FROM ANY
    $a_sorted AS a
JOIN
    $b AS b
ON
    a.x == b.x
; -- PruneAdjacentKeys(a_sorted)

SELECT
    a.x,
    a.t,
    b.x
FROM
    $a AS a
JOIN ANY
    $b_sorted AS b
ON
    a.x == b.x
; -- PruneAdjacentKeys(b_sorted)
