PRAGMA config.flags('OptimizerFlags', 'PredicatePushdownOverEquiJoinBothSides');

SELECT
    *
FROM
    AS_TABLE([
        <|k1: -2|>,
        <|k1: -1|>,
        <|k1: 0|>,
        <|k1: 0|>,
        <|k1: 1|>,
        <|k1: 2|>,
        <|k1: 3|>,
        <|k1: 4|>,
        <|k1: NULL|>,
    ]) AS t1
LEFT JOIN
    AS_TABLE([
        <|k2: -2|>,
        <|k2: 0|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 4|>,
        <|k2: 6|>,
        <|k2: NULL|>,
    ]) AS t2
ON
    t1.k1 == t2.k2
WHERE
    t1.k1 > 0
;

SELECT
    *
FROM
    AS_TABLE([
        <|k1: -2|>,
        <|k1: -1|>,
        <|k1: 0|>,
        <|k1: 0|>,
        <|k1: 1|>,
        <|k1: 2|>,
        <|k1: 3|>,
        <|k1: 4|>,
        <|k1: NULL|>,
    ]) AS t1
RIGHT JOIN
    AS_TABLE([
        <|k2: -2|>,
        <|k2: 0|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 4|>,
        <|k2: 6|>,
        <|k2: NULL|>,
    ]) AS t2
ON
    t1.k1 == t2.k2
WHERE
    t2.k2 > 0
;

SELECT
    *
FROM
    AS_TABLE([
        <|k1: -2|>,
        <|k1: -1|>,
        <|k1: 0|>,
        <|k1: 0|>,
        <|k1: 1|>,
        <|k1: 2|>,
        <|k1: 3|>,
        <|k1: 4|>,
        <|k1: NULL|>,
    ]) AS t1
LEFT SEMI JOIN
    AS_TABLE([
        <|k2: -2|>,
        <|k2: 0|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 4|>,
        <|k2: 6|>,
        <|k2: NULL|>,
    ]) AS t2
ON
    t1.k1 == t2.k2
WHERE
    t1.k1 > 0
;

SELECT
    *
FROM
    AS_TABLE([
        <|k1: -2|>,
        <|k1: -1|>,
        <|k1: 0|>,
        <|k1: 0|>,
        <|k1: 1|>,
        <|k1: 2|>,
        <|k1: 3|>,
        <|k1: 4|>,
        <|k1: NULL|>,
    ]) AS t1
RIGHT SEMI JOIN
    AS_TABLE([
        <|k2: -2|>,
        <|k2: 0|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 4|>,
        <|k2: 6|>,
        <|k2: NULL|>,
    ]) AS t2
ON
    t1.k1 == t2.k2
WHERE
    t2.k2 > 0
;

SELECT
    *
FROM
    AS_TABLE([
        <|k1: -2|>,
        <|k1: -1|>,
        <|k1: 0|>,
        <|k1: 0|>,
        <|k1: 1|>,
        <|k1: 2|>,
        <|k1: 3|>,
        <|k1: 4|>,
        <|k1: NULL|>,
    ]) AS t1
LEFT ONLY JOIN
    AS_TABLE([
        <|k2: -2|>,
        <|k2: 0|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 4|>,
        <|k2: 6|>,
        <|k2: NULL|>,
    ]) AS t2
ON
    t1.k1 == t2.k2
WHERE
    t1.k1 > 0
;

SELECT
    *
FROM
    AS_TABLE([
        <|k1: -2|>,
        <|k1: -1|>,
        <|k1: 0|>,
        <|k1: 0|>,
        <|k1: 1|>,
        <|k1: 2|>,
        <|k1: 3|>,
        <|k1: 4|>,
        <|k1: NULL|>,
    ]) AS t1
RIGHT ONLY JOIN
    AS_TABLE([
        <|k2: -2|>,
        <|k2: 0|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 2|>,
        <|k2: 4|>,
        <|k2: 6|>,
        <|k2: NULL|>,
    ]) AS t2
ON
    t1.k1 == t2.k2
WHERE
    t2.k2 > 0
;
