PRAGMA config.flags('OptimizerFlags', 'EqualityFilterOverJoin');

$a = (
    SELECT
        *
    FROM
        as_table([
            <|x: Just(1), t: 1, u: 1, extra: 1|>,
            <|x: 1, t: 1, u: 5, extra: 2|>,
        ])
);

$b = (
    SELECT
        *
    FROM
        as_table([
            <|y: 1|>,
            <|y: 1|>,
        ])
);

$c = (
    SELECT
        *
    FROM
        as_table([
            <|z: 1|>,
            <|z: 1|>,
        ])
);

$d = (
    SELECT
        *
    FROM
        as_table([
            <|c: 2, d: 3|>,
            <|c: 3, d: 3|>,
        ])
);

SELECT
    *
FROM (
    SELECT
        c.z AS cz,
        b.y AS by,
        a.u AS au,
        a.t AS at,
        a.x AS ax,
        d.c AS dc,
        d.d AS dd
    FROM
        $a AS a
    RIGHT JOIN
        $b AS b
    ON
        a.x == b.y
    CROSS JOIN
        $d AS d
    FULL JOIN
        $c AS c
    ON
        b.y == c.z
)
WHERE
    cz == at AND by == au AND ax == by AND dc == dd
;
