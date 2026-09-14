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

$distinct_a = (
    SELECT DISTINCT
        *
    FROM
        $a
);

SELECT
    a.x
FROM ANY
    $distinct_a AS a
JOIN ANY
    $b AS b
ON
    a.x == b.y
;
