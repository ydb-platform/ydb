PRAGMA config.flags('OptimizerFlags', 'EmitSkipNullOnPushdownUsingUnessential');

SELECT
    *
FROM
    AS_TABLE([
        <|k1: 1|>,
        <|k1: 2|>,
        <|k1: 3|>,
        <|k1: 4|>,
        <|k1: NULL|>,
    ]) AS t1
INNER JOIN
    AS_TABLE([
        <|k2: 2|>,
        <|k2: 3|>,
        <|k2: 4|>,
        <|k2: 5|>,
        <|k2: NULL|>,
    ]) AS t2
ON
    t1.k1 == t2.k2
WHERE
    t1.k1 > 2
;
