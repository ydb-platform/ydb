PRAGMA config.flags('OptimizerFlags', 'EmitSkipNullOnPushdownUsingUnessential');

SELECT
    *
FROM
    AS_TABLE([
        <|k1: (1, 0)|>,
        <|k1: (2, 0)|>,
        <|k1: (3, 0)|>,
        <|k1: (4, 0)|>,
        <|k1: (NULL, 0)|>,
    ]) AS t1
INNER JOIN
    AS_TABLE([
        <|k2: (2, 0)|>,
        <|k2: (3, 0)|>,
        <|k2: (4, 0)|>,
        <|k2: (5, 0)|>,
        <|k2: (NULL, 0)|>,
    ]) AS t2
ON
    t1.k1 == t2.k2
WHERE
    t1.k1.0 > 2
;
