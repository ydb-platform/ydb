/* ignore runonopt plan diff - extra LogicalOptimizer-PushdownOpColumns */
PRAGMA yt.PruneKeyFilterLambda = 'true';

USE plato;

$src = (
    SELECT
        *
    FROM
        Input
    WHERE
        key == '1' || '5' || '0'
);

SELECT
    key,
    subkey
FROM
    $src
;

SELECT
    key,
    value
FROM
    $src
WHERE
    key >= '000' AND key < '999' AND len(value) > 0
;
