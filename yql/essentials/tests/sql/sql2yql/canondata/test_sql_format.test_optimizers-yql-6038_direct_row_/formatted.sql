/* syntax version 1 */
/* postgres can not */
USE plato;

$queries_0 = (
    SELECT DISTINCT
        key
    FROM
        Input
);

$queries = (
    SELECT
        TableRecordIndex() AS j,
        key
    FROM
        $queries_0
);

$count = (
    SELECT
        count(*)
    FROM
        $queries
);

$users_0 = (
    SELECT
        ListFromRange(0, 3) AS lst,
        TableRecordIndex() AS idx,
        subkey
    FROM
        Input AS t
);

$users = (
    SELECT
        CAST(Random(idx + x) AS Uint64) % $count AS j,
        subkey
    FROM
        $users_0
        FLATTEN BY lst AS x
);

SELECT
    *
FROM
    $queries AS queries
JOIN
    $users AS users
USING (j)
ORDER BY
    key,
    subkey
;
