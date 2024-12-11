/* postgres can not */
PRAGMA config.flags("OptimizerFlags", "FieldSubsetEnableMultiusage");

USE plato;

$input = (
    SELECT
        key,
        key || subkey AS subkey,
        value
    FROM
        Input
);

$total_count = (
    SELECT
        count(1)
    FROM
        $input
);

$filtered = (
    SELECT
        *
    FROM
        $input
    WHERE
        key IN ("023", "037", "075")
);

$filtered_cnt = (
    SELECT
        count(1)
    FROM
        $filtered
);

SELECT
    $filtered_cnt / CAST($total_count AS Double) AS cnt
;
