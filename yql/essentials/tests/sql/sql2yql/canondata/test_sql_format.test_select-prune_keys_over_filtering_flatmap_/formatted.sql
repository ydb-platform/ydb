PRAGMA config.flags('OptimizerFlags', 'EmitPruneKeys');

$table = [<|some_value: 9, some_key: 'key'|>, <|some_value: 7, some_key: 'key'|>];

$keys = (
    SELECT
        'key' AS some_key
);

$with_value = (
    SELECT
        some_key,
        1 AS has_value,
    FROM
        as_table($table)
    WHERE
        some_value == 7 AND some_key == 'key'
);

SELECT
    has_value
FROM
    $keys AS A
LEFT JOIN ANY
    $with_value AS B
ON
    A.some_key == B.some_key
;
