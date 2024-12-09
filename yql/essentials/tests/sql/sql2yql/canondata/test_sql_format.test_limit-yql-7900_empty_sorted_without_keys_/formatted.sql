/* postgres can not */
USE plato;

$cnt = (
    SELECT
        count(*)
    FROM Input
);
$offset = ($cnt + 10) ?? 0;

$data_limited = (
    SELECT
        *
    FROM Input
    ORDER BY
        key || value
    LIMIT 1 OFFSET $offset
);

$result_top = (
    SELECT
        subkey,
        Length(key) AS l,
        key
    FROM $data_limited
);

SELECT
    *
FROM $result_top;
