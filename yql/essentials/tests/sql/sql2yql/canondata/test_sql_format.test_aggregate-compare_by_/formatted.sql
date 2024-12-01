/* syntax version 1 */
/* postgres can not */
SELECT
    min_by(sub, key) AS min,
    max_by(value, sub) AS max,
    min_by(key, length(sub), 2) AS min_list,
    min_by(empty, length(sub), 2) AS empty_result,
    max_by(key, empty, 2) AS empty_by
FROM (
    SELECT
        CAST(key AS int) AS key,
        Unwrap(CAST(subkey AS int)) AS sub,
        value AS value,
        CAST(value AS int) AS empty
    FROM plato.Input
);
