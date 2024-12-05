/* syntax version 1 */
/* postgres can not */
USE plato;

$src =
    SELECT
        NULL AS key,
        value
    FROM Input;

$src_opt =
    SELECT
        NULL AS key,
        Just(value) AS value
    FROM Input;

$src_null =
    SELECT
        NULL AS key,
        NULL AS value
    FROM Input;

SELECT
    min_by(value, key)
FROM $src;

SELECT
    max_by(value, key)
FROM $src_opt;

SELECT
    min_by(value, key)
FROM $src_null;

SELECT
    max_by(value, key)
FROM (
    SELECT
        *
    FROM $src
    LIMIT 0
);

SELECT
    min_by(value, key)
FROM (
    SELECT
        *
    FROM $src_opt
    LIMIT 0
);

SELECT
    max_by(value, key)
FROM (
    SELECT
        *
    FROM $src_null
    LIMIT 0
);

SELECT
    min_by(value, key)
FROM (
    SELECT
        Nothing(String?) AS key,
        value
    FROM Input
);
