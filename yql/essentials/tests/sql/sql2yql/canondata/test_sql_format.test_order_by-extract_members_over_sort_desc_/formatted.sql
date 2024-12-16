/* postgres can not */
/* multirun can not */
USE plato;

$top = (
    SELECT
        *
    FROM
        Input
    ORDER BY
        value DESC
    LIMIT 100
);

INSERT INTO Output
SELECT
    key,
    value
FROM
    $top
ORDER BY
    value DESC
;

SELECT
    *
FROM
    $top
;
