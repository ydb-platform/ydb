/* postgres can not */
USE plato;

INSERT INTO Output1
SELECT
    key AS key,
    '' AS subkey,
    'value:' || value AS value
FROM
    Input
WHERE
    key < '100'
ORDER BY
    key
;

INSERT INTO Output2
SELECT
    key AS key,
    '' AS subkey,
    'value:' || value AS value
FROM
    Input
WHERE
    key < '200'
ORDER BY
    key
;

INSERT INTO Output1
SELECT
    *
FROM
    Input
ORDER BY
    key
;

INSERT INTO Output2
SELECT
    *
FROM
    Input
ORDER BY
    key
;
