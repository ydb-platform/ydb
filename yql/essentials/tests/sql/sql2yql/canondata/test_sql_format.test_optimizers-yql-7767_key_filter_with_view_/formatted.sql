/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    key,
    value || '_y' ?? '' AS value
FROM
    range('', 'Input1', 'Input2')
WHERE
    key > '010'
ORDER BY
    key,
    value
;
