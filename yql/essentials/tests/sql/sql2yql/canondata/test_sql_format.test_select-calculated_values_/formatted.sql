/* postgres can not */
/* can not check this with postgres becouse order of columns is not specified here */
SELECT
    key,
    (value || "ab"),
    (value || "a"),
    value
FROM plato.Input
ORDER BY
    key;
