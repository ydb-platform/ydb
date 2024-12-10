/* postgres can not */
/* kikimr can not - no ref select mode */
SELECT
    key,
    subkey,
    value
FROM
    plato.Input
ORDER BY
    key,
    subkey,
    value
;
