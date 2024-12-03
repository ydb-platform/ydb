/* postgres can not */
/* kikimr can not - no ref select mode */
SELECT
    key,
    subkey,
    value || "foo" AS new_value
FROM plato.Input;
