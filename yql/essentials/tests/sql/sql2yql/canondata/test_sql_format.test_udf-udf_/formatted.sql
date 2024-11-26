/* postgres can not */
SELECT
    key,
    subkey,
    SimpleUdf::Concat(value, "test") AS value
FROM plato.Input;
