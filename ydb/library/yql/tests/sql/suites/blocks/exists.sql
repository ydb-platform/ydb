USE plato;

SELECT
    key,
    maybe_null is NULL as is_maybe_null,
    always_null is NULL as is_always_null,
    never_null is NULL as is_never_null,
FROM Input
ORDER BY key;
