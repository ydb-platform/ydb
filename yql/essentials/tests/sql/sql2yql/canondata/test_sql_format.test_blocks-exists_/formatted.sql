USE plato;

SELECT
    key,
    maybe_null IS NULL AS is_maybe_null,
    always_null IS NULL AS is_always_null,
    never_null IS NULL AS is_never_null,
FROM
    Input
ORDER BY
    key
;
