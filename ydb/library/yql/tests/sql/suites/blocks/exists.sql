USE plato;
/* XXX: Enable UseBlocks pragma and provide input to trigger block execution. */
PRAGMA UseBlocks;

SELECT
    key,
    maybe_null is NULL as is_maybe_null,
    always_null is NULL as is_always_null,
    never_null is NULL as is_never_null,
FROM Input
ORDER BY key;
