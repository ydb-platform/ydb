SELECT
    WithSideEffectsMode((NOT WithSideEffects(x)), 'None')
    AND FALSE
FROM (
    SELECT
        Ensure(FALSE, FALSE) AS x
);
