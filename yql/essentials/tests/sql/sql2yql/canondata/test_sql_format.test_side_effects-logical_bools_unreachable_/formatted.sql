SELECT
    FALSE AND (NOT WithSideEffects(x))
FROM (
    SELECT
        Ensure(FALSE, FALSE) AS x
);
