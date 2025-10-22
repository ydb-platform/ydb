SELECT
    (NOT WithSideEffects(x)) AND TRUE
FROM (
    SELECT
        Ensure(FALSE, TRUE) AS x
);
