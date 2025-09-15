/* custom error: Condition violated */
SELECT
    (NOT WithSideEffects(x)) AND FALSE
FROM (
    SELECT
        Ensure(FALSE, FALSE) AS x
);
