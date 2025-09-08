/* custom error: Condition violated */
SELECT
    (1, WithSideEffects(Ensure(2, FALSE))).0
;
