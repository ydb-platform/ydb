/* custom error: Condition violated */
SELECT
    ListLength([WithSideEffects(Ensure(1, FALSE))])
;
