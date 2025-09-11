/* custom error: Condition violated */
SELECT
    ListHasItems([WithSideEffects(Ensure(1, FALSE))])
;
