/* custom error: Condition violated */
SELECT
    ListTop(WithSideEffects(Ensure([1], FALSE)), 0)
;
