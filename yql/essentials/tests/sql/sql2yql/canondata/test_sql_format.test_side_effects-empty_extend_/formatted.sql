/* custom error: Condition violated */
SELECT
    ListExtend([1], WithSideEffects(Ensure(ListTake([2], 0), FALSE)))
;
