/* custom error: Condition violated */
SELECT
    if(TRUE, WithSideEffects(ensure(NULL, FALSE)), 1)
;
