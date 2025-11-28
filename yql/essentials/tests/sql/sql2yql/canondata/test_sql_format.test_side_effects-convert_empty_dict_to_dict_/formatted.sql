/* custom error: Condition violated */
SELECT
    if(TRUE, WithSideEffects(ensure({}, FALSE)), {1, 2, 3})
;
