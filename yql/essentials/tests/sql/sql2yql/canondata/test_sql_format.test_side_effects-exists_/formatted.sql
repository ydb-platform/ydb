/* custom error: Condition violated */
SELECT
    just(WithSideEffects(Ensure(1, FALSE))) IS NOT NULL
;
