/* custom error: Condition violated */
SELECT
    <|a: 1, b: WithSideEffects(Ensure(2, FALSE))|>.a
;
