SELECT
    k1 OR (k1 AND WithSideEffects(ENSURE(TRUE, FALSE, 'should_fail'))),
FROM
    AS_TABLE([
        <|k1: TRUE|>,
        <|k1: FALSE|>,
    ])
;
