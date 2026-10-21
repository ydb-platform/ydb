SELECT
    k1 AND (k1 OR WithSideEffects(ENSURE(TRUE, FALSE, 'should_fail'))),
FROM
    AS_TABLE([
        <|k1: TRUE|>,
        <|k1: FALSE|>,
    ])
;
