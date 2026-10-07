/* custom error:should_fail*/
SELECT
    k1 OR (WithSideEffects(ENSURE(TRUE, FALSE, 'should_fail')) AND k1),
FROM
    AS_TABLE([
        <|k1: TRUE|>,
        <|k1: FALSE|>,
    ])
;
