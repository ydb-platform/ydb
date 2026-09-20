/* custom error:should_fail*/
SELECT
    (k1 AND WithSideEffects(ENSURE(TRUE, FALSE, 'should_fail'))) OR k1,
FROM
    AS_TABLE([
        <|k1: TRUE|>,
        <|k1: FALSE|>,
    ])
;
