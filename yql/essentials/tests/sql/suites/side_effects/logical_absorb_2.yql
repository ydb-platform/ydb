/* custom error:should_fail*/
SELECT
    (WithSideEffects(ENSURE(TRUE, FALSE, 'should_fail')) AND k1) OR k1,
FROM
    AS_TABLE([
        <|k1: TRUE|>,
        <|k1: FALSE|>,
    ])
;
