/* custom error:shoud_fail*/
pragma config.flags("OptimizerFlags", "OptimizeXNotX");
pragma warning("disable","4510");

SELECT
    k1 AND Yql::WithSideEffectsMode(ENSURE(TRUE, FALSE, 'shoud_fail'), AsAtom('General')) AND NOT k1,
FROM
    AS_TABLE([
        <|k1: TRUE|>,
        <|k1: FALSE|>,
    ])
;
