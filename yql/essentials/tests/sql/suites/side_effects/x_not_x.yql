pragma config.flags("OptimizerFlags", "OptimizeXNotX");
pragma warning("disable","4510");

SELECT
    k1 AND Yql::WithSideEffectsMode(k2, AsAtom('General')) AND NOT k1,
    k1 AND Yql::WithSideEffectsMode(k2, AsAtom('General')) AND k1 AND NOT k1,
    k1 AND NOT k1 AND Yql::WithSideEffectsMode(k2, AsAtom('General')),
    k1 AND NOT k1 AND Yql::WithSideEffectsMode(k2, AsAtom('General')) AND k1,
    k1 AND NOT k1 AND Yql::WithSideEffectsMode(k2, AsAtom('General')) AND k1 AND NOT k1,
    k1 AND NOT k1 AND Yql::WithSideEffectsMode(k2, AsAtom('General')) AND k2 AND NOT k2,
    Yql::WithSideEffectsMode(k1, AsAtom('General')) AND NOT Yql::WithSideEffectsMode(k1, AsAtom('General')),
FROM
    AS_TABLE([
        <|k1: TRUE,  k2: TRUE|>,
        <|k1: TRUE,  k2: FALSE|>,
        <|k1: FALSE, k2: TRUE|>,
        <|k1: FALSE, k2: FALSE|>,
    ])
;
