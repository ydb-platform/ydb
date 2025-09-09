PRAGMA config.flags('OptimizerFlags', 'OptimizeXNotX');

SELECT
    k1 AND WithSideEffects(k2) AND NOT k1,
    k1 AND WithSideEffects(k2) AND k1 AND NOT k1,
    k1 AND NOT k1 AND WithSideEffects(k2),
    k1 AND NOT k1 AND WithSideEffects(k2) AND k1,
    k1 AND NOT k1 AND WithSideEffects(k2) AND k1 AND NOT k1,
    k1 AND NOT k1 AND WithSideEffects(k2) AND k2 AND NOT k2,
    WithSideEffects(k1) AND NOT WithSideEffects(k1),
FROM
    AS_TABLE([
        <|k1: TRUE, k2: TRUE|>,
        <|k1: TRUE, k2: FALSE|>,
        <|k1: FALSE, k2: TRUE|>,
        <|k1: FALSE, k2: FALSE|>,
    ])
;
