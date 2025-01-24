/* postgres can not */
SELECT
    o1 XOR o2 AS xor,
    o1 XOR o1 AS xor_same_o1,
    o2 XOR o2 AS xor_same_o2,
    o1 XOR o2 XOR o1 XOR o2 XOR o1 XOR o2 AS xor_triple_dups,
    o1 XOR o2 XOR Unwrap(o1) XOR Unwrap(o2) AS xor_with_unwraps,
FROM AS_TABLE([
    <|o1: FALSE, o2: FALSE|>,
    <|o1: TRUE, o2: TRUE|>,
    <|o1: TRUE, o2: FALSE|>,
    <|o1: FALSE, o2: TRUE|>,
    <|o1: TRUE, o2: NULL|>,
    <|o1: FALSE, o2: NULL|>,
    <|o1: NULL, o2: TRUE|>,
    <|o1: NULL, o2: FALSE|>
]);
