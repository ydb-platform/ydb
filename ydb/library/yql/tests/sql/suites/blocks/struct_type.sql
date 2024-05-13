USE plato;
/* XXX: Enable UseBlocks pragma and provide input to trigger block execution. */
pragma UseBlocks;

SELECT
    key,
    SOME(val) as someVal,
FROM Input
GROUP BY key
ORDER BY key
