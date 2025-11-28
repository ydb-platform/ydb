/* syntax version 1 */
/* postgres can not */

PRAGMA warning("disable", "4520");
PRAGMA warning("disable", "4521");
PRAGMA AnsiRankForNullableKeys;

SELECT
    key,
    subkey,
    RANK()                              over w1 as r1,
    DENSE_RANK()                        over w1 as r2,

    RANK()                              over w2 as r3,
    DENSE_RANK()                        over w2 as r4,
FROM plato.Input
WINDOW
    w1 as (PARTITION BY key ROWS BETWEEN UNBOUNDED PRECEDING AND 5 PRECEDING),
    w2 as (                 ROWS BETWEEN 3 FOLLOWING AND 2 FOLLOWING)
ORDER BY key,subkey;
