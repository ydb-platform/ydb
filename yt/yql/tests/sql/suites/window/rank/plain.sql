/* syntax version 1 */
/* postgres can not */

PRAGMA warning("disable", "4520");
PRAGMA AnsiRankForNullableKeys;

SELECT
    key,
    subkey,
    RANK()                              over w1 as r1,
    DENSE_RANK()                        over w1 as r2,
    RANK(subkey)                        over w1 as r3,
    DENSE_RANK(subkey)                  over w1 as r4,

    RANK()                              over w2 as r5,
    DENSE_RANK()                        over w2 as r6,
    RANK(subkey || subkey)              over w2 as r7,
    DENSE_RANK(subkey || subkey)        over w2 as r8,
FROM (SELECT * FROM plato.Input WHERE key = '1')
WINDOW
    w1 as (PARTITION BY key ORDER BY subkey ROWS BETWEEN UNBOUNDED PRECEDING AND 5 PRECEDING),
    w2 as (                 ORDER BY key, subkey ROWS BETWEEN 3 FOLLOWING AND 2 FOLLOWING)
ORDER BY key,subkey;
