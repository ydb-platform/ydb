/* syntax version 1 */
/* postgres can not */

PRAGMA warning("disable", "4520");
PRAGMA DisableAnsiRankForNullableKeys;

SELECT
    key,
    RANK()                              over w1 as r1,
    DENSE_RANK()                        over w1 as r2,
FROM AS_TABLE([<|key:1|>, <|key:null|>, <|key:null|>, <|key:1|>, <|key:2|>])
WINDOW
    w1 as (ORDER BY key ROWS BETWEEN UNBOUNDED PRECEDING AND 5 PRECEDING)
ORDER BY key;
