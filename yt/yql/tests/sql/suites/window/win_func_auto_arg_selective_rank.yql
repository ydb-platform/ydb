/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA DisableSimpleColumns;
PRAGMA DisableAnsiRankForNullableKeys;

--INSERT INTO Output
SELECT
    RANK() over w as rank_noarg,
    DENSE_RANK() over w as dense_rank_noarg,
    RANK(AsTuple(key, value)) over w as rank,
    DENSE_RANK(AsTuple(key, value)) over w as dense_rank,
    zz.*
FROM
    Input4 as zz
WINDOW
    w as (ORDER BY key, subkey, value)
;
