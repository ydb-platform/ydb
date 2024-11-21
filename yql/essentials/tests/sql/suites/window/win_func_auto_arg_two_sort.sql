/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA DisableAnsiRankForNullableKeys;

--INSERT INTO Output
SELECT
    RANK() over w as rank_noarg,
    DENSE_RANK() over w as dense_rank_noarg,
    RANK(subkey) over w as rank,
    DENSE_RANK(subkey) over w as dense_rank,
    zz.*
FROM
    Input4 as zz
WINDOW
    w as (PARTITION BY key ORDER BY subkey, value)
ORDER BY key, subkey, value
;
