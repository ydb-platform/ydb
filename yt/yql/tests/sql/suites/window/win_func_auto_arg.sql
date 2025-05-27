/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA DisableAnsiRankForNullableKeys;

SELECT
    RANK() over w as rank_noarg,
    DENSE_RANK() over w as dense_rank_noarg,
    RANK(cast(subkey as uint32) / 10 % 2) over w as rank,
    DENSE_RANK(cast(subkey as uint32) / 10 % 2) over w as dense_rank,
    zz.*
FROM
    Input4 as zz
WINDOW
    w as (PARTITION BY key ORDER BY subkey)
ORDER BY key, subkey, value
;
