/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA OrderedColumns;
PRAGMA yt.PoolTrees = 'physical,cloud';

INSERT INTO Input
SELECT
    key,
    subkey,
    value
FROM
    Input
;
