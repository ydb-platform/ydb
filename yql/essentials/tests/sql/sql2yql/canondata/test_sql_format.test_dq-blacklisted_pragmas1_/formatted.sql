/* postgres can not */
/* dq can not */
/* syntax version 1 */
USE plato;
PRAGMA OrderedColumns;
PRAGMA yt.PoolTrees = 'test';

INSERT INTO Input
SELECT
    key,
    subkey,
    value
FROM
    Input
;
