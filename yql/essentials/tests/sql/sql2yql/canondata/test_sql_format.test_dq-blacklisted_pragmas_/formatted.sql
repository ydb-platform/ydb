/* postgres can not */
/* dq can not */
/* syntax version 1 */
USE plato;

PRAGMA OrderedColumns;
PRAGMA yt.MaxRowWeight = '32M';

INSERT INTO Input
SELECT
    key,
    subkey,
    value
FROM
    Input
;
