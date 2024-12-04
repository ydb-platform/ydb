/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA OrderedColumns;

INSERT INTO Output WITH truncate
SELECT
    a.*,
    count(key) OVER (
        PARTITION BY
            subkey
    ) AS cnt
FROM Input
    AS a;
