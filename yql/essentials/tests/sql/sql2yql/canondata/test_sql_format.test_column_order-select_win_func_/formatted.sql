/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA OrderedColumns;

SELECT
    min(subkey) OVER (
        PARTITION BY
            key
    ) AS zz,
    row_number() OVER (
        ORDER BY
            key,
            subkey
    ) AS z,
    a.*
FROM Input
    AS a
ORDER BY
    key,
    subkey;
