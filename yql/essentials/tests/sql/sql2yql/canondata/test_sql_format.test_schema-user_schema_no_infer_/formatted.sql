/* syntax version 1 */
USE plato;

PRAGMA yt.InferSchema;

SELECT
    *
FROM
    Input WITH SCHEMA Struct<key: String>
;
