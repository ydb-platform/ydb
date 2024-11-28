/* postgres can not */
/* hybridfile can not YQL-17743 */
/* multirun can not */
/* syntax version 1 */
USE plato;

INSERT INTO Output
SELECT
    -(CAST(key AS Int32) ?? 0) AS key,
    subkey,
    value
FROM Input
ASSUME ORDER BY
    key DESC;
