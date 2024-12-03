/* postgres can not */
/* multirun can not */
/* syntax version 1 */
/* hybridfile can not YQL-17743 */
USE plato;

INSERT INTO Output
SELECT
    *
FROM concat(`Input[#3]`, `Input[#0]`, `Input[#2]`)
ASSUME ORDER BY
    key DESC;
