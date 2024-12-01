/* postgres can not */
/* hybridfile can not YQL-17743 */
USE plato;

INSERT INTO Output
SELECT
    x
FROM (
    SELECT
        ListFromRange(0, 100) AS x
)
    FLATTEN BY
        x
ORDER BY
    x DESC;
