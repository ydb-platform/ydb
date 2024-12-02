/* syntax version 1 */
/* postgres can not */
USE plato;
PRAGMA yt.EvaluationTableSizeLimit = "1";

SELECT
    *
FROM Input
LIMIT 1;

$tables = (
    SELECT
        aggregate_list(Path) AS dates
    FROM folder("")
    WHERE Path LIKE "Input%"
);

SELECT
    count(*)
FROM each($tables);
