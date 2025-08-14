/* postgres can not */
/* syntax version 1 */
PRAGMA Library("agg.sql");
PRAGMA Library("lib.sql");

IMPORT lib SYMBOLS $Square, $Agg_sum, $Agg_max;
SELECT $Square(2);

SELECT
  AGGREGATE_BY(x, $Agg_sum),
  AGGREGATE_BY(x, $Agg_max)
FROM (
  SELECT 2 AS x
  UNION ALL
  SELECT 3 AS x
)
