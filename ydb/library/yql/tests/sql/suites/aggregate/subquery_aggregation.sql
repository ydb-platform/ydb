/* syntax version 1 */
/* postgres can not */
USE plato;

$total_count = (SELECT Count(*) from Input);

SELECT
  common,
  count(*) as rec_count,
  100. * count(*) / $total_count as part_percent
FROM Input
GROUP BY subkey as common
ORDER BY common
;
