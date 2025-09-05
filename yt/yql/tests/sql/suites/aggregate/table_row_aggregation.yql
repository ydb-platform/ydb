/* syntax version 1 */
/* postgres can not */
USE plato;

$data = (
  SELECT SOME(TableRow())
  FROM Input4
  GROUP BY key
);

SELECT *
FROM $data
FLATTEN COLUMNS
ORDER BY key;
