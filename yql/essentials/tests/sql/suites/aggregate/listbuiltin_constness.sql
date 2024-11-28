/* syntax version 1 */
/* postgres can not */

SELECT
  ListMap([1,2,3], ($x) -> ($x)) AS x
FROM AS_TABLE([<|key:1|>,<|key:2|>])
GROUP BY key;

