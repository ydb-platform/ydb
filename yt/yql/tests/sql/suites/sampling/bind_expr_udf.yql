/* syntax version 1 */
/* postgres can not */
USE plato;

$percent = Math::Ceil(0.2);

SELECT * FROM Input TABLESAMPLE BERNOULLI(Math::Ceil(100 * $percent)) ORDER BY key; -- 100% sample
