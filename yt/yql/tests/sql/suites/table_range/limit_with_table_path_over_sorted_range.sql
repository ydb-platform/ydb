/* postgres can not */
/* kikimr can not */

SELECT key, value, SUBSTRING(TablePath(), NULL, CAST(LENGTH(TablePath()) - 1 as Uint32)) as path
FROM plato.range("","Input1", "Input2")
LIMIT 2
;