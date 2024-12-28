/* syntax version 1 */
/* postgres can not */

$i1 = SELECT CAST(key AS Int32) ?? 0 as key, '' as value FROM plato.Input1;
$i2 = SELECT 0 as key, value from plato.Input2 UNION ALL SELECT 1 as key, value from plato.Input3;
$i3 = (SELECT 2 as key, value from plato.Input4 UNION ALL SELECT 3 as key, value from plato.Input5);

$udf = ($x) -> { return Yql::VariantItem($x) };

PROCESS $i1, $i2, $i3 using $udf(TableRow());
