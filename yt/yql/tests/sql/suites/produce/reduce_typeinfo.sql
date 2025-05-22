/* postgres can not */
/* syntax version 1 */
/* ignore runonopt plan diff */
use plato;

pragma warning("disable", "4510");

$r1 = REDUCE Input0 ON key USING ALL SimpleUdf::GenericAsStruct(TableRows());
$r2 = REDUCE Input0 ON key USING SimpleUdf::GenericAsStruct(cast(TableRow().subkey as Int32));
$r3 = REDUCE Input0 ON key USING ALL SimpleUdf::GenericAsStruct(TableRow().key);


select * from (select * from $r1 flatten list by arg_0) flatten columns order by key, subkey;
select arg_0 as key, ListSort(YQL::Collect(arg_1)) as values from $r2 order by key;


select FormatType(TypeOf(TableRow())) from $r1 limit 1;
select FormatType(TypeOf(TableRow())) from $r3 limit 1;
