/* syntax version 1 */
/* postgres can not */
$udfScript = @@
def processRow(row, tag, separator):
	return {"value":row.Name + separator + row.Value + separator + tag};
@@;

$udf = Python::processRow(
    Callable<(Struct<Name:String, Tag:String, Value:String>, String, String)->Struct<value:String>>,
    $udfScript
);

$data = (
    SELECT key AS Name, value AS Value, subkey AS Tag FROM plato.Input0
);

$separator = "|";

PROCESS $data USING $udf(TableRow(), Tag, $separator);
