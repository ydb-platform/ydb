/* syntax version 1 */
/* postgres can not */
$udfScript = @@
def processRows(prefix, rowList, separator):
	result = [];
	for row in rowList:
		result.append({"Data" : prefix + row.Name + separator + row.Value});
	
	return result;
@@;

$udf = Python::processRows(
    Callable<(String, Stream<Struct<Name:String, Value:String>>, String)->Stream<Struct<Data:String>>>,
    $udfScript
);

$data = (
	SELECT key AS Name, value AS Value FROM plato.Input1
);

$prefix = ">>";

PROCESS $data USING $udf($prefix, TableRows(), "=") WHERE Name != "foo";
