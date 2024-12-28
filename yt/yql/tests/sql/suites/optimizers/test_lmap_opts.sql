/* syntax version 1 */
-- not supported on windows
/* postgres can not */
$udfScript = @@

import collections;

def processRows(prefix, rowList, separator):
    ResultRow = collections.namedtuple("ResultRow", ["Result"]);

    result = [];
    for row in rowList:
        resultValue = prefix + row.Name + separator + row.Value;
        resultItem = ResultRow(Result=resultValue);
        result.append(resultItem);
    
    return result;
@@;

$udf = Python::processRows(
    Callable<(String, List<Struct<Name:String, Value:String>>, String)->List<Struct<Result:String>>>,
    $udfScript
);

$data = (
    SELECT key AS Name, value AS Value FROM plato.Input0
);

$prefix = ">>";

$p1 = (
    PROCESS $data USING $udf($prefix, TableRows(), "=") WHERE Name != "foo"
);

$p2 = (
    SELECT Result AS Data FROM $p1
);

$p3 = (
    PROCESS $p2 USING Streaming::Process(TableRows(), "grep", AsList("180"))
);

$p4 = (
    SELECT Data AS FinalResult FROM $p3
);

SELECT Avg(Length(FinalResult)) AS AvgResultLength FROM $p4;
