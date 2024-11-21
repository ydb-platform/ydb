/* postgres can not */

$data = AsList(
    AsStruct(1u AS Key, "v1" AS Value1, 100 AS Value2),
    AsStruct(2u AS Key, "v2" AS Value1, 200 AS Value2),
    AsStruct(3u AS Key, "v3" AS Value1, 300 AS Value2));

SELECT Key, Value3 FROM AS_TABLE($data);

