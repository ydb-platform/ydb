/* postgres can not */

$data = AsList(1u, 2u, 3u);

SELECT Key, Value3 FROM AS_TABLE($data);
