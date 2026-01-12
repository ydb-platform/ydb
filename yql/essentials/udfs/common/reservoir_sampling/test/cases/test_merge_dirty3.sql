/* syntax version 1 */
USE plato;

$serialize = Udf(ReservoirSampling::Serialize, String);
$merge = Udf(ReservoirSampling::Merge, String);
SELECT $serialize($merge(ReservoirSampling::Create("cde", 123ul), $merge(ReservoirSampling::Create("abc", 123ul), ReservoirSampling::Create("cde", 123ul))));
