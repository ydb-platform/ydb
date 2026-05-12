/* syntax version 1 */
USE plato;

$serialize = Udf(ReservoirSampling::Serialize, String);
SELECT $serialize(ReservoirSampling::Add(ReservoirSampling::Create("abc", 123ul), "123"));
