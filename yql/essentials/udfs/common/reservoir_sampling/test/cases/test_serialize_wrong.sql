$serialize = Udf(ReservoirSampling::Serialize, Int32);
SELECT $serialize(ReservoirSampling::Create("abc", 123ul));
