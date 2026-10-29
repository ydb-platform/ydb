-- Tagged numeric vectors declare List<Float>, so empty vectors must return a list.
$empty = ListCreate(Float);
select Knn::FloatFromBinaryString(Knn::ToBinaryStringFloat($empty));
select Knn::FloatFromBinaryString(Knn::ToBinaryStringFloat16($empty));
select Knn::FloatFromBinaryString(Knn::ToBinaryStringBFloat16($empty));
select Knn::FloatFromBinaryString(Knn::ToBinaryStringFloat8E4M3($empty));
select Knn::FloatFromBinaryString(Knn::ToBinaryStringFloat8E5M2($empty));
select Knn::FloatFromBinaryString(Knn::ToBinaryStringInt8(ListCreate(Int8)));
select Knn::FloatFromBinaryString(Knn::ToBinaryStringUint8(ListCreate(Uint8)));

-- Optional and raw inputs retain their existing nullable result semantics.
select Knn::FloatFromBinaryString(Just(Knn::ToBinaryStringFloat8E4M3($empty))) is null;
select Knn::FloatFromBinaryString(Just(Knn::ToBinaryStringFloat8E5M2($empty))) is null;
select Knn::FloatFromBinaryString(Untag(Knn::ToBinaryStringFloat8E4M3($empty), "Float8E4M3Vector")) is null;
select Knn::FloatFromBinaryString(Untag(Knn::ToBinaryStringFloat8E5M2($empty), "Float8E5M2Vector")) is null;
