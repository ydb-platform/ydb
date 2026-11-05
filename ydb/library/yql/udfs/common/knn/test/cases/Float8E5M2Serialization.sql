-- FP8 serialization, rounding, saturation, and exact decoding.
$vector = [1.0f, 2.0f, 0.5f, -1.0f, 4.0f];
$encoded = Knn::ToBinaryStringFloat8E5M2($vector);
select Len(Untag($encoded, "Float8E5M2Vector")) == 6;
select Knn::FloatFromBinaryString($encoded) = $vector;
select Knn::FloatFromBinaryString(Knn::ToBinaryStringFloat8E5M2([1.125f])) = [1.0f];
select Knn::FloatFromBinaryString(Knn::ToBinaryStringFloat8E5M2([1.1251f])) = [1.25f];
select Knn::FloatFromBinaryString(Knn::ToBinaryStringFloat8E5M2([100000.0f, -100000.0f])) = [57344.0f, -57344.0f];
select Knn::FloatFromBinaryString(Knn::ToBinaryStringFloat8E5M2([0.0000152587890625f])) = [0.0000152587890625f];

-- SIMD-sized lazy lists and tails; references use decoded FP8 coordinates.
$a = Knn::ToBinaryStringFloat8E5M2(ListFlatten([ListFlatten(ListReplicate([1.03f, 2.3f, 0.07f, -1.1f], 16)), [0.01f, 2.0f, 3.0f]]));
$b = Knn::ToBinaryStringFloat8E5M2(ListFlatten([ListFlatten(ListReplicate([4.3f, 0.01f, 2.1f, 1.2f], 16)), [4.0f, 5.0f, 6.0f]]));
$af = Knn::ToBinaryStringFloat(Knn::FloatFromBinaryString($a));
$bf = Knn::ToBinaryStringFloat(Knn::FloatFromBinaryString($b));
select Abs(Knn::CosineDistance($a, $b) - Knn::CosineDistance($af, $bf)) < 0.00001f;
select Abs(Knn::CosineSimilarity($a, $b) - Knn::CosineSimilarity($af, $bf)) < 0.00001f;
select Abs(Knn::InnerProductSimilarity($a, $b) - Knn::InnerProductSimilarity($af, $bf)) < 0.0001f;
select Abs(Knn::EuclideanDistance($a, $b) - Knn::EuclideanDistance($af, $bf)) < 0.0001f;
select Abs(Knn::ManhattanDistance($a, $b) - Knn::ManhattanDistance($af, $bf)) < 0.0001f;

-- Raw and tagged overloads, nulls, optional auto-unpacking, and validation.
select Knn::FloatFromBinaryString(Untag($encoded, "Float8E5M2Vector")) = $vector;
select Knn::CosineDistance(Untag($a, "Float8E5M2Vector"), $b) = Knn::CosineDistance($a, $b);
select Knn::ToBinaryStringFloat8E5M2(null) is null;
select Knn::ToBinaryStringFloat8E5M2(Nothing(List<Float>?)) is null;
select Knn::FloatFromBinaryString(Just($encoded)) = $vector;
select Knn::ManhattanDistance($encoded, Knn::ToBinaryStringFloat8E5M2([1.0f])) is null;
select Knn::CosineDistance(Untag($encoded, "Float8E5M2Vector"), Untag(Knn::ToBinaryStringInt8([1t, 2t, 3t, 4t, 5t]), "Int8Vector")) is null;
select Knn::EuclideanDistance("", Untag($encoded, "Float8E5M2Vector")) is null;
select Knn::InnerProductSimilarity(Untag($encoded, "Float8E5M2Vector"), "") is null;
select Knn::ManhattanDistance("", "") is null;
select Knn::CosineSimilarity("", "") is null;
select Knn::FloatFromBinaryString("") is null;
select Knn::FloatFromBinaryString("\x00\x7f") is null;
select Knn::FloatFromBinaryString(Untag(Knn::ToBinaryStringFloat8E5M2(ListCreate(Float)), "Float8E5M2Vector")) is null;
