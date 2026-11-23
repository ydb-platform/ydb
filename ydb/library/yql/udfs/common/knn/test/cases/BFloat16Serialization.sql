--list serialization
$vector = [1.0f, 2.0f, 0.5f, -1.0f, 4.0f];
$vector_binary_str = Knn::ToBinaryStringBFloat16($vector);
select $vector_binary_str;
select Len(Untag($vector_binary_str, "BFloat16Vector")) == 11;

--deserialization
$deserialized_vector = Knn::FloatFromBinaryString($vector_binary_str);
select $deserialized_vector;
select $deserialized_vector = $vector;

--round-to-nearest-even: 1.004f -> 1.0078125, 1.003f -> 1.0
$rne_up = Knn::FloatFromBinaryString(Knn::ToBinaryStringBFloat16([1.004f]));
select $rne_up;
select $rne_up = [1.0078125f];
$rne_stay = Knn::FloatFromBinaryString(Knn::ToBinaryStringBFloat16([1.003f]));
select $rne_stay;
select $rne_stay = [1.0f];

--fixed size vector
$vector1 = Knn::ToBinaryStringBFloat16([1.0f, 2.0f, 3.0f]);
$vector2 = Knn::ToBinaryStringBFloat16([4.0f, 5.0f, 6.0f]);

select Knn::CosineDistance($vector1, $vector2);
select Knn::InnerProductSimilarity($vector1, $vector2);
select Knn::EuclideanDistance($vector1, $vector2);
select Knn::ManhattanDistance($vector1, $vector2);

--simd-sized vectors: 64 + 3 tail, values exact in float32 and bfloat16
$long1 = ListFlatten([ListFlatten(ListReplicate([1.0f, 2.0f, 0.5f, -1.0f], 16)), [1.0f, 2.0f, 3.0f]]);
$long2 = ListFlatten([ListFlatten(ListReplicate([4.0f, 0.5f, 2.0f, 1.0f], 16)), [4.0f, 5.0f, 6.0f]]);
$float_long1 = Knn::ToBinaryStringFloat($long1);
$float_long2 = Knn::ToBinaryStringFloat($long2);
$bfloat16_long1 = Knn::ToBinaryStringBFloat16($long1);
$bfloat16_long2 = Knn::ToBinaryStringBFloat16($long2);
select Knn::CosineDistance($bfloat16_long1, $bfloat16_long2) = Knn::CosineDistance($float_long1, $float_long2);
select Knn::InnerProductSimilarity($bfloat16_long1, $bfloat16_long2) = Knn::InnerProductSimilarity($float_long1, $float_long2);
select Knn::EuclideanDistance($bfloat16_long1, $bfloat16_long2) = Knn::EuclideanDistance($float_long1, $float_long2);
select Knn::ManhattanDistance($bfloat16_long1, $bfloat16_long2) = Knn::ManhattanDistance($float_long1, $float_long2);
