--list serialization
$vector = [1.0f, 2.0f, 0.5f, -1.0f, 4.0f];
$vector_binary_str = Knn::ToBinaryStringFloat16($vector);
select $vector_binary_str;
select Len(Untag($vector_binary_str, "Float16Vector")) == 11;

--deserialization
$deserialized_vector = Knn::FloatFromBinaryString($vector_binary_str);
select $deserialized_vector;
select $deserialized_vector = $vector;

--fixed size vector
$vector1 = Knn::ToBinaryStringFloat16([1.0f, 2.0f, 3.0f]);
$vector2 = Knn::ToBinaryStringFloat16([4.0f, 5.0f, 6.0f]);

select Knn::CosineDistance($vector1, $vector2);
select Knn::InnerProductSimilarity($vector1, $vector2);
select Knn::EuclideanDistance($vector1, $vector2);
select Knn::ManhattanDistance($vector1, $vector2);

--simd-sized vectors: 64 + 3 tail, values exact in float32 and float16
$long1 = ListFlatten([ListFlatten(ListReplicate([1.0f, 2.0f, 0.5f, -1.0f], 16)), [1.0f, 2.0f, 3.0f]]);
$long2 = ListFlatten([ListFlatten(ListReplicate([4.0f, 0.5f, 2.0f, 1.0f], 16)), [4.0f, 5.0f, 6.0f]]);
$float_long1 = Knn::ToBinaryStringFloat($long1);
$float_long2 = Knn::ToBinaryStringFloat($long2);
$float16_long1 = Knn::ToBinaryStringFloat16($long1);
$float16_long2 = Knn::ToBinaryStringFloat16($long2);
select Knn::CosineDistance($float16_long1, $float16_long2) = Knn::CosineDistance($float_long1, $float_long2);
select Knn::InnerProductSimilarity($float16_long1, $float16_long2) = Knn::InnerProductSimilarity($float_long1, $float_long2);
select Knn::EuclideanDistance($float16_long1, $float16_long2) = Knn::EuclideanDistance($float_long1, $float_long2);
select Knn::ManhattanDistance($float16_long1, $float16_long2) = Knn::ManhattanDistance($float_long1, $float_long2);
