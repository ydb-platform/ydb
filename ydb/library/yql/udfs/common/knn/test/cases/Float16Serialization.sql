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
