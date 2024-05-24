--list serialization
$vector = Cast([1, 2, 3, 4, 5] AS List<Float>);
$vector_binary_str = Knn::ToBinaryStringByte($vector);
select $vector_binary_str;
select Len(Untag($vector_binary_str, "ByteVector")) == 6;

--deserialization
$deserialized_vector = Knn::FloatFromBinaryString($vector_binary_str);
select $deserialized_vector;

--fixed size vector
$vector1 = Knn::ToBinaryStringByte([1.0f, 2.0f, 3.0f]);
$vector2 = Knn::ToBinaryStringByte([4.0f, 5.0f, 6.0f]);
select Knn::CosineDistance($vector1, $vector2);
