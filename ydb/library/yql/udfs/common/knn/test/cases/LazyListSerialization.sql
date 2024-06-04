--lazy list serialization
$vector = ListFromRange(1.0f, 5.1f);
$vector_binary_str = Knn::ToBinaryStringFloat($vector);
select $vector_binary_str;

--deserialization
$deserialized_vector = Knn::FloatFromBinaryString($vector_binary_str);
select $deserialized_vector;
select $deserialized_vector = $vector;
