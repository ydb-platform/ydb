--lazy list serialization
$vector = ListFromRange(1.0f, 5.1f);
$vector_binary_str = Knn::ToBinaryString($vector);
select $vector_binary_str;

--deserialization
$deserialized_vector = Knn::FromBinaryString($vector_binary_str);
select $deserialized_vector;
select $deserialized_vector = $vector;
