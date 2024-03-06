--list serialization
$vector = [1.2, 2.3, 3.4, 4.5, 5.6];
$vector_binary_str = Knn::ToBinaryString($vector);
select $vector_binary_str;

--deserialization
$deserialized_vector = Knn::FromBinaryString($vector_binary_str);
select $deserialized_vector;
select $deserialized_vector = $vector;
