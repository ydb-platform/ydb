--list serialization
$vector = [1.2f, 2.3f, 3.4f, 4.5f, 5.6f];
$vector_binary_str = Knn::ToBinaryStringFloat($vector);
select $vector_binary_str;

--deserialization
$deserialized_vector = Knn::FloatFromBinaryString($vector_binary_str);
select $deserialized_vector;
select $deserialized_vector = $vector;
