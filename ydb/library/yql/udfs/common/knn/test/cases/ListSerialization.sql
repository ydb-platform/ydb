--list serialization
$vector = [1.2f, 2.3f, 3.4f, 4.5f, 5.6f];
$vector_binary_str = Knn::ToBinaryStringFloat($vector);
select $vector_binary_str;

--deserialization
$deserialized_vector = Knn::FloatFromBinaryString($vector_binary_str);
select $deserialized_vector;
select $deserialized_vector = $vector;

$vector_d  = Cast([0, 1, 0, 0, 0, 0, 0, 0] AS List<Double>);
$vector_f  = Cast([0, 1, 0, 0, 0, 0, 0, 0] AS List<Float>);
$vector_u8 = Cast([0, 1, 0, 0, 0, 0, 0, 0] AS List<Uint8>);
$vector_i8 = Cast([0, 1, 0, 0, 0, 0, 0, 0] AS List<Int8>);
select Knn::ToBinaryStringBit($vector_d);
select Knn::ToBinaryStringBit($vector_f);
select Knn::ToBinaryStringBit($vector_u8);
select Knn::ToBinaryStringBit($vector_i8);
