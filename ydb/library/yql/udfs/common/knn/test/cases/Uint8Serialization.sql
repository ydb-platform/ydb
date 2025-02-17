--list serialization
$vector = Cast([1, 2, 3, 4, 5] AS List<Uint8>);
$vector_binary_str = Knn::ToBinaryStringUint8($vector);
select $vector_binary_str;
select Len(Untag($vector_binary_str, "Uint8Vector")) == 6;

--deserialization
$deserialized_vector = Knn::FloatFromBinaryString($vector_binary_str);
select $deserialized_vector;

--fixed size vector
$vector1 = Knn::ToBinaryStringUint8([1ut, 2ut, 3ut]);
$vector2 = Knn::ToBinaryStringUint8([4ut, 5ut, 6ut]);

select Knn::CosineDistance($vector1, $vector2);
select Knn::CosineDistance($vector1, "\u0004\u0005\u0006\u0002");
select Knn::CosineDistance("\u0001\u0002\u0003\u0002", $vector2);
select Knn::CosineDistance("\u0001\u0002\u0003\u0002", "\u0004\u0005\u0006\u0002");
