--list serialization
$vector = Cast([1, 2, 3, 4, 5] AS List<Int8>);
$vector_binary_str = Knn::ToBinaryStringInt8($vector);
select $vector_binary_str;
select Len(Untag($vector_binary_str, "Int8Vector")) == 6;

--deserialization
$deserialized_vector = Knn::FloatFromBinaryString($vector_binary_str);
select $deserialized_vector;

--fixed size vector
$vector1 = Knn::ToBinaryStringInt8([1ut, 2ut, 3ut]);
$vector2 = Knn::ToBinaryStringInt8([4ut, 5ut, 6ut]);

select Knn::CosineDistance($vector1, $vector2);
select Knn::CosineDistance($vector1, "\u0004\u0005\u0006\u0003");
select Knn::CosineDistance("\u0001\u0002\u0003\u0003", $vector2);
select Knn::CosineDistance("\u0001\u0002\u0003\u0003", "\u0004\u0005\u0006\u0003");
