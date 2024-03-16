--fixed size vector
$vector1 = [1.0f, 2.0f, 3.0f];
$vector2 = [4.0f, 5.0f, 6.0f];
select Knn::CosineDistance($vector1, $vector2);

--lazy vector
$lazy_vector1 = ListFromRange(1.0f, 4.0f);
$lazy_vector2 = ListFromRange(4.0f, 7.0f);
select Knn::CosineDistance($lazy_vector1, $lazy_vector2);

--lazy vector + fixed size vector
select Knn::CosineDistance($lazy_vector1, $vector2);
select Knn::CosineDistance($vector1, $lazy_vector2);

--good deserialized vector
$deserialized_vector2 = Knn::FromBinaryString(Knn::ToBinaryString($vector2));
select Knn::CosineDistance($vector1, $deserialized_vector2);

--exact vectors
select Knn::CosineDistance($vector1, $vector1);

--orthogonal vectors
$orthogonal_vector1 = [1.0f, 0.0f];
$orthogonal_vector2 = [0.0f, 2.0f];
select Knn::CosineDistance($orthogonal_vector1, $orthogonal_vector2);

--size mismatch
$bad_vector1 = [1.0f];
$bad_vector2 = [4.0f, 5.0f, 6.0f];
select Knn::CosineDistance($bad_vector1, $bad_vector2);

--bad deserialized vector
$bad_deserialized_vector2 = Knn::FromBinaryString("WrongString");
select Knn::CosineDistance($vector1, $bad_deserialized_vector2);