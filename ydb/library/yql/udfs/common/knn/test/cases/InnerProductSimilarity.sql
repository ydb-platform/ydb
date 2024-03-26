--fixed size vector
$vector1 = [1.0f, 2.0f, 3.0f];
$vector2 = [4.0f, 5.0f, 6.0f];
$extected_distance = 32;
$distance = Knn::InnerProductSimilarity($vector1, $vector2);
select $distance = $extected_distance;

--lazy vector
$lazy_vector1 = ListFromRange(1.0f, 4.0f);
$lazy_vector2 = ListFromRange(4.0f, 7.0f);
$distance = Knn::InnerProductSimilarity($lazy_vector1, $lazy_vector2);
select $distance = $extected_distance;

--lazy vector + fixed size vector
$distance = Knn::InnerProductSimilarity($lazy_vector1, $vector2);
select $distance = $extected_distance;
$distance = Knn::InnerProductSimilarity($vector1, $lazy_vector2);
select $distance = $extected_distance;

--good deserialized vector
$deserialized_vector2 = Knn::FromBinaryString(Knn::ToBinaryString($vector2));
$distance =  Knn::InnerProductSimilarity($vector1, $deserialized_vector2);
select $distance = $extected_distance;

--size mismatch
$bad_vector1 = [1.0f];
$bad_vector2 = [4.0f, 5.0f, 6.0f];
select Knn::InnerProductSimilarity($bad_vector1, $bad_vector2);

--bad deserialized vector
$bad_deserialized_vector2 = Knn::FromBinaryString("WrongString");
select Knn::InnerProductSimilarity($vector1, $bad_deserialized_vector2);