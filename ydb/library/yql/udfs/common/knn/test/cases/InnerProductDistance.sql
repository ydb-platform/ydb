--fixed size vector
$vector1 = [1.0, 2.0, 3.0];
$vector2 = [4.0, 5.0, 6.0];
$extected_distance = 32;
$distance = Knn::InnerProductDistance($vector1, $vector2);
select $distance = $extected_distance;

--lazy vector
$lazy_vector1 = ListFromRange(1.0, 4.0);
$lazy_vector2 = ListFromRange(4.0, 7.0);
$distance = Knn::InnerProductDistance($lazy_vector1, $lazy_vector2);
select $distance = $extected_distance;

--lazy vector + fixed size vector
$distance = Knn::InnerProductDistance($lazy_vector1, $vector2);
select $distance = $extected_distance;
$distance = Knn::InnerProductDistance($vector1, $lazy_vector2);
select $distance = $extected_distance;

--good deserialized vector
$deserialized_vector2 = Knn::FromBinaryString(Knn::ToBinaryString($vector2));
$distance =  Knn::InnerProductDistance($vector1, $deserialized_vector2);
select $distance = $extected_distance;

--size mismatch
$bad_vector1 = [1.0];
$bad_vector2 = [4.0, 5.0, 6.0];
select Knn::InnerProductDistance($bad_vector1, $bad_vector2);

--bad deserialized vector
$bad_deserialized_vector2 = Knn::FromBinaryString("WrongString");
select Knn::InnerProductDistance($vector1, $bad_deserialized_vector2);