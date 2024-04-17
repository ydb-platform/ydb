--fixed size vector
$vector1 = Knn::ToBinaryString([1.0f, 2.0f, 3.0f]);
$vector2 = Knn::ToBinaryString([4.0f, 5.0f, 6.0f]);
$extected_distance = 32;
$distance = Knn::InnerProductSimilarity($vector1, $vector2);
select $distance = $extected_distance;

--size mismatch
$bad_vector1 = Knn::ToBinaryString([1.0f]);
$bad_vector2 = Knn::ToBinaryString([4.0f, 5.0f, 6.0f]);
select Knn::InnerProductSimilarity($bad_vector1, $bad_vector2);

--bad deserialized vector
$bad_deserialized_vector2 = "WrongString";
select Knn::InnerProductSimilarity($vector1, $bad_deserialized_vector2);