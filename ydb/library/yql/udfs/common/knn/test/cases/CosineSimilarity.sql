--fixed size vector
$vector1 = Knn::ToBinaryString([1.0f, 2.0f, 3.0f]);
$vector2 = Knn::ToBinaryString([4.0f, 5.0f, 6.0f]);
select Knn::CosineSimilarity($vector1, $vector2);

--exact vectors
select Knn::CosineSimilarity($vector1, $vector1);

--orthogonal vectors
$orthogonal_vector1 = Knn::ToBinaryString([1.0f, 0.0f]);
$orthogonal_vector2 = Knn::ToBinaryString([0.0f, 2.0f]);
select Knn::CosineSimilarity($orthogonal_vector1, $orthogonal_vector2);

--size mismatch
$bad_vector1 = Knn::ToBinaryString([1.0f]);
$bad_vector2 = Knn::ToBinaryString([4.0f, 5.0f, 6.0f]);
select Knn::CosineSimilarity($bad_vector1, $bad_vector2);

--bad deserialized vector
$bad_deserialized_vector2 = "WrongString";
select Knn::CosineSimilarity($vector1, $bad_deserialized_vector2);