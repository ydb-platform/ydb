$vector1 = Knn::ToBinaryStringFloat([1.0f]);
$vector2 = Knn::ToBinaryStringByte([4.0f, 5.0f, 6.0f, 7.0f]);
select Knn::CosineDistance($vector1, $vector2);
