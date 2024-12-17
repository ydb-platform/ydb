$vector1 = Knn::ToBinaryStringBit([1.0f, 2.0f]);
$vector2 = Knn::ToBinaryStringBit([4.0f, 5.0f, 6.0f]);
select Knn::CosineDistance($vector1, $vector2);
