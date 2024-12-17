$vector1 = Knn::ToBinaryStringFloat([1.0f]);
$vector2 = Knn::ToBinaryStringUint8([4ut, 5ut, 6ut, 7ut]);
select Knn::CosineDistance($vector1, $vector2);
