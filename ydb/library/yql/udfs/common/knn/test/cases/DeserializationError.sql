select Knn::FromBinaryString("");
select Knn::FromBinaryString("WrongString");

--wrong format
$vector_wrong_format = Knn::ToBinaryString([1.0f, 2.0f, 3.0f], "wrong format");
select $vector_wrong_format;
select Knn::CosineDistance($vector_wrong_format, $vector_wrong_format);
