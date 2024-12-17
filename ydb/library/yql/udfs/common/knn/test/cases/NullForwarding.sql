select Knn::ToBinaryStringFloat(null) is null;
select Knn::ToBinaryStringUint8(null) is null;
select Knn::ToBinaryStringBit(null) is null;
select Knn::FloatFromBinaryString(null) is null;
select Knn::CosineDistance(null, null) is null;
select Knn::CosineDistance(null, "\u0002") is null;
select Knn::CosineDistance("\u0002", null) is null;
