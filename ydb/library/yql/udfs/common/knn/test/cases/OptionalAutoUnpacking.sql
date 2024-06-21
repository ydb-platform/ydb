select Knn::ToBinaryStringFloat(Nothing(List<Float>?)) is null;
select Knn::ToBinaryStringUint8(Nothing(List<Uint8>?)) is null;
select Knn::ToBinaryStringBit(Nothing(List<Float>?)) is null;
select Knn::FloatFromBinaryString(Nothing(String?)) is null;
select Knn::CosineDistance(Nothing(String?), Nothing(String?)) is null;
select Knn::CosineDistance(Nothing(String?), Just("\u0002")) is null;
select Knn::CosineDistance(Just("\u0002"), Nothing(String?)) is null;
select Knn::CosineDistance(Just("\u0002"), Just("\u0002")) == 0;
