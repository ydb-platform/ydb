--bit vector serialization
$positive = Knn::ToBinaryString(ListReplicate(1.0f, 64), "bit");
$positive_double_size = Knn::ToBinaryString(ListReplicate(1.0f, 128), "bit");
$negative = Knn::ToBinaryString(ListReplicate(-1.0f, 64), "bit");
$negative_and_positive = Knn::ToBinaryString(ListFromRange(-63.0f, 64.1f), "bit");
$negative_and_positive_striped = Knn::ToBinaryString(ListFlatten(ListReplicate([-1.0f, 1.0f], 32)), "bit");

-- manhattan distance
select Knn::ManhattenDistance($positive, $positive_double_size);
select Knn::ManhattenDistance($positive, $negative);
select Knn::ManhattenDistance($positive_double_size, $negative_and_positive);
select Knn::ManhattenDistance($positive, $negative_and_positive_striped);
