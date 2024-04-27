--bit vector serialization
$positive = Knn::ToBinaryString(ListReplicate(1.0f, 64), "bit");
select $positive;
$positive_double_size = Knn::ToBinaryString(ListReplicate(1.0f, 128), "bit");
select $positive_double_size;
$negative = Knn::ToBinaryString(ListReplicate(-1.0f, 64), "bit");
select $negative;
$negative_and_positive = Knn::ToBinaryString(ListFromRange(-63.0f, 64.1f), "bit");
select $negative_and_positive;
$negative_and_positive_striped = Knn::ToBinaryString(ListFlatten(ListReplicate([-1.0f, 1.0f], 32)), "bit");
select $negative_and_positive_striped;


--bit indexes
$stored_vector = Knn::ToBinaryString(ListFromRange(-319.0f, 640.1f), "bit");
$indexes = Knn::BitIndexes($positive, $stored_vector);
select $indexes;

