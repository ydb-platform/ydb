--bit vector serialization
$positive = Knn::ToBitString(ListReplicate(1.0f, 64));
select $positive;
$positive_double_size = Knn::ToBitString(ListReplicate(1.0f, 128));
select $positive_double_size;
$negative = Knn::ToBitString(ListReplicate(-1.0f, 64));
select $negative;
$negative_and_positive = Knn::ToBitString(ListFromRange(-63.0f, 64.1f));
select $negative_and_positive;
$negative_and_positive_striped = Knn::ToBitString(ListFlatten(ListReplicate([-1.0f, 1.0f], 32)));
select $negative_and_positive_striped;


--bit indexes
$stored_vector = Knn::ToBitString(ListFromRange(-319.0f, 640.1f));
$indexes = Knn::BitIndexes($positive, $stored_vector, 10, 1000, 0);
select $indexes;

