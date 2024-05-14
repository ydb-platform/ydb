--float vector
$float_vector1 = Knn::ToBinaryString([1.0f, 2.0f, 3.0f]);
$float_vector2 = Knn::ToBinaryString([4.0f, 5.0f, 6.0f]);
select Knn::EuclideanDistance($float_vector1, $float_vector2);

--byte vector
$byte_vector1 = Knn::ToBinaryString([1.0f, 2.0f, 3.0f], "byte");
$byte_vector2 = Knn::ToBinaryString([4.0f, 5.0f, 6.0f], "byte");
select Knn::EuclideanDistance($byte_vector1, $byte_vector2);

--bit vector
$bitvector_positive = Knn::ToBinaryString(ListReplicate(1.0f, 64), "bit");
$bitvector_positive_double_size = Knn::ToBinaryString(ListReplicate(1.0f, 128), "bit");
$bitvector_negative = Knn::ToBinaryString(ListReplicate(-1.0f, 64), "bit");
$bitvector_negative_and_positive = Knn::ToBinaryString(ListFromRange(-63.0f, 64.1f), "bit");
$bitvector_negative_and_positive_striped = Knn::ToBinaryString(ListFlatten(ListReplicate([-1.0f, 1.0f], 32)), "bit");

select Knn::EuclideanDistance($bitvector_positive, $bitvector_positive_double_size);
select Knn::EuclideanDistance($bitvector_positive, $bitvector_negative);
select Knn::EuclideanDistance($bitvector_positive_double_size, $bitvector_negative_and_positive);
select Knn::EuclideanDistance($bitvector_positive, $bitvector_negative_and_positive_striped);