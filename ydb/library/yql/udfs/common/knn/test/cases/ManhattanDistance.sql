--float vector
$float_vector1 = Knn::ToBinaryString([1.0f, 2.0f, 3.0f]);
$float_vector2 = Knn::ToBinaryString([4.0f, 5.0f, 6.0f]);
select Knn::ManhattanDistance($float_vector1, $float_vector2);

--byte vector
$byte_vector1 = Knn::ToBinaryString([1.0f, 2.0f, 3.0f], "byte");
$byte_vector2 = Knn::ToBinaryString([4.0f, 5.0f, 6.0f], "byte");
select Knn::ManhattanDistance($byte_vector1, $byte_vector2);

--bit vector
$bitvector_positive = Knn::ToBinaryString(ListReplicate(1.0f, 64), "bit");
$bitvector_positive_double_size = Knn::ToBinaryString(ListReplicate(1.0f, 128), "bit");
$bitvector_negative = Knn::ToBinaryString(ListReplicate(-1.0f, 64), "bit");
$bitvector_negative_and_positive = Knn::ToBinaryString(ListFromRange(-63.0f, 64.1f), "bit");
$bitvector_negative_and_positive_striped = Knn::ToBinaryString(ListFlatten(ListReplicate([-1.0f, 1.0f], 32)), "bit");

select Knn::ManhattanDistance($bitvector_positive, $bitvector_positive_double_size);
select Knn::ManhattanDistance($bitvector_positive, $bitvector_negative);
select Knn::ManhattanDistance($bitvector_positive_double_size, $bitvector_negative_and_positive);
select Knn::ManhattanDistance($bitvector_positive, $bitvector_negative_and_positive_striped);

--bit vector -- with tail
$bitvector_1_00 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 00), "bit");
$bitvector_1_04 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 04), "bit");
$bitvector_1_08 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 08), "bit");
$bitvector_1_16 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 16), "bit");
$bitvector_1_24 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 24), "bit");
$bitvector_1_32 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 32), "bit");
$bitvector_1_40 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 40), "bit");
$bitvector_1_48 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 48), "bit");
$bitvector_1_56 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 56), "bit");
$bitvector_1_60 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 60), "bit");

select Knn::ManhattanDistance($bitvector_1_00, $bitvector_1_00);
select Knn::ManhattanDistance($bitvector_1_04, $bitvector_1_04);
select Knn::ManhattanDistance($bitvector_1_08, $bitvector_1_08);
select Knn::ManhattanDistance($bitvector_1_16, $bitvector_1_16);
select Knn::ManhattanDistance($bitvector_1_24, $bitvector_1_24);
select Knn::ManhattanDistance($bitvector_1_32, $bitvector_1_32);
select Knn::ManhattanDistance($bitvector_1_40, $bitvector_1_40);
select Knn::ManhattanDistance($bitvector_1_48, $bitvector_1_48);
select Knn::ManhattanDistance($bitvector_1_56, $bitvector_1_56);
select Knn::ManhattanDistance($bitvector_1_60, $bitvector_1_60);

--bit vector -- only tail
$bitvector_00 = Knn::ToBinaryString(ListReplicate(1.0f, 00), "bit");
$bitvector_04 = Knn::ToBinaryString(ListReplicate(1.0f, 04), "bit");
$bitvector_08 = Knn::ToBinaryString(ListReplicate(1.0f, 08), "bit");
$bitvector_16 = Knn::ToBinaryString(ListReplicate(1.0f, 16), "bit");
$bitvector_24 = Knn::ToBinaryString(ListReplicate(1.0f, 24), "bit");
$bitvector_32 = Knn::ToBinaryString(ListReplicate(1.0f, 32), "bit");
$bitvector_40 = Knn::ToBinaryString(ListReplicate(1.0f, 40), "bit");
$bitvector_48 = Knn::ToBinaryString(ListReplicate(1.0f, 48), "bit");
$bitvector_56 = Knn::ToBinaryString(ListReplicate(1.0f, 56), "bit");
$bitvector_60 = Knn::ToBinaryString(ListReplicate(1.0f, 60), "bit");

select Knn::ManhattanDistance($bitvector_00, $bitvector_00);
select Knn::ManhattanDistance($bitvector_04, $bitvector_04);
select Knn::ManhattanDistance($bitvector_08, $bitvector_08);
select Knn::ManhattanDistance($bitvector_16, $bitvector_16);
select Knn::ManhattanDistance($bitvector_24, $bitvector_24);
select Knn::ManhattanDistance($bitvector_32, $bitvector_32);
select Knn::ManhattanDistance($bitvector_40, $bitvector_40);
select Knn::ManhattanDistance($bitvector_48, $bitvector_48);
select Knn::ManhattanDistance($bitvector_56, $bitvector_56);
select Knn::ManhattanDistance($bitvector_60, $bitvector_60);
