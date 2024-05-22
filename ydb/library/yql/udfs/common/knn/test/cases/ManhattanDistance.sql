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
$bitvector_pos_1_00 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 00), "bit");
$bitvector_pos_1_04 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 04), "bit");
$bitvector_pos_1_08 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 08), "bit");
$bitvector_pos_1_16 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 16), "bit");
$bitvector_pos_1_24 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 24), "bit");
$bitvector_pos_1_32 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 32), "bit");
$bitvector_pos_1_40 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 40), "bit");
$bitvector_pos_1_48 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 48), "bit");
$bitvector_pos_1_56 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 56), "bit");
$bitvector_pos_1_60 = Knn::ToBinaryString(ListReplicate(1.0f, 64 + 60), "bit");

$bitvector_neg_1_00 = Knn::ToBinaryString(ListReplicate(-1.0f, 64 + 00), "bit");
$bitvector_neg_1_04 = Knn::ToBinaryString(ListReplicate(-1.0f, 64 + 04), "bit");
$bitvector_neg_1_08 = Knn::ToBinaryString(ListReplicate(-1.0f, 64 + 08), "bit");
$bitvector_neg_1_16 = Knn::ToBinaryString(ListReplicate(-1.0f, 64 + 16), "bit");
$bitvector_neg_1_24 = Knn::ToBinaryString(ListReplicate(-1.0f, 64 + 24), "bit");
$bitvector_neg_1_32 = Knn::ToBinaryString(ListReplicate(-1.0f, 64 + 32), "bit");
$bitvector_neg_1_40 = Knn::ToBinaryString(ListReplicate(-1.0f, 64 + 40), "bit");
$bitvector_neg_1_48 = Knn::ToBinaryString(ListReplicate(-1.0f, 64 + 48), "bit");
$bitvector_neg_1_56 = Knn::ToBinaryString(ListReplicate(-1.0f, 64 + 56), "bit");
$bitvector_neg_1_60 = Knn::ToBinaryString(ListReplicate(-1.0f, 64 + 60), "bit");

select 64 + 00, Knn::ManhattanDistance($bitvector_pos_1_00, $bitvector_pos_1_00);
select 64 + 04, Knn::ManhattanDistance($bitvector_pos_1_04, $bitvector_pos_1_04);
select 64 + 08, Knn::ManhattanDistance($bitvector_pos_1_08, $bitvector_pos_1_08);
select 64 + 16, Knn::ManhattanDistance($bitvector_pos_1_16, $bitvector_pos_1_16);
select 64 + 24, Knn::ManhattanDistance($bitvector_pos_1_24, $bitvector_pos_1_24);
select 64 + 32, Knn::ManhattanDistance($bitvector_pos_1_32, $bitvector_pos_1_32);
select 64 + 40, Knn::ManhattanDistance($bitvector_pos_1_40, $bitvector_pos_1_40);
select 64 + 48, Knn::ManhattanDistance($bitvector_pos_1_48, $bitvector_pos_1_48);
select 64 + 56, Knn::ManhattanDistance($bitvector_pos_1_56, $bitvector_pos_1_56);
select 64 + 60, Knn::ManhattanDistance($bitvector_pos_1_60, $bitvector_pos_1_60);

select 64 + 00, Knn::ManhattanDistance($bitvector_neg_1_00, $bitvector_neg_1_00);
select 64 + 04, Knn::ManhattanDistance($bitvector_neg_1_04, $bitvector_neg_1_04);
select 64 + 08, Knn::ManhattanDistance($bitvector_neg_1_08, $bitvector_neg_1_08);
select 64 + 16, Knn::ManhattanDistance($bitvector_neg_1_16, $bitvector_neg_1_16);
select 64 + 24, Knn::ManhattanDistance($bitvector_neg_1_24, $bitvector_neg_1_24);
select 64 + 32, Knn::ManhattanDistance($bitvector_neg_1_32, $bitvector_neg_1_32);
select 64 + 40, Knn::ManhattanDistance($bitvector_neg_1_40, $bitvector_neg_1_40);
select 64 + 48, Knn::ManhattanDistance($bitvector_neg_1_48, $bitvector_neg_1_48);
select 64 + 56, Knn::ManhattanDistance($bitvector_neg_1_56, $bitvector_neg_1_56);
select 64 + 60, Knn::ManhattanDistance($bitvector_neg_1_60, $bitvector_neg_1_60);

select 64 + 00, Knn::ManhattanDistance($bitvector_pos_1_00, $bitvector_neg_1_00);
select 64 + 04, Knn::ManhattanDistance($bitvector_pos_1_04, $bitvector_neg_1_04);
select 64 + 08, Knn::ManhattanDistance($bitvector_pos_1_08, $bitvector_neg_1_08);
select 64 + 16, Knn::ManhattanDistance($bitvector_pos_1_16, $bitvector_neg_1_16);
select 64 + 24, Knn::ManhattanDistance($bitvector_pos_1_24, $bitvector_neg_1_24);
select 64 + 32, Knn::ManhattanDistance($bitvector_pos_1_32, $bitvector_neg_1_32);
select 64 + 40, Knn::ManhattanDistance($bitvector_pos_1_40, $bitvector_neg_1_40);
select 64 + 48, Knn::ManhattanDistance($bitvector_pos_1_48, $bitvector_neg_1_48);
select 64 + 56, Knn::ManhattanDistance($bitvector_pos_1_56, $bitvector_neg_1_56);
select 64 + 60, Knn::ManhattanDistance($bitvector_pos_1_60, $bitvector_neg_1_60);

select 64 + 00, Knn::ManhattanDistance($bitvector_neg_1_00, $bitvector_pos_1_00);
select 64 + 04, Knn::ManhattanDistance($bitvector_neg_1_04, $bitvector_pos_1_04);
select 64 + 08, Knn::ManhattanDistance($bitvector_neg_1_08, $bitvector_pos_1_08);
select 64 + 16, Knn::ManhattanDistance($bitvector_neg_1_16, $bitvector_pos_1_16);
select 64 + 24, Knn::ManhattanDistance($bitvector_neg_1_24, $bitvector_pos_1_24);
select 64 + 32, Knn::ManhattanDistance($bitvector_neg_1_32, $bitvector_pos_1_32);
select 64 + 40, Knn::ManhattanDistance($bitvector_neg_1_40, $bitvector_pos_1_40);
select 64 + 48, Knn::ManhattanDistance($bitvector_neg_1_48, $bitvector_pos_1_48);
select 64 + 56, Knn::ManhattanDistance($bitvector_neg_1_56, $bitvector_pos_1_56);
select 64 + 60, Knn::ManhattanDistance($bitvector_neg_1_60, $bitvector_pos_1_60);

--bit vector -- only tail
$bitvector_pos_00 = Knn::ToBinaryString(ListReplicate(1.0f, 00), "bit");
$bitvector_pos_04 = Knn::ToBinaryString(ListReplicate(1.0f, 04), "bit");
$bitvector_pos_08 = Knn::ToBinaryString(ListReplicate(1.0f, 08), "bit");
$bitvector_pos_16 = Knn::ToBinaryString(ListReplicate(1.0f, 16), "bit");
$bitvector_pos_24 = Knn::ToBinaryString(ListReplicate(1.0f, 24), "bit");
$bitvector_pos_32 = Knn::ToBinaryString(ListReplicate(1.0f, 32), "bit");
$bitvector_pos_40 = Knn::ToBinaryString(ListReplicate(1.0f, 40), "bit");
$bitvector_pos_48 = Knn::ToBinaryString(ListReplicate(1.0f, 48), "bit");
$bitvector_pos_56 = Knn::ToBinaryString(ListReplicate(1.0f, 56), "bit");
$bitvector_pos_60 = Knn::ToBinaryString(ListReplicate(1.0f, 60), "bit");

$bitvector_neg_00 = Knn::ToBinaryString(ListReplicate(-1.0f, 00), "bit");
$bitvector_neg_04 = Knn::ToBinaryString(ListReplicate(-1.0f, 04), "bit");
$bitvector_neg_08 = Knn::ToBinaryString(ListReplicate(-1.0f, 08), "bit");
$bitvector_neg_16 = Knn::ToBinaryString(ListReplicate(-1.0f, 16), "bit");
$bitvector_neg_24 = Knn::ToBinaryString(ListReplicate(-1.0f, 24), "bit");
$bitvector_neg_32 = Knn::ToBinaryString(ListReplicate(-1.0f, 32), "bit");
$bitvector_neg_40 = Knn::ToBinaryString(ListReplicate(-1.0f, 40), "bit");
$bitvector_neg_48 = Knn::ToBinaryString(ListReplicate(-1.0f, 48), "bit");
$bitvector_neg_56 = Knn::ToBinaryString(ListReplicate(-1.0f, 56), "bit");
$bitvector_neg_60 = Knn::ToBinaryString(ListReplicate(-1.0f, 60), "bit");

select 00, Knn::ManhattanDistance($bitvector_pos_00, $bitvector_pos_00);
select 04, Knn::ManhattanDistance($bitvector_pos_04, $bitvector_pos_04);
select 08, Knn::ManhattanDistance($bitvector_pos_08, $bitvector_pos_08);
select 16, Knn::ManhattanDistance($bitvector_pos_16, $bitvector_pos_16);
select 24, Knn::ManhattanDistance($bitvector_pos_24, $bitvector_pos_24);
select 32, Knn::ManhattanDistance($bitvector_pos_32, $bitvector_pos_32);
select 40, Knn::ManhattanDistance($bitvector_pos_40, $bitvector_pos_40);
select 48, Knn::ManhattanDistance($bitvector_pos_48, $bitvector_pos_48);
select 56, Knn::ManhattanDistance($bitvector_pos_56, $bitvector_pos_56);
select 60, Knn::ManhattanDistance($bitvector_pos_60, $bitvector_pos_60);

select 00, Knn::ManhattanDistance($bitvector_neg_00, $bitvector_neg_00);
select 04, Knn::ManhattanDistance($bitvector_neg_04, $bitvector_neg_04);
select 08, Knn::ManhattanDistance($bitvector_neg_08, $bitvector_neg_08);
select 16, Knn::ManhattanDistance($bitvector_neg_16, $bitvector_neg_16);
select 24, Knn::ManhattanDistance($bitvector_neg_24, $bitvector_neg_24);
select 32, Knn::ManhattanDistance($bitvector_neg_32, $bitvector_neg_32);
select 40, Knn::ManhattanDistance($bitvector_neg_40, $bitvector_neg_40);
select 48, Knn::ManhattanDistance($bitvector_neg_48, $bitvector_neg_48);
select 56, Knn::ManhattanDistance($bitvector_neg_56, $bitvector_neg_56);
select 60, Knn::ManhattanDistance($bitvector_neg_60, $bitvector_neg_60);

select 00, Knn::ManhattanDistance($bitvector_pos_00, $bitvector_neg_00);
select 04, Knn::ManhattanDistance($bitvector_pos_04, $bitvector_neg_04);
select 08, Knn::ManhattanDistance($bitvector_pos_08, $bitvector_neg_08);
select 16, Knn::ManhattanDistance($bitvector_pos_16, $bitvector_neg_16);
select 24, Knn::ManhattanDistance($bitvector_pos_24, $bitvector_neg_24);
select 32, Knn::ManhattanDistance($bitvector_pos_32, $bitvector_neg_32);
select 40, Knn::ManhattanDistance($bitvector_pos_40, $bitvector_neg_40);
select 48, Knn::ManhattanDistance($bitvector_pos_48, $bitvector_neg_48);
select 56, Knn::ManhattanDistance($bitvector_pos_56, $bitvector_neg_56);
select 60, Knn::ManhattanDistance($bitvector_pos_60, $bitvector_neg_60);

select 00, Knn::ManhattanDistance($bitvector_neg_00, $bitvector_pos_00);
select 04, Knn::ManhattanDistance($bitvector_neg_04, $bitvector_pos_04);
select 08, Knn::ManhattanDistance($bitvector_neg_08, $bitvector_pos_08);
select 16, Knn::ManhattanDistance($bitvector_neg_16, $bitvector_pos_16);
select 24, Knn::ManhattanDistance($bitvector_neg_24, $bitvector_pos_24);
select 32, Knn::ManhattanDistance($bitvector_neg_32, $bitvector_pos_32);
select 40, Knn::ManhattanDistance($bitvector_neg_40, $bitvector_pos_40);
select 48, Knn::ManhattanDistance($bitvector_neg_48, $bitvector_pos_48);
select 56, Knn::ManhattanDistance($bitvector_neg_56, $bitvector_pos_56);
select 60, Knn::ManhattanDistance($bitvector_neg_60, $bitvector_pos_60);
