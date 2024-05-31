--fixed size vector
$vector1 = Knn::ToBinaryStringFloat([1.0f, 2.0f, 3.0f]);
$vector2 = Knn::ToBinaryStringFloat([4.0f, 5.0f, 6.0f]);
$extected_distance = 32;
$distance = Knn::InnerProductSimilarity($vector1, $vector2);
select $distance = $extected_distance;

--exact vectors
select Knn::InnerProductSimilarity($vector1, $vector1);

--orthogonal vectors
$orthogonal_vector1 = Knn::ToBinaryStringUint8([1ut, 0ut]);
$orthogonal_vector2 = Knn::ToBinaryStringUint8([0ut, 2ut]);
select Knn::InnerProductSimilarity($orthogonal_vector1, $orthogonal_vector2);

--float vector
$float_vector1 = Knn::ToBinaryStringFloat([1.0f, 2.0f, 3.0f]);
$float_vector2 = Knn::ToBinaryStringFloat([4.0f, 5.0f, 6.0f]);
select Knn::InnerProductSimilarity($float_vector1, $float_vector2);

--byte vector
$byte_vector1 = Knn::ToBinaryStringUint8([1ut, 2ut, 3ut]);
$byte_vector2 = Knn::ToBinaryStringUint8([4ut, 5ut, 6ut]);
select Knn::InnerProductSimilarity($byte_vector1, $byte_vector2);

--bit vector
$bitvector_positive = Knn::ToBinaryStringBit(ListReplicate(1.0f, 64));
$bitvector_positive_double_size = Knn::ToBinaryStringBit(ListReplicate(1.0f, 128));
$bitvector_negative = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 64));
$bitvector_negative_and_positive = Knn::ToBinaryStringBit(ListFromRange(-63.0f, 64.1f));
$bitvector_negative_and_positive_striped = Knn::ToBinaryStringBit(ListFlatten(ListReplicate([-1.0f, 1.0f], 32)));

select Knn::InnerProductSimilarity($bitvector_positive, $bitvector_negative);
select Knn::InnerProductSimilarity($bitvector_positive_double_size, $bitvector_negative_and_positive);
select Knn::InnerProductSimilarity($bitvector_positive, $bitvector_negative_and_positive_striped);

--bit vector -- with tail
$bitvector_pos_1_00 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 64 + 00));
$bitvector_pos_1_04 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 64 + 04));
$bitvector_pos_1_08 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 64 + 08));
$bitvector_pos_1_16 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 64 + 16));
$bitvector_pos_1_24 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 64 + 24));
$bitvector_pos_1_32 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 64 + 32));
$bitvector_pos_1_40 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 64 + 40));
$bitvector_pos_1_48 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 64 + 48));
$bitvector_pos_1_56 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 64 + 56));
$bitvector_pos_1_60 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 64 + 60));

$bitvector_neg_1_00 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 64 + 00));
$bitvector_neg_1_04 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 64 + 04));
$bitvector_neg_1_08 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 64 + 08));
$bitvector_neg_1_16 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 64 + 16));
$bitvector_neg_1_24 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 64 + 24));
$bitvector_neg_1_32 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 64 + 32));
$bitvector_neg_1_40 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 64 + 40));
$bitvector_neg_1_48 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 64 + 48));
$bitvector_neg_1_56 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 64 + 56));
$bitvector_neg_1_60 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 64 + 60));

select 64 + 00, Knn::InnerProductSimilarity($bitvector_pos_1_00, $bitvector_pos_1_00);
select 64 + 04, Knn::InnerProductSimilarity($bitvector_pos_1_04, $bitvector_pos_1_04);
select 64 + 08, Knn::InnerProductSimilarity($bitvector_pos_1_08, $bitvector_pos_1_08);
select 64 + 16, Knn::InnerProductSimilarity($bitvector_pos_1_16, $bitvector_pos_1_16);
select 64 + 24, Knn::InnerProductSimilarity($bitvector_pos_1_24, $bitvector_pos_1_24);
select 64 + 32, Knn::InnerProductSimilarity($bitvector_pos_1_32, $bitvector_pos_1_32);
select 64 + 40, Knn::InnerProductSimilarity($bitvector_pos_1_40, $bitvector_pos_1_40);
select 64 + 48, Knn::InnerProductSimilarity($bitvector_pos_1_48, $bitvector_pos_1_48);
select 64 + 56, Knn::InnerProductSimilarity($bitvector_pos_1_56, $bitvector_pos_1_56);
select 64 + 60, Knn::InnerProductSimilarity($bitvector_pos_1_60, $bitvector_pos_1_60);

select 64 + 00, Knn::InnerProductSimilarity($bitvector_neg_1_00, $bitvector_neg_1_00);
select 64 + 04, Knn::InnerProductSimilarity($bitvector_neg_1_04, $bitvector_neg_1_04);
select 64 + 08, Knn::InnerProductSimilarity($bitvector_neg_1_08, $bitvector_neg_1_08);
select 64 + 16, Knn::InnerProductSimilarity($bitvector_neg_1_16, $bitvector_neg_1_16);
select 64 + 24, Knn::InnerProductSimilarity($bitvector_neg_1_24, $bitvector_neg_1_24);
select 64 + 32, Knn::InnerProductSimilarity($bitvector_neg_1_32, $bitvector_neg_1_32);
select 64 + 40, Knn::InnerProductSimilarity($bitvector_neg_1_40, $bitvector_neg_1_40);
select 64 + 48, Knn::InnerProductSimilarity($bitvector_neg_1_48, $bitvector_neg_1_48);
select 64 + 56, Knn::InnerProductSimilarity($bitvector_neg_1_56, $bitvector_neg_1_56);
select 64 + 60, Knn::InnerProductSimilarity($bitvector_neg_1_60, $bitvector_neg_1_60);

select 64 + 00, Knn::InnerProductSimilarity($bitvector_pos_1_00, $bitvector_neg_1_00);
select 64 + 04, Knn::InnerProductSimilarity($bitvector_pos_1_04, $bitvector_neg_1_04);
select 64 + 08, Knn::InnerProductSimilarity($bitvector_pos_1_08, $bitvector_neg_1_08);
select 64 + 16, Knn::InnerProductSimilarity($bitvector_pos_1_16, $bitvector_neg_1_16);
select 64 + 24, Knn::InnerProductSimilarity($bitvector_pos_1_24, $bitvector_neg_1_24);
select 64 + 32, Knn::InnerProductSimilarity($bitvector_pos_1_32, $bitvector_neg_1_32);
select 64 + 40, Knn::InnerProductSimilarity($bitvector_pos_1_40, $bitvector_neg_1_40);
select 64 + 48, Knn::InnerProductSimilarity($bitvector_pos_1_48, $bitvector_neg_1_48);
select 64 + 56, Knn::InnerProductSimilarity($bitvector_pos_1_56, $bitvector_neg_1_56);
select 64 + 60, Knn::InnerProductSimilarity($bitvector_pos_1_60, $bitvector_neg_1_60);

select 64 + 00, Knn::InnerProductSimilarity($bitvector_neg_1_00, $bitvector_pos_1_00);
select 64 + 04, Knn::InnerProductSimilarity($bitvector_neg_1_04, $bitvector_pos_1_04);
select 64 + 08, Knn::InnerProductSimilarity($bitvector_neg_1_08, $bitvector_pos_1_08);
select 64 + 16, Knn::InnerProductSimilarity($bitvector_neg_1_16, $bitvector_pos_1_16);
select 64 + 24, Knn::InnerProductSimilarity($bitvector_neg_1_24, $bitvector_pos_1_24);
select 64 + 32, Knn::InnerProductSimilarity($bitvector_neg_1_32, $bitvector_pos_1_32);
select 64 + 40, Knn::InnerProductSimilarity($bitvector_neg_1_40, $bitvector_pos_1_40);
select 64 + 48, Knn::InnerProductSimilarity($bitvector_neg_1_48, $bitvector_pos_1_48);
select 64 + 56, Knn::InnerProductSimilarity($bitvector_neg_1_56, $bitvector_pos_1_56);
select 64 + 60, Knn::InnerProductSimilarity($bitvector_neg_1_60, $bitvector_pos_1_60);

--bit vector -- only tail
$bitvector_pos_00 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 00));
$bitvector_pos_04 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 04));
$bitvector_pos_08 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 08));
$bitvector_pos_16 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 16));
$bitvector_pos_24 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 24));
$bitvector_pos_32 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 32));
$bitvector_pos_40 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 40));
$bitvector_pos_48 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 48));
$bitvector_pos_56 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 56));
$bitvector_pos_60 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 60));

$bitvector_neg_00 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 00));
$bitvector_neg_04 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 04));
$bitvector_neg_08 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 08));
$bitvector_neg_16 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 16));
$bitvector_neg_24 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 24));
$bitvector_neg_32 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 32));
$bitvector_neg_40 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 40));
$bitvector_neg_48 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 48));
$bitvector_neg_56 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 56));
$bitvector_neg_60 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 60));

select 00, Knn::InnerProductSimilarity($bitvector_pos_00, $bitvector_pos_00);
select 04, Knn::InnerProductSimilarity($bitvector_pos_04, $bitvector_pos_04);
select 08, Knn::InnerProductSimilarity($bitvector_pos_08, $bitvector_pos_08);
select 16, Knn::InnerProductSimilarity($bitvector_pos_16, $bitvector_pos_16);
select 24, Knn::InnerProductSimilarity($bitvector_pos_24, $bitvector_pos_24);
select 32, Knn::InnerProductSimilarity($bitvector_pos_32, $bitvector_pos_32);
select 40, Knn::InnerProductSimilarity($bitvector_pos_40, $bitvector_pos_40);
select 48, Knn::InnerProductSimilarity($bitvector_pos_48, $bitvector_pos_48);
select 56, Knn::InnerProductSimilarity($bitvector_pos_56, $bitvector_pos_56);
select 60, Knn::InnerProductSimilarity($bitvector_pos_60, $bitvector_pos_60);

select 00, Knn::InnerProductSimilarity($bitvector_neg_00, $bitvector_neg_00);
select 04, Knn::InnerProductSimilarity($bitvector_neg_04, $bitvector_neg_04);
select 08, Knn::InnerProductSimilarity($bitvector_neg_08, $bitvector_neg_08);
select 16, Knn::InnerProductSimilarity($bitvector_neg_16, $bitvector_neg_16);
select 24, Knn::InnerProductSimilarity($bitvector_neg_24, $bitvector_neg_24);
select 32, Knn::InnerProductSimilarity($bitvector_neg_32, $bitvector_neg_32);
select 40, Knn::InnerProductSimilarity($bitvector_neg_40, $bitvector_neg_40);
select 48, Knn::InnerProductSimilarity($bitvector_neg_48, $bitvector_neg_48);
select 56, Knn::InnerProductSimilarity($bitvector_neg_56, $bitvector_neg_56);
select 60, Knn::InnerProductSimilarity($bitvector_neg_60, $bitvector_neg_60);

select 00, Knn::InnerProductSimilarity($bitvector_pos_00, $bitvector_neg_00);
select 04, Knn::InnerProductSimilarity($bitvector_pos_04, $bitvector_neg_04);
select 08, Knn::InnerProductSimilarity($bitvector_pos_08, $bitvector_neg_08);
select 16, Knn::InnerProductSimilarity($bitvector_pos_16, $bitvector_neg_16);
select 24, Knn::InnerProductSimilarity($bitvector_pos_24, $bitvector_neg_24);
select 32, Knn::InnerProductSimilarity($bitvector_pos_32, $bitvector_neg_32);
select 40, Knn::InnerProductSimilarity($bitvector_pos_40, $bitvector_neg_40);
select 48, Knn::InnerProductSimilarity($bitvector_pos_48, $bitvector_neg_48);
select 56, Knn::InnerProductSimilarity($bitvector_pos_56, $bitvector_neg_56);
select 60, Knn::InnerProductSimilarity($bitvector_pos_60, $bitvector_neg_60);

select 00, Knn::InnerProductSimilarity($bitvector_neg_00, $bitvector_pos_00);
select 04, Knn::InnerProductSimilarity($bitvector_neg_04, $bitvector_pos_04);
select 08, Knn::InnerProductSimilarity($bitvector_neg_08, $bitvector_pos_08);
select 16, Knn::InnerProductSimilarity($bitvector_neg_16, $bitvector_pos_16);
select 24, Knn::InnerProductSimilarity($bitvector_neg_24, $bitvector_pos_24);
select 32, Knn::InnerProductSimilarity($bitvector_neg_32, $bitvector_pos_32);
select 40, Knn::InnerProductSimilarity($bitvector_neg_40, $bitvector_pos_40);
select 48, Knn::InnerProductSimilarity($bitvector_neg_48, $bitvector_pos_48);
select 56, Knn::InnerProductSimilarity($bitvector_neg_56, $bitvector_pos_56);
select 60, Knn::InnerProductSimilarity($bitvector_neg_60, $bitvector_pos_60);
