--list serialization
$vector = [1.2f, 2.3f, 3.4f, 4.5f, 5.6f];
$vector_binary_str = Knn::ToBinaryString($vector);
select $vector_binary_str;

--deserialization
$deserialized_vector = Knn::FromBinaryString($vector_binary_str);
select $deserialized_vector;
select $deserialized_vector = $vector;

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

select $bitvector_pos_1_00;
select $bitvector_pos_1_04;
select $bitvector_pos_1_08;
select $bitvector_pos_1_16;
select $bitvector_pos_1_24;
select $bitvector_pos_1_32;
select $bitvector_pos_1_40;
select $bitvector_pos_1_48;
select $bitvector_pos_1_56;
select $bitvector_pos_1_60;

select $bitvector_neg_1_00;
select $bitvector_neg_1_04;
select $bitvector_neg_1_08;
select $bitvector_neg_1_16;
select $bitvector_neg_1_24;
select $bitvector_neg_1_32;
select $bitvector_neg_1_40;
select $bitvector_neg_1_48;
select $bitvector_neg_1_56;
select $bitvector_neg_1_60;

select $bitvector_pos_00;
select $bitvector_pos_04;
select $bitvector_pos_08;
select $bitvector_pos_16;
select $bitvector_pos_24;
select $bitvector_pos_32;
select $bitvector_pos_40;
select $bitvector_pos_48;
select $bitvector_pos_56;
select $bitvector_pos_60;

select $bitvector_neg_00;
select $bitvector_neg_04;
select $bitvector_neg_08;
select $bitvector_neg_16;
select $bitvector_neg_24;
select $bitvector_neg_32;
select $bitvector_neg_40;
select $bitvector_neg_48;
select $bitvector_neg_56;
select $bitvector_neg_60;
