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

$bitvector_pos_04 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 04));
$bitvector_pos_08 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 08));
$bitvector_pos_16 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 16));
$bitvector_pos_24 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 24));
$bitvector_pos_32 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 32));
$bitvector_pos_40 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 40));
$bitvector_pos_48 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 48));
$bitvector_pos_56 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 56));
$bitvector_pos_60 = Knn::ToBinaryStringBit(ListReplicate(1.0f, 60));

$bitvector_neg_04 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 04));
$bitvector_neg_08 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 08));
$bitvector_neg_16 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 16));
$bitvector_neg_24 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 24));
$bitvector_neg_32 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 32));
$bitvector_neg_40 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 40));
$bitvector_neg_48 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 48));
$bitvector_neg_56 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 56));
$bitvector_neg_60 = Knn::ToBinaryStringBit(ListReplicate(-1.0f, 60));

select Len(Untag($bitvector_pos_1_00, "BitVector")) == 8 + 0 + 2, $bitvector_pos_1_00;
select Len(Untag($bitvector_pos_1_04, "BitVector")) == 8 + 1 + 2, $bitvector_pos_1_04;
select Len(Untag($bitvector_pos_1_08, "BitVector")) == 8 + 1 + 2, $bitvector_pos_1_08;
select Len(Untag($bitvector_pos_1_16, "BitVector")) == 8 + 2 + 2, $bitvector_pos_1_16;
select Len(Untag($bitvector_pos_1_24, "BitVector")) == 8 + 3 + 2, $bitvector_pos_1_24;
select Len(Untag($bitvector_pos_1_32, "BitVector")) == 8 + 4 + 2, $bitvector_pos_1_32;
select Len(Untag($bitvector_pos_1_40, "BitVector")) == 8 + 5 + 2, $bitvector_pos_1_40;
select Len(Untag($bitvector_pos_1_48, "BitVector")) == 8 + 6 + 2, $bitvector_pos_1_48;
select Len(Untag($bitvector_pos_1_56, "BitVector")) == 8 + 7 + 2, $bitvector_pos_1_56;
select Len(Untag($bitvector_pos_1_60, "BitVector")) == 8 + 8 + 2, $bitvector_pos_1_60;

select Len(Untag($bitvector_neg_1_00, "BitVector")) == 8 + 0 + 2, $bitvector_neg_1_00;
select Len(Untag($bitvector_neg_1_04, "BitVector")) == 8 + 1 + 2, $bitvector_neg_1_04;
select Len(Untag($bitvector_neg_1_08, "BitVector")) == 8 + 1 + 2, $bitvector_neg_1_08;
select Len(Untag($bitvector_neg_1_16, "BitVector")) == 8 + 2 + 2, $bitvector_neg_1_16;
select Len(Untag($bitvector_neg_1_24, "BitVector")) == 8 + 3 + 2, $bitvector_neg_1_24;
select Len(Untag($bitvector_neg_1_32, "BitVector")) == 8 + 4 + 2, $bitvector_neg_1_32;
select Len(Untag($bitvector_neg_1_40, "BitVector")) == 8 + 5 + 2, $bitvector_neg_1_40;
select Len(Untag($bitvector_neg_1_48, "BitVector")) == 8 + 6 + 2, $bitvector_neg_1_48;
select Len(Untag($bitvector_neg_1_56, "BitVector")) == 8 + 7 + 2, $bitvector_neg_1_56;
select Len(Untag($bitvector_neg_1_60, "BitVector")) == 8 + 8 + 2, $bitvector_neg_1_60;

select Len(Untag($bitvector_pos_04, "BitVector")) == 1 + 2, $bitvector_pos_04;
select Len(Untag($bitvector_pos_08, "BitVector")) == 1 + 2, $bitvector_pos_08;
select Len(Untag($bitvector_pos_16, "BitVector")) == 2 + 2, $bitvector_pos_16;
select Len(Untag($bitvector_pos_24, "BitVector")) == 3 + 2, $bitvector_pos_24;
select Len(Untag($bitvector_pos_32, "BitVector")) == 4 + 2, $bitvector_pos_32;
select Len(Untag($bitvector_pos_40, "BitVector")) == 5 + 2, $bitvector_pos_40;
select Len(Untag($bitvector_pos_48, "BitVector")) == 6 + 2, $bitvector_pos_48;
select Len(Untag($bitvector_pos_56, "BitVector")) == 7 + 2, $bitvector_pos_56;
select Len(Untag($bitvector_pos_60, "BitVector")) == 8 + 2, $bitvector_pos_60;

select Len(Untag($bitvector_neg_04, "BitVector")) == 1 + 2, $bitvector_neg_04;
select Len(Untag($bitvector_neg_08, "BitVector")) == 1 + 2, $bitvector_neg_08;
select Len(Untag($bitvector_neg_16, "BitVector")) == 2 + 2, $bitvector_neg_16;
select Len(Untag($bitvector_neg_24, "BitVector")) == 3 + 2, $bitvector_neg_24;
select Len(Untag($bitvector_neg_32, "BitVector")) == 4 + 2, $bitvector_neg_32;
select Len(Untag($bitvector_neg_40, "BitVector")) == 5 + 2, $bitvector_neg_40;
select Len(Untag($bitvector_neg_48, "BitVector")) == 6 + 2, $bitvector_neg_48;
select Len(Untag($bitvector_neg_56, "BitVector")) == 7 + 2, $bitvector_neg_56;
select Len(Untag($bitvector_neg_60, "BitVector")) == 8 + 2, $bitvector_neg_60;

-- serialization format
$flip_list_item = ($list, $idx) -> {
    $before = ListTake($list, $idx);
    $after = ListSkip($list, $idx + 1);
    RETURN ListExtend($before, [-Unwrap($list[$idx])], $after);
};

select
    Knn::ToBinaryStringBit(
        $flip_list_item(
            ListReplicate(`base_value`, `element_count`),
            `element_idx`
        )
    )
from (
    select
        ListFromRange(0, 130, 7) as `element_count`,  -- size of vector
        ListFromRange(0, 130, 3) as `element_idx`,    -- bit idx that differs
        [1.0f, -1.0f]            as `base_value`,     -- default bit value
)
flatten list by (`element_count`, `element_idx`, `base_value`)  -- cartesian product to cover all combinations
where `element_idx` < `element_count`;

select Knn::ToBinaryStringBit([-1.0]);  -- expected \0\7\10
select Knn::ToBinaryStringBit([1.0, 1.0]);  -- expected \3\6\10
select Knn::ToBinaryStringBit([1.0, 2.0, 3.0, 0.0, -1.0, -3.0, 0.0, 15.0]);  -- expected \225\0\10
select Knn::ToBinaryStringBit([1.0, 2.0, 3.0, 0.0, -1.0, -3.0, 0.0, 15.0, 6.0, -1.0, -1.0, 33.34]);  -- expected \0b00011001\0b1110\4\10

select Knn::ToBinaryStringBit(
    [
        1.0, 1.0, -1.0,  -- last byte in serialized data
        -1.0, -1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0,
        1.0, 1.0, -1.0, -1.0, 1.0, 1.0, -1.0, 1.0
    ]
);  -- expected \0b11001101\0b00001100\0b00000110\5\10
