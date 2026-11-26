-- Packed 4-bit serialization, saturation, odd and even dimensions.
$input = [-8t, -1t, 0t, 7t, 5t];
$encoded = Knn::ToBinaryStringInt4($input);
select Len(Untag($encoded, "Int4Vector")) == 5;
select Knn::FloatFromBinaryString($encoded) = ListMap($input, ($x) -> { return Cast($x AS Float); });
select Knn::FloatFromBinaryString(Knn::ToBinaryStringInt4([-128t, -9t, 8t, 127t])) = [-8.0f, -8.0f, 7.0f, 7.0f];
select Knn::FloatFromBinaryString(Knn::ToBinaryStringInt4(ListCreate(Int8)));
select Knn::FloatFromBinaryString(Just($encoded)) = Knn::FloatFromBinaryString($encoded);
select Knn::FloatFromBinaryString(Untag($encoded, "Int4Vector")) = Knn::FloatFromBinaryString($encoded);
select Knn::ToBinaryStringInt4(Nothing(List<Int8>?)) is null;

-- Lazy vectors exercise SIMD blocks and scalar tails at an odd dimension.
$a = Knn::ToBinaryStringInt4(ListFlatten([ListFlatten(ListReplicate([1t, 2t, 3t, 4t], 16)), [5t, 6t, 7t]]));
$b = Knn::ToBinaryStringInt4(ListFlatten([ListFlatten(ListReplicate([4t, 3t, 2t, 1t], 16)), [7t, 6t, 5t]]));
$af = Knn::ToBinaryStringFloat(Knn::FloatFromBinaryString($a));
$bf = Knn::ToBinaryStringFloat(Knn::FloatFromBinaryString($b));
select Knn::ManhattanDistance($a, $b) = Knn::ManhattanDistance($af, $bf);
select Knn::EuclideanDistance($a, $b) = Knn::EuclideanDistance($af, $bf);
select Knn::InnerProductSimilarity($a, $b) = Knn::InnerProductSimilarity($af, $bf);
select Abs(Knn::CosineSimilarity($a, $b) - Knn::CosineSimilarity($af, $bf)) < 0.000001f;
select Abs(Knn::CosineDistance($a, $b) - Knn::CosineDistance($af, $bf)) < 0.000001f;
select Knn::ManhattanDistance(Untag($a, "Int4Vector"), $b) = Knn::ManhattanDistance($a, $b);
select Knn::EuclideanDistance($encoded, Knn::ToBinaryStringInt4([1t])) is null;
-- Padding 2, padding with no payload, nonzero unused high nibble, missing trailer.
select Knn::ManhattanDistance("\x10\x02\x09", "\x10\x02\x09") is null;
select Knn::CosineSimilarity("\x01\x09", "\x01\x09") is null;
select Knn::InnerProductSimilarity("\xf1\x01\x09", "\xf1\x01\x09") is null;
select Knn::EuclideanDistance("\x09", "\x09") is null;
select Knn::InnerProductSimilarity("\x00\x08", "\x00\x09") is null;
