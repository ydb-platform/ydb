-- Packed 4-bit serialization, saturation, odd and even dimensions.
$input = [0ut, 1ut, 7ut, 15ut, 5ut];
$encoded = Knn::ToBinaryStringUint4($input);
select Len(Untag($encoded, "Uint4Vector")) == 5;
select Knn::FloatFromBinaryString($encoded) = ListMap($input, ($x) -> { return Cast($x AS Float); });
select Knn::FloatFromBinaryString(Knn::ToBinaryStringUint4([0ut, 15ut, 16ut, 255ut])) = [0.0f, 15.0f, 15.0f, 15.0f];
select Knn::FloatFromBinaryString(Knn::ToBinaryStringUint4(ListCreate(Uint8)));
select Knn::FloatFromBinaryString(Just($encoded)) = Knn::FloatFromBinaryString($encoded);
select Knn::FloatFromBinaryString(Untag($encoded, "Uint4Vector")) = Knn::FloatFromBinaryString($encoded);
select Knn::ToBinaryStringUint4(Nothing(List<Uint8>?)) is null;

-- Lazy vectors exercise SIMD blocks and scalar tails at an odd dimension.
$a = Knn::ToBinaryStringUint4(ListFlatten([ListFlatten(ListReplicate([1ut, 2ut, 3ut, 4ut], 16)), [5ut, 6ut, 7ut]]));
$b = Knn::ToBinaryStringUint4(ListFlatten([ListFlatten(ListReplicate([4ut, 3ut, 2ut, 1ut], 16)), [7ut, 6ut, 5ut]]));
$af = Knn::ToBinaryStringFloat(Knn::FloatFromBinaryString($a));
$bf = Knn::ToBinaryStringFloat(Knn::FloatFromBinaryString($b));
select Knn::ManhattanDistance($a, $b) = Knn::ManhattanDistance($af, $bf);
select Knn::EuclideanDistance($a, $b) = Knn::EuclideanDistance($af, $bf);
select Knn::InnerProductSimilarity($a, $b) = Knn::InnerProductSimilarity($af, $bf);
select Abs(Knn::CosineSimilarity($a, $b) - Knn::CosineSimilarity($af, $bf)) < 0.000001f;
select Abs(Knn::CosineDistance($a, $b) - Knn::CosineDistance($af, $bf)) < 0.000001f;
select Knn::ManhattanDistance(Untag($a, "Uint4Vector"), $b) = Knn::ManhattanDistance($a, $b);
select Knn::EuclideanDistance($encoded, Knn::ToBinaryStringUint4([1ut])) is null;
-- Padding 2, padding with no payload, nonzero unused high nibble, missing trailer.
select Knn::ManhattanDistance("\x10\x02\x08", "\x10\x02\x08") is null;
select Knn::CosineSimilarity("\x01\x08", "\x01\x08") is null;
select Knn::InnerProductSimilarity("\xf1\x01\x08", "\xf1\x01\x08") is null;
select Knn::EuclideanDistance("\x08", "\x08") is null;
select Knn::InnerProductSimilarity("\x00\x08", "\x00\x09") is null;
