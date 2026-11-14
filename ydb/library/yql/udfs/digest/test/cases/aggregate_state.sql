$zero8 = "\x00\x00\x00\x00\x00\x00\x00\x00";
$ones8 = "\xff\xff\xff\xff\xff\xff\xff\xff";

$check = ($algorithm, $first, $second, $expected) -> {
    $empty = DigestAggregate::AggregateCreate($algorithm);
    $left = DigestAggregate::AggregateAddValue($empty, $first);
    $right = DigestAggregate::AggregateAddValue($empty, $second);
    $merged = DigestAggregate::AggregateFinalize(DigestAggregate::AggregateMerge($left, $right));
    RETURN DigestAggregate::AggregateFinalize($empty).count = 0ul
        AND DigestAggregate::AggregateFinalize($left).count = 1ul
        AND DigestAggregate::AggregateFinalize($right).count = 1ul
        AND $merged.count = 2ul
        AND $merged.hash = $expected;
};

SELECT
    $check("CityHash64", 18446744073709551615ul, 1ul, $zero8) AS city64,
    $check("CityHash128", AsTuple(18446744073709551615ul, 0ul), AsTuple(1ul, 0ul),
        "\x00\x00\x00\x00\x00\x00\x00\x01" || $zero8) AS city128,
    $check("Sha256", $zero8 || $zero8 || $zero8 || $ones8,
        $zero8 || $zero8 || $zero8 || "\x00\x00\x00\x00\x00\x00\x00\x01",
        $zero8 || $zero8 || "\x00\x00\x00\x00\x00\x00\x00\x01" || $zero8) AS sha256;
