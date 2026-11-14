PRAGMA Warning("disable", "4510");

$DigestFactory = ($algorithm) -> {
    $add = ($state, $row) -> {
        -- IfType selects a branch during compilation; TypeOf never evaluates the value.
        $bytes = YQL::IfType(
            $row, TypeOf(DigestAggregate::Canonicalize($row)),
            ($value) -> (StablePickle($value)),
            ($value) -> (StablePickle(DigestAggregate::Canonicalize($value)))
        );
        RETURN CASE $algorithm
            WHEN "CityHash64" THEN DigestAggregate::AggregateAddValue($state, Digest::CityHash($bytes))
            WHEN "CityHash128" THEN DigestAggregate::AggregateAddValue($state, Digest::CityHash128($bytes))
            ELSE DigestAggregate::AggregateAddValue($state, Digest::Sha256($bytes))
        END;
    };

    RETURN AggregationFactory(
        "UDAF",
        ($row, $_parent) -> {
            RETURN $add(DigestAggregate::AggregateCreate($algorithm), $row);
        },
        ($state, $row, $_parent) -> { RETURN $add($state, $row); },
        DigestAggregate::AggregateMerge,
        DigestAggregate::AggregateFinalize,
        DigestAggregate::AggregateSerialize,
        DigestAggregate::AggregateDeserialize,
        DigestAggregate::AggregateFinalize(DigestAggregate::AggregateCreate($algorithm))
    );
};

EXPORT $DigestFactory;
