SELECT DigestAggregate::AggregateFinalize(DigestAggregate::AggregateAddValue(
    DigestAggregate::AggregateCreate("CityHash128"), AsTuple(1ul, 2u)));
