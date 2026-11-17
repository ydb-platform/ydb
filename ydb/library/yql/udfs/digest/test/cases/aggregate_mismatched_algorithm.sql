SELECT DigestAggregate::AggregateFinalize(DigestAggregate::AggregateAddValue(
    DigestAggregate::AggregateCreate("CityHash64"), AsTuple(1ul, 0ul)));
