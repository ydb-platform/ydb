SELECT DigestAggregate::AggregateFinalize(DigestAggregate::AggregateAddValue(
    DigestAggregate::AggregateCreate("Sha256"), "short"));
