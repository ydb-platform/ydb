PRAGMA Library("aggregate.sql");
IMPORT aggregate SYMBOLS $DigestFactory;

$rows = AsList(
    AsStruct(1ul AS key, Just("one"u) AS value),
    AsStruct(11ul AS key, Nothing(Utf8?) AS value),
    AsStruct(21ul AS key, Just("three"u) AS value)
);

-- The result's type and all three hash widths are part of the public contract.
SELECT
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash64")) AS city64,
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash128")) AS city128,
    AGGREGATE_BY(TableRow(), $DigestFactory("Sha256")) AS sha256
FROM AS_TABLE($rows);

-- Reversing the rows must preserve every component of each digest.
SELECT
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash64")) AS city64,
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash128")) AS city128,
    AGGREGATE_BY(TableRow(), $DigestFactory("Sha256")) AS sha256
FROM AS_TABLE(ListReverse($rows));

-- Empty input returns a typed, non-optional zero digest.
SELECT
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash64")) AS city64,
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash128")) AS city128,
    AGGREGATE_BY(TableRow(), $DigestFactory("Sha256")) AS sha256
FROM AS_TABLE($rows)
WHERE key < 0ul;

-- A NULL value contributes differently from an empty UTF-8 string.
SELECT
    AGGREGATE_BY(AsStruct(key AS key, Just(COALESCE(value, ""u)) AS value), $DigestFactory("CityHash64")) AS city64,
    AGGREGATE_BY(AsStruct(key AS key, Just(COALESCE(value, ""u)) AS value), $DigestFactory("CityHash128")) AS city128,
    AGGREGATE_BY(AsStruct(key AS key, Just(COALESCE(value, ""u)) AS value), $DigestFactory("Sha256")) AS sha256
FROM AS_TABLE($rows);

SELECT
    Digest::Sha256("abc") = "\xba\x78\x16\xbf\x8f\x01\xcf\xea\x41\x41\x40\xde\x5d\xae\x22\x23\xb0\x03\x61\xa3\x96\x17\x7a\x9c\xb4\x10\xff\x61\xf2\x00\x15\xad" AS scalar_namespace_works;

$state = DigestAggregate::AggregateAddValue(
    DigestAggregate::AggregateCreate("Sha256"), Digest::Sha256(StablePickle($rows[0]))
);
SELECT
    DigestAggregate::AggregateFinalize(DigestAggregate::AggregateDeserialize(DigestAggregate::AggregateSerialize($state)))
        = DigestAggregate::AggregateFinalize($state) AS state_roundtrip_matches;
