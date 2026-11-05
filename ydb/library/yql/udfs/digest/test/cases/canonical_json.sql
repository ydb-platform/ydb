PRAGMA Library("aggregate.sql");
IMPORT aggregate SYMBOLS $DigestFactory;

$document = JsonDocument('[1,"alpha",2,"beta"]');
$json = Json('[1,"alpha",2,"beta"]');
$variant_type = VariantType(TupleType(DataType("JsonDocument"), OptionalType(DataType("JsonDocument"))));
$json_variant_type = VariantType(TupleType(DataType("Json"), OptionalType(DataType("Json"))));
$row = AsStruct(
    $document AS scalar,
    Just(Just($document)) AS optional,
    Just(Nothing(OptionalType(DataType("JsonDocument")))) AS nested_null,
    AsList(Just($document), Nothing(OptionalType(DataType("JsonDocument")))) AS list,
    AsDict(AsTuple("key", $document)) AS dict,
    AsTuple($document, 7) AS tuple,
    Variant($document, "0", $variant_type) AS variant,
    AsTagged($document, "document") AS tagged
);
$expected = AsStruct(
    $json AS scalar,
    Just(Just($json)) AS optional,
    Just(Nothing(OptionalType(DataType("Json")))) AS nested_null,
    AsList(Just($json), Nothing(OptionalType(DataType("Json")))) AS list,
    AsDict(AsTuple("key", $json)) AS dict,
    AsTuple($json, 7) AS tuple,
    Variant($json, "0", $json_variant_type) AS variant,
    AsTagged($json, "document") AS tagged
);

SELECT
    StablePickle(DigestAggregate::Canonicalize($row)) = StablePickle($expected) AS nested_values_match,
    StablePickle(DigestAggregate::Canonicalize($expected)) = StablePickle($expected) AS other_types_unchanged,
    StablePickle(DigestAggregate::Canonicalize(Nothing(OptionalType(DataType("JsonDocument")))))
        = StablePickle(Nothing(OptionalType(DataType("Json")))) AS null_matches;

$actual_digest = SELECT
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash64")) AS city64,
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash128")) AS city128,
    AGGREGATE_BY(TableRow(), $DigestFactory("Sha256")) AS sha256
FROM AS_TABLE(AsList($row));
$expected_digest = SELECT
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash64")) AS city64,
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash128")) AS city128,
    AGGREGATE_BY(TableRow(), $DigestFactory("Sha256")) AS sha256
FROM AS_TABLE(AsList($expected));

SELECT
    actual.city64 = expected.city64 AS city64_matches,
    actual.city128 = expected.city128 AS city128_matches,
    actual.sha256 = expected.sha256 AS sha256_matches
FROM $actual_digest AS actual CROSS JOIN $expected_digest AS expected;
