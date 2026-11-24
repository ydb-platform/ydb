PRAGMA Library("aggregate.sql");
IMPORT aggregate SYMBOLS $DigestFactory;

-- Pin Digest v1's StablePickle representation for every supported scalar type.
$row = AsStruct(
    Bool("true") AS b,
    Int8("-8") AS i8, Uint8("200") AS u8,
    Int16("-16000") AS i16, Uint16("60000") AS u16,
    Int32("-100000") AS i32, Uint32("100000") AS u32,
    Int64("-5000000000") AS i64, Uint64("5000000000") AS u64,
    Float("1.5") AS f, Double("2.25") AS d,
    String("hello\x00world") AS s, Utf8("мир") AS u,
    Yson("[1;2]") AS y, Json("{\"a\":1}") AS j,
    JsonDocument("{\"b\":2}") AS jd,
    Uuid("00112233-4455-6677-8899-aabbccddeeff") AS uuid,
    DyNumber("-10.23") AS dyn,
    Date("1970-01-02") AS date,
    Datetime("1970-01-02T00:00:00Z") AS dt,
    Timestamp("1970-01-02T00:00:00.000000Z") AS ts,
    Interval("P1D") AS iv,
    Date32("1969-12-31") AS date32,
    Datetime64("1969-12-31T00:00:00Z") AS dt64,
    Timestamp64("1969-12-31T00:00:00.000000Z") AS ts64,
    Interval64("P1D") AS iv64,
    Decimal("11.3", 5, 2) AS dec
);

SELECT
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash64")) AS city64,
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash128")) AS city128,
    AGGREGATE_BY(TableRow(), $DigestFactory("Sha256")) AS sha256
FROM AS_TABLE(AsList($row));

$optional_rows = AsList(
    StaticMap($row, ($value) -> (Just($value))),
    StaticMap($row, ($value) -> (Nothing(OptionalType(TypeOf($value)))))
);

-- Optional masks and an all-NULL row are also part of the frozen encoding.
SELECT
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash64")) AS city64,
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash128")) AS city128,
    AGGREGATE_BY(TableRow(), $DigestFactory("Sha256")) AS sha256
FROM AS_TABLE($optional_rows);

$fp_rows = AsList(
    AsStruct(Float("0") AS f, Double("0") AS d),
    AsStruct(Float("-0") AS f, Double("-0") AS d),
    AsStruct(Float("nan") AS f, Double("nan") AS d),
    AsStruct(Float("-nan") AS f, Double("-nan") AS d)
);

-- StablePickle canonicalizes signed zero and NaN representations.
SELECT
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash64")) AS city64,
    AGGREGATE_BY(TableRow(), $DigestFactory("CityHash128")) AS city128,
    AGGREGATE_BY(TableRow(), $DigestFactory("Sha256")) AS sha256
FROM AS_TABLE($fp_rows);
