/* syntax version 1 */
-- BridgeTypes: runnable samples for every manifest-supported YQL type.
-- Upload sdk + BridgeTypes WASM modules before running.

$vt = Variant<Uint32, Timestamp>;

SELECT
    BridgeTypes::EchoBool(true) AS echo_bool,
    BridgeTypes::EchoInt32(7) AS echo_int32,
    BridgeTypes::EchoUint32(8u) AS echo_uint32,
    BridgeTypes::EchoInt64(9l) AS echo_int64,
    BridgeTypes::EchoUint64(10ul) AS echo_uint64,
    BridgeTypes::EchoFloat(1.5f) AS echo_float,
    BridgeTypes::EchoDouble(2.5) AS echo_double,
    BridgeTypes::EchoString("abc") AS echo_string,
    BridgeTypes::EchoUtf8("привет") AS echo_utf8,
    BridgeTypes::ReadDateAsUint64(Date("2020-01-02")) AS read_date,
    BridgeTypes::ReadDatetimeAsUint32(Datetime("2020-01-02T03:04:05Z")) AS read_datetime,
    BridgeTypes::ReadTimestampAsUint64(Timestamp("2020-01-02T03:04:05.000006Z")) AS read_timestamp,
    -- Manifest maps plain "decimal" to Decimal(35,0); scale must match.
    BridgeTypes::ReadDecimalChecksum(Decimal("12345", 35, 0)) AS read_decimal,
    BridgeTypes::ListSumInt64(AsList(1l, 2l, 3l)) AS list_sum,
    BridgeTypes::DictGetInt64(AsDict(AsTuple("k", 42l)), "k") AS dict_get,
    BridgeTypes::TupleKindSum(AsTuple(1l, "x", 1.0f)) AS tuple_kind_sum,
    BridgeTypes::StructGetScore(AsStruct(1 AS id, 2.5f AS score, "n" AS name)) AS struct_score,
    BridgeTypes::VariantIndex(Variant(5u, "0", $vt)) AS variant_index,
    BridgeTypes::OptionalListPresentCount(AsList(Just(1l), Nothing(Int64?), Just(3l))) AS optional_list,
    BridgeTypes::MakeGreetingStruct() AS greeting,
    BridgeTypes::MakeIntList() AS int_list,
    BridgeTypes::MakeNameDict() AS name_dict,
    -- Way() returns the alternative index; the ydb CLI cannot format Variant.
    Way(BridgeTypes::MakeVariantUint32()) AS variant_uint32,
    BridgeTypes::RunCallableInt64(($x) -> { RETURN $x + 1; }, 41l) AS run_callable;
