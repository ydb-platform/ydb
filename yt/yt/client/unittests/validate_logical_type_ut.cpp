#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/validate_logical_type.h>

#include <util/string/escape.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NLogicalTypeShortcuts;
using namespace NTzTypes;

////////////////////////////////////////////////////////////////////////////////

#define ERROR_ATTRS(logicalType, ysonString) \
        " at " << __FILE__ ":" << __LINE__ << '\n' \
        << "Type: " << ToString(*logicalType) << '\n' \
        << "YsonString: " << '"' << EscapeC(ysonString) << '"' << '\n'\

#define EXPECT_BAD_TYPE(logicalTypeExpr, ysonString) \
    do { \
        auto logicalType = logicalTypeExpr; \
        try { \
            ValidateComplexLogicalType(ysonString, logicalType); \
            ADD_FAILURE() << "Expected type failure" << ERROR_ATTRS(logicalType, ysonString); \
        } catch (const std::exception& ex) { \
            if (!IsSchemaViolationError(ex)) { \
                ADD_FAILURE() << "Unexpected error" << ERROR_ATTRS(logicalType, ysonString) \
                    << "what: " << ex.what(); \
            } \
        } \
    } while (0)

#define EXPECT_GOOD_TYPE(logicalTypeExpr, ysonString) \
    do { \
        auto logicalType = logicalTypeExpr; \
        try { \
            ValidateComplexLogicalType(ysonString, logicalType); \
        } catch (const std::exception& ex) { \
            ADD_FAILURE() << "Unexpected error" << ERROR_ATTRS(logicalType, ysonString) \
                    << "what: " << ex.what(); \
        } \
    } while (0)

bool IsSchemaViolationError(const std::exception& ex) {
    auto errorException = dynamic_cast<const TErrorException*>(&ex);
    if (!errorException) {
        return false;
    }
    return errorException->Error().FindMatching(NYT::NTableClient::EErrorCode::SchemaViolation).has_value();
}

TEST(TValidateLogicalTypeTest, TestBasicTypes)
{
    EXPECT_GOOD_TYPE(String(), " foo ");
    EXPECT_GOOD_TYPE(Int64(), " 42 ");
    EXPECT_GOOD_TYPE(Uint64(), " 42u ");
    EXPECT_GOOD_TYPE(Double(), " 3.14 ");
    EXPECT_GOOD_TYPE(Float(), " 3.14 ");
    EXPECT_GOOD_TYPE(Bool(), " %false ");
    EXPECT_GOOD_TYPE(Null(), " # ");

    EXPECT_BAD_TYPE(String(), " 76 ");
    EXPECT_BAD_TYPE(Int64(), " %true ");
    EXPECT_BAD_TYPE(Uint64(), " 14 ");
    EXPECT_BAD_TYPE(Double(), " bar ");
    EXPECT_BAD_TYPE(Float(), " rab ");
    EXPECT_BAD_TYPE(Bool(), " 1 ");
    EXPECT_BAD_TYPE(Null(), " 0 ");
}

TEST(TValidateLogicalTypeTest, TestSimpleOptionalTypes)
{
    EXPECT_GOOD_TYPE(Optional(String()), " foo ");
    EXPECT_GOOD_TYPE(Optional(String()), " # ");
    EXPECT_BAD_TYPE(Optional(String()), " 42 ");

    EXPECT_GOOD_TYPE(Optional(Uint64()), " 42u ");
    EXPECT_GOOD_TYPE(Optional(Uint64()), " # ");
    EXPECT_BAD_TYPE(Optional(Uint64()), " 42 ");
}

TEST(TValidateLogicalTypeTest, TestOptionalNull)
{
    EXPECT_GOOD_TYPE(Optional(Null()), " # ");
    EXPECT_GOOD_TYPE(Optional(Null()), " [#] ");

    EXPECT_BAD_TYPE(Optional(Null()), " [] ");
    EXPECT_BAD_TYPE(Optional(Null()), " [[#]] ");

    EXPECT_GOOD_TYPE(Optional(Optional(Null())), " [[#]] ");
    EXPECT_GOOD_TYPE(Optional(Optional(Null())), " [#] ");
    EXPECT_GOOD_TYPE(Optional(Optional(Null())), " # ");
    EXPECT_BAD_TYPE(Optional(Optional(Null())), " [[[#]]] ");
    EXPECT_BAD_TYPE(Optional(Optional(Null())), " [[[]]] ");
}

TEST(TValidateLogicalTypeTest, TestLogicalTypes)
{
    // Int8
    EXPECT_GOOD_TYPE(Int8(), " 127 ");
    EXPECT_GOOD_TYPE(Int8(), " -128 ");
    EXPECT_BAD_TYPE(Int8(), " 128 ");
    EXPECT_BAD_TYPE(Int8(), " -129 ");

    // Uint8
    EXPECT_GOOD_TYPE(Uint8(), " 127u ");
    EXPECT_GOOD_TYPE(Uint8(), " 128u ");
    EXPECT_GOOD_TYPE(Uint8(), " 255u ");
    EXPECT_BAD_TYPE(Uint8(), " 256u ");
    EXPECT_BAD_TYPE(Uint8(), " 100500u ");

    // Int16
    EXPECT_GOOD_TYPE(Int16(), " 32767 ");
    EXPECT_GOOD_TYPE(Int16(), " -32768 ");
    EXPECT_BAD_TYPE(Int16(), " 32768 ");
    EXPECT_BAD_TYPE(Int16(), " -32769 ");

    // Uint16
    EXPECT_GOOD_TYPE(Uint16(), " 32768u ");
    EXPECT_GOOD_TYPE(Uint16(), " 65535u ");
    EXPECT_BAD_TYPE(Uint16(), " 65536u ");

    // Int32
    EXPECT_GOOD_TYPE(Int32(), " 2147483647 ");
    EXPECT_GOOD_TYPE(Int32(), " -2147483648 ");
    EXPECT_BAD_TYPE(Int32(), " 2147483648 ");
    EXPECT_BAD_TYPE(Int32(), " -2147483649 ");

    // Uint32
    EXPECT_GOOD_TYPE(Uint32(), " 2147483648u ");
    EXPECT_GOOD_TYPE(Uint32(), " 4294967295u ");
    EXPECT_BAD_TYPE(Uint32(), " 4294967297u ");

    // Uint64
    EXPECT_GOOD_TYPE(Int64(), " 2147483648 ");
    EXPECT_GOOD_TYPE(Int64(), " -2147483649 ");

    // Uint64
    EXPECT_GOOD_TYPE(Uint64(), " 4294967297u ");

    // Utf8
    EXPECT_GOOD_TYPE(Utf8(), " foo ");
    EXPECT_GOOD_TYPE(Utf8(), " \"фу\" ");
    EXPECT_BAD_TYPE(Utf8(), " \"\244\" ");

    // Float
    EXPECT_GOOD_TYPE(Float(), " 3.14 ");
    EXPECT_GOOD_TYPE(Float(), " -3.14e35 ");
    EXPECT_GOOD_TYPE(Float(), " 3.14e35 ");
    EXPECT_BAD_TYPE(Float(), " 3.14e39 ");
    EXPECT_BAD_TYPE(Float(), " -3.14e39 ");
    EXPECT_BAD_TYPE(Float(), " blah");
}

TEST(TValidateLogicalTypeTest, TestAnyType)
{
    EXPECT_GOOD_TYPE(Yson(), " foo ");
    EXPECT_GOOD_TYPE(Yson(), " 42 ");
    EXPECT_GOOD_TYPE(Yson(), " 15u ");
    EXPECT_GOOD_TYPE(Yson(), " %true ");
    EXPECT_GOOD_TYPE(Yson(), " 3.14 ");
    EXPECT_BAD_TYPE(Yson(), "#");

    EXPECT_GOOD_TYPE(Yson(), "[142; 53u; {foo=bar; bar=[baz]};]");

    EXPECT_GOOD_TYPE(Optional(Yson()), "#");
    EXPECT_GOOD_TYPE(Optional(Yson()), "[{bar=<type=list>[]}; bar; [baz];]");

    EXPECT_BAD_TYPE(Yson(), "<>1");
    EXPECT_GOOD_TYPE(Yson(), "[<>1]");
}

TEST(TValidateLogicalTypeTest, TestTimezoneType)
{
    // Simple example.
    auto correctValue = MakeTzString<ui16>(42, "Europe/Moscow");
    EXPECT_NO_THROW(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDate>(correctValue));

    // Short buffer.
    std::string shortValue = "1";
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDate>(shortValue), "Not a valid timezone type");

    // Wrong timezone.
    auto wrongTimezoneValue = MakeTzString<ui16>(42, "Wrong/Timezone");
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDate>(wrongTimezoneValue), "Not a valid timezone type");

    // Empty timezone.
    auto emptyTimezoneValue = MakeTzString<ui16>(42, "");
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDate>(emptyTimezoneValue), "Not a valid timezone type");

    // Validate max value.
    auto maxDate = MakeTzString<ui16>(DateUpperBound - 1, "Europe/Moscow");
    EXPECT_NO_THROW(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDate>(maxDate));
    auto maxDatetime = MakeTzString<ui32>(DatetimeUpperBound - 1, "Europe/Moscow");
    EXPECT_NO_THROW(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDatetime>(maxDatetime));
    auto maxTimestamp = MakeTzString<ui64>(TimestampUpperBound - 1, "Europe/Moscow");
    EXPECT_NO_THROW(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzTimestamp>(maxTimestamp));
    auto maxDate32 = MakeTzString<i32>(Date32UpperBound - 1, "Europe/Moscow");
    EXPECT_NO_THROW(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDate32>(maxDate32));
    auto maxDatetime64 = MakeTzString<i64>(Datetime64UpperBound - 1, GetTzName(1));
    EXPECT_NO_THROW(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDatetime64>(maxDatetime64));
    auto maxTimestamp64 = MakeTzString<i64>(Timestamp64UpperBound - 1, GetTzName(1));
    EXPECT_NO_THROW(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzTimestamp64>(maxTimestamp64));

    // Validate too big value.
    auto tooBigDate = MakeTzString<ui16>(DateUpperBound, GetTzName(1));
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDate>(tooBigDate), "Not a valid timezone type");
    auto tooBigDatetime = MakeTzString<ui32>(DatetimeUpperBound, GetTzName(1));
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDatetime>(tooBigDatetime), "Not a valid timezone type");
    auto tooBigTimestamp = MakeTzString<ui64>(TimestampUpperBound, GetTzName(1));
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzTimestamp>(tooBigTimestamp), "Not a valid timezone type");
    auto tooBigDate32 = MakeTzString<i32>(Date32UpperBound, GetTzName(1));
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDate32>(tooBigDate32), "Not a valid timezone type");
    auto tooBigDatetime64 = MakeTzString<i64>(Datetime64UpperBound, GetTzName(1));
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDatetime64>(tooBigDatetime64), "Not a valid timezone type");
    auto tooBigTimestamp64 = MakeTzString<i64>(Timestamp64UpperBound, GetTzName(1));
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzTimestamp64>(tooBigTimestamp64), "Not a valid timezone type");

    // Validate min value.
    auto minDate32 = MakeTzString<i32>(Date32LowerBound, GetTzName(1));
    EXPECT_NO_THROW(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDate32>(minDate32));
    auto minDatetime64 = MakeTzString<i64>(Datetime64LowerBound, GetTzName(1));
    EXPECT_NO_THROW(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDatetime64>(minDatetime64));
    auto minTimestamp64 = MakeTzString<i64>(Timestamp64LowerBound, GetTzName(1));
    EXPECT_NO_THROW(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzTimestamp64>(minTimestamp64));

    // Validate too small value.
    auto tooSmallDate32 = MakeTzString<i32>(Date32LowerBound - 1, GetTzName(1));
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDate32>(tooSmallDate32), "Not a valid timezone type");
    auto tooSmallDatetime64 = MakeTzString<i64>(Datetime64LowerBound - 1, GetTzName(1));
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzDatetime64>(tooSmallDatetime64), "Not a valid timezone type");
    auto tooSmallTimestamp64 = MakeTzString<i64>(Timestamp64LowerBound - 1, GetTzName(1));
    EXPECT_THROW_WITH_SUBSTRING(ValidateSimpleLogicalType<ESimpleLogicalValueType::TzTimestamp64>(tooSmallTimestamp64), "Not a valid timezone type");
}

TEST(TValidateLogicalTypeTest, TestJsonType)
{
    EXPECT_GOOD_TYPE(Json(), R"( "\"foo\"" )");
    EXPECT_GOOD_TYPE(Json(), R"( "42" )");
    EXPECT_GOOD_TYPE(Json(), R"( "true" )");
    EXPECT_GOOD_TYPE(Json(), R"( "3.14" )");
    EXPECT_GOOD_TYPE(Json(), R"( "null" )");

    // Infinities are disallowed.
    EXPECT_BAD_TYPE(Json(), R"( "Infinity" )");

    EXPECT_GOOD_TYPE(Json(), R"( "[142, 53, {\"foo\":\"bar\", \"bar\":[\"baz\"]}]" )");
    // Extra comma.
    EXPECT_BAD_TYPE(Json(), R"( "[142, 53,]" )");
    // Wrong delimiter.
    EXPECT_BAD_TYPE(Json(), R"( "[142; 53]" )");

    EXPECT_GOOD_TYPE(Optional(Json()), "#");
    EXPECT_GOOD_TYPE(Optional(Json()), R"( "[{\"bar\": []}, \"bar\", [\"baz\"]]" )");

    // Non-UTF-8.
    EXPECT_BAD_TYPE(Json(), R"( "\xFF" )");
}

TEST(TValidateLogicalTypeTest, TestDecimalType)
{
    EXPECT_GOOD_TYPE(Decimal(3, 2), R"("\x80\x00\x01\x3a")"); // 314
    EXPECT_BAD_TYPE(Decimal(3, 2), R"("\x80\x00\x11\x3a")");
    EXPECT_BAD_TYPE(Decimal(3, 2), R"("\x80\x11\x3a")");
    EXPECT_BAD_TYPE(Decimal(3, 2), R"("")");
    EXPECT_BAD_TYPE(Decimal(3, 2), R"(3.14)");
    EXPECT_BAD_TYPE(Decimal(3, 2), R"(3)");
    EXPECT_BAD_TYPE(Decimal(3, 2), R"(#)");

    EXPECT_GOOD_TYPE(Decimal(15, 0), R"("\x80\x03\x8d\x7e\xa4\xc6\x7f\xff")"); // 10 ** 15
    EXPECT_BAD_TYPE(Decimal(15, 0), R"("\x80\x03\x8d\x7e\xa4\xc6\x80\x00")");
    EXPECT_BAD_TYPE(Decimal(15, 0), R"("\x80\x00\x01\x3a")");
    EXPECT_BAD_TYPE(Decimal(15, 0), R"("")");

    EXPECT_GOOD_TYPE(Decimal(35, 0), R"("\x80\x13\x42\x61\x72\xc7\x4d\x82\x2b\x87\x8f\xe7\xff\xff\xff\xff")"); // 10 ** 35
    EXPECT_BAD_TYPE(Decimal(35, 0), R"("\x80\x13\x42\x61\x72\xc7\x4d\x82\x2b\x87\x8f\xe8\x00\x00\x00\x00")");
    EXPECT_BAD_TYPE(Decimal(35, 0), R"("\x80\x03\x8d\x7e\xa4\xc6\x7f\xff")");
    EXPECT_BAD_TYPE(Decimal(35, 0), R"("\x80\x00\x01\x3a")");
    EXPECT_BAD_TYPE(Decimal(35, 0), R"("")");
}

TEST(TValidateLogicalTypeTest, TestOptionalCompositeType)
{
    const auto optionalOptionalInt = Optional(Optional(Int64()));
    EXPECT_GOOD_TYPE(optionalOptionalInt, " [5] ");
    EXPECT_GOOD_TYPE(optionalOptionalInt, " [#] ");
    EXPECT_GOOD_TYPE(optionalOptionalInt, " # ");
    EXPECT_BAD_TYPE(optionalOptionalInt, " 5 ");
    EXPECT_BAD_TYPE(optionalOptionalInt, " [] ");
    EXPECT_BAD_TYPE(optionalOptionalInt, " [5; 5] ");

    const auto optionalListInt = Optional(List(Int64()));
    EXPECT_GOOD_TYPE(optionalListInt, "[5]");
    EXPECT_GOOD_TYPE(optionalListInt, "[5; 5]");
    EXPECT_GOOD_TYPE(optionalListInt, "[]");
    EXPECT_BAD_TYPE(optionalListInt, "[[5]]");

    const auto optionalOptionalListInt = Optional(Optional(List(Int64())));
    EXPECT_GOOD_TYPE(optionalOptionalListInt, "[[5]]");
    EXPECT_GOOD_TYPE(optionalOptionalListInt, "[[5; 5]]");
    EXPECT_GOOD_TYPE(optionalOptionalListInt, "[[]]");
    EXPECT_BAD_TYPE(optionalOptionalListInt, "[[[5]]]");
    EXPECT_BAD_TYPE(optionalOptionalListInt, "[5]");

    const auto optionalOptionalOptionalAny = Optional(Optional(Optional(Yson())));
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " [[5]] ");
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " [[#]] ");
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " [#] ");
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " # ");
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " [[[]]] ");
    EXPECT_GOOD_TYPE(optionalOptionalOptionalAny, " [[{foo=bar}]] ");
    EXPECT_BAD_TYPE(optionalOptionalOptionalAny, " [] ");
    EXPECT_BAD_TYPE(optionalOptionalOptionalAny, " [[]] ");
}

TEST(TValidateLogicalTypeTest, TestListType)
{
    const auto listInt = List(Int64());
    EXPECT_GOOD_TYPE(listInt, " [3] ");
    EXPECT_GOOD_TYPE(listInt, " [5] ");
    EXPECT_GOOD_TYPE(listInt, " [5;42;] ");

    EXPECT_BAD_TYPE(listInt, " [5;#;] ");
    EXPECT_BAD_TYPE(listInt, " {} ");
}

TEST(TValidateLogicalTypeTest, TestStructType)
{
    const auto struct1 = StructLogicalType({
        {"number", "number", Int64()},
        {"english", "english", String()},
        {"russian", "russian", Optional(String())},
    }, /*removedFieldStableNames*/ {});

    EXPECT_GOOD_TYPE(struct1, " [3; three; TRI ] ");
    EXPECT_GOOD_TYPE(struct1, " [1; one; # ] ");
    EXPECT_GOOD_TYPE(struct1, " [1; one ] ");

    EXPECT_BAD_TYPE(struct1, " [ # ; three; TRI ] ");
    EXPECT_BAD_TYPE(struct1, " [ 3 ; # ; TRI ] ");
    EXPECT_BAD_TYPE(struct1, " [ 1 ] ");
    EXPECT_BAD_TYPE(struct1, " [ ] ");

    const auto struct2 = StructLogicalType({
        {"key", "key", Optional(String())},
        {"subkey", "subkey", Optional(String())},
        {"value", "value", Optional(String())},
    }, /*removedFieldStableNames*/ {});

    EXPECT_GOOD_TYPE(struct2, " [k ; s ; v ] ");
    EXPECT_GOOD_TYPE(struct2, " [# ; # ; #] ");
    EXPECT_GOOD_TYPE(struct2, " [# ; # ;] ");
    EXPECT_GOOD_TYPE(struct2, " [# ; ] ");
    EXPECT_GOOD_TYPE(struct2, " [ ] ");
    EXPECT_BAD_TYPE(struct2, " [ 2 ] ");
}

TEST(TValidateLogicalTypeTest, TestTupleType)
{
    const auto tuple1 = Tuple(Int64(), String(), Optional(String()));

    EXPECT_GOOD_TYPE(tuple1, " [3; three; TRI ] ");
    EXPECT_GOOD_TYPE(tuple1, " [1; one; # ] ");

    EXPECT_BAD_TYPE(tuple1, " [3u; three; TRI ] ");
    EXPECT_BAD_TYPE(tuple1, " [1; one ] ");
    EXPECT_BAD_TYPE(tuple1, " [ # ; three; TRI ] ");
    EXPECT_BAD_TYPE(tuple1, " [ 3 ; # ; TRI ] ");
    EXPECT_BAD_TYPE(tuple1, " [ 1 ] ");
    EXPECT_BAD_TYPE(tuple1, " [ ] ");

    const auto tuple2 = Tuple(Optional(String()), Optional(String()), Optional(String()));

    EXPECT_GOOD_TYPE(tuple2, " [k ; s ; v ] ");
    EXPECT_GOOD_TYPE(tuple2, " [# ; # ; #] ");

    EXPECT_BAD_TYPE(tuple2, " [# ; # ;] ");
    EXPECT_BAD_TYPE(tuple2, " [# ; ] ");
    EXPECT_BAD_TYPE(tuple2, " [ ] ");
    EXPECT_BAD_TYPE(tuple2, " [ 2 ] ");
}

void TestVariantImpl(ELogicalMetatype metatype)
{
    auto createVariantType = [&] (std::vector<TStructField> fields) {
        if (metatype == ELogicalMetatype::VariantStruct) {
            return VariantStructLogicalType(std::move(fields));
        } else {
            YT_VERIFY(metatype == ELogicalMetatype::VariantTuple);
            std::vector<TLogicalTypePtr> elements;
            for (const auto& f : fields) {
                elements.push_back(f.Type);
            }
            return VariantTuple(std::move(elements));
        }
    };

    auto variant1 = createVariantType({
        {"field1", "field1", Bool()},
        {"field2", "field2", String()},
    });

    EXPECT_GOOD_TYPE(variant1, "[0; %true]");
    EXPECT_GOOD_TYPE(variant1, "[1; \"false\"]");

    EXPECT_BAD_TYPE(variant1, "[0; \"false\"]");
    EXPECT_BAD_TYPE(variant1, "[0; %true; %true]");
    EXPECT_BAD_TYPE(variant1, "[1; %false]");
    EXPECT_BAD_TYPE(variant1, "[]");
    EXPECT_BAD_TYPE(variant1, "[0]");
    EXPECT_BAD_TYPE(variant1, "[1]");
    EXPECT_BAD_TYPE(variant1, "[-1; %true]");
    EXPECT_BAD_TYPE(variant1, "[-1; \"true\"]");
    EXPECT_BAD_TYPE(variant1, "[2; \"false\"]");
}

TEST(TValidateLogicalTypeTest, TestVariantStructType)
{
    TestVariantImpl(ELogicalMetatype::VariantStruct);
}

TEST(TValidateLogicalTypeTest, TestVariantTupleType)
{
    TestVariantImpl(ELogicalMetatype::VariantTuple);
}

TEST(TValidateLogicalTypeTest, TestDictType)
{
    const auto stringToInt = Dict(String(), Int64());

    EXPECT_GOOD_TYPE(stringToInt, "[]");
    EXPECT_GOOD_TYPE(stringToInt, "[[\"foo\"; 0]]");
    EXPECT_GOOD_TYPE(stringToInt, "[[foo; 0;]]");

    EXPECT_BAD_TYPE(stringToInt, " # ");
    EXPECT_BAD_TYPE(stringToInt, " foo ");
    EXPECT_BAD_TYPE(stringToInt, "[foo; 0]");
    EXPECT_BAD_TYPE(stringToInt, "[[foo; 0; 0]]");
    EXPECT_BAD_TYPE(stringToInt, "[[foo;]]");
    EXPECT_BAD_TYPE(stringToInt, "{foo=0;}"); // <- This is not a dict!
}

TEST(TValidateLogicalTypeTest, TestTaggedType)
{
    const auto taggedDict = Tagged("tag", Dict(String(), Int64()));

    EXPECT_GOOD_TYPE(taggedDict, "[[\"foo\"; 0]]");
    EXPECT_BAD_TYPE(taggedDict, "[[foo; 0; 0]]");

    auto taggedVariant = Tagged("tag", VariantTuple(Bool(), String()));

    EXPECT_GOOD_TYPE(taggedVariant, "[0; %true]");
    EXPECT_BAD_TYPE(taggedVariant, "[0; \"false\"]");

    const auto taggedStruct = Tagged(
        "tag",
        StructLogicalType({
            {"number", "number",  Int64()},
            {"english", "english", String()},
            {"russian", "russian", Optional(String())},
        }, /*removedFieldStableNames*/ {}));

    EXPECT_GOOD_TYPE(taggedStruct, " [3; three; TRI ] ");
    EXPECT_GOOD_TYPE(taggedStruct, " [1; one; # ] ");
    EXPECT_GOOD_TYPE(taggedStruct, " [1; one ] ");

    EXPECT_BAD_TYPE(taggedStruct, " [ # ; three; TRI ] ");
    EXPECT_BAD_TYPE(taggedStruct, " [ 3 ; # ; TRI ] ");
    EXPECT_BAD_TYPE(taggedStruct, " [ 1 ] ");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
