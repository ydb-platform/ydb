#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/validate_logical_type.h>

#include <util/string/escape.h>


namespace NYT::NTableClient {

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

// TODO (ermolovd): we should use functions from NLogicalTypeShortcuts here

TEST(TValidateLogicalTypeTest, TestBasicTypes)
{
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::String), " foo ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Int64), " 42 ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Uint64), " 42u ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Double), " 3.14 ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Float), " 3.14 ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Boolean), " %false ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Null), " # ");

    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::String), " 76 ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Int64), " %true ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Uint64), " 14 ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Double), " bar ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Float), " rab ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Boolean), " 1 ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Null), " 0 ");
}

TEST(TValidateLogicalTypeTest, TestSimpleOptionalTypes)
{
    EXPECT_GOOD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)), " foo ");
    EXPECT_GOOD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)), " # ");
    EXPECT_BAD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)), " 42 ");

    EXPECT_GOOD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64)), " 42u ");
    EXPECT_GOOD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64)), " # ");
    EXPECT_BAD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64)), " 42 ");
}

TEST(TValidateLogicalTypeTest, TestOptionalNull)
{
    EXPECT_GOOD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null)), " # ");
    EXPECT_GOOD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null)), " [#] ");

    EXPECT_BAD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null)), " [] ");
    EXPECT_BAD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null)), " [[#]] ");

    EXPECT_GOOD_TYPE(OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null))), " [[#]] ");
    EXPECT_GOOD_TYPE(OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null))), " [#] ");
    EXPECT_GOOD_TYPE(OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null))), " # ");
    EXPECT_BAD_TYPE(OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null))), " [[[#]]] ");
    EXPECT_BAD_TYPE(OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null))), " [[[]]] ");
}

TEST(TValidateLogicalTypeTest, TestLogicalTypes)
{
#define CHECK_GOOD(type, value) \
    do { \
        EXPECT_GOOD_TYPE(SimpleLogicalType(type), value); \
    } while (0)

#define CHECK_BAD(type, value) \
    do { \
        EXPECT_BAD_TYPE(SimpleLogicalType(type), value); \
    } while (0)

    // Int8
    CHECK_GOOD(ESimpleLogicalValueType::Int8, " 127 ");
    CHECK_GOOD(ESimpleLogicalValueType::Int8, " -128 ");
    CHECK_BAD(ESimpleLogicalValueType::Int8, " 128 ");
    CHECK_BAD(ESimpleLogicalValueType::Int8, " -129 ");

    // Uint8
    CHECK_GOOD(ESimpleLogicalValueType::Uint8, " 127u ");
    CHECK_GOOD(ESimpleLogicalValueType::Uint8, " 128u ");
    CHECK_GOOD(ESimpleLogicalValueType::Uint8, " 255u ");
    CHECK_BAD(ESimpleLogicalValueType::Uint8, " 256u ");
    CHECK_BAD(ESimpleLogicalValueType::Uint8, " 100500u ");

    // Int16
    CHECK_GOOD(ESimpleLogicalValueType::Int16, " 32767 ");
    CHECK_GOOD(ESimpleLogicalValueType::Int16, " -32768 ");
    CHECK_BAD(ESimpleLogicalValueType::Int16, " 32768 ");
    CHECK_BAD(ESimpleLogicalValueType::Int16, " -32769 ");

    // Uint16
    CHECK_GOOD(ESimpleLogicalValueType::Uint16, " 32768u ");
    CHECK_GOOD(ESimpleLogicalValueType::Uint16, " 65535u ");
    CHECK_BAD(ESimpleLogicalValueType::Uint16, " 65536u ");

    // Int32
    CHECK_GOOD(ESimpleLogicalValueType::Int32, " 2147483647 ");
    CHECK_GOOD(ESimpleLogicalValueType::Int32, " -2147483648 ");
    CHECK_BAD(ESimpleLogicalValueType::Int32, " 2147483648 ");
    CHECK_BAD(ESimpleLogicalValueType::Int32, " -2147483649 ");

    // Uint32
    CHECK_GOOD(ESimpleLogicalValueType::Uint32, " 2147483648u ");
    CHECK_GOOD(ESimpleLogicalValueType::Uint32, " 4294967295u ");
    CHECK_BAD(ESimpleLogicalValueType::Uint32, " 4294967297u ");

    // Uint64
    CHECK_GOOD(ESimpleLogicalValueType::Int64, " 2147483648 ");
    CHECK_GOOD(ESimpleLogicalValueType::Int64, " -2147483649 ");

    // Uint64
    CHECK_GOOD(ESimpleLogicalValueType::Uint64, " 4294967297u ");

    // Utf8
    CHECK_GOOD(ESimpleLogicalValueType::Utf8, " foo ");
    CHECK_GOOD(ESimpleLogicalValueType::Utf8, " \"фу\" ");
    CHECK_BAD(ESimpleLogicalValueType::Utf8, " \"\244\" ");

    // Float
    CHECK_GOOD(ESimpleLogicalValueType::Float, " 3.14 ");
    CHECK_GOOD(ESimpleLogicalValueType::Float, " -3.14e35 ");
    CHECK_GOOD(ESimpleLogicalValueType::Float, " 3.14e35 ");
    CHECK_BAD(ESimpleLogicalValueType::Float, " 3.14e39 ");
    CHECK_BAD(ESimpleLogicalValueType::Float, " -3.14e39 ");
    CHECK_BAD(ESimpleLogicalValueType::Float, " blah");
}

TEST(TValidateLogicalTypeTest, TestAnyType)
{
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), " foo ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), " 42 ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), " 15u ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), " %true ");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), " 3.14 ");
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), "#");

    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), "[142; 53u; {foo=bar; bar=[baz]};]");

    EXPECT_GOOD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any)), "#");
    EXPECT_GOOD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any)), "[{bar=<type=list>[]}; bar; [baz];]");

    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), "<>1");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Any), "[<>1]");
}


TEST(TValidateLogicalTypeTest, TestJsonType)
{
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Json), R"( "\"foo\"" )");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Json), R"( "42" )");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Json), R"( "true" )");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Json), R"( "3.14" )");
    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Json), R"( "null" )");

    // Infinities are disallowed.
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Json), R"( "Infinity" )");

    EXPECT_GOOD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Json), R"( "[142, 53, {\"foo\":\"bar\", \"bar\":[\"baz\"]}]" )");
    // Extra comma.
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Json), R"( "[142, 53,]" )");
    // Wrong delimiter.
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Json), R"( "[142; 53]" )");

    EXPECT_GOOD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Json)), "#");
    EXPECT_GOOD_TYPE(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Json)), R"( "[{\"bar\": []}, \"bar\", [\"baz\"]]" )");

    // Non-UTF-8.
    EXPECT_BAD_TYPE(SimpleLogicalType(ESimpleLogicalValueType::Json), R"( "\xFF" )");
}

TEST(TValidateLogicalTypeTest, TestDecimalType)
{
    using namespace NLogicalTypeShortcuts;

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
    const auto optionalOptionalInt = OptionalLogicalType(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)));
    EXPECT_GOOD_TYPE(optionalOptionalInt, " [5] ");
    EXPECT_GOOD_TYPE(optionalOptionalInt, " [#] ");
    EXPECT_GOOD_TYPE(optionalOptionalInt, " # ");
    EXPECT_BAD_TYPE(optionalOptionalInt, " 5 ");
    EXPECT_BAD_TYPE(optionalOptionalInt, " [] ");
    EXPECT_BAD_TYPE(optionalOptionalInt, " [5; 5] ");

    const auto optionalListInt = OptionalLogicalType(
        ListLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Int64)));
    EXPECT_GOOD_TYPE(optionalListInt, "[5]");
    EXPECT_GOOD_TYPE(optionalListInt, "[5; 5]");
    EXPECT_GOOD_TYPE(optionalListInt, "[]");
    EXPECT_BAD_TYPE(optionalListInt, "[[5]]");

    const auto optionalOptionalListInt = OptionalLogicalType(
        OptionalLogicalType(
            ListLogicalType(
                SimpleLogicalType(ESimpleLogicalValueType::Int64))));
    EXPECT_GOOD_TYPE(optionalOptionalListInt, "[[5]]");
    EXPECT_GOOD_TYPE(optionalOptionalListInt, "[[5; 5]]");
    EXPECT_GOOD_TYPE(optionalOptionalListInt, "[[]]");
    EXPECT_BAD_TYPE(optionalOptionalListInt, "[[[5]]]");
    EXPECT_BAD_TYPE(optionalOptionalListInt, "[5]");

    const auto optionalOptionalOptionalAny = OptionalLogicalType(
        OptionalLogicalType(
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any))));
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
    const auto listInt = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64));
    EXPECT_GOOD_TYPE(listInt, " [3] ");
    EXPECT_GOOD_TYPE(listInt, " [5] ");
    EXPECT_GOOD_TYPE(listInt, " [5;42;] ");

    EXPECT_BAD_TYPE(listInt, " [5;#;] ");
    EXPECT_BAD_TYPE(listInt, " {} ");
}

TEST(TValidateLogicalTypeTest, TestStructType)
{
    const auto struct1 = StructLogicalType({
        {"number",  SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"english", SimpleLogicalType(ESimpleLogicalValueType::String)},
        {"russian", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
    });

    EXPECT_GOOD_TYPE(struct1, " [3; three; TRI ] ");
    EXPECT_GOOD_TYPE(struct1, " [1; one; # ] ");
    EXPECT_GOOD_TYPE(struct1, " [1; one ] ");

    EXPECT_BAD_TYPE(struct1, " [ # ; three; TRI ] ");
    EXPECT_BAD_TYPE(struct1, " [ 3 ; # ; TRI ] ");
    EXPECT_BAD_TYPE(struct1, " [ 1 ] ");
    EXPECT_BAD_TYPE(struct1, " [ ] ");

    const auto struct2 = StructLogicalType({
        {"key",  OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
        {"subkey", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
        {"value", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
    });

    EXPECT_GOOD_TYPE(struct2, " [k ; s ; v ] ");
    EXPECT_GOOD_TYPE(struct2, " [# ; # ; #] ");
    EXPECT_GOOD_TYPE(struct2, " [# ; # ;] ");
    EXPECT_GOOD_TYPE(struct2, " [# ; ] ");
    EXPECT_GOOD_TYPE(struct2, " [ ] ");
    EXPECT_BAD_TYPE(struct2, " [ 2 ] ");
}

TEST(TValidateLogicalTypeTest, TestTupleType)
{
    const auto tuple1 = TupleLogicalType({
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
        SimpleLogicalType(ESimpleLogicalValueType::String),
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)),
    });

    EXPECT_GOOD_TYPE(tuple1, " [3; three; TRI ] ");
    EXPECT_GOOD_TYPE(tuple1, " [1; one; # ] ");

    EXPECT_BAD_TYPE(tuple1, " [3u; three; TRI ] ");
    EXPECT_BAD_TYPE(tuple1, " [1; one ] ");
    EXPECT_BAD_TYPE(tuple1, " [ # ; three; TRI ] ");
    EXPECT_BAD_TYPE(tuple1, " [ 3 ; # ; TRI ] ");
    EXPECT_BAD_TYPE(tuple1, " [ 1 ] ");
    EXPECT_BAD_TYPE(tuple1, " [ ] ");

    const auto tuple2 = TupleLogicalType({
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)),
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)),
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)),
    });

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
            return VariantTupleLogicalType(std::move(elements));
        }
    };

    auto variant1 = createVariantType({
        {"field1", SimpleLogicalType(ESimpleLogicalValueType::Boolean)},
        {"field2", SimpleLogicalType(ESimpleLogicalValueType::String)},
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
    const auto stringToInt = DictLogicalType(
        SimpleLogicalType(ESimpleLogicalValueType::String),
        SimpleLogicalType(ESimpleLogicalValueType::Int64));

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
    const auto taggedDict = TaggedLogicalType(
        "tag",
        DictLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::String),
            SimpleLogicalType(ESimpleLogicalValueType::Int64)));

    EXPECT_GOOD_TYPE(taggedDict, "[[\"foo\"; 0]]");
    EXPECT_BAD_TYPE(taggedDict, "[[foo; 0; 0]]");

    auto taggedVariant = TaggedLogicalType(
        "tag",
        VariantTupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Boolean),
            SimpleLogicalType(ESimpleLogicalValueType::String),
        }));

    EXPECT_GOOD_TYPE(taggedVariant, "[0; %true]");
    EXPECT_BAD_TYPE(taggedVariant, "[0; \"false\"]");

    const auto taggedStruct = TaggedLogicalType(
        "tag",
        StructLogicalType({
            {"number",  SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"english", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"russian", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
        }));

    EXPECT_GOOD_TYPE(taggedStruct, " [3; three; TRI ] ");
    EXPECT_GOOD_TYPE(taggedStruct, " [1; one; # ] ");
    EXPECT_GOOD_TYPE(taggedStruct, " [1; one ] ");

    EXPECT_BAD_TYPE(taggedStruct, " [ # ; three; TRI ] ");
    EXPECT_BAD_TYPE(taggedStruct, " [ 3 ; # ; TRI ] ");
    EXPECT_BAD_TYPE(taggedStruct, " [ 1 ] ");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
