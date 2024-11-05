#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/complex_types/yson_format_conversion.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/parser.h>

#include <library/cpp/yt/misc/variant.h>
#include <library/cpp/yt/misc/tls.h>

#include <util/stream/mem.h>

namespace NYT::NComplexTypes {

using NFormats::EComplexTypeMode;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

const auto KeyValueStruct = StructLogicalType({
    {"key", SimpleLogicalType(ESimpleLogicalValueType::String)},
    {"value", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
});

const auto IntStringVariant = VariantStructLogicalType({
    {"int", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
    {"string", SimpleLogicalType(ESimpleLogicalValueType::String)},
});

YT_DEFINE_THREAD_LOCAL(TYsonConverterConfig, PositionalToNamedConfigInstance);

class TWithConfig
{
public:
    TWithConfig(const TYsonConverterConfig& config)
        : OldConfig_(PositionalToNamedConfigInstance())
    {
        PositionalToNamedConfigInstance() = config;
    }

    ~TWithConfig()
    {
        PositionalToNamedConfigInstance() = OldConfig_;
    }
private:
    TYsonConverterConfig OldConfig_;
};

TString CanonizeYson(TStringBuf yson)
{
    TString result;
    {
        TStringOutput out(result);
        TYsonWriter writer(&out, EYsonFormat::Pretty, EYsonType::Node);
        ParseYsonStringBuffer(yson, EYsonType::Node, &writer);
    }
    return result;
}

TString ConvertYson(
    bool namedToPositional,
    const TLogicalTypePtr& type,
    TStringBuf sourceYson)
{
    TComplexTypeFieldDescriptor descriptor("<test-field>", type);
    std::variant<TYsonServerToClientConverter, TYsonClientToServerConverter> converter;
    try {
        if (namedToPositional) {
            TYsonConverterConfig config{
                .ComplexTypeMode = EComplexTypeMode::Named,
            };
            converter = CreateYsonClientToServerConverter(descriptor, config);
        } else {
            converter = CreateYsonServerToClientConverter(descriptor, PositionalToNamedConfigInstance());
        }
    } catch (const std::exception& ex) {
        ADD_FAILURE() << "cannot create converter: " << ex.what();
        return "";
    }
    TString convertedYson;
    Visit(
        converter,
        [&] (const TYsonServerToClientConverter& serverToClientConverter) {
            if (serverToClientConverter) {
                TStringOutput out(convertedYson);
                TYsonWriter writer(&out, EYsonFormat::Pretty);
                serverToClientConverter(MakeUnversionedStringValue(sourceYson), &writer);
            } else {
                convertedYson = CanonizeYson(sourceYson);
            }
        },
        [&] (const TYsonClientToServerConverter& clientToServerConverter) {
            if (clientToServerConverter) {
                auto value = clientToServerConverter(MakeUnversionedStringValue(sourceYson));
                convertedYson = value.AsStringBuf();
            } else {
                convertedYson = CanonizeYson(sourceYson);
            }
        });
    return convertedYson;
}

void CheckYsonConversion(
    bool namedToPositional,
    const TLogicalTypePtr& type,
    TStringBuf sourceYson,
    TStringBuf expectedConvertedYson)
{
    TString convertedYson;
    try {
        convertedYson = ConvertYson(namedToPositional, type, sourceYson);
    } catch (const std::exception& ex) {
        ADD_FAILURE() << "conversion error: " << ex.what();
        return;
    }

    EXPECT_EQ(CanonizeYson(convertedYson), CanonizeYson(expectedConvertedYson));
}

#define CHECK_POSITIONAL_TO_NAMED(type, positionalYson, namedYson) \
    do { \
        SCOPED_TRACE("positional -> named error"); \
        CheckYsonConversion(false, type, positionalYson, namedYson); \
    } while (0)

#define CHECK_NAMED_TO_POSITIONAL(type, namedYson, positionalYson) \
    do { \
        SCOPED_TRACE("named -> positional error"); \
        CheckYsonConversion(true, type, namedYson, positionalYson); \
    } while (0)

#define CHECK_NAMED_TO_POSITIONAL_THROWS(type, namedYson, exceptionSubstring) \
    do { \
        TString tmp; \
        EXPECT_THROW_WITH_SUBSTRING(ConvertYson(true, type, namedYson), exceptionSubstring); \
    } while (0)

#define CHECK_POSITIONAL_TO_NAMED_THROWS(type, namedYson, exceptionSubstring) \
    do { \
        TString tmp; \
        EXPECT_THROW_WITH_SUBSTRING(ConvertYson(false, type, namedYson), exceptionSubstring); \
    } while (0)

#define CHECK_BIDIRECTIONAL(type, positionalYson, namedYson) \
    do { \
        CHECK_POSITIONAL_TO_NAMED(type, positionalYson, namedYson); \
        CHECK_NAMED_TO_POSITIONAL(type, namedYson, positionalYson); \
    } while (0)

TEST(TNamedPositionalYsonConverter, TestSimpleTypes)
{
    CHECK_BIDIRECTIONAL(
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
        "-42",
        "-42");

    CHECK_BIDIRECTIONAL(
        SimpleLogicalType(ESimpleLogicalValueType::String),
        "foo",
        "foo");
}

TEST(TNamedPositionalYsonConverter, TestStruct)
{
    CHECK_BIDIRECTIONAL(KeyValueStruct, "[foo; bar]", "{key=foo; value=bar}");
    CHECK_BIDIRECTIONAL(KeyValueStruct, "[qux; #]", "{key=qux; value=#}");

    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[baz]", "{key=baz; value=#}");
    CHECK_NAMED_TO_POSITIONAL(KeyValueStruct, "{key=baz}", "[baz; #]");

    CHECK_NAMED_TO_POSITIONAL_THROWS(KeyValueStruct, "{}", "is missing while parsing");
    CHECK_NAMED_TO_POSITIONAL_THROWS(KeyValueStruct, "{value=baz}", "is missing while parsing");
}

TEST(TNamedPositionalYsonConverter, TestStructSkipNullValues)
{
    TYsonConverterConfig config{
        .SkipNullValues = true,
    };
    TWithConfig g(config);

    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[foo; bar]", "{key=foo; value=bar}");
    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[foo; #]", "{key=foo}");
    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[foo]", "{key=foo}");

    auto type2 = StructLogicalType({
        {"opt_int", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"opt_opt_int", OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))},
        {"list_null", ListLogicalType(NullLogicalType())},
        {"null", NullLogicalType()},
    });
    CHECK_POSITIONAL_TO_NAMED(type2, "[42; [#]; []; #]", "{opt_int=42; opt_opt_int=[#]; list_null=[]}");
    CHECK_POSITIONAL_TO_NAMED(type2, "[#; #; [#; #;]]", "{list_null=[#; #;]}");
}

TEST(TNamedPositionalYsonConverter, TestVariantStruct)
{
    CHECK_BIDIRECTIONAL(IntStringVariant, "[0; 42]", "[int; 42]");
    CHECK_BIDIRECTIONAL(IntStringVariant, "[1; foo]", "[string; foo]");

    CHECK_NAMED_TO_POSITIONAL_THROWS(IntStringVariant, "[str; foo]", "Unknown variant field");
}

TEST(TNamedPositionalYsonConverter, TestOptional)
{
    CHECK_BIDIRECTIONAL(
        OptionalLogicalType(KeyValueStruct),
        "[foo; bar]",
        "{key=foo; value=bar}");

    CHECK_BIDIRECTIONAL(
        OptionalLogicalType(KeyValueStruct),
        "#",
        "#");

    CHECK_BIDIRECTIONAL(
        OptionalLogicalType(OptionalLogicalType(KeyValueStruct)),
        "[[foo; bar]]",
        "[{key=foo; value=bar}]");

    CHECK_BIDIRECTIONAL(
        OptionalLogicalType(OptionalLogicalType(KeyValueStruct)),
        "#",
        "#");

    CHECK_BIDIRECTIONAL(
        OptionalLogicalType(OptionalLogicalType(KeyValueStruct)),
        "[#]",
        "[#]");
}

TEST(TNamedPositionalYsonConverter, TestList)
{
    CHECK_BIDIRECTIONAL(
        ListLogicalType(KeyValueStruct),
        "[[foo; bar]; [qux; #]]",
        "[{key=foo; value=bar}; {key=qux; value=#};]");
}

TEST(TNamedPositionalYsonConverter, TestTuple)
{
    CHECK_BIDIRECTIONAL(
        TupleLogicalType({KeyValueStruct, IntStringVariant, SimpleLogicalType(ESimpleLogicalValueType::Utf8)}),
        "[[foo; bar]; [0; 5]; foo]",
        "[{key=foo; value=bar}; [int; 5]; foo;]");
}

TEST(TNamedPositionalYsonConverter, TestVariantTuple)
{
    auto type = VariantTupleLogicalType({KeyValueStruct, IntStringVariant, SimpleLogicalType(ESimpleLogicalValueType::Utf8)});
    CHECK_BIDIRECTIONAL(
        type,
        "[0; [foo; #]]",
        "[0; {key=foo; value=#}]");

    CHECK_BIDIRECTIONAL(
        type,
        "[1; [1; bar]]",
        "[1; [string; bar]]");

    CHECK_BIDIRECTIONAL(
        type,
        "[2; qux]",
        "[2; qux]");
}

TEST(TNamedPositionalYsonConverter, TestDict)
{
    CHECK_BIDIRECTIONAL(
        DictLogicalType(KeyValueStruct, IntStringVariant),
        "[ [[foo; #]; [0; 0]] ; [[bar; qux;]; [1; baz;]]; ]",
        "[ [{key=foo; value=#}; [int; 0]] ; [{key=bar; value=qux;}; [string; baz;]] ]");
}

TEST(TNamedPositionalYsonConverter, TestStringDictAsYsonMap)
{
    TYsonConverterConfig config{
        .StringKeyedDictMode = NFormats::EDictMode::Named,
    };
    TWithConfig g(config);

    CHECK_POSITIONAL_TO_NAMED(
        DictLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::String),
            SimpleLogicalType(ESimpleLogicalValueType::Int64)),
        "[[key1; 1]; [key2; 2]]", "{key1=1; key2=2}");
}

TEST(TNamedPositionalYsonConverter, TestTagged)
{
    CHECK_BIDIRECTIONAL(
        TaggedLogicalType("foo", KeyValueStruct),
        "[foo; bar]",
        "{key=foo; value=bar}");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
