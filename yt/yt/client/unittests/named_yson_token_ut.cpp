#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/complex_types/yson_format_conversion.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/parser.h>

#include <library/cpp/yt/misc/variant.h>
#include <library/cpp/yt/misc/tls.h>

#include <util/stream/mem.h>

namespace NYT::NComplexTypes {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient::NLogicalTypeShortcuts;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

using NFormats::EComplexTypeMode;

////////////////////////////////////////////////////////////////////////////////

const auto KeyValueStruct = StructLogicalType({
    {"key", "key", String()},
    {"value", "value", Optional(String())},
}, /*removedFieldStableNames*/ {});

const auto IntStringVariant = VariantStructLogicalType({
    {"int", "int", Int64()},
    {"string", "string", String()},
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

std::string ConvertYson(
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
    std::string convertedYson;
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
        std::string tmp; \
        EXPECT_THROW_WITH_SUBSTRING(ConvertYson(true, type, namedYson), exceptionSubstring); \
    } while (0)

#define CHECK_POSITIONAL_TO_NAMED_THROWS(type, namedYson, exceptionSubstring) \
    do { \
        std::string tmp; \
        EXPECT_THROW_WITH_SUBSTRING(ConvertYson(false, type, namedYson), exceptionSubstring); \
    } while (0)

#define CHECK_BIDIRECTIONAL(type, positionalYson, namedYson) \
    do { \
        CHECK_POSITIONAL_TO_NAMED(type, positionalYson, namedYson); \
        CHECK_NAMED_TO_POSITIONAL(type, namedYson, positionalYson); \
    } while (0)

TEST(TNamedPositionalYsonConverterTest, TestSimpleTypes)
{
    CHECK_BIDIRECTIONAL(Int64(), "-42", "-42");
    CHECK_BIDIRECTIONAL(String(), "foo", "foo");
}

TEST(TNamedPositionalYsonConverterTest, TestStruct)
{
    CHECK_BIDIRECTIONAL(KeyValueStruct, "[foo; bar]", "{key=foo; value=bar}");
    CHECK_BIDIRECTIONAL(KeyValueStruct, "[qux; #]", "{key=qux; value=#}");

    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[baz]", "{key=baz; value=#}");
    CHECK_NAMED_TO_POSITIONAL(KeyValueStruct, "{key=baz}", "[baz; #]");

    CHECK_NAMED_TO_POSITIONAL_THROWS(KeyValueStruct, "{}", "is missing while parsing");
    CHECK_NAMED_TO_POSITIONAL_THROWS(KeyValueStruct, "{value=baz}", "is missing while parsing");
}

TEST(TNamedPositionalYsonConverterTest, TestStructSkipNullValues)
{
    TYsonConverterConfig config{
        .SkipNullValues = true,
    };
    TWithConfig g(config);

    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[foo; bar]", "{key=foo; value=bar}");
    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[foo; #]", "{key=foo}");
    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[foo]", "{key=foo}");

    auto type2 = StructLogicalType({
        {"opt_int", "opt_int", Optional(Int64())},
        {"opt_opt_int", "opt_opt_int", Optional(Optional(Int64()))},
        {"list_null", "list_null", List(Null())},
        {"null", "null", Null()},
    }, /*removedFieldStableNames*/ {});
    CHECK_POSITIONAL_TO_NAMED(type2, "[42; [#]; []; #]", "{opt_int=42; opt_opt_int=[#]; list_null=[]}");
    CHECK_POSITIONAL_TO_NAMED(type2, "[#; #; [#; #;]]", "{list_null=[#; #;]}");
}

TEST(TNamedPositionalYsonConverterTest, TestVariantStruct)
{
    CHECK_BIDIRECTIONAL(IntStringVariant, "[0; 42]", "[int; 42]");
    CHECK_BIDIRECTIONAL(IntStringVariant, "[1; foo]", "[string; foo]");

    CHECK_NAMED_TO_POSITIONAL_THROWS(IntStringVariant, "[str; foo]", "Unknown variant field");
}

TEST(TNamedPositionalYsonConverterTest, TestOptional)
{
    CHECK_BIDIRECTIONAL(
        Optional(KeyValueStruct),
        "[foo; bar]",
        "{key=foo; value=bar}");

    CHECK_BIDIRECTIONAL(
        Optional(KeyValueStruct),
        "#",
        "#");

    CHECK_BIDIRECTIONAL(
        Optional(Optional(KeyValueStruct)),
        "[[foo; bar]]",
        "[{key=foo; value=bar}]");

    CHECK_BIDIRECTIONAL(
        Optional(Optional(KeyValueStruct)),
        "#",
        "#");

    CHECK_BIDIRECTIONAL(
        Optional(Optional(KeyValueStruct)),
        "[#]",
        "[#]");
}

TEST(TNamedPositionalYsonConverterTest, TestList)
{
    CHECK_BIDIRECTIONAL(
        List(KeyValueStruct),
        "[[foo; bar]; [qux; #]]",
        "[{key=foo; value=bar}; {key=qux; value=#};]");
}

TEST(TNamedPositionalYsonConverterTest, TestTuple)
{
    CHECK_BIDIRECTIONAL(
        Tuple(KeyValueStruct, IntStringVariant, Utf8()),
        "[[foo; bar]; [0; 5]; foo]",
        "[{key=foo; value=bar}; [int; 5]; foo;]");
}

TEST(TNamedPositionalYsonConverterTest, TestVariantTuple)
{
    auto type = VariantTupleLogicalType({KeyValueStruct, IntStringVariant, Utf8()});

    CHECK_BIDIRECTIONAL(type, "[0; [foo; #]]", "[0; {key=foo; value=#}]");
    CHECK_BIDIRECTIONAL(type, "[1; [1; bar]]", "[1; [string; bar]]");
    CHECK_BIDIRECTIONAL(type, "[2; qux]", "[2; qux]");
}

TEST(TNamedPositionalYsonConverterTest, TestDict)
{
    CHECK_BIDIRECTIONAL(
        Dict(KeyValueStruct, IntStringVariant),
        "[ [[foo; #]; [0; 0]] ; [[bar; qux;]; [1; baz;]]; ]",
        "[ [{key=foo; value=#}; [int; 0]] ; [{key=bar; value=qux;}; [string; baz;]] ]");
}

TEST(TNamedPositionalYsonConverterTest, TestStringDictAsYsonMap)
{
    TYsonConverterConfig config{
        .StringKeyedDictMode = NFormats::EDictMode::Named,
    };
    TWithConfig g(config);

    CHECK_POSITIONAL_TO_NAMED(
        Dict(String(), Int64()),
        "[[key1; 1]; [key2; 2]]",
        "{key1=1; key2=2}");
}

TEST(TNamedPositionalYsonConverterTest, TestTagged)
{
    CHECK_BIDIRECTIONAL(
        Tagged("foo", KeyValueStruct),
        "[foo; bar]",
        "{key=foo; value=bar}");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NComplexTypes
