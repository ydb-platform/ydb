#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/library/formats/skiff_yson_converter.h>

#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/token_writer.h>
#include <yt/yt/core/yson/writer.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/skiff/skiff_schema.h>

#include <util/string/hex.h>

#include <util/stream/mem.h>

namespace NYT::NFormats {
namespace {

using namespace NTableClient;
using namespace NSkiff;
using namespace NYson;
using namespace NTableClient::NLogicalTypeShortcuts;

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TSkiffSchema> SkiffOptional(std::shared_ptr<TSkiffSchema> skiffSchema)
{
    return CreateVariant8Schema({
        CreateSimpleTypeSchema(EWireType::Nothing),
        std::move(skiffSchema)
    });
}

TString ConvertYsonHex(
    const TLogicalTypePtr& logicalType,
    const std::shared_ptr<TSkiffSchema>& skiffSchema,
    TStringBuf ysonString,
    const TYsonToSkiffConverterConfig& config = {})
{
    auto converter = CreateYsonToSkiffConverter(
        TComplexTypeFieldDescriptor("test-field", logicalType),
        skiffSchema,
        config);

    // Yson parsers have a bug when they can't parse some values that end unexpectedly.
    TString spacedYsonInput = TString{ysonString} + " ";

    TStringStream out;
    {
        TCheckedInDebugSkiffWriter writer(skiffSchema, &out);

        TMemoryInput in(spacedYsonInput);
        TYsonPullParser pullParser(&in, EYsonType::Node);
        TYsonPullParserCursor cursor(&pullParser);

        converter(&cursor, &writer);

        EXPECT_EQ(cursor.GetCurrent().GetType(), EYsonItemType::EndOfStream);
        writer.Finish();
    }

    auto result = HexEncode(out.Str());
    result.to_lower();
    return result;
}

TString ConvertHexToTextYson(
    const TLogicalTypePtr& logicalType,
    const std::shared_ptr<TSkiffSchema>& skiffSchema,
    TStringBuf hexString,
    const TSkiffToYsonConverterConfig& config = {})
{
    auto converter = CreateSkiffToYsonConverter(TComplexTypeFieldDescriptor("test-field", logicalType), skiffSchema, config);


    TStringStream binaryOut;
    {
        TString binaryString = HexDecode(hexString);
        TMemoryInput in(binaryString);
        TCheckedInDebugSkiffParser parser(skiffSchema, &in);

        auto writer = TCheckedInDebugYsonTokenWriter(&binaryOut);
        converter(&parser, &writer);
        EXPECT_EQ(parser.GetReadBytesCount(), binaryString.size());
    }
    binaryOut.Finish();

    TStringStream out;
    {
        auto writer = TYsonWriter(&out, EYsonFormat::Text);
        ParseYsonStringBuffer(binaryOut.Str(), EYsonType::Node, &writer);
    }
    out.Finish();

    return out.Str();
}


#define CHECK_BIDIRECTIONAL_CONVERSION(logicalType, skiffSchema, ysonString, skiffString, ...) \
    do { \
        std::tuple<TYsonToSkiffConverterConfig, TSkiffToYsonConverterConfig> cfg = {__VA_ARGS__}; \
        auto actualSkiffString = ConvertYsonHex(logicalType, skiffSchema, ysonString, std::get<0>(cfg)); \
        EXPECT_EQ(actualSkiffString, skiffString) << "Yson -> Skiff conversion error"; \
        auto actualYsonString = ConvertHexToTextYson(logicalType, skiffSchema, skiffString, std::get<1>(cfg)); \
        EXPECT_EQ(actualYsonString, ysonString) << "Skiff -> Yson conversion error"; \
    } while (0)


TEST(TYsonSkiffConverterTest, TestSimpleTypes)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        Int8(),
        CreateSimpleTypeSchema(EWireType::Int64),
        "-42",
        "d6ffffff" "ffffffff");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Uint64(),
        CreateSimpleTypeSchema(EWireType::Uint64),
        "42u",
        "2a000000" "00000000");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Uint64(),
        CreateSimpleTypeSchema(EWireType::Uint64),
        "8u",
        "08000000" "00000000");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Bool(),
        CreateSimpleTypeSchema(EWireType::Boolean),
        "%true",
        "01");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Double(),
        CreateSimpleTypeSchema(EWireType::Double),
        "0.",
        "00000000" "00000000");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Float(),
        CreateSimpleTypeSchema(EWireType::Double),
        "0.",
        "00000000" "00000000");

    CHECK_BIDIRECTIONAL_CONVERSION(
        String(),
        CreateSimpleTypeSchema(EWireType::String32),
        "\"foo\"",
        "03000000" "666f6f");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Null(),
        CreateSimpleTypeSchema(EWireType::Nothing),
        "#",
        "");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Uuid(),
        CreateSimpleTypeSchema(EWireType::Uint128),
        "\"\\xF0\\xF1\\xF2\\xF3\\xF4\\xF5\\xF6\\xF7\\xF8\\xF9\\xFA\\xFB\\xFC\\xFD\\xFE\\xFF\"",
        "fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Uuid(),
        CreateSimpleTypeSchema(EWireType::String32),
        "\"\\xF0\\xF1\\xF2\\xF3\\xF4\\xF5\\xF6\\xF7\\xF8\\xF9\\xFA\\xFB\\xFC\\xFD\\xFE\\xFF\"",
        "10000000f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff");
}

TEST(TYsonSkiffConverterTest, TestYson32)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        Yson(),
        CreateSimpleTypeSchema(EWireType::Yson32),
        "-42",
        "02000000" "0253");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Yson(),
        CreateSimpleTypeSchema(EWireType::Yson32),
        "#",
        "01000000" "23");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Yson(),
        CreateSimpleTypeSchema(EWireType::Yson32),
        "[1;2;[3;];]",
        "0e000000" "5b02023b02043b5b02063b5d3b5d");
}

TEST(TYsonSkiffConverterTest, TestOptionalTypes)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        Optional(Int64()),
        SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64)),
        "-42",
        "01" "d6ffffff" "ffffffff");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Optional(Int64()),
        SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64)),
        "#",
        "00");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Optional(Optional(Bool())),
        SkiffOptional(SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean))),
        "[%true;]",
        "01" "01" "01");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Optional(Optional(Bool())),
        SkiffOptional(SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean))),
        "[#;]",
        "01" "00");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Optional(Optional(Bool())),
        SkiffOptional(SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean))),
        "#",
        "00");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Optional(List(Bool())),
        SkiffOptional(CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Boolean)})),
        "#",
        "00");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Optional(Optional(List(Bool()))),
        SkiffOptional(
            SkiffOptional(
                CreateRepeatedVariant8Schema({
                    CreateSimpleTypeSchema(EWireType::Boolean)
                }))),
        "[[%true;%false;%true;];]",
        "01" "01" "0001" "0000" "0001" "ff");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Optional(Optional(List(Bool()))),
        SkiffOptional(
            SkiffOptional(
                CreateRepeatedVariant8Schema({
                    CreateSimpleTypeSchema(EWireType::Boolean)
                }))),
        "[#;]",
        "0100");

    EXPECT_THROW_WITH_SUBSTRING(
        ConvertYsonHex(
            Optional(Optional(Bool())),
            SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean)),
            " [ %true ] "),
        "Optional nesting mismatch");

    EXPECT_THROW_WITH_SUBSTRING(
        ConvertHexToTextYson(
            Optional(Bool()),
            CreateSimpleTypeSchema(EWireType::Boolean),
            "00"),
        "Optional nesting mismatch");

    TYsonToSkiffConverterConfig ysonToSkiffConfig;
    ysonToSkiffConfig.AllowOmitTopLevelOptional = true;

    TSkiffToYsonConverterConfig skiffToYsonConfig;
    skiffToYsonConfig.AllowOmitTopLevelOptional = true;

    CHECK_BIDIRECTIONAL_CONVERSION(
        Optional(Optional(Bool())),
        SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean)),
        "[%true;]",
        "01" "01",
        ysonToSkiffConfig,
        skiffToYsonConfig);

    CHECK_BIDIRECTIONAL_CONVERSION(
        Optional(Optional(Bool())),
        SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean)),
        "[#;]",
        "00",
        ysonToSkiffConfig,
        skiffToYsonConfig);

    EXPECT_THROW_WITH_SUBSTRING(
        ConvertYsonHex(
            Optional(Optional(Bool())),
            SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean)),
            " # ",
            ysonToSkiffConfig),
        "value expected to be nonempty");
}

TEST(TYsonSkiffConverterTest, TestListTypes)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        List(Bool()),
        CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Boolean)}),
        "[]",
        "ff");

    CHECK_BIDIRECTIONAL_CONVERSION(
        List(Bool()),
        CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Boolean)}),
        "[%true;%true;%true;]",
        "00" "01" "00" "01" "00" "01" "ff");

    CHECK_BIDIRECTIONAL_CONVERSION(
        List(List(Bool())),
        CreateRepeatedVariant8Schema({CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Boolean)})}),
        "[[];[%true;];[%true;%true;];]",
        "00" "ff" "00" "0001ff" "00" "00010001ff" "ff");
}

TEST(TYsonSkiffConverterTest, TestStruct)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        Struct(
            "key", String(),
            "value", Bool()),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("value"),
        }),
        "[\"true\";%true;]",
        "04000000" "74727565" "01");
}

TEST(TYsonSkiffConverterTest, TestSkippedFields)
{
    TString skiffString;
    skiffString = ConvertYsonHex(
        Struct(
            "key", String(),
            "subkey", Int64(),
            "value", Bool()),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("value"),
        }),
        " [ true ; 1; %true ] ");
    EXPECT_EQ(skiffString, "04000000" "74727565" "01"sv);

    skiffString = ConvertYsonHex(
        Struct(
            "key", String(),
            "subkey", Int64(),
            "value", Bool()),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("subkey"),
        }),
        " [ true ; 1; %true ] ");
    EXPECT_EQ(skiffString, "01000000" "00000000"sv);

    try {
        ConvertHexToTextYson(
            Struct(
                "key", String(),
                "subkey", Int64(),
                "value", Bool()),
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::Int64)->SetName("subkey"),
            }),
            "01000000" "00000000");
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::ContainsRegex("Non optional struct field .* is missing"));
    }

    CHECK_BIDIRECTIONAL_CONVERSION(
        Struct(
            "key", Optional(String()),
            "subkey", Int64(),
            "value", Optional(Bool())),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("subkey"),
        }),
        "[#;15;#;]",
        "0f000000" "00000000");
}

TEST(TYsonSkiffConverterTest, TestUnknownSkiffFields)
{
    TString skiffString;
    skiffString = ConvertYsonHex(
        Struct(
            "key", String(),
            "subkey", Int64(),
            "value", Bool()),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
            SkiffOptional(CreateSimpleTypeSchema(EWireType::String32))->SetName("key2"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("value"),
        }),
        " [ true ; 1; %true ] ");
    EXPECT_EQ(skiffString, "04000000" "74727565" "00" "01"sv);

    skiffString = ConvertYsonHex(
        Struct(
            "key", String(),
            "subkey", Int64(),
            "value", Bool()),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("value"),
            SkiffOptional(CreateSimpleTypeSchema(EWireType::Yson32))->SetName("value2"),
        }),
        " [ true ; 1; %true ] ");
    EXPECT_EQ(skiffString, "04000000" "74727565" "01" "00"sv);


    try {
        ConvertYsonHex(
            Struct(
                "key", String(),
                "subkey", Int64(),
                "value", Bool()),
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
                CreateSimpleTypeSchema(EWireType::Boolean)->SetName("value"),
                CreateSimpleTypeSchema(EWireType::Yson32)->SetName("value2"),
            }),
            " [ true ; 1; %true ] ");
        GTEST_FAIL() << "exception expected";
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::ContainsRegex("Non optional Skiff field .* is missing corresponding logical struct field"));
    }

    try {
        ConvertHexToTextYson(
            Struct(
                "key", String(),
                "subkey", Int64(),
                "value", Bool()),
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
                SkiffOptional(CreateSimpleTypeSchema(EWireType::String32))->SetName("key2"),
                CreateSimpleTypeSchema(EWireType::Boolean)->SetName("value"),
            }),
            "04000000" "74727565" "00" "01"sv);
        GTEST_FAIL() << "expected_exception";
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::ContainsRegex("is not found in logical type"));
    }
}

TEST(TYsonSkiffConverterTest, TestTuple)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        Tuple(String(), Bool()),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32),
            CreateSimpleTypeSchema(EWireType::Boolean),
        }),
        "[\"true\";%true;]",
        "04000000" "74727565" "01");

    CHECK_BIDIRECTIONAL_CONVERSION(
        Tuple(Int64(), Optional(Int64())),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Int64),
            SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64)),
        }),
        "[2;42;]",
        "02000000" "00000000" "01" "2a000000" "00000000");
}

TEST(TYsonSkiffConverterTest, TestTupleSkippedFields)
{
    TString skiffString;
    skiffString = ConvertYsonHex(
        Tuple(String(), Int64(), Bool()),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32),
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Boolean),
        }),
        " [ true ; 1; %true ] ");
    EXPECT_EQ(skiffString, "04000000" "74727565" "01"sv);

    skiffString = ConvertYsonHex(
        Tuple(String(), Int64(), Bool()),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Nothing),
        }),
        " [ true ; 1; %true ] ");
    EXPECT_EQ(skiffString, "01000000" "00000000"sv);

    skiffString = ConvertYsonHex(
        Tuple(Optional(String()), Int64(), Optional(Bool())),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Nothing)
        }),
        "[#;15;#;]");
    EXPECT_EQ(skiffString, "0f000000" "00000000"sv);
}

TEST(TYsonSkiffConverterTest, TestDict)
{
    const auto logicalType = Dict(String(), Int64());
    const auto skiffSchema = CreateRepeatedVariant8Schema({
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32),
            CreateSimpleTypeSchema(EWireType::Int64)
        })
    });

    CHECK_BIDIRECTIONAL_CONVERSION(
        logicalType,
        skiffSchema,
        "[[\"one\";1;];[\"two\";2;];]",
        "00" "03000000" "6f6e65" "01000000" "00000000"
        "00" "03000000" "74776f" "02000000" "00000000"
        "ff");

    EXPECT_THROW_WITH_SUBSTRING(
        ConvertHexToTextYson(logicalType, skiffSchema, "01" "01000000" "6f" "01000000" "00000000" "ff"),
        "Unexpected \"repeated_variant8\" tag");

    EXPECT_THROW_WITH_SUBSTRING(
        ConvertHexToTextYson(logicalType, skiffSchema, "00" "01000000" "6f" "01000000" "00000000"),
        "Premature end of stream");
}

TEST(TYsonSkiffConverterTest, TestTagged)
{
    const auto logicalType = Tagged(
        "tag",
        Dict(Tagged("tag", String()), Int64()));
    const auto skiffSchema = CreateRepeatedVariant8Schema({
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32),
            CreateSimpleTypeSchema(EWireType::Int64)
        })
    });
    CHECK_BIDIRECTIONAL_CONVERSION(
        logicalType,
        skiffSchema,
        "[[\"one\";1;];[\"two\";2;];]",
        "00" "03000000" "6f6e65" "01000000" "00000000"
        "00" "03000000" "74776f" "02000000" "00000000"
        "ff");
}

TEST(TYsonSkiffConverterTest, TestOptionalVariantSimilarity)
{
    auto logicalType = Optional(
        VariantTuple(Null(), Int64()));

    CHECK_BIDIRECTIONAL_CONVERSION(
        logicalType,
        SkiffOptional(SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64))),
        "[1;42;]",
        "01" "01" "2a000000" "00000000");

    CHECK_BIDIRECTIONAL_CONVERSION(
        logicalType,
        SkiffOptional(SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64))),
        "[0;#;]",
        "01" "00");

    CHECK_BIDIRECTIONAL_CONVERSION(
        logicalType,
        SkiffOptional(SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64))),
        "#",
        "00");

    TYsonToSkiffConverterConfig ysonToSkiffConfig;
    ysonToSkiffConfig.AllowOmitTopLevelOptional = true;

    TSkiffToYsonConverterConfig skiffToYsonConfig;
    skiffToYsonConfig.AllowOmitTopLevelOptional = true;

    CHECK_BIDIRECTIONAL_CONVERSION(
        logicalType,
        SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64)),
        "[1;42;]",
        "01" "2a000000" "00000000",
        ysonToSkiffConfig,
        skiffToYsonConfig);

    CHECK_BIDIRECTIONAL_CONVERSION(
        logicalType,
        SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64)),
        "[0;#;]",
        "00",
        ysonToSkiffConfig,
        skiffToYsonConfig);

    EXPECT_THROW_WITH_SUBSTRING(
        ConvertYsonHex(
            logicalType,
            SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64)),
            "#",
            ysonToSkiffConfig),
        "value expected to be nonempty");
}

class TYsonSkiffConverterTestVariant
    : public ::testing::TestWithParam<std::tuple<ELogicalMetatype, EWireType>>
{
public:
    TLogicalTypePtr VariantLogicalType(const std::vector<TLogicalTypePtr>& elements)
    {
        auto [metatype, wireType] = GetParam();
        if (metatype == ELogicalMetatype::VariantTuple) {
            return VariantTupleLogicalType(elements);
        } else {
            std::vector<TStructField> fields;
            for (size_t i = 0; i < elements.size(); ++i) {
                fields.push_back({Format("field%v", i), elements[i]});
            }
            return VariantStructLogicalType(fields);
        }
    }

    std::shared_ptr<TSkiffSchema> VariantSkiffSchema(std::vector<std::shared_ptr<TSkiffSchema>> elements)
    {
        for (size_t i = 0; i < elements.size(); ++i) {
            elements[i]->SetName(Format("field%v", i));
        }
        auto [metatype, wireType] = GetParam();
        if (wireType == EWireType::Variant8) {
            return CreateVariant8Schema(std::move(elements));
        } else if (wireType == EWireType::Variant16) {
            return CreateVariant16Schema(std::move(elements));
        }
        Y_UNREACHABLE();
    }

    TString VariantTagInfix() const
    {
        auto [metatype, wireType] = GetParam();
        if (wireType == EWireType::Variant16) {
            return "00";
        }
        return {};
    }
};

TEST_P(TYsonSkiffConverterTestVariant, TestVariant)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        VariantLogicalType({
            Int64(),
            Bool()
        }),
        VariantSkiffSchema({
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Boolean),
        }),
        "[0;42;]",
        "00" + VariantTagInfix() + "2a000000" "00000000");

    CHECK_BIDIRECTIONAL_CONVERSION(
        VariantLogicalType({
            Int64(),
            Bool()
        }),
        VariantSkiffSchema({
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Boolean),
        }),
        "[1;%true;]",
        "01" + VariantTagInfix() + "01");
}

TEST_P(TYsonSkiffConverterTestVariant, TestMalformedVariants)
{
    auto logicalType = VariantLogicalType({
        Bool(),
        Int64(),
    });
    auto skiffSchema = VariantSkiffSchema({
        CreateSimpleTypeSchema(EWireType::Boolean),
        CreateSimpleTypeSchema(EWireType::Int64),
    });

    EXPECT_THROW_WITH_SUBSTRING(ConvertYsonHex(logicalType, skiffSchema, "[2; 42]"), "Yson to Skiff conversion error");
    EXPECT_THROW_WITH_SUBSTRING(ConvertYsonHex(logicalType, skiffSchema, "[]"), "Yson to Skiff conversion error");
    EXPECT_THROW_WITH_SUBSTRING(ConvertYsonHex(logicalType, skiffSchema, "[0]"), "Yson to Skiff conversion error");

    EXPECT_THROW_WITH_SUBSTRING(ConvertHexToTextYson(logicalType, skiffSchema, "02" + VariantTagInfix() + "00"),
        "Skiff to Yson conversion error");
}

INSTANTIATE_TEST_SUITE_P(
    Variants,
    TYsonSkiffConverterTestVariant,
    ::testing::Combine(
        ::testing::ValuesIn({ELogicalMetatype::VariantStruct, ELogicalMetatype::VariantTuple}),
        ::testing::ValuesIn({EWireType::Variant8, EWireType::Variant16}))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
