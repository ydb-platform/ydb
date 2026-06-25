#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/complex_types/positional_yson_translation.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/validate_logical_type.h>

#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/core/yson/writer.h>

namespace NYT::NComplexTypes {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient::NLogicalTypeShortcuts;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TComplexTypeFieldDescriptor CreateFieldDescriptor(TLogicalTypePtr type)
{
    return TComplexTypeFieldDescriptor("<test>", std::move(type));
}

std::optional<TPositionalYsonTranslator> CreatePositionalYsonTranslator(
    TLogicalTypePtr sourceType,
    TLogicalTypePtr targetType)
{
    return NComplexTypes::CreatePositionalYsonTranslator(
        CreateFieldDescriptor(std::move(sourceType)),
        CreateFieldDescriptor(std::move(targetType)));
}

TString ParseUnversionedValue(TUnversionedValue value)
{
    TString result;

    TStringOutput out(result);
    TYsonWriter writer(&out, EYsonFormat::Text, EYsonType::Node);
    UnversionedValueToYson(value, &writer);
    writer.Flush();

    return result;
}

TString TranslateYson(const TPositionalYsonTranslator& translator, TStringBuf source)
{
    auto sourceValue = MakeUnversionedCompositeValue(source);
    auto translated = translator(sourceValue);
    YT_VERIFY(translated.Type == EValueType::Composite);
    return ParseUnversionedValue(translated);
}

#define EXPECT_TRIVIAL_TRANSLATION(sourceType, targetType) \
    do { \
        auto translator = CreatePositionalYsonTranslator(sourceType, targetType); \
        EXPECT_FALSE(translator.has_value()); \
    } while (false)

#define CHECK_YSON_TRANSLATION(sourceType, targetType, source, expectedResult) \
    do { \
        ValidateComplexLogicalType(source, sourceType); \
        auto translator = CreatePositionalYsonTranslator(sourceType, targetType); \
        ASSERT_TRUE(translator.has_value()); \
        EXPECT_EQ(expectedResult, TranslateYson(*translator, source)); \
    } while (false)

#define EXPECT_FAILED_TRANSLATION(sourceType, targetType, source, errorSubstring) \
    do { \
        ValidateComplexLogicalType(source, sourceType); \
        auto translator = CreatePositionalYsonTranslator(sourceType, targetType); \
        ASSERT_TRUE(translator.has_value()); \
        EXPECT_THROW_WITH_SUBSTRING(TranslateYson(*translator, source), errorSubstring); \
    } while (false)

////////////////////////////////////////////////////////////////////////////////

TEST(TPostionalTranslationTest, IncompatibleMetatypes)
{
    static const std::array TypeDescriptors = {
        String(),
        List(Float()),
        Tuple(Double(), Utf8(), Timestamp()),
        Dict(String(), TzDate()),
        Decimal(5, 10),
        Struct("a", Uint32(), "b", Int16()),
        VariantStruct("a", TzDate32(), "b", Uuid()),
        VariantTuple(Json(), Interval()),
    };

    for (const auto& sourceType : TypeDescriptors) {
        for (const auto& targetType : TypeDescriptors) {
            SCOPED_TRACE(Format("%v -> %v", ToString(*sourceType), ToString(*targetType)));

            #define CHECK_TYPE_COMPATIBILITY(lhs, rhs) \
                if (&sourceType == &targetType) { \
                    EXPECT_NO_THROW(CreatePositionalYsonTranslator(lhs, rhs)); \
                } else { \
                    EXPECT_THROW_WITH_SUBSTRING( \
                        CreatePositionalYsonTranslator(lhs, rhs), \
                        "types are incompatible"); \
                }

            // Types themselves.
            CHECK_TYPE_COMPATIBILITY(sourceType, targetType);

            // Types wrapped in various containers.
            CHECK_TYPE_COMPATIBILITY(Tagged("a", sourceType), Tagged("a", targetType));
            CHECK_TYPE_COMPATIBILITY(Optional(sourceType), Optional(targetType));
            CHECK_TYPE_COMPATIBILITY(List(sourceType), List(targetType));
            CHECK_TYPE_COMPATIBILITY(Dict(String(), sourceType), Dict(String(), targetType));
            CHECK_TYPE_COMPATIBILITY(Tuple(sourceType), Tuple(targetType));
            CHECK_TYPE_COMPATIBILITY(VariantTuple(sourceType), VariantTuple(targetType));
            CHECK_TYPE_COMPATIBILITY(Struct("a", sourceType), Struct("a", targetType));
            CHECK_TYPE_COMPATIBILITY(
                VariantStruct("a", sourceType),
                VariantStruct("a", targetType));

            // Tagged and optional are special in that they can be wrapped and unwrapped.
            CHECK_TYPE_COMPATIBILITY(sourceType, Tagged("a", targetType));
            CHECK_TYPE_COMPATIBILITY(Tagged("a", sourceType), targetType);
            CHECK_TYPE_COMPATIBILITY(sourceType, Optional(targetType));
            CHECK_TYPE_COMPATIBILITY(Optional(sourceType), targetType);
        }

        #undef CHECK_TYPE_COMPATIBILITY
    }
}

TEST(TPostionalTranslationTest, IncompatibleTypes)
{
    #define EXPECT_COMPATIBLE(sourceType, targetType) \
        EXPECT_NO_THROW(CreatePositionalYsonTranslator(sourceType, targetType))

    #define EXPECT_INCOMPATIBLE(sourceType, targetType, substring) \
        EXPECT_THROW_WITH_SUBSTRING( \
            CreatePositionalYsonTranslator(sourceType, targetType), \
            substring)

    // Tuples.
    EXPECT_COMPATIBLE(Tuple(String()), Tuple(String()));
    EXPECT_INCOMPATIBLE(Tuple(String()), Tuple(String(), Float()), "tuples of different sizes");
    EXPECT_INCOMPATIBLE(Tuple(String(), Float()), Tuple(String()), "tuples of different sizes");

    // Variant tuples.
    EXPECT_COMPATIBLE(VariantTuple(String(), Float()), VariantTuple(String(), Float()));
    EXPECT_COMPATIBLE(VariantTuple(String()), VariantTuple(String(), Float()));
    EXPECT_INCOMPATIBLE(
        VariantTuple(String(), Float()),
        VariantTuple(String()),
        "cannot be larger than");

    // Structs.

    EXPECT_COMPATIBLE(Struct("a", String(), "b", Float()), Struct("a", String(), "b", Float()));

    // Reorder fields.
    EXPECT_COMPATIBLE(Struct("a", String(), "b", Float()), Struct("b", Float(), "a", String()));

    // Remove field.
    EXPECT_COMPATIBLE(
        Struct("a", String(), "c", Datetime(), "b", Float()),
        Struct("a", String(), "b", Float()));

    // Add non-nullable field.
    EXPECT_INCOMPATIBLE(
        Struct("a", String(), "b", Float()),
        Struct("a", String(), "c", Datetime(), "b", Float()),
        "is absent in source and is not nullable");

    // Add nullable field.
    EXPECT_COMPATIBLE(
        Struct("a", String(), "b", Float()),
        Struct("a", String(), "c", Optional(Datetime()), "b", Float()));

    // Variant structs.

    EXPECT_COMPATIBLE(
        VariantStruct("a", String(), "b", Float()),
        VariantStruct("a", String(), "b", Float()));

    // Change field order.
    EXPECT_COMPATIBLE(
        VariantStruct("a", String(), "b", Float()),
        VariantStruct("b", Float(), "a", String()));

    // Add new field.
    EXPECT_COMPATIBLE(
        VariantStruct("a", String(), "b", Float()),
        VariantStruct("a", String(), "c", Datetime(), "b", Float()));

    // Remove existing field.
    EXPECT_COMPATIBLE(
        VariantStruct("a", String(), "c", Datetime(), "b", Float()),
        VariantStruct("a", String(), "b", Float()));

    // Add one field and remove another, preserving some common fields.
    EXPECT_COMPATIBLE(
        VariantStruct("a", String(), "b", Float()),
        VariantStruct("c", Datetime(), "b", Float()));

    // Variant structs have no fields in common.
    EXPECT_INCOMPATIBLE(
        VariantStruct("a", String(), "b", Float()),
        VariantStruct("c", String(), "d", Float()),
        "no common fields");

    #undef EXPECT_INCOMPATIBLE
    #undef EXPECT_COMPATIBLE
}

TEST(TTrivialTranslationTest, BasicTypes)
{
    EXPECT_TRIVIAL_TRANSLATION(Int16(), Int32());
    EXPECT_TRIVIAL_TRANSLATION(Int32(), Int8());

    EXPECT_TRIVIAL_TRANSLATION(Tagged("foo", Tagged("bar", Bool())), Bool());

    EXPECT_TRIVIAL_TRANSLATION(Optional(String()), String());
    EXPECT_TRIVIAL_TRANSLATION(String(), Optional(String()));
    EXPECT_TRIVIAL_TRANSLATION(Optional(String()), Optional(String()));

    // When conversion is trivial, no actual unwrapping of optionals is taking place.
    EXPECT_TRIVIAL_TRANSLATION(Optional(Optional(Int8())), Int8());

    EXPECT_TRIVIAL_TRANSLATION(
        List(Tuple(Int32(), String())),
        List(Tuple(Int64(), Optional(String()))));

    EXPECT_TRIVIAL_TRANSLATION(
        Dict(String(), List(Dict(String(), Int8()))),
        Dict(String(), List(Dict(String(), Int8()))));

    EXPECT_TRIVIAL_TRANSLATION(
        VariantTuple(String(), Int8(), Float()),
        VariantTuple(String(), Int8(), Float(), Double()));
}

TEST(TTrivialTranslationTest, Structs)
{
    EXPECT_TRIVIAL_TRANSLATION(
        Struct("name", String(), "age", Float()),
        Struct("name", String(), "age", Double()));

    // Nested in Optional.
    EXPECT_TRIVIAL_TRANSLATION(
        Struct("name", String(), "age", Float()),
        Optional(Struct("name", String(), "age", Double())));

    // Field was appended, no need for non-trivial translation.
    EXPECT_TRIVIAL_TRANSLATION(
        Struct("name", String(), "age", Float()),
        Struct("name", String(), "age", Float(), "is_funny", Optional(Bool())));

    // Field was renamed, no need for non-trivial translation.
    EXPECT_TRIVIAL_TRANSLATION(
        Struct("breed", String(), "weight", Float()),
        StructLogicalType(
            {
                {"breed", "breed", String()},
                {"weight_kg", "weight", Double()},
            },
            /*removedFieldStableNames*/ {}));

    // Nested in List.
    EXPECT_TRIVIAL_TRANSLATION(
        List(Struct("name", String(), "age", Float())),
        List(Struct("name", String(), "age", Float(), "is_funny", Optional(Bool()))));

    // Nested in Dict.
    EXPECT_TRIVIAL_TRANSLATION(
        Dict(String(), Struct("name", String(), "age", Float())),
        Dict(String(), Struct("name", String(), "age", Float(), "is_funny", Optional(Bool()))));

    // Nested in Tuple.
    EXPECT_TRIVIAL_TRANSLATION(
        Tuple(
            Struct("name", String(), "age", Float()),
            Struct("breed", String(), "weight", Float())),
        Tuple(
            Struct("name", String(), "age", Float(), "is_funny", Optional(Bool())),
            StructLogicalType(
            {
                {"breed", "breed", String()},
                {"weight_kg", "weight", Double()},
            },
            /*removedFieldStableNames*/ {})));

    // Nested struct.
    EXPECT_TRIVIAL_TRANSLATION(
        Struct(
            "owner",
            Struct("name", String(), "age", Float()),
            "animal",
            Struct("breed", String(), "weight", Float())),
        StructLogicalType(
            {
                {
                    "owner_info",
                    "owner",
                    Struct("name", String(), "age", Float(), "is_funny", Optional(Bool())),
                },
                {
                    "animal_info",
                    "animal",
                    StructLogicalType(
                    {
                        {"breed", "breed", String()},
                        {"weight_kg", "weight", Double()},
                    },
                    /*removedFieldStableNames*/ {}),
                },
            },
            /*removedFieldStableNames*/ {}));
}

TEST(TTrivialTranslationTest, VariantStructs)
{
    EXPECT_TRIVIAL_TRANSLATION(
        VariantStruct("duration_sec", Int32(), "duration_str", String()),
        VariantStruct("duration_sec", Int64(), "duration_str", String()));

    // Nested in Optional.
    EXPECT_TRIVIAL_TRANSLATION(
        VariantStruct("duration_sec", Int32(), "duration_str", String()),
        Optional(VariantStruct("duration_sec", Int64(), "duration_str", String())));

    // Field was appended, no need for non-trivial translation.
    EXPECT_TRIVIAL_TRANSLATION(
        VariantStruct("duration_sec", Int32(), "duration_str", String()),
        VariantStruct("duration_sec", Int64(), "duration_str", String(), "duration_min", Int32()));

    // Field was renamed, no need for non-trivial translation.
    EXPECT_TRIVIAL_TRANSLATION(
        VariantStruct("duration_sec", Int32(), "duration_str", String()),
        VariantStructLogicalType({
            {"duration_seconds", "duration_sec", Int64()},
            {"duration_str", "duration_str", String()},
        }));

    // Nested in List.
    EXPECT_TRIVIAL_TRANSLATION(
        List(VariantStruct("sec", Int32(), "duration_str", String())),
        List(VariantStruct("sec", Int64(), "duration_str", String(), "min", Int32())));

    // Nested in Dict.
    EXPECT_TRIVIAL_TRANSLATION(
        Dict(String(), VariantStruct("sec", Int32(), "duration_str", String())),
        Dict(String(), VariantStruct("sec", Int64(), "duration_str", String(), "min", Int32())));

    // Nested in Tuple.
    EXPECT_TRIVIAL_TRANSLATION(
        Tuple(
            VariantStruct("duration_sec", Int32(), "duration_str", String()),
            VariantStruct("weight_kg", Float(), "weight_lbs", Int32())),
        Tuple(
            VariantStructLogicalType({
                {"duration_seconds", "duration_sec", Int64()},
                {"duration_str", "duration_str", String()},
            }),
            VariantStruct("weight_kg", Float(), "weight_lbs", Int32())));

    // Nested variant struct.
    EXPECT_TRIVIAL_TRANSLATION(
        VariantStruct(
            "oauth_token",
            VariantStruct("value", String(), "value_b64", String()),
            "usr_ticket",
            VariantStruct("value", String(), "value_b64", String())),
        VariantStructLogicalType({
            {
                "OAuth token",
                "oauth_token",
                VariantStructLogicalType({
                    {"Value", "value", String()},
                    {"Value base 64", "value_b64", String()},
                }),
            },
            {
                "User ticket",
                "usr_ticket",
                VariantStructLogicalType({
                    {"Value", "value", String()},
                    {"Value base 64", "value_b64", String()},
                }),
            },
        }));
}

TEST(TPostionalTranslationTest, Structs)
{
    // Reorder fields - simplest case where non-trivial translation is necessary.
    CHECK_YSON_TRANSLATION(
        Struct("breed", String(), "weight", Double()),
        Struct("weight", Double(), "breed", String()),
        R"(["Golden retriever";12.34;])",
        R"([12.34;"Golden retriever";])");

    // Rename, remove, add, and reorder.
    CHECK_YSON_TRANSLATION(
        Struct("name", String(), "age", Int8(), "login", String(), "password", String()),
        StructLogicalType(
        {
            {"staff_login", "login", String()},
            {"name_ru", "name", String()},
            {"is_dismissed", "is_dismissed", Optional(Bool())},
            {"age", "age", Optional(Int16())},
            {"password_hash", "password_hash", Optional(String())},
        },
        /*removedFieldStableNames*/ {"password"}),
        R"(["sergey";23;"s-berdnikov";"qwerty";])",
        R"(["s-berdnikov";"sergey";#;23;#;])");

    // Nested in Optional.

    CHECK_YSON_TRANSLATION(
        Optional(Struct("breed", String(), "weight", Double())),
        Optional(Struct("weight", Double(), "breed", String())),
        R"(["Golden retriever";12.34;])",
        R"([12.34;"Golden retriever";])");

    CHECK_YSON_TRANSLATION(
        Optional(Struct("breed", String(), "weight", Double())),
        Optional(Struct("weight", Double(), "breed", String())),
        "#",
        "#");

    // Wrap in Optional.
    CHECK_YSON_TRANSLATION(
        Struct("breed", String(), "weight", Double()),
        Optional(Struct("weight", Double(), "breed", String())),
        R"(["Golden retriever";12.34;])",
        R"([12.34;"Golden retriever";])");

    // Unwrap from Optional.

    CHECK_YSON_TRANSLATION(
        Optional(Struct("breed", String(), "weight", Double())),
        Struct("weight", Double(), "breed", String()),
        R"(["Golden retriever";12.34;])",
        R"([12.34;"Golden retriever";])");

    EXPECT_FAILED_TRANSLATION(
        Optional(Struct("breed", String(), "weight", Double())),
        Struct("weight", Double(), "breed", String()),
        "#",
        "Cannot unwrap empty optional");

    // Nested in List.
    CHECK_YSON_TRANSLATION(
        List(Struct("breed", String(), "typical_weight", Int16())),
        List(StructLogicalType(
            {
                {"typical_height", "typical_height", Optional(Int8())},
                {"breed", "breed", String()},
            },
            /*removedFieldStableNames*/ {"typical_weight"})),
        R"([["Afghan Hound";25;];["Australian Terrier";6;];["Chihuahua";2;]])",
        R"([[#;"Afghan Hound";];[#;"Australian Terrier";];[#;"Chihuahua";];])");

    // Nested in Dict.
    CHECK_YSON_TRANSLATION(
        Dict(String(), Struct("typical_weight", Float())),
        Dict(
            String(),
            StructLogicalType(
                {
                    {"Additional info", "additional_info", Optional(Struct())},
                    {"Typical weight", "typical_weight", Float()},
                },
                /*removedFieldStableNames*/ {})),
        R"([["Afghan Hound";[25.;];];["Australian Terrier";[6.;];];["Chihuahua";[2.;];];])",
        "["
            R"(["Afghan Hound";[#;25.;];];)"
            R"(["Australian Terrier";[#;6.;];];)"
            R"(["Chihuahua";[#;2.;];];)"
        "]");

    // Nested in Tuple.
    CHECK_YSON_TRANSLATION(
        Tuple(
            Struct("name", String(), "age", Int8()),
            Struct("breed", String(), "weight", Double())),
        Tuple(
            Struct("age", String(), "name", Float()),
            StructLogicalType(
                {
                    {"name", "name", Optional(String())},
                    {"breed_en", "breed", String()},
                },
                /*removedFieldStableNames*/ {"weight"})),
        R"([["Dmitry";55;];["Chihuahua";3.5];])",
        R"([[55;"Dmitry";];[#;"Chihuahua";];])");

    // Nested struct.
    CHECK_YSON_TRANSLATION(
        Struct(
            "owner",
            Struct("name", String(), "age", Float()),
            "animal",
            Struct("breed", String(), "weight", Float())),
        StructLogicalType(
            {
                {
                    "animal_info",
                    "animal",
                    StructLogicalType(
                    {
                        {"name", "name", Optional(String())},
                        {"breed", "breed", String()},
                        {"weight_kg", "weight", Double()},
                    },
                    /*removedFieldStableNames*/ {}),
                },
                {
                    "owner_info",
                    "owner",
                    Struct("is_funny", Optional(Bool()), "name", String(), "age", Float()),
                },
            },
            /*removedFieldStableNames*/ {}),
        R"([["jovana";27.99;];["Golden retriever";15.34;];])",
        R"([[#;"Golden retriever";15.34;];[#;"jovana";27.99;];])");
}

TEST(TPostionalTranslationTest, VariantStructs)
{
    // Reorder fields and remove one of them.
    CHECK_YSON_TRANSLATION(
        List(VariantStruct("time_str", String(), "timestamp", Int64(), "time_posix", Double())),
        List(VariantStruct("time_posix", Double(), "time_str", String())),
        R"([[2;1772204673.5;];[0;"2026-02-27T18:08:30.38";];[2;1772204725.18;];])",
        R"([[0;1772204673.5;];[1;"2026-02-27T18:08:30.38";];[0;1772204725.18;];])");

    // Add new field in the middle.
    CHECK_YSON_TRANSLATION(
        List(VariantStruct("time_str", String(), "time_posix", Double())),
        List(VariantStruct("time_str", String(), "timestamp", Int64(), "time_posix", Double())),
        R"([[1;1772204673.5;];[0;"2026-02-27T18:08:30.38";];[1;1772204725.18;];])",
        R"([[2;1772204673.5;];[0;"2026-02-27T18:08:30.38";];[2;1772204725.18;];])");

    // Remove field that is present in the data.
    EXPECT_FAILED_TRANSLATION(
        List(VariantStruct("time_str", String(), "time_posix", Double())),
        List(VariantStruct("time_posix", Double())),
        R"([[1;1772204673.5;];[0;"2026-02-27T18:08:30.38";];[1;1772204725.18;];])",
        "encountered unknown variant tag (0)");

    // Nested variant struct.
    CHECK_YSON_TRANSLATION(
        VariantStruct(
            "oauth_token",
            VariantStruct("value", String(), "value_b64", String()),
            "usr_ticket",
            VariantStruct("value", String(), "value_b64", String())),
        VariantStructLogicalType({
            {
                "User ticket",
                "usr_ticket",
                VariantStructLogicalType({
                    {"value_encoded", "value_encoded", String()},
                    {"Value", "value", String()},
                    {"Value base 64", "value_b64", String()},
                }),
            },
            {
                "OAuth token",
                "oauth_token",
                VariantStructLogicalType({
                    {"Value", "value", String()},
                    {"Value base 64", "value_b64", String()},
                }),
            },
        }),
        R"([1;[1;"SWYgeW91IGFyZSByZWFkaW5nIHRoaXMsIGhpIQ==";];])",
        R"([0;[2;"SWYgeW91IGFyZSByZWFkaW5nIHRoaXMsIGhpIQ==";];])");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NComplexTypes
