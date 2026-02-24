#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/complex_types/positional_yson_translation.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_row.h>

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

TPositionalYsonTranslator CreatePositionalYsonTranslator(
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

#define CHECK_YSON_TRANSLATION(translator, source, expectedResult) \
    EXPECT_EQ(expectedResult, TranslateYson(translator, source))

#define EXPECT_TRIVIAL_TRANSLATION(sourceType, targetType, source) \
    do { \
        auto translator = CreatePositionalYsonTranslator(sourceType, targetType); \
        auto sourceValue = MakeUnversionedCompositeValue(source); \
        auto translated = translator(sourceValue); \
        YT_VERIFY(translated.Type == EValueType::Composite); \
        EXPECT_EQ(sourceValue.Data.String, translated.Data.String); \
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

TEST(TTrivialTranslationTest, Basic)
{
    // No actual type checks are performed here, unversioned value is simply passed as is.
    EXPECT_TRIVIAL_TRANSLATION(Int8(), Int32(), "1000");
    EXPECT_TRIVIAL_TRANSLATION(Int32(), Int8(), "2000");

    EXPECT_TRIVIAL_TRANSLATION(Tagged("foo", Tagged("bar", Bool())), Bool(), "%true");

    EXPECT_TRIVIAL_TRANSLATION(Optional(String()), String(), "qwerty");
    EXPECT_TRIVIAL_TRANSLATION(String(), Optional(String()), "I love YT");
    EXPECT_TRIVIAL_TRANSLATION(Optional(String()), Optional(String()), "Some string");

    // When conversion is trivial, no actual unwrapping of optionals is taking place.
    EXPECT_TRIVIAL_TRANSLATION(Optional(Optional(Int8())), Int8(), "[5]");

    EXPECT_TRIVIAL_TRANSLATION(
        List(Tuple(Int32(), String())),
        List(Tuple(Int64(), Optional(String()))),
        "[[54, qwerty], [42, dvorak]]");

    EXPECT_TRIVIAL_TRANSLATION(
        Dict(String(), List(Dict(String(), Int8()))),
        Dict(String(), List(Dict(String(), Int8()))),
        "[key1, [[foo, 5], [bar, 10]], [key2, [[baz, -1]]]]");

    EXPECT_TRIVIAL_TRANSLATION(
        VariantTuple(String(), Int8(), Float()),
        VariantTuple(String(), Int8(), Float(), Double()),
        "[2, 3.14]");
}

TEST(TStructTranslationTest, Basic)
{
    auto sourceType = StructLogicalType(
        {
            {"name", "name", String()},
            {"age", "age", Int8()},
            {"login", "login", String()},
            {"password", "password", String()},
        },
        /*removedFieldStableNames*/ {});

    auto targetType = StructLogicalType(
        {
            {"staff_login", "login", String()},
            {"name_ru", "name", String()},
            {"is_dismissed", "is_dismissed", Optional(Bool())},
            {"age", "age", Optional(Int16())},
            {"password_hash", "password_hash", Optional(String())},
        },
        /*removedFieldStableNames*/ {"password"});

    auto translator = CreatePositionalYsonTranslator(sourceType, targetType);
    CHECK_YSON_TRANSLATION(
        translator,
        R"(["sergey";23;"s-berdnikov";"qwerty";])",
        R"(["s-berdnikov";"sergey";#;23;#;])");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NComplexTypes
