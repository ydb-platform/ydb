#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/complex_types/check_type_compatibility.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NComplexTypes;
using namespace NLogicalTypeShortcuts;

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]]
void PrintTo(ESchemaCompatibility typeCompatibility, std::ostream* stream)
{
    (*stream) << "ESchemaCompatibility::" << ToString(typeCompatibility).c_str();
}

TStructField GenerateSimpleField(std::string name, std::string stableName)
{
    return TStructField{
        .Name = std::move(name),
        .StableName = std::move(stableName),
        .Type = Optional(Int32()),
    };
}

ESchemaCompatibility CheckTypeCompatibilitySimple(
    const TLogicalTypePtr& oldType,
    const TLogicalTypePtr& newType)
{
    static constexpr TTypeCompatibilityOptions Options{};
    return CheckTypeCompatibility(oldType, newType, Options).first;
}

ESchemaCompatibility CheckTypeCompatibilityWithAllowedRenaming(
    const TLogicalTypePtr& oldType,
    const TLogicalTypePtr& newType)
{
    static constexpr TTypeCompatibilityOptions Options{
        .AllowStructFieldRenaming = true,
    };
    return CheckTypeCompatibility(oldType, newType, Options).first;
}

[[maybe_unused]]
ESchemaCompatibility CheckTypeCompatibilityWithAllowedRemoval(
    const TLogicalTypePtr& oldType,
    const TLogicalTypePtr& newType)
{
    static constexpr TTypeCompatibilityOptions Options{
        .AllowStructFieldRemoval = true,
    };
    return CheckTypeCompatibility(oldType, newType, Options).first;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCheckTypeCompatibilityTest, SimpleTypes)
{
    // Int
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibilitySimple(Int8(), Int16()));
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibilitySimple(Int8(), Int64()));
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibilitySimple(Int16(), Int64()));
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibilitySimple(Int16(), Int8()));
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibilitySimple(Int64(), Int8()));
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibilitySimple(Int64(), Int32()));

    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibilitySimple(Int8(), Uint64()));
    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibilitySimple(Int8(), Double()));
    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibilitySimple(Int8(), String()));

    // Uint
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibilitySimple(Uint8(), Uint16()));
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibilitySimple(Uint8(), Uint64()));
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibilitySimple(Uint16(), Uint64()));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibilitySimple(Uint16(), Uint8()));
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibilitySimple(Uint64(), Uint8()));
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibilitySimple(Uint64(), Uint32()));

    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibilitySimple(Uint8(), Int64()));

    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibilitySimple(Uint8(), Double()));

    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibilitySimple(Uint8(), String()));

    // Float
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibilitySimple(Float(), Double()));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibilitySimple(Double(), Float()));

    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibilitySimple(Float(), String()));
    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibilitySimple(Double(), Int64()));

    // String
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibilitySimple(Utf8(), String()));
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibilitySimple(String(), Utf8()));
    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibilitySimple(String(), Json()));
}

TEST(TCheckTypeCompatibilityTest, Optional)
{
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Int8(),
            Optional(Int8())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            Optional(Int8()),
            Int8()));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Int8(),
            Optional(Optional(Int8()))));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Optional(Optional(Int8())),
            Int8()));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Optional(Int8()),
            Optional(Optional(Int8()))));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Optional(Optional(Int8())),
            Optional(Int8())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Optional(Optional(Int8())),
            Optional(Optional(Int8()))));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Optional(Int8()),
            Optional(Int16())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Int8(),
            Optional(Int16())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            Int32(),
            Optional(Int16())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            Optional(List(Int64())),
            List(Int64())));
}

TEST(TCheckTypeCompatibilityTest, List)
{
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Int8(),
            List(Int8())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            List(Int8()),
            Int8()));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            List(List(Int8())),
            List(List(Int8()))));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            List(Int8()),
            List(Int16())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            List(Int8()),
            List(Optional(Int16()))));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            List(Int32()),
            List(Optional(Int16()))));
}

TEST(TCheckTypeCompatibilityTest, Tuple)
{
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Int8(),
            Tuple(Int8())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Tuple(Int8()),
            Int8()));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Tuple(Int8()),
            Tuple(Int8())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Tuple(Int8()),
            Tuple(Int16())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Tuple(Int8(), Int8()),
            Tuple(Int16(), Int8())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Tuple(Int8(), Int8()),
            Tuple(Int16(), Int32())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Tuple(Int8()),
            Tuple(Optional(Int16()))));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            Tuple(Int32()),
            Tuple(Optional(Int16()))));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Tuple(Int8(), Int8()),
            Tuple(Int8(), Int8(), Int8())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Tuple(Int8(), Int8()),
            Tuple(Int8(), Int8(), Int8())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Tuple(Int8(), Int8(), Int8()),
            Tuple(Int8(), Int8())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Tuple(Int8(), Int8()),
            Tuple(Int8(), Int8(), Optional(Int8()))));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Tuple(Int8(), Int8(), Optional(Int8())),
            Tuple(Int8(), Int8())));
}

TEST(TCheckTypeCompatibilityTest, VariantTuple)
{
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Int8(),
            VariantTuple(Int8())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int8()),
            Int8()));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int8()),
            VariantTuple(Int16())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int8(), Uint8()),
            VariantTuple(Int16(), Uint8())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int8(), Uint8()),
            VariantTuple(Int8(), Uint16())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int8(), Uint8()),
            VariantTuple(Int32(), Uint16())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int16(), Uint8()),
            VariantTuple(Int8(), Uint16())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int16(), Uint16()),
            VariantTuple(Int8(), Uint16())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int16(), Uint16()),
            VariantTuple(Int16(), Uint8())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int16(), Uint16()),
            VariantTuple(Int16(), Uint8())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int16(), Uint16()),
            VariantTuple(Int16(), Uint16(), String())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int16(), Uint16(), String()),
            VariantTuple(Int16(), Uint16())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            VariantTuple(Int16(), Uint16(), String()),
            VariantTuple(Int16(), Uint16())));
}

TEST(TCheckTypeCompatibilityTest, Dict)
{
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Dict(Utf8(), Int16()),
            Dict(Utf8(), Int16())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Dict(Utf8(), Int16()),
            Dict(Utf8(), Int32())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Dict(Utf8(), Int16()),
            Dict(String(), Int16())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            Dict(Utf8(), Int16()),
            Dict(String(), Int32())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            Dict(String(), Int32()),
            Dict(Utf8(), Int16())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            Dict(String(), Int32()),
            Dict(Utf8(), Int16())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            Dict(Utf8(), Int32()),
            Dict(String(), Int16())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Dict(Utf8(), Int32()),
            Dict(String(), Uint16())));
}

////////////////////////////////////////////////////////////////////////////////

struct TCommonStructTypesTest
    : public ::testing::TestWithParam<ELogicalMetatype>
{
    template <typename... T>
    TLogicalTypePtr ConstructType(std::vector<TStructField> fields) const
    {
        switch (GetParam()) {
            case ELogicalMetatype::Struct:
                return StructLogicalType(std::move(fields), /*removedFieldStableNames*/ {});
            case ELogicalMetatype::VariantStruct:
                return VariantStructLogicalType(std::move(fields));
            default:
                YT_ABORT();
        }
    }

    template <typename... T>
    TLogicalTypePtr ConstructTypeShortcut(const T&... args) const
    {
        switch (GetParam()) {
            case ELogicalMetatype::Struct:
                return Struct(args...);
            case ELogicalMetatype::VariantStruct:
                return VariantStruct(args...);
            default:
                YT_ABORT();
        }
    }

    ESchemaCompatibility GetIncompatibleIfStructOrElse(ESchemaCompatibility whatElse) const
    {
        switch (GetParam()) {
            case ELogicalMetatype::Struct:
                return ESchemaCompatibility::Incompatible;
            case ELogicalMetatype::VariantStruct:
                return whatElse;
            default:
                YT_ABORT();
        }
    }
};

INSTANTIATE_TEST_SUITE_P(
    Tests,
    TCommonStructTypesTest,
    ::testing::ValuesIn({ELogicalMetatype::Struct, ELogicalMetatype::VariantStruct}));

TEST_P(TCommonStructTypesTest, TrivialCases)
{
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            Int8(),
            ConstructTypeShortcut("a", Int8())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8()),
            Int8()));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8()),
            ConstructTypeShortcut("a", Int8())));
}

TEST_P(TCommonStructTypesTest, WideningIntegerConversion)
{
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8()),
            ConstructTypeShortcut("a", Int16())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8(), "b", Uint8()),
            ConstructTypeShortcut("a", Int16(), "b", Uint8())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8(), "b", Uint8()),
            ConstructTypeShortcut("a", Int8(), "b", Uint16())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8(), "b", Uint8()),
            ConstructTypeShortcut("a", Int32(), "b", Uint16())));
}

TEST_P(TCommonStructTypesTest, NarrowingIntegerConversion)
{
    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int16()),
            ConstructTypeShortcut("a", Int8())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int16(), "b", Uint8()),
            ConstructTypeShortcut("a", Int8(), "b", Uint16())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int16(), "b", Uint16()),
            ConstructTypeShortcut("a", Int8(), "b", Uint16())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int16(), "b", Uint16()),
            ConstructTypeShortcut("a", Int16(), "b", Uint8())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int16(), "b", Uint16()),
            ConstructTypeShortcut("a", Int16(), "b", Uint8())));
}

TEST_P(TCommonStructTypesTest, AppendingFields)
{
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8()),
            ConstructTypeShortcut("a", Int8(), "b", Optional(Int8()))));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8()),
            ConstructTypeShortcut("a", Int8(), "b", Optional(Optional(Int8())))));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8()),
            ConstructTypeShortcut("a", Int8(), "b", Null())));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8()),
            ConstructTypeShortcut("a", Int8(), "b", Optional(List(String())))));

    // In case of structs, added fields must be optional.
    EXPECT_EQ(GetIncompatibleIfStructOrElse(ESchemaCompatibility::FullyCompatible),
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int16(), "b", Uint16()),
            ConstructTypeShortcut("a", Int16(), "b", Uint16(), "c", String())));

    EXPECT_EQ(GetIncompatibleIfStructOrElse(ESchemaCompatibility::RequireValidation),
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int16()),
            ConstructTypeShortcut("a", Int8(), "b", Int8())));
}

TEST_P(TCommonStructTypesTest, RenamingFields)
{
    // Field renaming is not allowed by default.
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8()),
            ConstructTypeShortcut("not_a", Int8())));

    // Renaming fields isn't as simple as removing an old one and creating a new one.
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilityWithAllowedRenaming(
            ConstructTypeShortcut("a", Int8()),
            ConstructTypeShortcut("not_a", Int8())));

    // Proper way to rename a field.
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilityWithAllowedRenaming(
            ConstructType({GenerateSimpleField("a", "a")}),
            ConstructType({GenerateSimpleField("not_a", "a")})));
}

TEST_P(TCommonStructTypesTest, RemovingFields)
{
    // Field removal is not allowed by default.
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8(), "b", Int8()),
            ConstructTypeShortcut("a", Int8())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int8(), "b", Optional(Int8())),
            ConstructTypeShortcut("a", Int8())));

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilitySimple(
            ConstructTypeShortcut("a", Int16(), "b", Uint16(), "c", String()),
            ConstructTypeShortcut("a", Int16(), "b", Uint16())));
}

TEST_P(TCommonStructTypesTest, InsertingFields)
{
    // Inserting fields in the middle is not allowed by default.
    auto check = [&] (bool allowRenaming, bool allowRemoval) {
        TTypeCompatibilityOptions options{
            .AllowStructFieldRenaming = allowRenaming,
            .AllowStructFieldRemoval = allowRemoval,
        };

        auto expected = allowRenaming && allowRemoval
            ? ESchemaCompatibility::FullyCompatible
            : ESchemaCompatibility::Incompatible;

        EXPECT_EQ(GetIncompatibleIfStructOrElse(expected),
            CheckTypeCompatibility(
                ConstructTypeShortcut("a", Int8(), "b", Int8()),
                ConstructTypeShortcut("a", Int8(), "c", Int8(), "b", Int8()),
                options).first);

        EXPECT_EQ(expected,
            CheckTypeCompatibility(
                ConstructTypeShortcut("a", Int8(), "b", Int8()),
                ConstructTypeShortcut("a", Int8(), "c", Optional(Int8()), "b", Int8()),
                options).first);

        EXPECT_EQ(expected,
            CheckTypeCompatibility(
                ConstructTypeShortcut("a", Int8(), "b", Int8()),
                ConstructTypeShortcut("c", Optional(Int8()), "a", Int8(), "b", Int8()),
                options).first);
    };
    check(false, false);
    check(true, false);
    check(false, true);
    check(true, true);
}

TEST_P(TCommonStructTypesTest, ReorderingFields)
{
    auto check = [&] (bool allowRenaming, bool allowRemoval) {
        return CheckTypeCompatibility(
            ConstructTypeShortcut("a", Int16(), "b", Uint16()),
            ConstructTypeShortcut("b", Uint16(), "a", Int16()),
            TTypeCompatibilityOptions{
                .AllowStructFieldRenaming = allowRenaming,
                .AllowStructFieldRemoval = allowRemoval,
            }).first;
    };

    // Field reordering is not allowed by default.
    EXPECT_EQ(ESchemaCompatibility::Incompatible, check(false, false));

    // AllowFieldRenaming is not sufficient...
    EXPECT_EQ(ESchemaCompatibility::Incompatible, check(true, false));

    // ...and neither is AllowFieldRemoval.
    EXPECT_EQ(ESchemaCompatibility::Incompatible, check(false, true));

    // When both renaming and removal are allowed, fields can be reordered.
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, check(true, true));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStructsTest, TestRemovingFields)
{
    // Removing fields is not as simple as that.
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            Struct("a", Int8(), "b", Int8()),
            Struct("a", Int8())));

    // Proper way to remove fields.
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            StructLogicalType(
                {
                    GenerateSimpleField("a", "a"),
                    GenerateSimpleField("b", "b"),
                },
                /*removedFieldStableNames*/ {}),
            StructLogicalType(
                {GenerateSimpleField("a", "a")},
                /*removedFieldStableNames*/ {"b"})));

    // When field is removed, its stable name, not its regular name must be added to
    // removed field stable names list.
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {}),
            StructLogicalType({}, /*removedFieldStableNames*/ {"a"})));

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {}),
            StructLogicalType({}, /*removedFieldStableNames*/ {"stable_a"})));

    // Field removal is not limited to removing fields at the beginning or at the end;
    // any field can be removed.
    auto generateField = [] (int index) {
        auto name = Format("field_%v", index);
        return GenerateSimpleField(name, Format("stable_%v", name));
    };
    auto fieldRange = std::views::iota(0, 5) | std::views::transform(generateField);

    std::vector<TStructField> fields(fieldRange.begin(), fieldRange.end());
    auto type = StructLogicalType(fields, /*removedFieldStableNames*/ {});
    for (auto index : std::views::iota(0, 5)) {
        auto newFieldsRange = std::views::iota(0, 5)
            | std::views::filter([&] (int newIndex) { return newIndex != index; })
            | std::views::transform(generateField);

        auto newType = StructLogicalType(
            {newFieldsRange.begin(), newFieldsRange.end()},
            /*removedFieldStableNames*/ {generateField(index).StableName});

        EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
            CheckTypeCompatibilityWithAllowedRemoval(type, newType));
    }
}

TEST_P(TCommonStructTypesTest, RemovedFieldNames)
{
    // Unknown removed filed names can be added to the struct...
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {}),
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {"legacy_a"})));

    // ...unless it is explicitly prohibited...
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {}),
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {"legacy_a"}),
            TTypeCompatibilityOptions{
                .AllowStructFieldRemoval = true,
                .IgnoreUnknownRemovedFieldNames = false,
            }).first);

    // ...but once added, they cannot be removed.
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {"legacy_a"}),
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {})));

    // Raising from the dead is also not allowed...
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {"legacy_a"}),
            StructLogicalType(
                {
                    GenerateSimpleField("a", "stable_a"),
                    GenerateSimpleField("new_a", "legacy_a"),
                },
                /*removedFieldStableNames*/ {})));

    // ...although regular (non-stable) field names *can* be reused.
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {"legacy_a"}),
            StructLogicalType(
                {
                    GenerateSimpleField("a", "stable_a"),
                    GenerateSimpleField("legacy_a", "not_legacy"),
                },
                /*removedFieldStableNames*/ {"legacy_a"})));

    // Correct way of removing fields.
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            StructLogicalType(
                {GenerateSimpleField("a", "legacy_a")},
                /*removedFieldStableNames*/ {}),
            StructLogicalType({}, /*removedFieldStableNames*/ {"legacy_a"})));

    // Doing nothing is perfectly fine as well.
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {"legacy_a"}),
            StructLogicalType(
                {GenerateSimpleField("a", "stable_a")},
                /*removedFieldStableNames*/ {"legacy_a"})));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TVariantStructsTest, TestRemovingFields)
{
    // Removing fields is simpler than with structs...
    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilityWithAllowedRemoval(
            VariantStruct("a", Int8(), "b", Int8()),
            VariantStruct("a", Int8())));


    // ...however removing all fields is invalid.
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            VariantStruct("a", Int8(), "b", Int8()),
            VariantStruct()));

    // At least one field must me retained.
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibilityWithAllowedRemoval(
            VariantStruct("a", Int8(), "b", String(), "c", Bool()),
            VariantStruct("d", Float())));

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibilityWithAllowedRemoval(
            VariantStruct("a", Int8(), "b", String(), "c", Bool()),
            VariantStruct("b", String(), "d", Float())));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
