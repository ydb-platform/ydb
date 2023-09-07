#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/complex_types/check_type_compatibility.h>

using namespace NYT::NComplexTypes;
using namespace NYT::NTableClient::NLogicalTypeShortcuts;

using NYT::NTableClient::ESchemaCompatibility;

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

static void PrintTo(ESchemaCompatibility typeCompatibility, std::ostream* stream)
{
    (*stream) << "ESchemaCompatibility::" << ToString(typeCompatibility).c_str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes

TEST(TCheckTypeCompatibilityTest, SimpleTypes)
{
    // Int
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibility(Int8(), Int16()).first);
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibility(Int8(), Int64()).first);
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibility(Int16(), Int64()).first);
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibility(Int16(), Int8()).first);
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibility(Int64(), Int8()).first);
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibility(Int64(), Int32()).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibility(Int8(), Uint64()).first);
    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibility(Int8(), Double()).first);
    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibility(Int8(), String()).first);

    // Uint
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibility(Uint8(), Uint16()).first);
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibility(Uint8(), Uint64()).first);
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibility(Uint16(), Uint64()).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibility(Uint16(), Uint8()).first);
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibility(Uint64(), Uint8()).first);
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibility(Uint64(), Uint32()).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibility(Uint8(), Int64()).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibility(Uint8(), Double()).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibility(Uint8(), String()).first);

    // Float
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibility(Float(), Double()).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibility(Double(), Float()).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibility(Float(), String()).first);
    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibility(Double(), Int64()).first);

    // String
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible, CheckTypeCompatibility(Utf8(), String()).first);
    EXPECT_EQ(ESchemaCompatibility::RequireValidation, CheckTypeCompatibility(String(), Utf8()).first);
    EXPECT_EQ(ESchemaCompatibility::Incompatible, CheckTypeCompatibility(String(), Json()).first);
}

TEST(TCheckTypeCompatibilityTest, Optional)
{
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Int8(),
            Optional(Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            Optional(Int8()),
            Int8()).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Int8(),
            Optional(Optional(Int8()))).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Optional(Optional(Int8())),
            Int8()).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Optional(Int8()),
            Optional(Optional(Int8()))).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Optional(Optional(Int8())),
            Optional(Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Optional(Optional(Int8())),
            Optional(Optional(Int8()))).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Optional(Int8()),
            Optional(Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Int8(),
            Optional(Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            Int32(),
            Optional(Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            Optional(List(Int64())),
            List(Int64())).first);
}

TEST(TCheckTypeCompatibilityTest, List)
{
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Int8(),
            List(Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            List(Int8()),
            Int8()).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            List(List(Int8())),
            List(List(Int8()))).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            List(Int8()),
            List(Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            List(Int8()),
            List(Optional(Int16()))).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            List(Int32()),
            List(Optional(Int16()))).first);
}

TEST(TCheckTypeCompatibilityTest, Tuple)
{
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Int8(),
            Tuple(Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Tuple(Int8()),
            Int8()).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Tuple(Int8()),
            Tuple(Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Tuple(Int8()),
            Tuple(Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Tuple(Int8(), Int8()),
            Tuple(Int16(), Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Tuple(Int8(), Int8()),
            Tuple(Int16(), Int32())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Tuple(Int8()),
            Tuple(Optional(Int16()))).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            Tuple(Int32()),
            Tuple(Optional(Int16()))).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Tuple(Int8(), Int8()),
            Tuple(Int8(), Int8(), Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Tuple(Int8(), Int8()),
            Tuple(Int8(), Int8(), Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Tuple(Int8(), Int8(), Int8()),
            Tuple(Int8(), Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Tuple(Int8(), Int8()),
            Tuple(Int8(), Int8(), Optional(Int8()))).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Tuple(Int8(), Int8(), Optional(Int8())),
            Tuple(Int8(), Int8())).first);
}

TEST(TCheckTypeCompatibilityTest, Struct)
{
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Int8(),
            Struct("a", Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Struct("a", Int8()),
            Int8()).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Struct("a", Int8()),
            Struct("a", Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Struct("a", Int8()),
            Struct("b", Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Struct("a", Int8()),
            Struct("a", Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            Struct("a", Int16()),
            Struct("a", Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Struct("a", Int16()),
            Struct("a", Int8(), "b", Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Struct("a", Int8()),
            Struct("a", Int8(), "b", Optional(Int8()))).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Struct("a", Int8()),
            Struct("a", Int8(), "b", Optional(Optional(Int8())))).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Struct("a", Int8()),
            Struct("a", Int8(), "b", Null())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Struct("a", Int8()),
            Struct("a", Int8(), "b", Optional(List(String())))).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Struct("a", Int8(), "b", Int8()),
            Struct("a", Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Struct("a", Int8(), "b", Optional(Int8())),
            Struct("a", Int8())).first);
}

TEST(TCheckTypeCompatibilityTest, VariantTuple)
{
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Int8(),
            VariantTuple(Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            VariantTuple(Int8()),
            Int8()).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            VariantTuple(Int8()),
            VariantTuple(Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            VariantTuple(Int8(), Uint8()),
            VariantTuple(Int16(), Uint8())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            VariantTuple(Int8(), Uint8()),
            VariantTuple(Int8(), Uint16())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            VariantTuple(Int8(), Uint8()),
            VariantTuple(Int32(), Uint16())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            VariantTuple(Int16(), Uint8()),
            VariantTuple(Int8(), Uint16())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            VariantTuple(Int16(), Uint16()),
            VariantTuple(Int8(), Uint16())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            VariantTuple(Int16(), Uint16()),
            VariantTuple(Int16(), Uint8())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            VariantTuple(Int16(), Uint16()),
            VariantTuple(Int16(), Uint8())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            VariantTuple(Int16(), Uint16()),
            VariantTuple(Int16(), Uint16(), String())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            VariantTuple(Int16(), Uint16(), String()),
            VariantTuple(Int16(), Uint16())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            VariantTuple(Int16(), Uint16(), String()),
            VariantTuple(Int16(), Uint16())).first);
}

TEST(TCheckTypeCompatibilityTest, VariantStruct)
{
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Int8(),
            VariantStruct("a", Int8())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            VariantStruct("a", Int8()),
            Int8()).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            VariantStruct("a", Int8()),
            VariantStruct("a", Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            VariantStruct("a", Int8(), "b", Uint8()),
            VariantStruct("a", Int16(), "b", Uint8())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            VariantStruct("a", Int8(), "b", Uint8()),
            VariantStruct("a", Int8(), "b", Uint16())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            VariantStruct("a", Int8(), "b", Uint8()),
            VariantStruct("a", Int32(), "b", Uint16())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            VariantStruct("a", Int16(), "b", Uint8()),
            VariantStruct("a", Int8(), "b", Uint16())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            VariantStruct("a", Int16(), "b", Uint16()),
            VariantStruct("a", Int8(), "b", Uint16())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            VariantStruct("a", Int16(), "b", Uint16()),
            VariantStruct("a", Int16(), "b", Uint8())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            VariantStruct("a", Int16(), "b", Uint16()),
            VariantStruct("a", Int16(), "b", Uint8())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            VariantStruct("a", Int16(), "b", Uint16()),
            VariantStruct("a", Int16(), "b", Uint16(), "c", String())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            VariantStruct("a", Int16(), "b", Uint16(), "c", String()),
            VariantStruct("a", Int16(), "b", Uint16())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            VariantStruct("a", Int16(), "b", Uint16(), "c", String()),
            VariantStruct("a", Int16(), "b", Uint16())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            VariantStruct("a", Int16(), "b", Uint16()),
            VariantStruct("b", Uint16(), "a", Int16())).first);
}

TEST(TCheckTypeCompatibilityTest, Dict)
{
    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Dict(Utf8(), Int16()),
            Dict(Utf8(), Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Dict(Utf8(), Int16()),
            Dict(Utf8(), Int32())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Dict(Utf8(), Int16()),
            Dict(String(), Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::FullyCompatible,
        CheckTypeCompatibility(
            Dict(Utf8(), Int16()),
            Dict(String(), Int32())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            Dict(String(), Int32()),
            Dict(Utf8(), Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            Dict(String(), Int32()),
            Dict(Utf8(), Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::RequireValidation,
        CheckTypeCompatibility(
            Dict(Utf8(), Int32()),
            Dict(String(), Int16())).first);

    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTypeCompatibility(
            Dict(Utf8(), Int32()),
            Dict(String(), Uint16())).first);
}
