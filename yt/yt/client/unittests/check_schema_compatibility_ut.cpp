#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>

namespace NYT::NTableClient {

static void PrintTo(ESchemaCompatibility typeCompatibility, std::ostream* stream)
{
    (*stream) << "ESchemaCompatibility::" << ToString(typeCompatibility).c_str();
}

namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TTableSchemaCompatibilityTest
    : public ::testing::Test
{ };

TEST_F(TTableSchemaCompatibilityTest, CheckTableSchemaCompatibilityTest)
{
    EXPECT_EQ(
        ESchemaCompatibility::Incompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("foo", EValueType::Int64),
            }, /*strict*/ false),
            TTableSchema({
                TColumnSchema("foo", EValueType::Int64),
            }),
            {.IgnoreSortOrder = true}).first);

    EXPECT_EQ(
        ESchemaCompatibility::FullyCompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("foo", EValueType::Int64),
            }),
            TTableSchema({
                TColumnSchema("foo", EValueType::Int64),
            }),
            {.IgnoreSortOrder = true}).first);

    EXPECT_EQ(
        ESchemaCompatibility::Incompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("foo", EValueType::Int64),
                TColumnSchema("bar", EValueType::Int64),
            }),
            TTableSchema({
                TColumnSchema("foo", EValueType::Int64),
            }),
            {.IgnoreSortOrder = true}).first);

    EXPECT_EQ(
        ESchemaCompatibility::Incompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("foo", EValueType::Int64),
            }),
            TTableSchema({
                TColumnSchema("foo", EValueType::Uint64),
            }),
            {.IgnoreSortOrder = true}).first);

    EXPECT_EQ(
        ESchemaCompatibility::FullyCompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("foo", ESimpleLogicalValueType::Int32),
            }),
            TTableSchema({
                TColumnSchema("foo", ESimpleLogicalValueType::Int64),
            }),
            {.IgnoreSortOrder = true}).first);

    EXPECT_EQ(
        ESchemaCompatibility::RequireValidation,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("foo", ESimpleLogicalValueType::Int32),
            }),
            TTableSchema({
                TColumnSchema("foo", ESimpleLogicalValueType::Int8),
            }),
        {.IgnoreSortOrder = true}).first);

    EXPECT_EQ(
        ESchemaCompatibility::FullyCompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("foo", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
            }),
            TTableSchema({
                TColumnSchema("foo", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
            }),
        {.IgnoreSortOrder = true}).first);

    EXPECT_EQ(
        ESchemaCompatibility::RequireValidation,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("foo", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
            }),
            TTableSchema({
                TColumnSchema("foo", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
            }),
            {.IgnoreSortOrder = true}).first);

    // Missing "foo" values are filled with nulls that's OK.
    EXPECT_EQ(
        ESchemaCompatibility::FullyCompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({}, /*strict*/ true),
            TTableSchema({
                TColumnSchema("foo", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
            }),
        {.IgnoreSortOrder = true}).first);

    // First table might have column foo with value of any type
    EXPECT_EQ(
        ESchemaCompatibility::Incompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({}, /*strict*/ false),
            TTableSchema({
                TColumnSchema("foo", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
            }),
            {.IgnoreSortOrder = true}).first);

    // Missing "foo" values are filled with nulls that's OK.
    EXPECT_EQ(
        ESchemaCompatibility::FullyCompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({}, /*strict*/ true),
            TTableSchema({
                TColumnSchema("foo", ESimpleLogicalValueType::Int64),
            }),
            {.IgnoreSortOrder = true}).first);

    EXPECT_EQ(
        ESchemaCompatibility::Incompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("a", ListLogicalType(SimpleLogicalType((ESimpleLogicalValueType::Int64)))),
                TColumnSchema("b", ESimpleLogicalValueType::Int64),
            }, /*strict*/ true),
            TTableSchema({
                TColumnSchema("b", ESimpleLogicalValueType::Int64),
            }, /*strict*/ false),
            {.IgnoreSortOrder = true}).first);

    //
    // ignoreSortOrder = false
    EXPECT_EQ(
        ESchemaCompatibility::FullyCompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
                TColumnSchema("b", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
            }),
            TTableSchema({
                TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
                TColumnSchema("b", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
            }),
            {.IgnoreSortOrder = false}).first);

    EXPECT_EQ(
        ESchemaCompatibility::Incompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
                TColumnSchema("b", ESimpleLogicalValueType::Int64),
            }),
            TTableSchema({
                TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
                TColumnSchema("b", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
            }),
            {.IgnoreSortOrder = false}).first);

    EXPECT_EQ(
        ESchemaCompatibility::FullyCompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
                TColumnSchema("b", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
            }),
            TTableSchema({
                TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
                TColumnSchema("b", ESimpleLogicalValueType::Int64),
            }),
            {.IgnoreSortOrder = false}).first);

    EXPECT_EQ(
        ESchemaCompatibility::Incompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
                TColumnSchema("b", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
            }, /*strict*/ true, /*uniqueKeys*/ true),
            TTableSchema({
                TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
                TColumnSchema("b", ESimpleLogicalValueType::Int64),
            }, /*strict*/ true, /*uniqueKeys*/ true),
            {.IgnoreSortOrder = false}).first);

    EXPECT_EQ(
        ESchemaCompatibility::FullyCompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
                TColumnSchema("b", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
            }, /*strict*/ true, /*uniqueKeys*/ true),
            TTableSchema({
                TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
                TColumnSchema("b", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
            }, /*strict*/ true, /*uniqueKeys*/ true),
            {.IgnoreSortOrder = false}).first);

    EXPECT_EQ(
        ESchemaCompatibility::Incompatible,
        CheckTableSchemaCompatibility(
        TTableSchema({
            TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("b", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
        }),
        TTableSchema({
            TColumnSchema("b", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("a", ESimpleLogicalValueType::Int64, ESortOrder::Ascending),
        }),
        {.IgnoreSortOrder = false}).first);
}

TEST_F(TTableSchemaCompatibilityTest, TestCheckTableSchemaCompatibility)
{
    EXPECT_EQ(ESchemaCompatibility::Incompatible,
        CheckTableSchemaCompatibility(
        TTableSchema({
            TColumnSchema("key1", ESimpleLogicalValueType::String, ESortOrder::Ascending),
            TColumnSchema("key2", ESimpleLogicalValueType::String, ESortOrder::Ascending),
            TColumnSchema("value", ESimpleLogicalValueType::String),
        }),
        TTableSchema({
            TColumnSchema("key1", ESimpleLogicalValueType::String, ESortOrder::Ascending),
            TColumnSchema("key2", ESimpleLogicalValueType::String, ESortOrder::Descending),
            TColumnSchema("value", ESimpleLogicalValueType::String),
        }),
        {.IgnoreSortOrder = false}).first);

    EXPECT_EQ(
        ESchemaCompatibility::FullyCompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("key1", ESimpleLogicalValueType::String, ESortOrder::Descending),
                TColumnSchema("key2", ESimpleLogicalValueType::String, ESortOrder::Descending),
                TColumnSchema("value", ESimpleLogicalValueType::String),
            }),
            TTableSchema({
                TColumnSchema("key1", ESimpleLogicalValueType::String, ESortOrder::Descending),
                TColumnSchema("key2", ESimpleLogicalValueType::String),
                TColumnSchema("value", ESimpleLogicalValueType::String),
            }),
            {.IgnoreSortOrder = false}).first);

    EXPECT_EQ(
        ESchemaCompatibility::Incompatible,
        CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("key1", ESimpleLogicalValueType::String, ESortOrder::Descending),
                TColumnSchema("key2", ESimpleLogicalValueType::String),
                TColumnSchema("value", ESimpleLogicalValueType::String),
            }),
            TTableSchema({
                TColumnSchema("key1", ESimpleLogicalValueType::String, ESortOrder::Descending),
                TColumnSchema("key2", ESimpleLogicalValueType::String, ESortOrder::Descending),
                TColumnSchema("value", ESimpleLogicalValueType::String),
            }),
            {.IgnoreSortOrder = false}).first);
}

TEST_F(TTableSchemaCompatibilityTest, DeletedColumns)
{
    auto comp = CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("key1", ESimpleLogicalValueType::String, ESortOrder::Descending),
                TColumnSchema("value", ESimpleLogicalValueType::String),
            }, true, true, ETableSchemaModification::None,
            {
                TDeletedColumn(TColumnStableName("value1")),
            }),
            TTableSchema({
                TColumnSchema("key1", ESimpleLogicalValueType::String, ESortOrder::Descending),
                TColumnSchema("value", ESimpleLogicalValueType::String),
                TColumnSchema("value1", ESimpleLogicalValueType::String),
            }, true, true, {}),
        {.IgnoreSortOrder = false});

    EXPECT_EQ(ESchemaCompatibility::Incompatible, comp.first);
    EXPECT_EQ("Column \"value1\" in output schema was deleted in the input schema",
        comp.second.InnerErrors()[0].GetMessage());


    comp = CheckTableSchemaCompatibility(
            TTableSchema({
                TColumnSchema("key1", ESimpleLogicalValueType::String, ESortOrder::Descending),
                TColumnSchema("value", ESimpleLogicalValueType::String),
            }, true, true, ETableSchemaModification::None, {}),
            TTableSchema({
                TColumnSchema("key1", ESimpleLogicalValueType::String, ESortOrder::Descending),
                TColumnSchema("value", ESimpleLogicalValueType::String),
            }, true, true, ETableSchemaModification::None,
            {
                TDeletedColumn(TColumnStableName("value1")),
            }),
        {.IgnoreSortOrder = false});

    EXPECT_EQ(ESchemaCompatibility::Incompatible, comp.first);
    EXPECT_EQ("Deleted column \"value1\" is missing in the input schema",
        comp.second.InnerErrors()[0].GetMessage());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
