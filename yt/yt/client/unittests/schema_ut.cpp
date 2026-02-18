#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/schema_serialization_helpers.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <random>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NLogicalTypeShortcuts;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TColumnSchema ColumnFromYson(std::string_view yson)
{
    return ConvertTo<TConstrainedColumnSchema>(TYsonStringBuf(yson));
}

TEST(TTableSchemaTest, ColumnTypeV1Deserialization)
{
    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type=int64;"
            "}");
        EXPECT_EQ(*column.LogicalType(), *Optional(Int64()));
        EXPECT_EQ(column.IsOfV1Type(), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Int64), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Uint64), false);
        EXPECT_EQ(column.CastToV1Type(), ESimpleLogicalValueType::Int64);
        EXPECT_EQ(column.GetWireType(), EValueType::Int64);
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(IsV3Composite(column.LogicalType()), false);
    }

    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type=uint64;"
            "  required=%true"
            "}");
        EXPECT_EQ(*column.LogicalType(), *Uint64());
        EXPECT_EQ(column.IsOfV1Type(), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Uint64), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Int64), false);
        EXPECT_EQ(column.CastToV1Type(), ESimpleLogicalValueType::Uint64);
        EXPECT_EQ(column.GetWireType(), EValueType::Uint64);
        EXPECT_EQ(column.Required(), true);
        EXPECT_EQ(IsV3Composite(column.LogicalType()), false);
    }

    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type=null;"
            "}");
        EXPECT_EQ(*column.LogicalType(), *Null());
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(column.IsOfV1Type(), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Null), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Int64), false);
        EXPECT_EQ(column.CastToV1Type(), ESimpleLogicalValueType::Null);
        EXPECT_EQ(column.GetWireType(), EValueType::Null);
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(IsV3Composite(column.LogicalType()), false);
    }

    EXPECT_ANY_THROW(ColumnFromYson(
        "{"
        " name=x;"
        " type=null;"
        " required=%true;"
        "}"));
}

TEST(TTableSchemaTest, ColumnTypeV3Deserialization)
{
    auto listUtf8Column = ColumnFromYson(R"(
        {
          name=x;
          type_v3={
            type_name=list;
            item=utf8;
          }
        }
    )");
    EXPECT_EQ(*listUtf8Column.LogicalType(), *List(Utf8()));
    EXPECT_EQ(listUtf8Column.Required(), true);
    EXPECT_EQ(listUtf8Column.IsOfV1Type(), false);
    EXPECT_EQ(listUtf8Column.IsOfV1Type(ESimpleLogicalValueType::Utf8), false);
    EXPECT_EQ(listUtf8Column.IsOfV1Type(ESimpleLogicalValueType::Any), false);
    EXPECT_EQ(listUtf8Column.CastToV1Type(), ESimpleLogicalValueType::Any);
    EXPECT_EQ(listUtf8Column.GetWireType(), EValueType::Composite);
    EXPECT_EQ(IsV3Composite(listUtf8Column.LogicalType()), true);

    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=list;
                item=utf8;
              };
              required=%true;
            }
        )");
        EXPECT_EQ(column, listUtf8Column);
    }

    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=list;
                item=utf8;
              };
              type=any;
            }
        )");
        EXPECT_EQ(column, listUtf8Column);
    }

    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=optional;
                item={
                  type_name=optional;
                  item=utf8;
                }
              };
              type=any;
              required=%false;
            }
        )");
        EXPECT_EQ(*column.LogicalType(), *Optional(Optional(Utf8())));
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(column.IsOfV1Type(), false);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Utf8), false);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Any), false);
        EXPECT_EQ(column.CastToV1Type(), ESimpleLogicalValueType::Any);
        EXPECT_EQ(column.GetWireType(), EValueType::Composite);
        EXPECT_EQ(IsV3Composite(column.LogicalType()), true);
    }

    {
        auto decimalColumn = ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=decimal;
                precision=4;
                scale=2;
              }
            }
        )");
        EXPECT_EQ(*decimalColumn.LogicalType(), *Decimal(4, 2));
        EXPECT_EQ(decimalColumn.Required(), true);
        EXPECT_EQ(decimalColumn.IsOfV1Type(), false);
        EXPECT_EQ(decimalColumn.IsOfV1Type(ESimpleLogicalValueType::String), false);
        EXPECT_EQ(decimalColumn.IsOfV1Type(ESimpleLogicalValueType::Any), false);
        EXPECT_EQ(decimalColumn.CastToV1Type(), ESimpleLogicalValueType::String);
        EXPECT_EQ(decimalColumn.GetWireType(), EValueType::String);
        EXPECT_EQ(IsV3Composite(decimalColumn.LogicalType()), false);
    }

    EXPECT_THROW_WITH_SUBSTRING(
        ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=optional;
                item={
                  type_name=optional;
                  item=utf8;
                }
              };
              required=%true;
            }
        )"),
        R"("type_v3" does not match "required")");

    EXPECT_THROW_WITH_SUBSTRING(
        ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=optional;
                item={
                  type_name=optional;
                  item=utf8
                }
              };
              type=utf8;
            }
        )"),
        R"("type_v3" does not match "type")");
}

TEST(TTableSchemaTest, MaxInlineHunkSizeSerialization)
{
    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type=string;
            }
        )");
        auto serializedColumn = ConvertToAttributes(column);
        EXPECT_FALSE(serializedColumn->FindYson("max_inline_hunk_size").operator bool());
    }

    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type=string;
              max_inline_hunk_size=100
            }
        )");
        auto serializedColumn = ConvertToAttributes(column);
        EXPECT_EQ(100, serializedColumn->Get<i64>("max_inline_hunk_size"));
    }
}

TEST(TTableSchemaTest, MaxInlineHunkSizeDeserialization)
{
    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type=string;
            }
        )");
        EXPECT_FALSE(column.MaxInlineHunkSize().has_value());
    }

    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type=string;
              max_inline_hunk_size=100
            }
        )");
        EXPECT_EQ(column.MaxInlineHunkSize(), 100);
    }
}

TEST(TTableSchemaTest, ColumnSchemaValidation)
{
    auto expectBad = [] (const auto& schema) {
        EXPECT_THROW(ValidateColumnSchema(schema, true, true), std::exception);
    };

    // Empty names are not ok.
    expectBad(TColumnSchema("", EValueType::String));

    // Names starting from SystemColumnNamePrefix are not ok.
    expectBad(TColumnSchema(SystemColumnNamePrefix + "Name", EValueType::String));

    // Names longer than MaxColumnNameLength are not ok.
    expectBad(TColumnSchema(std::string(MaxColumnNameLength + 1, 'z'), EValueType::String));

    // Empty lock names are not ok.
    expectBad(
        TColumnSchema("Name", EValueType::String)
            .SetLock(""));

    // Locks on key columns are not ok.
    expectBad(
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetLock("LockName"));

    // Locks longer than MaxColumnLockLength are not ok.
    expectBad(
        TColumnSchema("Name", EValueType::String)
            .SetLock(std::string(MaxColumnLockLength + 1, 'z')));

    // Column type should be valid according to the ValidateSchemaValueType function.
    // Non-key columns can't be computed.
    expectBad(
        TColumnSchema("Name", EValueType::String)
            .SetExpression("SomeExpression"));

    // Key columns can't be aggregated.
    expectBad(
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetAggregate(std::string("sum")));

    ValidateColumnSchema(TColumnSchema("Name", EValueType::String));
    ValidateColumnSchema(TColumnSchema("Name", EValueType::Any));
    ValidateColumnSchema(
        TColumnSchema(std::string(256, 'z'), EValueType::String)
            .SetLock(std::string(256, 'z')));
    ValidateColumnSchema(
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression("SomeExpression"));
    ValidateColumnSchema(
        TColumnSchema("Name", EValueType::String)
            .SetAggregate(std::string("sum")));

    // Struct field validation
    expectBad(
        TColumnSchema("Column", StructLogicalType({
            {"", "", Int8()}
        }, /*removedFieldStableNames*/ {})));
    expectBad(
        TColumnSchema("Column", StructLogicalType({
            {std::string(257, 'a'), std::string(257, 'a'), Int8()}
        }, /*removedFieldStableNames*/ {})));

    expectBad(
        TColumnSchema("Column", StructLogicalType({
            {"\255", "\255", Int8()}
        }, /*removedFieldStableNames*/ {})));

    ValidateColumnSchema(
        TColumnSchema("Column", List(Int8()), ESortOrder::Ascending));

    expectBad(
        TColumnSchema("Column", List(Optional(Yson())), ESortOrder::Ascending));

    expectBad(
        TColumnSchema("Column", EValueType::String)
            .SetMaxInlineHunkSize(0));

    expectBad(
        TColumnSchema("Column", EValueType::String)
            .SetMaxInlineHunkSize(-1));

    expectBad(
        TColumnSchema("Column", EValueType::Int64)
            .SetMaxInlineHunkSize(100));

    expectBad(
        TColumnSchema("Column", EValueType::String, ESortOrder::Ascending)
            .SetMaxInlineHunkSize(100));

    ValidateColumnSchema(
        TColumnSchema("Column", EValueType::String)
            .SetMaxInlineHunkSize(100));

    ValidateColumnSchema(
        TColumnSchema("Column", EValueType::Any)
            .SetMaxInlineHunkSize(100));

    expectBad(
        TColumnSchema("Column", StructLogicalType({
            {"foo", "foo", Int64()},
            {"bar", "bar", String()},
        }, /*removedFieldStableNames*/ {}), ESortOrder::Ascending));

    // Allow some names starting from SystemColumnNamePrefix
    EXPECT_NO_THROW(
        ValidateColumnSchema(
            TColumnSchema(RowIndexColumnName, EValueType::Int64),
            /*isTableSorted*/ false,
            /*isTableDynamic*/ false,
            /*options*/ {.AllowOperationColumns = true})
    );
    EXPECT_NO_THROW(
        ValidateColumnSchema(
            TColumnSchema(RangeIndexColumnName, EValueType::Int64),
            /*isTableSorted*/ false,
            /*isTableDynamic*/ false,
            /*options*/ {.AllowOperationColumns = true})
    );
    EXPECT_THROW(
        ValidateColumnSchema(
            TColumnSchema(TableIndexColumnName, EValueType::Int64),
            /*isTableSorted*/ false,
            /*isTableDynamic*/ false,
            /*options*/ {.AllowOperationColumns = true}),
        std::exception);
    EXPECT_THROW(
        ValidateColumnSchema(
            TColumnSchema(EmptyValueColumnName, EValueType::Int64),
            /*isTableSorted*/ false,
            /*isTableDynamic*/ false,
            /*options*/ {.AllowOperationColumns = true}),
        std::exception);

}

TEST(TTableSchemaTest, ValidateTableSchemaTest)
{
    auto expectBad = [] (const auto& schemaString) {
        TTableSchema schema;
        Deserialize(schema, ConvertToNode(TYsonString(TStringBuf(schemaString))));

        EXPECT_THROW(ValidateTableSchema(schema, true), std::exception);
    };
    expectBad("[{name=x;type=int64;sort_order=ascending;expression=z}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]");
    expectBad("[{name=x;type=int64;sort_order=ascending;expression=y}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]");
    expectBad("[{name=x;type=int64;sort_order=ascending;expression=x}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]");
    expectBad("[{name=x;type=int64;sort_order=ascending;expression=\"uint64(y)\"}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]");
}

TEST(TTableSchemaTest, ColumnSchemaProtobufBackwardCompatibility)
{
    NProto::TColumnSchema columnSchemaProto;
    columnSchemaProto.set_name("foo");
    columnSchemaProto.set_type(ToProto(EValueType::Uint64));

    TColumnSchema columnSchema;
    FromProto(&columnSchema, columnSchemaProto);

    EXPECT_EQ(*columnSchema.LogicalType(), *Optional(Uint64()));
    EXPECT_EQ(columnSchema.GetWireType(), EValueType::Uint64);
    EXPECT_EQ(columnSchema.Name(), "foo");
    EXPECT_EQ(columnSchema.StableName().Underlying(), "foo");

    columnSchemaProto.set_simple_logical_type(ToProto(ESimpleLogicalValueType::Uint32));
    columnSchemaProto.set_name("foo");
    columnSchemaProto.set_stable_name("foo_stable");
    FromProto(&columnSchema, columnSchemaProto);

    EXPECT_EQ(*columnSchema.LogicalType(), *Optional(Uint32()));
    EXPECT_EQ(columnSchema.GetWireType(), EValueType::Uint64);
    EXPECT_EQ(columnSchema.Name(), "foo");
    EXPECT_EQ(columnSchema.StableName().Underlying(), "foo_stable");
}

TEST(TTableSchemaTest, EqualIgnoringRequiredness)
{
    auto schema1 = TTableSchema({
        TColumnSchema("foo", Int64()),
    });

    auto schema2 = TTableSchema({
        TColumnSchema("foo", Optional(Int64())),
    });

    auto schema3 = TTableSchema({
        TColumnSchema("foo", String()),
    });

    EXPECT_TRUE(schema1 != schema2);
    EXPECT_TRUE(IsEqualIgnoringRequiredness(schema1, schema2));
    EXPECT_FALSE(IsEqualIgnoringRequiredness(schema1, schema3));
}

TEST(TTableSchemaTest, ValidateTableSchemaNestedColumns)
{
    auto expectGood = [] (std::vector<TColumnSchema> columns) {
        columns.insert(columns.begin(), {
            TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("v", EValueType::Int64),
        });
        EXPECT_NO_THROW(ValidateTableSchema(TTableSchema(columns, true, true), true));
    };

    auto expectBad = [] (std::vector<TColumnSchema> columns) {
        columns.insert(columns.begin(), {
            TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("v", EValueType::Int64),
        });
        EXPECT_THROW(ValidateTableSchema(TTableSchema(columns, true, true), true), std::exception);
    };

    expectGood({
        TColumnSchema("nk", List(Int64()))
            .SetAggregate("nested_key(n)"),
    });

    // Invalid nested key description.
    expectBad({
        TColumnSchema("nk", List(Int64()))
            .SetAggregate("nested_key()"),
    });

    expectGood({
        TColumnSchema("nk", List(Int64()))
            .SetAggregate("nested_key(n)"),
        TColumnSchema("nv", List(Int64()))
            .SetAggregate("nested_value(n)")
    });

    expectGood({
        TColumnSchema("nk", List(Int64()))
            .SetAggregate("nested_key(n)"),
        TColumnSchema("nv", Optional(List(Int64())))
            .SetAggregate("nested_value(n)")
    });

    // Invalid nested value description.
    expectBad({
        TColumnSchema("nk", List(Int64()))
            .SetAggregate("nested_key(n)"),
        TColumnSchema("nv", List(Int64()))
            .SetAggregate("nested_value()"),
    });

    // No nested key column.
    expectBad({
        TColumnSchema("nv", List(Int64()))
            .SetAggregate("nested_value(n)"),
    });

    // No corresponding nested key column for nested value column.
    expectBad({
        TColumnSchema("nk", List(Int64()))
            .SetAggregate("nested_key(n)"),
        TColumnSchema("nv", List(Int64()))
            .SetAggregate("nested_value(m)"),
    });

    // Invalid aggregate.
    expectBad({
        TColumnSchema("a", List(Int64()))
            .SetAggregate("nested_()")
    });

    // Bad type of columns nv.
    expectBad({
        TColumnSchema("nk", List(Int64()))
            .SetAggregate("nested_key(n)"),
        TColumnSchema("nv", Int64())
            .SetAggregate("nested_value(n)")
    });

    expectGood({
        TColumnSchema("nk", List(Int64()))
            .SetAggregate("nested_key(n)"),
        TColumnSchema("nv1", List(Int64()))
            .SetAggregate("nested_value(n, sum)"),
        TColumnSchema("nv2", List(String()))
            .SetAggregate("nested_value(n)"),
    });

    expectGood({
        TColumnSchema("nk1", List(Int64()))
            .SetAggregate("nested_key(n)"),
        TColumnSchema("nk2", List(Int64()))
            .SetAggregate("nested_key(n)"),
        TColumnSchema("nv1", List(Int64()))
            .SetAggregate("nested_value(n, sum)"),
        TColumnSchema("nv2", List(String()))
            .SetAggregate("nested_value(n)"),
    });
}

TEST(TTableSchemaTest, WithSystemColumns)
{
    const auto schema1 = TTableSchema({
        TColumnSchema("foo", Int64()),
    });

    const auto schema2Ptr = schema1.WithSystemColumns({
        .EnableRangeIndex = true,
    });

    EXPECT_EQ(schema2Ptr->Columns().size(), 2u);
    EXPECT_TRUE(schema2Ptr->FindColumn("foo"));
    EXPECT_TRUE(schema2Ptr->FindColumn(RangeIndexColumnName));

    const auto schema3Ptr = schema1.WithSystemColumns({
        .EnableTableIndex = true,
        .EnableRowIndex = true,
        .EnableRangeIndex = true,
    });

    EXPECT_EQ(schema3Ptr->Columns().size(), 4u);
    EXPECT_TRUE(schema3Ptr->FindColumn("foo"));
    EXPECT_TRUE(schema3Ptr->FindColumn(TableIndexColumnName));
    EXPECT_TRUE(schema3Ptr->FindColumn(RowIndexColumnName));
    EXPECT_TRUE(schema3Ptr->FindColumn(RangeIndexColumnName));

    EXPECT_EQ(*schema3Ptr, *schema2Ptr->WithSystemColumns({
        .EnableTableIndex = true,
        .EnableRowIndex = true,
        .EnableRangeIndex = true,
    }));

    EXPECT_THROW_WITH_SUBSTRING(
        TTableSchema({
            TColumnSchema(RowIndexColumnName, String()),
        }).WithSystemColumns({.EnableRowIndex = true}),
        "Cannot add column");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TLockMaskTest, Simple)
{
    TLockMask mask;
    mask.Set(17, ELockType::SharedStrong);
    EXPECT_EQ(mask.Get(17), ELockType::SharedStrong);
}

TEST(TLockMaskTest, RandomChanges)
{
    constexpr int IterationCount = 1'000'000;
    constexpr int MaxLockIndex = 256;
    constexpr int MaxLockValue = static_cast<int>(TEnumTraits<ELockType>::GetMaxValue());

    std::mt19937 rng(42);

    TLockMask mask;
    std::vector<ELockType> locks(MaxLockIndex, ELockType::None);
    for (int iteration = 0; iteration < IterationCount; ++iteration) {
        if (rng() % 2 == 0) {
            int index = rng() % MaxLockIndex;
            EXPECT_EQ(locks[index], mask.Get(index));
        } else {
            int index = rng() % MaxLockIndex;
            auto value = CheckedEnumCast<ELockType>(rng() % MaxLockValue);
            mask.Set(index, value);
            locks[index] = value;
        }
    }
}

TEST(TLockMaskTest, ConvertToLegacy)
{
    TLockMask mask;
    mask.Set(2, ELockType::Exclusive);
    mask.Set(18, ELockType::SharedStrong);

    EXPECT_FALSE(mask.HasNewLocks());
    auto legacyLocks = mask.ToLegacyMask();
    for (int index = 0; index < TLegacyLockMask::MaxCount; ++index) {
        auto lock = legacyLocks.Get(index);
        if (index == 2) {
            EXPECT_EQ(lock, ELockType::Exclusive);
        } else if (index == 18) {
            EXPECT_EQ(lock, ELockType::SharedStrong);
        } else {
            EXPECT_EQ(lock, ELockType::None);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
