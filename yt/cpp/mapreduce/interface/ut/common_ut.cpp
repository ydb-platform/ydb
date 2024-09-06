#include "common_ut.h"

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/fluent.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_builder.h>

#include <util/generic/xrange.h>

using namespace NYT;

template <class T>
TString SaveToString(const T& obj)
{
    TString s;
    TStringOutput out(s);
    ::Save(&out, obj);
    return s;
}

template <class T>
T LoadFromString(TStringBuf s)
{
    TMemoryInput in(s);
    T obj;
    ::Load(&in, obj);
    return obj;
}

template <class T>
T SaveLoad(const T& obj)
{
    return LoadFromString<T>(SaveToString(obj));
}

TEST(TCommonTest, SortColumnsLegacy)
{
    TSortColumns keys1("a", "b");
    EXPECT_TRUE((keys1.Parts_ == TSortColumns{"a", "b"}));

    keys1.Add("c", "d");
    EXPECT_TRUE((keys1.Parts_ == TSortColumns{"a", "b", "c", "d"}));

    auto keys2 = TSortColumns(keys1).Add("e", "f");
    EXPECT_TRUE((keys1.Parts_ == TSortColumns{"a", "b", "c", "d"}));
    EXPECT_TRUE((keys2.Parts_ == TSortColumns{"a", "b", "c", "d", "e", "f"}));

    auto keys3 = TSortColumns(keys1).Add("e").Add("f").Add("g");
    EXPECT_TRUE((keys1.Parts_ == TSortColumns{"a", "b", "c", "d"}));
    EXPECT_TRUE((keys3.Parts_ == TSortColumns{"a", "b", "c", "d", "e", "f", "g"}));
}

TEST(TCommonTest, SortColumn)
{
    auto ascending = TSortColumn("a");
    EXPECT_EQ(ascending.Name(), "a");
    EXPECT_EQ(ascending.SortOrder(), ESortOrder::SO_ASCENDING);
    EXPECT_EQ(ascending, TSortColumn("a", ESortOrder::SO_ASCENDING));
    EXPECT_NE(ascending, TSortColumn("a", ESortOrder::SO_DESCENDING));

    EXPECT_NO_THROW(ascending.EnsureAscending());
    EXPECT_EQ(static_cast<TString>(ascending), "a");
    EXPECT_EQ(ascending, "a");

    auto another = ascending;
    EXPECT_NO_THROW(another = "another");
    EXPECT_EQ(another.Name(), "another");
    EXPECT_EQ(another.SortOrder(), ESortOrder::SO_ASCENDING);
    EXPECT_EQ(another, TSortColumn("another", ESortOrder::SO_ASCENDING));
    EXPECT_NE(another, TSortColumn("another", ESortOrder::SO_DESCENDING));

    auto ascendingNode = BuildYsonNodeFluently().Value(ascending);
    EXPECT_EQ(ascendingNode, TNode("a"));

    EXPECT_EQ(SaveLoad(ascending), ascending);
    EXPECT_NE(SaveToString(ascending), SaveToString(TString("a")));

    auto descending = TSortColumn("a", ESortOrder::SO_DESCENDING);
    EXPECT_EQ(descending.Name(), "a");
    EXPECT_EQ(descending.SortOrder(), ESortOrder::SO_DESCENDING);
    EXPECT_EQ(descending, TSortColumn("a", ESortOrder::SO_DESCENDING));
    EXPECT_NE(descending, TSortColumn("a", ESortOrder::SO_ASCENDING));

    EXPECT_THROW(descending.EnsureAscending(), yexception);
    EXPECT_THROW(Y_UNUSED(static_cast<TString>(descending)), yexception);
    EXPECT_THROW(Y_UNUSED(descending == "a"), yexception);
    EXPECT_THROW(descending = "a", yexception);

    auto descendingNode = BuildYsonNodeFluently().Value(descending);
    EXPECT_EQ(descendingNode, TNode()("name", "a")("sort_order", "descending"));

    EXPECT_EQ(SaveLoad(descending), descending);
    EXPECT_NE(SaveToString(descending), SaveToString("a"));

    EXPECT_EQ(ToString(TSortColumn("blah")), "blah");
    EXPECT_EQ(ToString(TSortColumn("blah", ESortOrder::SO_DESCENDING)), "{\"name\"=\"blah\";\"sort_order\"=\"descending\"}");
}

TEST(TCommonTest, SortColumns)
{
    TSortColumns ascending("a", "b");
    EXPECT_TRUE(ascending.Parts_ == (TSortColumns{"a", "b"}));
    EXPECT_NO_THROW(ascending.EnsureAscending());
    EXPECT_EQ(static_cast<TColumnNames>(ascending).Parts_, (TVector<TString>{"a", "b"}));
    EXPECT_EQ(ascending.GetNames(), (TVector<TString>{"a", "b"}));

    auto mixed = ascending;
    mixed.Add(TSortColumn("c", ESortOrder::SO_DESCENDING), "d");
    EXPECT_TRUE((mixed.Parts_ != TVector<TSortColumn>{"a", "b", "c", "d"}));
    EXPECT_TRUE((mixed.Parts_ == TVector<TSortColumn>{"a", "b", TSortColumn("c", ESortOrder::SO_DESCENDING), "d"}));
    EXPECT_EQ(mixed.GetNames(), (TVector<TString>{"a", "b", "c", "d"}));
    EXPECT_THROW(mixed.EnsureAscending(), yexception);
    EXPECT_THROW(Y_UNUSED(static_cast<TColumnNames>(mixed)), yexception);
}

TEST(TCommonTest, KeyBound)
{
    auto keyBound = TKeyBound(ERelation::Greater, TKey(7, "a", TNode()("x", "y")));
    EXPECT_EQ(keyBound.Relation(), ERelation::Greater);
    EXPECT_EQ(keyBound.Key(), TKey(7, "a", TNode()("x", "y")));

    auto keyBound1 = TKeyBound().Relation(ERelation::Greater).Key(TKey(7, "a", TNode()("x", "y")));
    auto expectedNode = TNode()
        .Add(">")
        .Add(TNode().Add(7).Add("a").Add(TNode()("x", "y")));

    EXPECT_EQ(expectedNode, BuildYsonNodeFluently().Value(keyBound));
    EXPECT_EQ(expectedNode, BuildYsonNodeFluently().Value(keyBound1));

    keyBound.Relation(ERelation::LessOrEqual);
    keyBound.Key(TKey("A", 7));
    EXPECT_EQ(keyBound.Relation(), ERelation::LessOrEqual);
    EXPECT_EQ(keyBound.Key(), TKey("A", 7));

    EXPECT_EQ(
        BuildYsonNodeFluently().Value(keyBound),
        TNode()
            .Add("<=")
            .Add(TNode().Add("A").Add(7)));
}

TEST(TCommonTest, TTableSchema)
{
    TTableSchema schema;
    schema
        .AddColumn(TColumnSchema().Name("a").Type(EValueType::VT_STRING).SortOrder(SO_ASCENDING))
        .AddColumn(TColumnSchema().Name("b").Type(EValueType::VT_UINT64))
        .AddColumn(TColumnSchema().Name("c").Type(EValueType::VT_INT64));
    auto checkSortBy = [](TTableSchema schema, const TVector<TString>& columns) {
        auto initialSchema = schema;
        schema.SortBy(columns);
        for (auto i: xrange(columns.size())) {
            EXPECT_EQ(schema.Columns()[i].Name(), columns[i]);
            EXPECT_EQ(schema.Columns()[i].SortOrder(), ESortOrder::SO_ASCENDING);
        }
        for (auto i: xrange(columns.size(), (size_t)initialSchema.Columns().size())) {
            EXPECT_EQ(schema.Columns()[i].SortOrder(), Nothing());
        }
        EXPECT_EQ(initialSchema.Columns().size(), schema.Columns().size());
        return schema;
    };
    auto newSchema = checkSortBy(schema, {"b"});
    EXPECT_EQ(newSchema.Columns()[1].Name(), TString("a"));
    EXPECT_EQ(newSchema.Columns()[2].Name(), TString("c"));
    checkSortBy(schema, {"b", "c"});
    checkSortBy(schema, {"c", "a"});
    EXPECT_THROW(checkSortBy(schema, {"b", "b"}), yexception);
    EXPECT_THROW(checkSortBy(schema, {"a", "junk"}), yexception);
}

TEST(TCommonTest, TTableSchema_Decimal)
{
    NYT::TTableSchema tableSchema;

    tableSchema.AddColumn("a", NTi::Decimal(35, 18));
    tableSchema.AddColumn("b", NTi::Optional(NTi::Decimal(35, 18)));
    tableSchema.AddColumn("c", NTi::List(NTi::Decimal(35, 18)));

    auto tableSchemaNode = tableSchema.ToNode();
    const auto& tableSchemaNodeList = tableSchemaNode.AsList();

    // There was a bug in the serialization of decimal type: https://github.com/ytsaurus/ytsaurus/issues/173
    {
        const auto& currentType = tableSchemaNodeList[0];
        EXPECT_EQ(currentType.ChildAsString("type"), "string");
        EXPECT_TRUE(currentType.ChildAsBool("required"));
        EXPECT_TRUE(currentType.HasKey("type_v3"));
        EXPECT_EQ(currentType.At("type_v3").ChildAsString("type_name"), "decimal");
    }
    {
        const auto& currentType = tableSchemaNodeList[1];
        EXPECT_EQ(currentType.ChildAsString("type"), "string");
        EXPECT_TRUE(!currentType.ChildAsBool("required"));
        EXPECT_TRUE(currentType.HasKey("type_v3"));
        EXPECT_EQ(currentType.At("type_v3").ChildAsString("type_name"), "optional");
        EXPECT_EQ(currentType.At("type_v3").At("item").ChildAsString("type_name"), "decimal");
    }
    {
        const auto& currentType = tableSchemaNodeList[2];
        EXPECT_EQ(currentType.ChildAsString("type"), "any");
        EXPECT_TRUE(currentType.ChildAsBool("required"));
        EXPECT_TRUE(currentType.HasKey("type_v3"));
        EXPECT_EQ(currentType.At("type_v3").ChildAsString("type_name"), "list");
        EXPECT_EQ(currentType.At("type_v3").At("item").ChildAsString("type_name"), "decimal");
    }

    EXPECT_EQ(tableSchema, TTableSchema::FromNode(tableSchemaNode));
}

TEST(TCommonTest, TColumnSchema_TypeV3)
{
    {
        auto column = TColumnSchema().Type(NTi::Interval());
        EXPECT_EQ(column.Required(), true);
        EXPECT_EQ(column.Type(), VT_INTERVAL);
    }
    {
        auto column = TColumnSchema().Type(NTi::Optional(NTi::Date()));
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(column.Type(), VT_DATE);
    }
    {
        auto column = TColumnSchema().Type(NTi::Interval64());
        EXPECT_EQ(column.Required(), true);
        EXPECT_EQ(column.Type(), VT_INTERVAL64);
    }
    {
        auto column = TColumnSchema().Type(NTi::Optional(NTi::Date32()));
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(column.Type(), VT_DATE32);
    }
    {
        auto column = TColumnSchema().Type(NTi::Null());
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(column.Type(), VT_NULL);
    }
    {
        auto column = TColumnSchema().Type(NTi::Optional(NTi::Null()));
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(column.Type(), VT_ANY);
    }
    {
        auto column = TColumnSchema().Type(NTi::Decimal(35, 18));
        EXPECT_EQ(column.Required(), true);
        EXPECT_EQ(column.Type(), VT_STRING);
    }
}

TEST(TCommonTest, ToTypeV3)
{
    EXPECT_EQ(*ToTypeV3(VT_INT32, true), *NTi::Int32());
    EXPECT_EQ(*ToTypeV3(VT_UTF8, false), *NTi::Optional(NTi::Utf8()));
}

TEST(TCommonTest, DeserializeColumn)
{
    auto deserialize = [] (TStringBuf yson) {
        auto node = NodeFromYsonString(yson);
        TColumnSchema column;
        Deserialize(column, node);
        return column;
    };

    auto column = deserialize("{name=foo; type=int64; required=%false}");
    EXPECT_EQ(column.Name(), "foo");
    EXPECT_EQ(*column.TypeV3(), *NTi::Optional(NTi::Int64()));

    column = deserialize("{name=bar; type=utf8; required=%true; type_v3=utf8}");
    EXPECT_EQ(column.Name(), "bar");
    EXPECT_EQ(*column.TypeV3(), *NTi::Utf8());
}

TEST(TCommonTest, ColumnSchemaEquality)
{
    auto base = TColumnSchema()
        .Name("col")
        .TypeV3(NTi::Optional(NTi::List(NTi::String())))
        .SortOrder(ESortOrder::SO_ASCENDING)
        .Lock("lock")
        .Expression("x + 12")
        .Aggregate("sum")
        .Group("group");

    auto other = base;
    ASSERT_SERIALIZABLES_EQ(other, base);
    other.Name("other");
    ASSERT_SERIALIZABLES_NE(other, base);

    other = base;
    other.TypeV3(NTi::List(NTi::String()));
    ASSERT_SERIALIZABLES_NE(other, base);

    other = base;
    other.ResetSortOrder();
    ASSERT_SERIALIZABLES_NE(other, base);

    other = base;
    other.Lock("lock1");
    ASSERT_SERIALIZABLES_NE(other, base);

    other = base;
    other.Expression("x + 13");
    ASSERT_SERIALIZABLES_NE(other, base);

    other = base;
    other.ResetAggregate();
    ASSERT_SERIALIZABLES_NE(other, base);

    other = base;
    other.Group("group1");
    ASSERT_SERIALIZABLES_NE(other, base);
}

TEST(TCommonTest, TableSchemaEquality)
{
    auto col1 = TColumnSchema()
        .Name("col1")
        .TypeV3(NTi::Optional(NTi::List(NTi::String())))
        .SortOrder(ESortOrder::SO_ASCENDING);

    auto col2 = TColumnSchema()
        .Name("col2")
        .TypeV3(NTi::Uint32());

    auto schema = TTableSchema()
        .AddColumn(col1)
        .AddColumn(col2)
        .Strict(true)
        .UniqueKeys(true);

    auto other = schema;
    ASSERT_SERIALIZABLES_EQ(other, schema);

    other.Strict(false);
    ASSERT_SERIALIZABLES_NE(other, schema);

    other = schema;
    other.MutableColumns()[0].TypeV3(NTi::List(NTi::String()));
    ASSERT_SERIALIZABLES_NE(other, schema);

    other = schema;
    other.MutableColumns().push_back(col1);
    ASSERT_SERIALIZABLES_NE(other, schema);

    other = schema;
    other.UniqueKeys(false);
    ASSERT_SERIALIZABLES_NE(other, schema);
}
