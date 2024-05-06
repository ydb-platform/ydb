#include "common_ut.h"

#include "fluent.h"

#include <yt/cpp/mapreduce/interface/common.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

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

Y_UNIT_TEST_SUITE(Common)
{
    Y_UNIT_TEST(SortColumnsLegacy)
    {
        TSortColumns keys1("a", "b");
        UNIT_ASSERT((keys1.Parts_ == TSortColumns{"a", "b"}));

        keys1.Add("c", "d");
        UNIT_ASSERT((keys1.Parts_ == TSortColumns{"a", "b", "c", "d"}));

        auto keys2 = TSortColumns(keys1).Add("e", "f");
        UNIT_ASSERT((keys1.Parts_ == TSortColumns{"a", "b", "c", "d"}));
        UNIT_ASSERT((keys2.Parts_ == TSortColumns{"a", "b", "c", "d", "e", "f"}));

        auto keys3 = TSortColumns(keys1).Add("e").Add("f").Add("g");
        UNIT_ASSERT((keys1.Parts_ == TSortColumns{"a", "b", "c", "d"}));
        UNIT_ASSERT((keys3.Parts_ == TSortColumns{"a", "b", "c", "d", "e", "f", "g"}));
    }

    Y_UNIT_TEST(SortColumn)
    {
        auto ascending = TSortColumn("a");
        UNIT_ASSERT_VALUES_EQUAL(ascending.Name(), "a");
        UNIT_ASSERT_VALUES_EQUAL(ascending.SortOrder(), ESortOrder::SO_ASCENDING);
        UNIT_ASSERT_VALUES_EQUAL(ascending, TSortColumn("a", ESortOrder::SO_ASCENDING));
        UNIT_ASSERT_VALUES_UNEQUAL(ascending, TSortColumn("a", ESortOrder::SO_DESCENDING));

        UNIT_ASSERT_NO_EXCEPTION(ascending.EnsureAscending());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<TString>(ascending), "a");
        UNIT_ASSERT_VALUES_EQUAL(ascending, "a");

        auto another = ascending;
        UNIT_ASSERT_NO_EXCEPTION(another = "another");
        UNIT_ASSERT_VALUES_EQUAL(another.Name(), "another");
        UNIT_ASSERT_VALUES_EQUAL(another.SortOrder(), ESortOrder::SO_ASCENDING);
        UNIT_ASSERT_VALUES_EQUAL(another, TSortColumn("another", ESortOrder::SO_ASCENDING));
        UNIT_ASSERT_VALUES_UNEQUAL(another, TSortColumn("another", ESortOrder::SO_DESCENDING));

        auto ascendingNode = BuildYsonNodeFluently().Value(ascending);
        UNIT_ASSERT_VALUES_EQUAL(ascendingNode, TNode("a"));

        UNIT_ASSERT_VALUES_EQUAL(SaveLoad(ascending), ascending);
        UNIT_ASSERT_VALUES_UNEQUAL(SaveToString(ascending), SaveToString(TString("a")));

        auto descending = TSortColumn("a", ESortOrder::SO_DESCENDING);
        UNIT_ASSERT_VALUES_EQUAL(descending.Name(), "a");
        UNIT_ASSERT_VALUES_EQUAL(descending.SortOrder(), ESortOrder::SO_DESCENDING);
        UNIT_ASSERT_VALUES_EQUAL(descending, TSortColumn("a", ESortOrder::SO_DESCENDING));
        UNIT_ASSERT_VALUES_UNEQUAL(descending, TSortColumn("a", ESortOrder::SO_ASCENDING));

        UNIT_ASSERT_EXCEPTION(descending.EnsureAscending(), yexception);
        UNIT_ASSERT_EXCEPTION(static_cast<TString>(descending), yexception);
        UNIT_ASSERT_EXCEPTION(descending == "a", yexception);
        UNIT_ASSERT_EXCEPTION(descending = "a", yexception);

        auto descendingNode = BuildYsonNodeFluently().Value(descending);
        UNIT_ASSERT_VALUES_EQUAL(descendingNode, TNode()("name", "a")("sort_order", "descending"));

        UNIT_ASSERT_VALUES_EQUAL(SaveLoad(descending), descending);
        UNIT_ASSERT_VALUES_UNEQUAL(SaveToString(descending), SaveToString("a"));

        UNIT_ASSERT_VALUES_EQUAL(ToString(TSortColumn("blah")), "blah");
        UNIT_ASSERT_VALUES_EQUAL(ToString(TSortColumn("blah", ESortOrder::SO_DESCENDING)), "{\"name\"=\"blah\";\"sort_order\"=\"descending\"}");
    }

    Y_UNIT_TEST(SortColumns)
    {
        TSortColumns ascending("a", "b");
        UNIT_ASSERT(ascending.Parts_ == (TSortColumns{"a", "b"}));
        UNIT_ASSERT_NO_EXCEPTION(ascending.EnsureAscending());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<TColumnNames>(ascending).Parts_, (TVector<TString>{"a", "b"}));
        UNIT_ASSERT_VALUES_EQUAL(ascending.GetNames(), (TVector<TString>{"a", "b"}));

        auto mixed = ascending;
        mixed.Add(TSortColumn("c", ESortOrder::SO_DESCENDING), "d");
        UNIT_ASSERT((mixed.Parts_ != TVector<TSortColumn>{"a", "b", "c", "d"}));
        UNIT_ASSERT((mixed.Parts_ == TVector<TSortColumn>{"a", "b", TSortColumn("c", ESortOrder::SO_DESCENDING), "d"}));
        UNIT_ASSERT_VALUES_EQUAL(mixed.GetNames(), (TVector<TString>{"a", "b", "c", "d"}));
        UNIT_ASSERT_EXCEPTION(mixed.EnsureAscending(), yexception);
        UNIT_ASSERT_EXCEPTION(static_cast<TColumnNames>(mixed), yexception);
    }

    Y_UNIT_TEST(KeyBound)
    {
        auto keyBound = TKeyBound(ERelation::Greater, TKey(7, "a", TNode()("x", "y")));
        UNIT_ASSERT_VALUES_EQUAL(keyBound.Relation(), ERelation::Greater);
        UNIT_ASSERT_EQUAL(keyBound.Key(), TKey(7, "a", TNode()("x", "y")));

        auto keyBound1 = TKeyBound().Relation(ERelation::Greater).Key(TKey(7, "a", TNode()("x", "y")));
        auto expectedNode = TNode()
            .Add(">")
            .Add(TNode().Add(7).Add("a").Add(TNode()("x", "y")));

        UNIT_ASSERT_VALUES_EQUAL(expectedNode, BuildYsonNodeFluently().Value(keyBound));
        UNIT_ASSERT_VALUES_EQUAL(expectedNode, BuildYsonNodeFluently().Value(keyBound1));

        keyBound.Relation(ERelation::LessOrEqual);
        keyBound.Key(TKey("A", 7));
        UNIT_ASSERT_VALUES_EQUAL(keyBound.Relation(), ERelation::LessOrEqual);
        UNIT_ASSERT_EQUAL(keyBound.Key(), TKey("A", 7));

        UNIT_ASSERT_VALUES_EQUAL(
            BuildYsonNodeFluently().Value(keyBound),
            TNode()
                .Add("<=")
                .Add(TNode().Add("A").Add(7)));
    }

    Y_UNIT_TEST(TTableSchema)
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
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[i].Name(), columns[i]);
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[i].SortOrder(), ESortOrder::SO_ASCENDING);
            }
            for (auto i: xrange(columns.size(), (size_t)initialSchema.Columns().size())) {
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[i].SortOrder(), Nothing());
            }
            UNIT_ASSERT_VALUES_EQUAL(initialSchema.Columns().size(), schema.Columns().size());
            return schema;
        };
        auto newSchema = checkSortBy(schema, {"b"});
        UNIT_ASSERT_VALUES_EQUAL(newSchema.Columns()[1].Name(), TString("a"));
        UNIT_ASSERT_VALUES_EQUAL(newSchema.Columns()[2].Name(), TString("c"));
        checkSortBy(schema, {"b", "c"});
        checkSortBy(schema, {"c", "a"});
        UNIT_ASSERT_EXCEPTION(checkSortBy(schema, {"b", "b"}), yexception);
        UNIT_ASSERT_EXCEPTION(checkSortBy(schema, {"a", "junk"}), yexception);
    }

    Y_UNIT_TEST(TTableSchema_Decimal)
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
            UNIT_ASSERT_VALUES_EQUAL(currentType.ChildAsString("type"), "string");
            UNIT_ASSERT(currentType.ChildAsBool("required"));
            UNIT_ASSERT(currentType.HasKey("type_v3"));
            UNIT_ASSERT_VALUES_EQUAL(currentType.At("type_v3").ChildAsString("type_name"), "decimal");
        }
        {
            const auto& currentType = tableSchemaNodeList[1];
            UNIT_ASSERT_VALUES_EQUAL(currentType.ChildAsString("type"), "string");
            UNIT_ASSERT(!currentType.ChildAsBool("required"));
            UNIT_ASSERT(currentType.HasKey("type_v3"));
            UNIT_ASSERT_VALUES_EQUAL(currentType.At("type_v3").ChildAsString("type_name"), "optional");
            UNIT_ASSERT_VALUES_EQUAL(currentType.At("type_v3").At("item").ChildAsString("type_name"), "decimal");
        }
        {
            const auto& currentType = tableSchemaNodeList[2];
            UNIT_ASSERT_VALUES_EQUAL(currentType.ChildAsString("type"), "any");
            UNIT_ASSERT(currentType.ChildAsBool("required"));
            UNIT_ASSERT(currentType.HasKey("type_v3"));
            UNIT_ASSERT_VALUES_EQUAL(currentType.At("type_v3").ChildAsString("type_name"), "list");
            UNIT_ASSERT_VALUES_EQUAL(currentType.At("type_v3").At("item").ChildAsString("type_name"), "decimal");
        }

        UNIT_ASSERT_EQUAL(tableSchema, TTableSchema::FromNode(tableSchemaNode));
    }

    Y_UNIT_TEST(TColumnSchema_TypeV3)
    {
        {
            auto column = TColumnSchema().Type(NTi::Interval());
            UNIT_ASSERT_VALUES_EQUAL(column.Required(), true);
            UNIT_ASSERT_VALUES_EQUAL(column.Type(), VT_INTERVAL);
        }
        {
            auto column = TColumnSchema().Type(NTi::Optional(NTi::Date()));
            UNIT_ASSERT_VALUES_EQUAL(column.Required(), false);
            UNIT_ASSERT_VALUES_EQUAL(column.Type(), VT_DATE);
        }
        {
            auto column = TColumnSchema().Type(NTi::Interval64());
            UNIT_ASSERT_VALUES_EQUAL(column.Required(), true);
            UNIT_ASSERT_VALUES_EQUAL(column.Type(), VT_INTERVAL64);
        }
        {
            auto column = TColumnSchema().Type(NTi::Optional(NTi::Date32()));
            UNIT_ASSERT_VALUES_EQUAL(column.Required(), false);
            UNIT_ASSERT_VALUES_EQUAL(column.Type(), VT_DATE32);
        }
        {
            auto column = TColumnSchema().Type(NTi::Null());
            UNIT_ASSERT_VALUES_EQUAL(column.Required(), false);
            UNIT_ASSERT_VALUES_EQUAL(column.Type(), VT_NULL);
        }
        {
            auto column = TColumnSchema().Type(NTi::Optional(NTi::Null()));
            UNIT_ASSERT_VALUES_EQUAL(column.Required(), false);
            UNIT_ASSERT_VALUES_EQUAL(column.Type(), VT_ANY);
        }
        {
            auto column = TColumnSchema().Type(NTi::Decimal(35, 18));
            UNIT_ASSERT_VALUES_EQUAL(column.Required(), true);
            UNIT_ASSERT_VALUES_EQUAL(column.Type(), VT_STRING);
        }
    }

    Y_UNIT_TEST(ToTypeV3)
    {
        UNIT_ASSERT_VALUES_EQUAL(*ToTypeV3(VT_INT32, true), *NTi::Int32());
        UNIT_ASSERT_VALUES_EQUAL(*ToTypeV3(VT_UTF8, false), *NTi::Optional(NTi::Utf8()));
    }

    Y_UNIT_TEST(DeserializeColumn)
    {
        auto deserialize = [] (TStringBuf yson) {
            auto node = NodeFromYsonString(yson);
            TColumnSchema column;
            Deserialize(column, node);
            return column;
        };

        auto column = deserialize("{name=foo; type=int64; required=%false}");
        UNIT_ASSERT_VALUES_EQUAL(column.Name(), "foo");
        UNIT_ASSERT_VALUES_EQUAL(*column.TypeV3(), *NTi::Optional(NTi::Int64()));

        column = deserialize("{name=bar; type=utf8; required=%true; type_v3=utf8}");
        UNIT_ASSERT_VALUES_EQUAL(column.Name(), "bar");
        UNIT_ASSERT_VALUES_EQUAL(*column.TypeV3(), *NTi::Utf8());
    }

    Y_UNIT_TEST(ColumnSchemaEquality)
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
        ASSERT_SERIALIZABLES_EQUAL(other, base);
        other.Name("other");
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.TypeV3(NTi::List(NTi::String()));
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.ResetSortOrder();
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.Lock("lock1");
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.Expression("x + 13");
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.ResetAggregate();
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.Group("group1");
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);
    }

    Y_UNIT_TEST(TableSchemaEquality)
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
        ASSERT_SERIALIZABLES_EQUAL(other, schema);

        other.Strict(false);
        ASSERT_SERIALIZABLES_UNEQUAL(other, schema);

        other = schema;
        other.MutableColumns()[0].TypeV3(NTi::List(NTi::String()));
        ASSERT_SERIALIZABLES_UNEQUAL(other, schema);

        other = schema;
        other.MutableColumns().push_back(col1);
        ASSERT_SERIALIZABLES_UNEQUAL(other, schema);

        other = schema;
        other.UniqueKeys(false);
        ASSERT_SERIALIZABLES_UNEQUAL(other, schema);
    }
}
