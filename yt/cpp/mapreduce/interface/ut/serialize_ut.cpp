#include <yt/cpp/mapreduce/interface/serialize.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <library/cpp/yson/node/node_builder.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/serialized_enum.h>

using namespace NYT;

TEST(TSerializationTest, TableSchema)
{
    auto schema = TTableSchema()
        .AddColumn(TColumnSchema().Name("a").Type(EValueType::VT_STRING).SortOrder(SO_ASCENDING))
        .AddColumn(TColumnSchema().Name("b").Type(EValueType::VT_UINT64))
        .AddColumn(TColumnSchema().Name("c").Type(EValueType::VT_INT64, true));

    auto schemaNode = schema.ToNode();
    EXPECT_TRUE(schemaNode.IsList());
    EXPECT_EQ(schemaNode.Size(), 3u);


    EXPECT_EQ(schemaNode[0]["name"], "a");
    EXPECT_EQ(schemaNode[0]["type"], "string");
    EXPECT_EQ(schemaNode[0]["required"], false);
    EXPECT_EQ(schemaNode[0]["sort_order"], "ascending");

    EXPECT_EQ(schemaNode[1]["name"], "b");
    EXPECT_EQ(schemaNode[1]["type"], "uint64");
    EXPECT_EQ(schemaNode[1]["required"], false);

    EXPECT_EQ(schemaNode[2]["name"], "c");
    EXPECT_EQ(schemaNode[2]["type"], "int64");
    EXPECT_EQ(schemaNode[2]["required"], true);
}

TEST(TSerializationTest, ValueTypeSerialization)
{
    for (const auto value : GetEnumAllValues<EValueType>()) {
        TNode serialized = NYT::NDetail::ToString(value);
        EValueType deserialized;
        Deserialize(deserialized, serialized);
        EXPECT_EQ(value, deserialized);
    }
}
