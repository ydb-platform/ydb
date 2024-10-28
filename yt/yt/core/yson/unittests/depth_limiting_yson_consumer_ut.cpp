#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/yson_consumer_mock.h>

#include <yt/yt/core/yson/depth_limiting_yson_consumer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NYson {
namespace {

using namespace NYTree;

using ::testing::StrictMock;
using ::testing::InSequence;

////////////////////////////////////////////////////////////////////////////////

class TDepthLimitingYsonConsumerTest
    : public ::testing::Test
{
protected:
    StrictMock<TMockYsonConsumer> MockConsumer_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDepthLimitingYsonConsumerTest, NegativeDepthLimit)
{
    EXPECT_THROW(CreateDepthLimitingYsonConsumer(&MockConsumer_, /*depthLimit*/ -1), std::exception);
}

TEST_F(TDepthLimitingYsonConsumerTest, ZeroDepth)
{
    auto consumer = CreateDepthLimitingYsonConsumer(&MockConsumer_, /*depthLimit*/ 0);
    consumer->OnStringScalar("value");
}

TEST_F(TDepthLimitingYsonConsumerTest, MapTruncated)
{
    auto consumer = CreateDepthLimitingYsonConsumer(&MockConsumer_, /*depthLimit*/ 1);

    EXPECT_CALL(MockConsumer_, OnStringScalar("<truncated>"));

    BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("key1").Value("value1")
            .Item("key2").BeginMap()
                .Item("subkey1").Value("subvalue1")
                .Item("subkey2").Value("subvalue2")
                .Item("subkey3").Value("subvalue3")
            .EndMap()
        .EndMap();
}

TEST_F(TDepthLimitingYsonConsumerTest, NestedMapTruncated)
{
    InSequence dummy;

    auto consumer = CreateDepthLimitingYsonConsumer(&MockConsumer_, /*depthLimit*/ 2);

    EXPECT_CALL(MockConsumer_, OnBeginMap());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key1"));
    EXPECT_CALL(MockConsumer_, OnStringScalar("value1"));
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key2"));
    EXPECT_CALL(MockConsumer_, OnStringScalar("<truncated>"));
    EXPECT_CALL(MockConsumer_, OnEndMap());

    BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("key1").Value("value1")
            .Item("key2").BeginMap()
                .Item("subkey1").Value("subvalue1")
                .Item("subkey2").Value("subvalue2")
                .Item("subkey4").Value("subvalue3")
            .EndMap()
        .EndMap();
}

TEST_F(TDepthLimitingYsonConsumerTest, ListTruncated)
{
    InSequence dummy;

    auto consumer = CreateDepthLimitingYsonConsumer(&MockConsumer_, /*depthLimit*/ 1);

    EXPECT_CALL(MockConsumer_, OnStringScalar("<truncated>"));

    BuildYsonFluently(consumer.get())
        .BeginList()
            .Item().Value("value1")
            .Item().BeginList()
                .Item().Value("subvalue1")
                .Item().Value("subvalue2")
                .Item().Value("subvalue3")
            .EndList()
        .EndList();
}

TEST_F(TDepthLimitingYsonConsumerTest, NestedListTruncated)
{
    InSequence dummy;

    auto consumer = CreateDepthLimitingYsonConsumer(&MockConsumer_, /*depthLimit*/ 2);

    EXPECT_CALL(MockConsumer_, OnBeginList());
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnStringScalar("value1"));
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnStringScalar("<truncated>"));
    EXPECT_CALL(MockConsumer_, OnEndList());

    BuildYsonFluently(consumer.get())
        .BeginList()
            .Item().Value("value1")
            .Item().BeginList()
                .Item().Value("subvalue1")
                .Item().Value("subvalue2")
                .Item().Value("subvalue3")
            .EndList()
        .EndList();
}

TEST_F(TDepthLimitingYsonConsumerTest, AttributesTruncated)
{
    InSequence dummy;

    auto consumer = CreateDepthLimitingYsonConsumer(&MockConsumer_, /*depthLimit*/ 1);

    EXPECT_CALL(MockConsumer_, OnStringScalar("value"));

    BuildYsonFluently(consumer.get())
        .BeginAttributes()
            .Item("key1").Value("value1")
            .Item("key2").BeginList()
                .Item().Value("subvalue1")
                .Item().Value("subvalue2")
                .Item().Value("subvalue3")
            .EndList()
        .EndAttributes()
        .Value("value");
}

TEST_F(TDepthLimitingYsonConsumerTest, NestedAttributesTruncated)
{
    InSequence dummy;

    auto consumer = CreateDepthLimitingYsonConsumer(&MockConsumer_, /*depthLimit*/ 2);

    EXPECT_CALL(MockConsumer_, OnBeginAttributes());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key1"));
    EXPECT_CALL(MockConsumer_, OnStringScalar("value1"));
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key2"));
    EXPECT_CALL(MockConsumer_, OnStringScalar("<truncated>"));
    EXPECT_CALL(MockConsumer_, OnEndAttributes());
    EXPECT_CALL(MockConsumer_, OnStringScalar("value"));

    BuildYsonFluently(consumer.get())
        .BeginAttributes()
            .Item("key1").Value("value1")
            .Item("key2").BeginList()
                .Item().Value("subvalue1")
                .Item().Value("subvalue2")
                .Item().Value("subvalue3")
            .EndList()
        .EndAttributes()
        .Value("value");
}

TEST_F(TDepthLimitingYsonConsumerTest, ComplexYsonTruncated)
{
    InSequence dummy;

    auto consumer = CreateDepthLimitingYsonConsumer(&MockConsumer_, /*depthLimit*/ 3);

    EXPECT_CALL(MockConsumer_, OnBeginAttributes());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key1"));
    EXPECT_CALL(MockConsumer_, OnBeginAttributes());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("attrkey1"));
    EXPECT_CALL(MockConsumer_, OnStringScalar("attrvalue1"));
    EXPECT_CALL(MockConsumer_, OnEndAttributes());
    EXPECT_CALL(MockConsumer_, OnStringScalar("value1"));
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key2"));
    EXPECT_CALL(MockConsumer_, OnBeginList());
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnStringScalar("subvalue1"));
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnStringScalar("subvalue2"));
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnStringScalar("<truncated>"));
    EXPECT_CALL(MockConsumer_, OnEndList());
    EXPECT_CALL(MockConsumer_, OnEndAttributes());
    EXPECT_CALL(MockConsumer_, OnBeginMap());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key3"));
    EXPECT_CALL(MockConsumer_, OnBeginAttributes());
    EXPECT_CALL(MockConsumer_, OnEndAttributes());
    EXPECT_CALL(MockConsumer_, OnEntity());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key4"));
    EXPECT_CALL(MockConsumer_, OnBeginList());
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnDoubleScalar(1.0));
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnDoubleScalar(2.0));
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnStringScalar("<truncated>"));
    EXPECT_CALL(MockConsumer_, OnEndList());
    EXPECT_CALL(MockConsumer_, OnEndMap());

    BuildYsonFluently(consumer.get())
        .BeginAttributes()
            .Item("key1")
            .BeginAttributes()
                .Item("attrkey1").Value("attrvalue1")
            .EndAttributes()
            .Value("value1")
            .Item("key2").BeginList()
                .Item().BeginAttributes()
                    .Item("subattrkey1").Value("subattrvalue1")
                .EndAttributes()
                .Value("subvalue1")
                .Item().Value("subvalue2")
                .Item().BeginMap()
                .EndMap()
            .EndList()
        .EndAttributes()
        .BeginMap()
            .Item("key3").BeginAttributes()
            .EndAttributes()
            .Entity()
            .Item("key4").BeginList()
                .Item().Value(1.0)
                .Item().Value(2.0)
                .Item().BeginMap()
                    .Item("subkey3").Value("subvalue3")
                .EndMap()
            .EndList()
        .EndMap();
}

TEST_F(TDepthLimitingYsonConsumerTest, ComplexYsonNotTruncated)
{
    InSequence dummy;

    auto consumer = CreateDepthLimitingYsonConsumer(&MockConsumer_, /*depthLimit*/ 4);

    EXPECT_CALL(MockConsumer_, OnBeginAttributes());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key1"));
    EXPECT_CALL(MockConsumer_, OnBeginAttributes());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("attrkey1"));
    EXPECT_CALL(MockConsumer_, OnStringScalar("attrvalue1"));
    EXPECT_CALL(MockConsumer_, OnEndAttributes());
    EXPECT_CALL(MockConsumer_, OnStringScalar("value1"));
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key2"));
    EXPECT_CALL(MockConsumer_, OnBeginList());
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnBeginAttributes());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("subattrkey1"));
    EXPECT_CALL(MockConsumer_, OnStringScalar("subattrvalue1"));
    EXPECT_CALL(MockConsumer_, OnEndAttributes());
    EXPECT_CALL(MockConsumer_, OnStringScalar("subvalue1"));
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnStringScalar("subvalue2"));
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnBeginMap());
    EXPECT_CALL(MockConsumer_, OnEndMap());
    EXPECT_CALL(MockConsumer_, OnEndList());
    EXPECT_CALL(MockConsumer_, OnEndAttributes());
    EXPECT_CALL(MockConsumer_, OnBeginMap());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key3"));
    EXPECT_CALL(MockConsumer_, OnBeginAttributes());
    EXPECT_CALL(MockConsumer_, OnEndAttributes());
    EXPECT_CALL(MockConsumer_, OnEntity());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key4"));
    EXPECT_CALL(MockConsumer_, OnBeginList());
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnDoubleScalar(1.0));
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnDoubleScalar(2.0));
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnBeginMap());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("subkey3"));
    EXPECT_CALL(MockConsumer_, OnStringScalar("subvalue3"));
    EXPECT_CALL(MockConsumer_, OnEndMap());
    EXPECT_CALL(MockConsumer_, OnEndList());
    EXPECT_CALL(MockConsumer_, OnEndMap());

    BuildYsonFluently(consumer.get())
        .BeginAttributes()
            .Item("key1")
            .BeginAttributes()
                .Item("attrkey1").Value("attrvalue1")
            .EndAttributes()
            .Value("value1")
            .Item("key2").BeginList()
                .Item().BeginAttributes()
                    .Item("subattrkey1").Value("subattrvalue1")
                .EndAttributes()
                .Value("subvalue1")
                .Item().Value("subvalue2")
                .Item().BeginMap()
                .EndMap()
            .EndList()
        .EndAttributes()
        .BeginMap()
            .Item("key3").BeginAttributes()
            .EndAttributes()
            .Entity()
            .Item("key4").BeginList()
                .Item().Value(1.0)
                .Item().Value(2.0)
                .Item().BeginMap()
                    .Item("subkey3").Value("subvalue3")
                .EndMap()
            .EndList()
        .EndMap();
}

TEST_F(TDepthLimitingYsonConsumerTest, MapFragment)
{
    InSequence dummy;

    auto consumer = CreateDepthLimitingYsonConsumer(&MockConsumer_, /*depthLimit*/ 2, EYsonType::MapFragment);

    EXPECT_CALL(MockConsumer_, OnKeyedItem("key1"));
    EXPECT_CALL(MockConsumer_, OnBeginAttributes());
    EXPECT_CALL(MockConsumer_, OnKeyedItem("attrkey1"));
    EXPECT_CALL(MockConsumer_, OnStringScalar("attrvalue1"));
    EXPECT_CALL(MockConsumer_, OnEndAttributes());
    EXPECT_CALL(MockConsumer_, OnStringScalar("value1"));
    EXPECT_CALL(MockConsumer_, OnKeyedItem("key2"));
    EXPECT_CALL(MockConsumer_, OnBeginList());
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnStringScalar("subvalue1"));
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnStringScalar("subvalue2"));
    EXPECT_CALL(MockConsumer_, OnListItem());
    EXPECT_CALL(MockConsumer_, OnStringScalar("<truncated>"));
    EXPECT_CALL(MockConsumer_, OnEndList());

    BuildYsonMapFragmentFluently(consumer.get())
        .Item("key1")
        .BeginAttributes()
            .Item("attrkey1").Value("attrvalue1")
        .EndAttributes()
        .Value("value1")
        .Item("key2").BeginList()
            .Item().BeginAttributes()
                .Item("subattrkey1").Value("subattrvalue1")
            .EndAttributes()
            .Value("subvalue1")
            .Item().Value("subvalue2")
            .Item().BeginMap()
            .EndMap()
        .EndList();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson

