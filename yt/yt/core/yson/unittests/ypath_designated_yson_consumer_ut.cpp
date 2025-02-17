#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/yson_consumer_mock.h>

#include <yt/yt/core/yson/ypath_designated_consumer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NYson {
namespace {

using namespace NYTree;

using ::testing::StrictMock;
using ::testing::InSequence;

////////////////////////////////////////////////////////////////////////////////

class TYPathDesignatedYsonConsumerTest
    : public ::testing::Test
{
public:
    StrictMock<TMockYsonConsumer> Mock;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYPathDesignatedYsonConsumerTest, EmptyPath)
{
    InSequence dummy;

    auto consumer = CreateYPathDesignatedConsumer("", EMissingPathMode::ThrowError, &Mock);

    EXPECT_CALL(Mock, OnBeginList());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnStringScalar("foo"));
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndList());

    BuildYsonFluently(consumer.get())
        .BeginList()
            .Item().Value("foo")
            .Item().Value("bar")
        .EndList();
}

TEST_F(TYPathDesignatedYsonConsumerTest, FetchStringValue)
{
    InSequence dummy;

    auto buildTree = [] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("key1").Value("value1")
                .Item("key2").BeginMap()
                    .Item("subkey1").Value("subvalue1")
                    .Item("subkey2").Value("subvalue2")
                    .Item("subkey4").Value("subvalue3")
                .EndMap()
            .EndMap();
    };

    {
        EXPECT_CALL(Mock, OnStringScalar("value1"));
        auto consumer = CreateYPathDesignatedConsumer("/key1", EMissingPathMode::ThrowError, &Mock);
        buildTree(consumer.get());
    }

    {
        EXPECT_CALL(Mock, OnStringScalar("subvalue2"));
        auto consumer = CreateYPathDesignatedConsumer("/key2/subkey2", EMissingPathMode::ThrowError, &Mock);
        buildTree(consumer.get());
    }
}

TEST_F(TYPathDesignatedYsonConsumerTest, FetchStringAttribute)
{
    InSequence dummy;

    auto buildTree = [] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("key1").Value("value1")
                .Item("key2")
                    .BeginAttributes()
                        .Item("attr1").Value("attr_value1")
                        .Item("attr2").Value("attr_value2")
                        .Item("attr3").Value("attr_value3")
                    .EndAttributes()
                    .BeginMap()
                        .Item("subkey1").Value("subvalue1")
                    .EndMap()
            .EndMap();
    };

    EXPECT_CALL(Mock, OnStringScalar("attr_value1"));
    auto consumer = CreateYPathDesignatedConsumer("/key2/@attr1", EMissingPathMode::ThrowError, &Mock);
    buildTree(consumer.get());
}

TEST_F(TYPathDesignatedYsonConsumerTest, FetchAllAttributes)
{
    InSequence dummy;

    auto buildTree = [] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("key1").Value("value1")
                .Item("key2")
                    .BeginAttributes()
                        .Item("attr1").Value("attr_value1")
                        .Item("attr2").Value("attr_value2")
                        .Item("attr3").Value("attr_value3")
                    .EndAttributes()
                    .BeginMap()
                        .Item("subkey1").Value("subvalue1")
                    .EndMap()
            .EndMap();
    };

    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("attr1"));
    EXPECT_CALL(Mock, OnStringScalar("attr_value1"));
    EXPECT_CALL(Mock, OnKeyedItem("attr2"));
    EXPECT_CALL(Mock, OnStringScalar("attr_value2"));
    EXPECT_CALL(Mock, OnKeyedItem("attr3"));
    EXPECT_CALL(Mock, OnStringScalar("attr_value3"));
    EXPECT_CALL(Mock, OnEndMap());
    auto consumer = CreateYPathDesignatedConsumer("/key2/@", EMissingPathMode::ThrowError, &Mock);
    buildTree(consumer.get());
}

TEST_F(TYPathDesignatedYsonConsumerTest, FetchMapNodeWithAttributes)
{
    InSequence dummy;

    auto buildTree = [] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("key1").Value("value1")
                .Item("key2")
                    .BeginAttributes()
                        .Item("attr1").Value("attr_value1")
                        .Item("attr2").Value("attr_value2")
                        .Item("attr3").Value("attr_value3")
                    .EndAttributes()
                    .BeginMap()
                        .Item("subkey1").Value("subvalue1")
                    .EndMap()
            .EndMap();
    };

    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnKeyedItem("attr1"));
    EXPECT_CALL(Mock, OnStringScalar("attr_value1"));
    EXPECT_CALL(Mock, OnKeyedItem("attr2"));
    EXPECT_CALL(Mock, OnStringScalar("attr_value2"));
    EXPECT_CALL(Mock, OnKeyedItem("attr3"));
    EXPECT_CALL(Mock, OnStringScalar("attr_value3"));
    EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("subkey1"));
    EXPECT_CALL(Mock, OnStringScalar("subvalue1"));
    EXPECT_CALL(Mock, OnEndMap());
    auto consumer = CreateYPathDesignatedConsumer("/key2", EMissingPathMode::ThrowError, &Mock);
    buildTree(consumer.get());
}

TEST_F(TYPathDesignatedYsonConsumerTest, FetchAllAttributesOfNodeWithoutAttributes)
{
    InSequence dummy;

    auto buildTree = [] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("key1").Value("value1")
                .Item("key2")
                    .BeginAttributes()
                        .Item("attr1").Value("attr_value1")
                        .Item("attr2").Value("attr_value2")
                        .Item("attr3").Value("attr_value3")
                    .EndAttributes()
                    .BeginMap()
                        .Item("subkey1").Value("subvalue1")
                    .EndMap()
            .EndMap();
    };

    {
        auto consumer = CreateYPathDesignatedConsumer("/key1/@", EMissingPathMode::Ignore, &Mock);
        buildTree(consumer.get());
    }

    {
        auto consumer = CreateYPathDesignatedConsumer("/key1/@", EMissingPathMode::ThrowError, &Mock);
        EXPECT_THROW(buildTree(consumer.get()), std::exception);
    }
}

TEST_F(TYPathDesignatedYsonConsumerTest, MissingSubtreeIgnore)
{
    InSequence dummy;

    auto consumer = CreateYPathDesignatedConsumer("/key3", EMissingPathMode::Ignore, &Mock);

    BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("key1").Value("value1")
            .Item("key2").Value("value2")
        .EndMap();
}

TEST_F(TYPathDesignatedYsonConsumerTest, MissingSubtreeError)
{
    InSequence dummy;

    auto buildTree = [] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("key1").Value("value1")
                .Item("key2").Value("value2")
                .Item("key3").BeginMap()
                    .Item("subkey1").Value("subvalue1")
                .EndMap()
            .EndMap();
    };

    {
        auto consumer = CreateYPathDesignatedConsumer("/key4", EMissingPathMode::ThrowError, &Mock);
        EXPECT_THROW(buildTree(consumer.get()), std::exception);
    }

    {
        auto consumer = CreateYPathDesignatedConsumer("/key3/subkey2", EMissingPathMode::ThrowError, &Mock);
        EXPECT_THROW(buildTree(consumer.get()), std::exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson

