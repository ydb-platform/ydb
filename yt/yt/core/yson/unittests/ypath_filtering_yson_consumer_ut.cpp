#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/ypath_filtering_consumer.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TYPathFilteringConsumerTest
    : public ::testing::Test
{
public:
    TYPathFilteringConsumerTest()
        : Output_(ValueString_)
        , Consumer_(CreateYsonWriter(&Output_, EYsonFormat::Text, EYsonType::Node, /*enableRaw*/ false))
    { }

    IYsonConsumer* GetConsumer()
    {
        return Consumer_.get();
    }

    TString FlushOutput()
    {
        auto result = std::move(ValueString_);
        ValueString_.clear();
        return result;
    }

private:
    TString ValueString_;
    TStringOutput Output_;
    std::unique_ptr<IYsonConsumer> Consumer_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYPathFilteringConsumerTest, BlacklistMap1)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/forbidden_key"},
        EPathFilteringMode::Blacklist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("key").Value("value")
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"key"="value";})");
}

TEST_F(TYPathFilteringConsumerTest, BlacklistMap2)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/forbidden_key"},
        EPathFilteringMode::Blacklist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("forbidden_key").Value("value")
        .EndMap();

    ASSERT_EQ(FlushOutput(), "{}");
}

TEST_F(TYPathFilteringConsumerTest, BlacklistMap3)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/forbidden_key"},
        EPathFilteringMode::Blacklist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("forbidden_key").Value("value")
            .Item("key").Value("value")
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"key"="value";})");
}

TEST_F(TYPathFilteringConsumerTest, BlacklistList)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/1/key"},
        EPathFilteringMode::Blacklist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginList()
            .Item()
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
            .Item()
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
            .Item()
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
        .EndList();

    ASSERT_EQ(FlushOutput(), R"([{"key"="value";};{};{"key"="value";};])");
}

TEST_F(TYPathFilteringConsumerTest, BlacklistListNested)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/1/key"},
        EPathFilteringMode::Blacklist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginList()
            .Item()
                .BeginList()
                    .Item().Entity()
                    .Item().Entity()
                    .Item().Entity()
                .EndList()
            .Item()
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
            .Item()
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
        .EndList();

    ASSERT_EQ(FlushOutput(), R"([[#;#;#;];{};{"key"="value";};])");
}

TEST_F(TYPathFilteringConsumerTest, BlacklistAttributes)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/@"},
        EPathFilteringMode::Blacklist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginAttributes()
            .Item("some_attr").Entity()
        .EndAttributes()
        .Entity();

    ASSERT_EQ(FlushOutput(), R"(#)");
}

TEST_F(TYPathFilteringConsumerTest, BlacklistAttribute)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/@forbidden_attr"},
        EPathFilteringMode::Blacklist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginAttributes()
            .Item("forbidden_attr").Entity()
            .Item("allowed_attr").Entity()
        .EndAttributes()
        .Entity();

    ASSERT_EQ(FlushOutput(), R"(<"allowed_attr"=#;>#)");
}

TEST_F(TYPathFilteringConsumerTest, BlacklistNestedMap)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/forbidden_key", "/nested_map/forbidden_key"},
        EPathFilteringMode::Blacklist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("forbidden_key").Value("value")
            .Item("key").Value("value")
            .Item("nested_map")
                .BeginMap()
                    .Item("forbidden_key").Value("value")
                    .Item("key").Value("value")
                .EndMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"key"="value";"nested_map"={"key"="value";};})");
}

TEST_F(TYPathFilteringConsumerTest, BlacklistNestedAsterisk)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/*/forbidden_key"},
        EPathFilteringMode::Blacklist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("nested_map1")
                .BeginMap()
                    .Item("key").Value("value")
                    .Item("forbidden_key").Value("value")
                .EndMap()
            .Item("nested_map2")
                .BeginMap()
                    .Item("forbidden_key").Value("value")
                    .Item("key").Value("value")
                .EndMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"nested_map1"={"key"="value";};"nested_map2"={"key"="value";};})");
}

TEST_F(TYPathFilteringConsumerTest, WhitelistMap1)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {""},
        EPathFilteringMode::Whitelist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("key").Value("value")
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"key"="value";})");
}

TEST_F(TYPathFilteringConsumerTest, WhitelistMap2)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/allowed_key"},
        EPathFilteringMode::Whitelist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("allowed_key").Value("value")
            .Item("key").Value("value")
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"allowed_key"="value";})");
}

TEST_F(TYPathFilteringConsumerTest, WhitelistNestedMap)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/allowed_key", "/nested/allowed_key"},
        EPathFilteringMode::Whitelist);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("key").Value("value")
            .Item("allowed_key").Value("value")
            .Item("nested")
                .BeginMap()
                    .Item("key").Value("value")
                    .Item("allowed_key").Value("value")
                .EndMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"allowed_key"="value";"nested"={"allowed_key"="value";};})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedSimple)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/forced_key"},
        EPathFilteringMode::WhitelistWithForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"forced_key"=#;})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedNested)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/data/forced_key"},
        EPathFilteringMode::WhitelistWithForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"data"={"forced_key"=#;};})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedWhitelistMixedNested)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/data/forced_key"},
        EPathFilteringMode::WhitelistWithForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("data")
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"data"={"forced_key"=#;};})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedMixedNested)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/data/forced_key"},
        EPathFilteringMode::ForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("data")
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"data"={"key"="value";"forced_key"=#;};})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedAttribute)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/data/@forced_attr"},
        EPathFilteringMode::ForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("data")
                .BeginAttributes()
                .EndAttributes()
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"data"=<"forced_attr"=#;>{"key"="value";};})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedAttributes1)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/data/@"},
        EPathFilteringMode::ForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("data")
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"data"=<>{"key"="value";};})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedAttributes2)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/data/@forced_key"},
        EPathFilteringMode::ForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("data")
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"data"=<"forced_key"=#;>{"key"="value";};})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedMultiplePaths)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/forced_key1", "/forced_key2"},
        EPathFilteringMode::WhitelistWithForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"forced_key1"=#;"forced_key2"=#;})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedMultiplePathsNested)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/data/forced_key","/forced_key"},
        EPathFilteringMode::WhitelistWithForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"data"={"forced_key"=#;};"forced_key"=#;})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedFilteringSimple)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/forced_key"},
        EPathFilteringMode::WhitelistWithForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("key").Value("value")
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"forced_key"=#;})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedFilteringSimplePrefix)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/forced_key"},
        EPathFilteringMode::WhitelistWithForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("forced_key")
                .BeginMap()
                    .Item("inner").Entity()
                .EndMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"forced_key"={"inner"=#;};})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedFilteringNested)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/data/forced_key"},
        EPathFilteringMode::WhitelistWithForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("key").Value("value")
            .Item("data")
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"data"={"forced_key"=#;};})");
}

TEST_F(TYPathFilteringConsumerTest, ForcedFilteringMultiplePathsNested)
{
    auto consumer = CreateYPathFilteringConsumer(
        GetConsumer(),
        /*paths*/ {"/forced_key", "/data/forced_key"},
        EPathFilteringMode::WhitelistWithForcedEntities);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
            .Item("key").Value("value")
            .Item("some_map")
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
        .EndMap();

    ASSERT_EQ(FlushOutput(), R"({"forced_key"=#;"data"={"forced_key"=#;};})");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
