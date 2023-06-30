#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/service_combiner.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> YPathList(
    const IYPathServicePtr& service,
    const TYPath& path,
    std::optional<i64> limit = {})
{
    return AsyncYPathList(service, path, limit)
        .Get()
        .ValueOrThrow();
}

bool YPathExists(
    const IYPathServicePtr& service,
    const TYPath& path)
{
    return AsyncYPathExists(service, path)
        .Get()
        .ValueOrThrow();
}

TYsonString YPathGet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TAttributeFilter& attributeFilter = {})
{
    return ConvertToYsonString(AsyncYPathGet(service, path, attributeFilter)
        .Get()
        .ValueOrThrow(), EYsonFormat::Text);
}

auto FluentString()
{
    return BuildYsonStringFluently(EYsonFormat::Text);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYPathDesignatedServiceTest, TestGetSelf)
{
    auto service = IYPathService::YPathDesignatedServiceFromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("key1").Value(42)
                .EndMap();
        }));

    EXPECT_EQ(FluentString().BeginMap().Item("key1").Value(42).EndMap(), YPathGet(service, ""));
}

TEST(TYPathDesignatedServiceTest, SimpleTypes)
{
    auto service = IYPathService::YPathDesignatedServiceFromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("key1").Value(42)
                    .Item("key2").Value("abc")
                    .Item("key3").Value(42ull)
                    .Item("key4").Value(true)
                    .Item("key5").Value(0.1)
                    .Item("key6").Entity()
               .EndMap();
        }));

    EXPECT_EQ(FluentString().Value(42), YPathGet(service, "/key1"));
    EXPECT_EQ(FluentString().Value("abc"), YPathGet(service, "/key2"));
    EXPECT_EQ(FluentString().Value(42ull), YPathGet(service, "/key3"));
    EXPECT_EQ(FluentString().Value(true), YPathGet(service, "/key4"));
    EXPECT_EQ(FluentString().Value(0.1), YPathGet(service, "/key5"));
    EXPECT_EQ(FluentString().Entity(), YPathGet(service, "/key6"));
}

TEST(TYPathDesignatedServiceTest, QueryNestedKeySimple)
{
    auto service = IYPathService::YPathDesignatedServiceFromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("key1").Value(42)
                    .Item("key2").BeginMap()
                        .Item("subkey1").Value("abc")
                        .Item("subkey2").Value(43)
                    .EndMap()
               .EndMap();
        }));

    EXPECT_EQ(FluentString().Value("abc"), YPathGet(service, "/key2/subkey1"));
    EXPECT_EQ(FluentString().Value(43), YPathGet(service, "/key2/subkey2"));
}

TEST(TYPathDesignatedServiceTest, QueryNestedComplex)
{
    auto service = IYPathService::YPathDesignatedServiceFromProducer(BIND([] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("key1").BeginMap()
                    .Item("subkey1").Value("ab")
                .EndMap()
                .Item("key2").BeginMap()
                    .Item("subkey1").BeginMap()
                        .Item("string").Value("abc")
                        .Item("int").Value(40)
                    .EndMap()
                    .Item("subkey2").Value(43)
                .EndMap()
            .EndMap();
    }));

    auto expected = FluentString()
        .BeginMap()
            .Item("subkey1").BeginMap()
                .Item("string").Value("abc")
                .Item("int").Value(40)
            .EndMap()
            .Item("subkey2").Value(43)
        .EndMap();

    EXPECT_EQ(expected, YPathGet(service, "/key2"));
}

TEST(TYPathDesignatedServiceTest, GetList)
{
    auto service = IYPathService::YPathDesignatedServiceFromProducer(BIND([] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("key1").BeginMap()
                    .Item("subkey1").Value("ab")
                .EndMap()
                .Item("key2").BeginList()
                    .Item().BeginMap()
                        .Item("string").Value("abc")
                        .Item("int").Value(40)
                    .EndMap()
                    .Item().Value(43)
                .EndList()
            .EndMap();
    }));

    auto expected = FluentString()
        .BeginList()
            .Item().BeginMap()
                .Item("string").Value("abc")
                .Item("int").Value(40)
            .EndMap()
            .Item().Value(43)
        .EndList();

    EXPECT_EQ(expected, YPathGet(service, "/key2"));
}

TEST(TYPathDesignatedServiceTest, GetAttributes)
{
    auto service = IYPathService::YPathDesignatedServiceFromProducer(BIND([] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("key1")
                    .BeginAttributes()
                        .Item("attr1").Value(12)
                        .Item("attr2").BeginMap()
                            .Item("subkey1").Value("x")
                        .EndMap()
                    .EndAttributes()
                    .BeginMap()
                        .Item("subkey1").Value("ab")
                    .EndMap()
                .Item("key2").Value(12)
            .EndMap();
    }));

    auto expectedKey1 = FluentString()
        .BeginAttributes()
            .Item("attr1").Value(12)
            .Item("attr2").BeginMap()
                .Item("subkey1").Value("x")
            .EndMap()
        .EndAttributes()
        .BeginMap()
            .Item("subkey1").Value("ab")
        .EndMap();

    EXPECT_EQ(expectedKey1, YPathGet(service, "/key1"));
    EXPECT_EQ(FluentString().Value(12), YPathGet(service, "/key1/@attr1"));
    EXPECT_EQ(FluentString().BeginMap().Item("subkey1").Value("x").EndMap(), YPathGet(service, "/key1/@attr2"));
}

TEST(TYPathDesignatedServiceTest, InexistentPaths)
{
    auto service = IYPathService::YPathDesignatedServiceFromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("key1").Value(42)
                    .Item("key2").BeginMap()
                        .Item("subkey1").Value("abc")
                        .Item("subkey2").Value(43)
                    .EndMap()
               .EndMap();
        }));

    EXPECT_THROW_WITH_SUBSTRING(YPathGet(service, "/nonExistent"), "Failed to resolve YPath");
    EXPECT_THROW_WITH_SUBSTRING(YPathGet(service, "/key1/nonExistent"), "Failed to resolve YPath");
    EXPECT_THROW_WITH_SUBSTRING(YPathGet(service, "/key2/nonExistent"), "Failed to resolve YPath");
    EXPECT_THROW_WITH_SUBSTRING(YPathGet(service, "/key2/@attr"), "Path \"key2/\" has no attributes");
}

TEST(TYPathDesignatedServiceTest, ExistsVerb)
{
    auto service = IYPathService::YPathDesignatedServiceFromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("key1").Value(42)
                    .Item("key2")
                        .BeginAttributes()
                            .Item("attr1").Value(12)
                        .EndAttributes()
                        .BeginMap()
                            .Item("subkey1").Value("abc")
                            .Item("subkey2").Value(43)
                        .EndMap()
               .EndMap();
        }));

    EXPECT_TRUE(YPathExists(service, "/key1"));
    EXPECT_TRUE(YPathExists(service, "/key2"));
    EXPECT_TRUE(YPathExists(service, "/key2/subkey1"));
    EXPECT_TRUE(YPathExists(service, "/key2/@attr1"));

    EXPECT_FALSE(YPathExists(service, "/nonExistent"));
    EXPECT_FALSE(YPathExists(service, "/key1/nonExistent"));
    EXPECT_FALSE(YPathExists(service, "/key2/nonExistent"));
    EXPECT_FALSE(YPathExists(service, "/key2/@nonExistentAttr"));
}

TEST(TYPathDesignatedServiceTest, ListVerb)
{
    auto service = IYPathService::YPathDesignatedServiceFromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("key1").Value(42)
                    .Item("key2")
                        .BeginAttributes()
                            .Item("attr1").Value(12)
                        .EndAttributes()
                        .BeginMap()
                            .Item("subkey1").Value("abc")
                            .Item("subkey2").Value(43)
                        .EndMap()
               .EndMap();
        }));

    EXPECT_EQ((std::vector<TString> {"key1", "key2"}), YPathList(service, ""));
    EXPECT_EQ((std::vector<TString> {"subkey1", "subkey2"}), YPathList(service, "/key2"));
}

TEST(TYPathDesignatedServiceTest, RootAttributes)
{
    auto service = IYPathService::YPathDesignatedServiceFromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginAttributes()
                    .Item("attr1").Value(12)
                    .Item("attr2").Value(20)
                .EndAttributes()
                .BeginMap()
                    .Item("key1").Value(42)
               .EndMap();
        }));

    auto expectedAttrs = FluentString().BeginMap()
            .Item("attr1").Value(12)
            .Item("attr2").Value(20)
        .EndMap();


    EXPECT_EQ(expectedAttrs, YPathGet(service, "/@"));
    EXPECT_EQ(FluentString().Value(12), YPathGet(service, "/@attr1"));
    EXPECT_EQ((std::vector<TString>{"attr1", "attr2"}), YPathList(service, "/@"));
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree

