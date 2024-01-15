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
    return AsyncYPathGet(service, path, attributeFilter)
        .Get()
        .ValueOrThrow();
}

void YPathSet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TYsonString& value,
    bool recursive = false)
{
    AsyncYPathSet(service, path, value, recursive)
        .Get()
        .ThrowOnError();
}

void YPathRemove(
    const IYPathServicePtr& service,
    const TYPath& path,
    bool recursive = false,
    bool force = false)
{
    AsyncYPathRemove(service, path, recursive, force)
        .Get()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYPathServiceCombinerTest, Simple)
{
    IYPathServicePtr service1 = IYPathService::FromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("key1").Value(42)
                    .Item("key2").BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .BeginMap()
                        .Item("subkey1").Value("abc")
                        .Item("subkey2").Value(3.1415926)
                    .EndMap()
                .EndMap();
        }));
    IYPathServicePtr service2 = IYPathService::FromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("key3").Value(-1)
                    .Item("key4").BeginAttributes()
                        .Item("attribute1").Value(-1)
                    .EndAttributes().Entity()
                .EndMap();
        }));
    auto combinedService = New<TServiceCombiner>(std::vector<IYPathServicePtr> { service1, service2 });

    EXPECT_EQ(true, YPathExists(combinedService, ""));
    EXPECT_THROW(YPathExists(combinedService, "/"), std::exception);
    EXPECT_EQ(false, YPathExists(combinedService, "/keyNonExistent"));
    EXPECT_EQ(true, YPathExists(combinedService, "/key1"));
    EXPECT_EQ(true, YPathExists(combinedService, "/key3"));
    EXPECT_EQ(false, YPathExists(combinedService, "/key2/subkeyNonExistent"));
    EXPECT_EQ(true, YPathExists(combinedService, "/key2/subkey1"));
    EXPECT_EQ((std::vector<TString> { "key1", "key2", "key3", "key4" }), YPathList(combinedService, ""));
    EXPECT_EQ((std::vector<TString> { "subkey1", "subkey2" }), YPathList(combinedService, "/key2"));
    EXPECT_THROW(YPathList(combinedService, "/keyNonExistent"), std::exception);
    EXPECT_EQ(ConvertToYsonString(-1, EYsonFormat::Binary), YPathGet(combinedService, "/key4/@attribute1"));
    EXPECT_EQ(ConvertToYsonString("abc", EYsonFormat::Binary), YPathGet(combinedService, "/key2/subkey1"));
    EXPECT_THROW(YPathGet(combinedService, "/"), std::exception);
}

TEST(TYPathServiceCombinerTest, DynamicAndStatic)
{
    IYPathServicePtr dynamicService = GetEphemeralNodeFactory()->CreateMap();
    IYPathServicePtr staticService = IYPathService::FromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("static_key1").Value(-1)
                    .Item("static_key2").Value(false)
                    .Item("error_key").Value("this key will be shared leading to an error")
                .EndMap();
        }));

    auto combinedService = New<TServiceCombiner>(std::vector<IYPathServicePtr> { staticService, dynamicService }, TDuration::MilliSeconds(100));

    EXPECT_EQ(true, YPathExists(combinedService, "/static_key1"));
    EXPECT_EQ(false, YPathExists(combinedService, "/dynamic_key1"));
    EXPECT_EQ(true, YPathExists(combinedService, "/error_key"));
    EXPECT_EQ((std::vector<TString> { "static_key1", "static_key2", "error_key" }), YPathList(combinedService, ""));

    YPathSet(dynamicService, "/dynamic_key1", ConvertToYsonString(3.1415926));
    YPathSet(dynamicService, "/dynamic_key2", TYsonString(TStringBuf("#")));

    // Give service time to rebuild key mapping.
    Sleep(TDuration::MilliSeconds(200));

    EXPECT_EQ(true, YPathExists(combinedService, "/static_key1"));
    EXPECT_EQ(true, YPathExists(combinedService, "/dynamic_key1"));
    EXPECT_EQ(true, YPathExists(combinedService, "/error_key"));
    EXPECT_EQ((std::vector<TString> { "static_key1", "static_key2", "error_key", "dynamic_key1", "dynamic_key2" }), YPathList(combinedService, ""));
    EXPECT_EQ(TYsonString(TStringBuf("#")), YPathGet(combinedService, "/dynamic_key2"));

    YPathSet(dynamicService, "/error_key", ConvertToYsonString(42));

    // Give service time to rebuild key mapping and notice two services sharing the same key.
    Sleep(TDuration::MilliSeconds(200));

    EXPECT_THROW(YPathExists(combinedService, "/static_key1"), std::exception);
    EXPECT_THROW(YPathExists(combinedService, "/dynamic_key1"), std::exception);
    EXPECT_THROW(YPathExists(combinedService, "/error_key"), std::exception);
    EXPECT_THROW(YPathGet(combinedService, ""), std::exception);
    EXPECT_THROW(YPathList(combinedService, ""), std::exception);
    EXPECT_THROW(YPathGet(combinedService, "/static_key1"), std::exception);

    YPathRemove(dynamicService, "/error_key");

    // Give service time to return to the normal state.
    Sleep(TDuration::MilliSeconds(200));

    EXPECT_EQ(true, YPathExists(combinedService, "/static_key1"));
    EXPECT_EQ(true, YPathExists(combinedService, "/dynamic_key1"));
    EXPECT_EQ(true, YPathExists(combinedService, "/error_key"));
    EXPECT_EQ((std::vector<TString> { "static_key1", "static_key2", "error_key", "dynamic_key1", "dynamic_key2" }), YPathList(combinedService, ""));
    EXPECT_EQ(TYsonString(TStringBuf(TStringBuf("#"))), YPathGet(combinedService, "/dynamic_key2"));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYPathServiceCombinerTest, UpdateKeysOnMissingKey)
{
    IYPathServicePtr dynamicService = GetEphemeralNodeFactory()->CreateMap();
    IYPathServicePtr staticService = IYPathService::FromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("static_key1").Value(-1)
                    .Item("static_key2").Value(false)
                    .Item("error_key").Value("this key will be shared leading to an error")
                .EndMap();
        }));

    auto combinedService = New<TServiceCombiner>(std::vector<IYPathServicePtr> { staticService, dynamicService }, TDuration::Seconds(100), /*updateKeysOnMissingKey*/ true);

    EXPECT_EQ(true, YPathExists(combinedService, "/static_key1"));
    EXPECT_EQ(false, YPathExists(combinedService, "/dynamic_key1"));
    EXPECT_EQ(true, YPathExists(combinedService, "/error_key"));
    EXPECT_EQ((std::vector<TString> { "static_key1", "static_key2", "error_key" }), YPathList(combinedService, ""));

    YPathSet(dynamicService, "/dynamic_key1", ConvertToYsonString(3.1415926));
    YPathSet(dynamicService, "/dynamic_key2", TYsonString(TStringBuf("#")));

    EXPECT_EQ(true, YPathExists(combinedService, "/static_key1"));
    EXPECT_EQ(true, YPathExists(combinedService, "/dynamic_key1"));
    EXPECT_EQ(true, YPathExists(combinedService, "/error_key"));
    EXPECT_EQ((std::vector<TString> { "static_key1", "static_key2", "error_key", "dynamic_key1", "dynamic_key2" }), YPathList(combinedService, ""));
    EXPECT_EQ(TYsonString(TStringBuf("#")), YPathGet(combinedService, "/dynamic_key2"));
}

////////////////////////////////////////////////////////////////////////////////
} // namespace
} // namespace NYT::NYTree

