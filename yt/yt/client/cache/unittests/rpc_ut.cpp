#include <yt/yt/client/cache/rpc.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

TEST(RpcClientTest, SetClusterUrlWithoutProxy)
{
    TConfig config;
    SetClusterUrl(config, "markov");
    EXPECT_EQ("markov", config.GetClusterName());
    EXPECT_EQ("", config.GetProxyRole());
}

TEST(RpcClientTest, SetClusterUrlWithProxy)
{
    TConfig config;
    SetClusterUrl(config, "markov/bigb");
    EXPECT_EQ("markov", config.GetClusterName());
    EXPECT_EQ("bigb", config.GetProxyRole());
}

TEST(RpcClientTest, SetClusterUrlFqdnWithoutProxy)
{
    TConfig config;
    SetClusterUrl(config, "https://markov.yt.yandex.net:443");
    EXPECT_EQ("https://markov.yt.yandex.net:443", config.GetClusterName());
    EXPECT_EQ("", config.GetProxyRole());
}

TEST(RpcClientTest, SetClusterUrlFqdnWithProxy)
{
    TConfig config;
    SetClusterUrl(config, "https://markov.yt.yandex.net:443/bigb");
    EXPECT_EQ("https://markov.yt.yandex.net:443", config.GetClusterName());
    EXPECT_EQ("bigb", config.GetProxyRole());
}

TEST(RpcClientTest, ProxyRoleOverride)
{
    TConfig config;
    config.SetProxyRole("role");
    EXPECT_THROW(SetClusterUrl(config, "markov/bigb"), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
