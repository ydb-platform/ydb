#include <yt/yt/client/hedging/rpc.h>
#include <yt/yt_proto/yt/client/hedging/proto/config.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NClient::NHedging::NRpc {

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

TEST(RpcClientTest, ProxyRoleOverride)
{
    TConfig config;
    config.SetProxyRole("role");
    EXPECT_THROW(SetClusterUrl(config, "markov/bigb"), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
