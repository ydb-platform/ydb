#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/federated/cache.h>
#include <yt/yt/client/federated/config.h>
#include <yt/yt/client/api/options.h>
#include <yt/yt/client/cache/cache.h>

#include <yt/yt/core/misc/error.h>

#include <util/system/env.h>

namespace NYT::NClient::NFederated {
namespace {

using namespace NYT::NApi;
using namespace NYT::NClient::NCache;

////////////////////////////////////////////////////////////////////////////////

TEST(TFederatedClientsCacheTest, GetSameClient)
{
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto ytClientsCache = CreateFederatedClientsCache(
        New<TConnectionConfig>(),
        New<TClientsCacheConfig>(),
        NApi::GetClientOptionsFromEnvStatic());

    auto client1 = ytClientsCache->GetClient("localhost");
    auto client2 = ytClientsCache->GetClient("localhost");

    EXPECT_TRUE(client1 == client2);

    // This is needed for TConnection.OnProxyUpdate to stop
    // and to remove references to TConnection that it's holding.
    // It's because we don't actually create YT Server.
    client1->GetConnection()->Terminate();
}

TEST(TFederatedClientsCacheTest, GetFederatedWithEmptyConfig)
{
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto ytClientsCache = CreateFederatedClientsCache(
        New<TConnectionConfig>(),
        New<TClientsCacheConfig>(),
        NApi::GetClientOptionsFromEnvStatic());

    EXPECT_THROW(
        ytClientsCache->GetClient("primary+secondary"),
        NYT::TErrorException);
}

TEST(TFederatedClientsCacheTest, ConfigurationAndClusterUrlMismatch1)
{
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto connectionConfig = New<TConnectionConfig>();
    connectionConfig->BundleName = "my_bundle";
    connectionConfig->RpcProxyConnections.push_back(New<NApi::NRpcProxy::TConnectionConfig>());
    connectionConfig->RpcProxyConnections.back()->ClusterUrl = TString{"primary"};
    connectionConfig->RpcProxyConnections.push_back(New<NApi::NRpcProxy::TConnectionConfig>());
    connectionConfig->RpcProxyConnections.back()->ClusterUrl = TString{"secondary"};

    auto ytClientsCache = CreateFederatedClientsCache(
        connectionConfig,
        New<TClientsCacheConfig>(),
        NApi::GetClientOptionsFromEnvStatic());

    EXPECT_THROW(
        ytClientsCache->GetClient("primary+tertiary"),
        NYT::TErrorException);
}

TEST(TFederatedClientsCacheTest, ConfigurationAndClusterUrlMismatch2)
{
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto connectionConfig = New<TConnectionConfig>();
    connectionConfig->BundleName = "my_bundle";
    connectionConfig->RpcProxyConnections.push_back(New<NApi::NRpcProxy::TConnectionConfig>());
    connectionConfig->RpcProxyConnections.back()->ClusterUrl = TString{"primary"};
    connectionConfig->RpcProxyConnections.push_back(New<NApi::NRpcProxy::TConnectionConfig>());
    connectionConfig->RpcProxyConnections.back()->ClusterUrl = TString{"secondary"};
    connectionConfig->RpcProxyConnections.push_back(New<NApi::NRpcProxy::TConnectionConfig>());
    connectionConfig->RpcProxyConnections.back()->ClusterUrl = TString{"tertiary"};

    auto ytClientsCache = CreateFederatedClientsCache(
        connectionConfig,
        New<TClientsCacheConfig>(),
        NApi::GetClientOptionsFromEnvStatic());

    EXPECT_THROW(
        ytClientsCache->GetClient("primary+tertiary"),
        NYT::TErrorException);
}

TEST(TFederatedClientsCacheTest, ConfigurationMissingCluster)
{
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto connectionConfig = New<TConnectionConfig>();
    connectionConfig->BundleName = "my_bundle";
    connectionConfig->RpcProxyConnections.push_back(New<NApi::NRpcProxy::TConnectionConfig>());
    connectionConfig->RpcProxyConnections.back()->ClusterUrl = TString{"primary"};
    connectionConfig->RpcProxyConnections.push_back(New<NApi::NRpcProxy::TConnectionConfig>());
    connectionConfig->RpcProxyConnections.back()->ClusterUrl = TString{"secondary"};

    auto ytClientsCache = CreateFederatedClientsCache(
        connectionConfig,
        New<TClientsCacheConfig>(),
        NApi::GetClientOptionsFromEnvStatic());

    EXPECT_THROW(
        ytClientsCache->GetClient("primary+secondary+tertiary"),
        NYT::TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NClient::NFederated
