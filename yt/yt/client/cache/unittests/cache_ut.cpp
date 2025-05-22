#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/cache/config.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/string/format.h>

#include <util/generic/vector.h>

#include <util/system/env.h>

#include <thread>

namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

// YT does not create physical connection immediately, so try to use this fact to create connection to non existence server.
TEST(TClientsCacheTest, GetSameClient)
{
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto cache = CreateClientsCache();
    auto client1 = cache->GetClient("localhost");
    auto client2 = cache->GetClient("localhost");
    EXPECT_TRUE(client1 == client2);

    // This is needed for TConnection.OnProxyUpdate to stop
    // and to remove references to TConnection that it's holding.
    // It's because we don't actually create YT Server.
    client1->GetConnection()->Terminate();
    client2->GetConnection()->Terminate();
}

TEST(TClientsCacheTest, GetClientWithProxyRole)
{
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto cache = CreateClientsCache();
    auto client1 = cache->GetClient("bigb@localhost");
    auto client2 = cache->GetClient("localhost");
    EXPECT_TRUE(client1 != client2);

    // This is needed for TConnection.OnProxyUpdate to stop
    // and to remove references to TConnection that it's holding.
    // It's because we don't actually create YT Server.
    client1->GetConnection()->Terminate();
    client2->GetConnection()->Terminate();
}

TEST(TClientsCacheTest, GetClientByClusteName)
{
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->ClusterName = "test";
    connectionConfig->ClusterUrl = "localhost";
    auto cache = CreateClientsCache(connectionConfig);
    auto client = cache->GetClient("test");
    auto connection = client->GetConnection();

    EXPECT_EQ("test", connection->GetClusterName());
    auto newConnectionConfig = NYTree::ConvertTo<NApi::NRpcProxy::TConnectionConfigPtr>(connection->GetConfigYson());
    EXPECT_EQ("localhost", newConnectionConfig->ClusterUrl);

    // This is needed for TConnection.OnProxyUpdate to stop
    // and to remove references to TConnection that it's holding.
    // It's because we don't actually create YT Server.
    connection->Terminate();
}

TEST(TClientsCacheTest, MultiThreads)
{
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto cache = CreateClientsCache();
    TVector<std::thread> threads(Reserve(10));
    TVector<NApi::IClientPtr> clients(threads.capacity());
    TVector<size_t> collisions(threads.capacity());

    for (size_t i = 0 ; i < threads.capacity(); ++i) {
        threads.emplace_back([=, &clients, &collisions] {
            try {
                for (size_t j = 0; j < 1000; ++j) {
                    auto client = cache->GetClient(Format("localhost:6000%v", i));
                    if (client != clients[i]) {
                        clients[i] = client;
                        ++collisions[i];
                    }
                }
            } catch (...) {
                collisions[i] = 100500; // exception marker
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
    for (const auto& client : clients) {
        EXPECT_TRUE(client);
    }
    for (auto collision : collisions) {
        EXPECT_EQ(1u, collision);
    }

    // This is needed for TConnection.OnProxyUpdate to stop
    // and to remove references to TConnection that it's holding.
    // It's because we don't actually create YT Server.
    for (auto& client : clients) {
        client->GetConnection()->Terminate();
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClientsCacheTest, GetConnectionConfig)
{
    auto clientsCacheConfig = New<TClientsCacheConfig>();
    clientsCacheConfig->DefaultConnection = New<NApi::NRpcProxy::TConnectionConfig>();
    clientsCacheConfig->DefaultConnection->ClusterUrl = "seneca-nan"; // will be ignored
    clientsCacheConfig->DefaultConnection->ProxyRole = "default_role"; // can be overwritten
    clientsCacheConfig->DefaultConnection->DynamicChannelPool->MaxPeerCount = 42;

    auto senecaVlaConfig = New<NApi::NRpcProxy::TConnectionConfig>();
    senecaVlaConfig->ClusterUrl = ""; // will be ignored
    senecaVlaConfig->ProxyRole = "seneca_vla_role"; // can be overwritten
    senecaVlaConfig->DynamicChannelPool->MaxPeerCount = 43;
    clientsCacheConfig->PerClusterConnection["seneca-vla"] = senecaVlaConfig;

    {
        auto config = GetConnectionConfig(clientsCacheConfig, "seneca-man");
        EXPECT_EQ(config->ClusterUrl, "seneca-man");
        EXPECT_EQ(config->ProxyRole, "default_role");
        EXPECT_EQ(config->DynamicChannelPool->MaxPeerCount, 42);
    }
    {
        auto config = GetConnectionConfig(clientsCacheConfig, "seneca-man/overwriting_role");
        EXPECT_EQ(config->ClusterUrl, "seneca-man");
        EXPECT_EQ(config->ProxyRole, "overwriting_role");
        EXPECT_EQ(config->DynamicChannelPool->MaxPeerCount, 42);
    }
    {
        auto config = GetConnectionConfig(clientsCacheConfig, "seneca-vla");
        EXPECT_EQ(config->ClusterUrl, "seneca-vla");
        EXPECT_EQ(config->ProxyRole, "seneca_vla_role");
        EXPECT_EQ(config->DynamicChannelPool->MaxPeerCount, 43);
    }
    {
        auto config = GetConnectionConfig(clientsCacheConfig, "seneca-vla/overwriting_role");
        EXPECT_EQ(config->ClusterUrl, "seneca-vla");
        EXPECT_EQ(config->ProxyRole, "overwriting_role");
        EXPECT_EQ(config->DynamicChannelPool->MaxPeerCount, 43);
    }
    {
        auto config = GetConnectionConfig(clientsCacheConfig, "seneca-vla.yt.yandex.net");
        EXPECT_EQ(config->ClusterUrl, "seneca-vla.yt.yandex.net");
        EXPECT_EQ(config->ProxyRole, "seneca_vla_role");
        EXPECT_EQ(config->DynamicChannelPool->MaxPeerCount, 43);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
