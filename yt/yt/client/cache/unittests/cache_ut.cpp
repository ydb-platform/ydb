#include <yt/yt/client/cache/cache.h>
#include <yt/yt_proto/yt/client/cache/proto/config.pb.h>

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

TEST(TClientsCacheTest, MakeClusterConfig) {
    TClustersConfig clustersCfg;
    clustersCfg.MutableDefaultConfig()->SetClusterName("seneca-nan"); // will be ignored
    clustersCfg.MutableDefaultConfig()->SetProxyRole("default_role"); // can be overwritten
    clustersCfg.MutableDefaultConfig()->SetChannelPoolSize(42u);
    auto& senecaVlaCfg = (*clustersCfg.MutableClusterConfigs())["seneca-vla"];
    senecaVlaCfg.SetClusterName(""); // will be ignored
    senecaVlaCfg.SetProxyRole("seneca_vla_role"); // can be overwritten
    senecaVlaCfg.SetChannelPoolSize(43u);

    {
        auto cfg = MakeClusterConfig(clustersCfg, "seneca-man");
        EXPECT_EQ(cfg.GetClusterName(), "seneca-man");
        EXPECT_EQ(cfg.GetProxyRole(), "default_role");
        EXPECT_EQ(cfg.GetChannelPoolSize(), 42u);
    }
    {
        auto cfg = MakeClusterConfig(clustersCfg, "seneca-man/overwriting_role");
        EXPECT_EQ(cfg.GetClusterName(), "seneca-man");
        EXPECT_EQ(cfg.GetProxyRole(), "overwriting_role");
        EXPECT_EQ(cfg.GetChannelPoolSize(), 42u);
    }
    {
        auto cfg = MakeClusterConfig(clustersCfg, "seneca-vla");
        EXPECT_EQ(cfg.GetClusterName(), "seneca-vla");
        EXPECT_EQ(cfg.GetProxyRole(), "seneca_vla_role");
        EXPECT_EQ(cfg.GetChannelPoolSize(), 43u);
    }
    {
        auto cfg = MakeClusterConfig(clustersCfg, "seneca-vla/overwriting_role");
        EXPECT_EQ(cfg.GetClusterName(), "seneca-vla");
        EXPECT_EQ(cfg.GetProxyRole(), "overwriting_role");
        EXPECT_EQ(cfg.GetChannelPoolSize(), 43u);
    }
    {
        auto cfg = MakeClusterConfig(clustersCfg, "seneca-vla.yt.yandex.net");
        EXPECT_EQ(cfg.GetClusterName(), "seneca-vla.yt.yandex.net");
        EXPECT_EQ(cfg.GetProxyRole(), "seneca_vla_role");
        EXPECT_EQ(cfg.GetChannelPoolSize(), 43u);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
