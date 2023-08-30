#include <yt/yt/client/federated/cache.h>
#include <yt/yt/client/federated/config.h>

#include <yt/yt/client/cache/cache.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/system/env.h>

namespace NYT::NClient::NFederated {

using namespace NYT::NApi;

////////////////////////////////////////////////////////////////////////////////

TEST(TFederatedClientsCacheTest, GetSameClient) {
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto cache = CreateFederatedClientsCache("my_bundle");
    auto client1 = cache->GetClient("localhost");
    auto client2 = cache->GetClient("localhost");

    EXPECT_TRUE(client1 == client2);

    // This is needed for TConnection.OnProxyUpdate to stop
    // and to remove references to TConnection that it's holding.
    // It's because we don't actually create YT Server.
    client1->GetConnection()->Terminate();
    client2->GetConnection()->Terminate();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
