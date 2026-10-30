#include "simple_server.h"

#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/http/host_manager.h>
#include <yt/cpp/mapreduce/http/http.h>
#include <yt/cpp/mapreduce/http/http_client.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/testing/common/network.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/scope.h>

#include <atomic>

using namespace NYT;

TEST(THostManagerTest, ReusesRefreshedHostList)
{
    std::atomic<int> requestCount = 0;
    auto port = NTesting::GetFreePort();
    TSimpleServer server(port, [&] (IInputStream* input, IOutputStream* output) {
        THttpInput httpInput(input);
        httpInput.ReadAll();

        auto requestNumber = ++requestCount;
        THttpOutput httpOutput(output);
        httpOutput << "HTTP/1.1 200 OK\r\n\r\n";
        httpOutput << (requestNumber == 1 ? "[\"old-proxy:80\"]" : "[\"new-proxy:80\"]");
        httpOutput.Flush();
    });

    TClientContext context;
    context.ServerName = server.GetAddress();
    context.HttpClient = NHttpClient::CreateDefaultHttpClient();
    context.Config = MakeIntrusive<TConfig>();
    context.Config->HostListUpdateInterval = TDuration::Hours(1);

    auto& hostManager = NYT::NPrivate::THostManager::Get();
    hostManager.Reset();
    Y_DEFER {
        hostManager.Reset();
    };
    ASSERT_EQ(hostManager.GetProxyForHeavyRequest(context), "old-proxy:80");
    EXPECT_EQ(requestCount.load(), 1);

    context.Config->HostListUpdateInterval = TDuration::Zero();
    ASSERT_EQ(hostManager.GetProxyForHeavyRequest(context), "new-proxy:80");
    EXPECT_EQ(requestCount.load(), 2);

    context.Config->HostListUpdateInterval = TDuration::Hours(1);
    EXPECT_EQ(hostManager.GetProxyForHeavyRequest(context), "new-proxy:80");
    EXPECT_EQ(hostManager.GetProxyForHeavyRequest(context), "new-proxy:80");
    EXPECT_EQ(requestCount.load(), 2);
}
