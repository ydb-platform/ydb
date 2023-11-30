#include <util/system/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <ydb/library/grpc/client/grpc_common.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <stdio.h>

#include <thread>

using namespace NYdb;
using namespace NTestHelpers;
using namespace NYdb::NDiscovery;

static TString CreateHostWithPort(const TEndpointInfo& info) {
    return info.Address + ":" + ToString(info.Port);
}

static TString Exec(const TString& cmd) {
    std::array<char, 256> buffer;
    TString result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.data(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("Unable to create pipe with process: " + cmd);
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

Y_UNIT_TEST_SUITE(KqpQuerySession)
{
    Y_UNIT_TEST(NoLocalAttach)
    {
        if (Exec("uname -s") != "Linux\n") {
            Cerr << "This test works on linux only" << Endl;
            return;
        }

        UNIT_ASSERT_C(Exec("whoami") != "root\n", "Do not run this test as root user. "
            "This test will kill processes");

        using namespace NYdbGrpc;

        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);

        auto hs = [](const TEndpointInfo& info) {
            return info.Port; //Actualy uniq for test
        };

        auto eq = [](const TEndpointInfo& l, const TEndpointInfo& r) {
            return l.Address == r.Address && l.Port == r.Port;
        };

        std::unordered_map<TEndpointInfo, ui64, decltype(hs), decltype(eq)> endpoints;
        {
            auto client = TDiscoveryClient(driver);
            auto res = client.ListEndpoints().GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            for (const auto& x : res.GetEndpointsInfo()) {
                auto cmd = Sprintf("ps axu | grep grpc-port=%d | grep -v \"grep\" | awk {'print $2'}", x.Port);
                endpoints[x] = std::stoull(Exec(cmd).data());
            }
        }

        UNIT_ASSERT_C(endpoints.size() > 2, "We must run at least 3 unique node to perform this test");

        auto host1 = endpoints.begin();
        auto host2 = ++endpoints.begin();

        auto sessionId = CreateQuerySession(TGRpcClientConfig(CreateHostWithPort(host1->first)));

        bool allDoneOk = true;

        NYdbGrpc::TGRpcClientLow clientLow;

        auto readyToKill = NThreading::NewPromise<NYdbGrpc::IStreamRequestCtrl::TPtr>();
        auto doCheckAttach = [&] {
            auto p = CheckAttach(clientLow, TGRpcClientConfig(CreateHostWithPort(host2->first)), sessionId, Ydb::StatusIds::SUCCESS, allDoneOk);
            readyToKill.SetValue(p);
        };

        std::thread checkAttachThread(doCheckAttach);

        auto processor = readyToKill.GetFuture().GetValueSync();

        // Stop node which proxyed session stream
        Exec(Sprintf("kill -19 %lu", host2->second));

        // Give some time for interconnect to decide node is dead
        Sleep(TDuration::Seconds(30));

        // Resume node
        Exec(Sprintf("kill -18 %lu", host2->second));

        {
            // Try to attach to same session - extect session was destroyed
            // Using host where session actor worked
            CheckAttach(TGRpcClientConfig(CreateHostWithPort(host1->first)), sessionId, Ydb::StatusIds::BAD_SESSION, allDoneOk);
            // Using kqp proxy
            CheckAttach(TGRpcClientConfig(CreateHostWithPort(host2->first)), sessionId, Ydb::StatusIds::BAD_SESSION, allDoneOk);
        }

        // Right now we requered to cancel stream from client side
        processor->Cancel();

        checkAttachThread.join();

        UNIT_ASSERT(allDoneOk);
    }
}
