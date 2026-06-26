#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include "coordination_grpc_mock.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NYdb;
using namespace NYdb::NCoordination;
using namespace NCoordinationTest;

Y_UNIT_TEST_SUITE(Coordination) {

    Y_UNIT_TEST(SessionStartTimeout) {
        TPortManager pm;

        ui16 fakeEndpointPort = pm.GetPort();
        TInet6StreamSocket fakeEndpointSocket;
        {
            TSockAddrInet6 addr("::", fakeEndpointPort);
            SetReuseAddressAndPort(fakeEndpointSocket);
            Y_ABORT_UNLESS(fakeEndpointSocket.Bind(&addr) == 0,
                "Failed to bind to port %" PRIu16, fakeEndpointPort);
            Y_ABORT_UNLESS(fakeEndpointSocket.Listen(1) == 0,
                "Failed to listen on port %" PRIu16, fakeEndpointPort);
        }

        TMockDiscoveryService discoveryService;
        {
            auto& dbResult = discoveryService.MockResults["/Root/My/DB"];
            auto* endpoint = dbResult.add_endpoints();
            endpoint->set_address("localhost");
            endpoint->set_port(fakeEndpointPort);
        }

        ui16 discoveryPort = pm.GetPort();
        std::string discoveryAddr = TStringBuilder() << "0.0.0.0:" << discoveryPort;
        auto discoveryServer = StartGrpcServer(discoveryAddr, discoveryService);

        auto config = TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << discoveryPort)
            .SetDatabase("/Root/My/DB");
        TDriver driver(config);
        TClient client(driver);

        auto settings = TSessionSettings()
            .Timeout(TDuration::MilliSeconds(500));

        auto startTimestamp = TInstant::Now();
        auto res = client.StartSession("/Some/Path", settings).ExtractValueSync();
        auto endTimestamp = TInstant::Now();
        auto elapsed = endTimestamp - startTimestamp;

        UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::TIMEOUT, res.GetIssues().ToString());
        UNIT_ASSERT_C(elapsed < TDuration::Seconds(5), "Timeout after too much time: " << elapsed);
    }

    Y_UNIT_TEST(SessionPingTimeout) {
        TPortManager pm;

        TMockCoordinationService coordinationService;
        coordinationService.MaxPingResponses.store(2);
        ui16 coordinationPort = pm.GetPort();
        auto coordinationServer = StartGrpcServer(
                TStringBuilder() << "0.0.0.0:" << coordinationPort,
                coordinationService);

        TMockDiscoveryService discoveryService;
        {
            auto& dbResult = discoveryService.MockResults["/Root/My/DB"];
            auto* endpoint = dbResult.add_endpoints();
            endpoint->set_address("localhost");
            endpoint->set_port(coordinationPort);
        }

        ui16 discoveryPort = pm.GetPort();
        auto discoveryServer = StartGrpcServer(
                TStringBuilder() << "0.0.0.0:" << discoveryPort,
                discoveryService);

        auto config = TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << discoveryPort)
            .SetDatabase("/Root/My/DB");
        TDriver driver(config);
        TClient client(driver);

        auto stoppedPromise = NThreading::NewPromise();
        auto stoppedFuture = stoppedPromise.GetFuture();
        auto settings = TSessionSettings()
            .OnStateChanged([](auto state) {
                std::cerr << "Session state: " << ToString(state) << std::endl;
            })
            .OnStopped([stoppedPromise]() mutable {
                stoppedPromise.SetValue();
            })
            .Timeout(TDuration::MilliSeconds(1000));

        auto startTimestamp = TInstant::Now();
        auto res = client.StartSession("/Some/Path", settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());

        auto session = res.ExtractResult();
        stoppedFuture.Wait();
        auto endTimestamp = TInstant::Now();
        auto elapsed = endTimestamp - startTimestamp;

        UNIT_ASSERT_C(elapsed > TDuration::Seconds(1), "Elapsed time too short: " << elapsed);
        UNIT_ASSERT_C(elapsed < TDuration::Seconds(4), "Elapsed time too large: " << elapsed);

        auto res2 = session.Close().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(res2.GetStatus(), EStatus::TIMEOUT, res2.GetIssues().ToString());
    }

    Y_UNIT_TEST(SessionCancelByDriver) {
        TPortManager pm;

        TMockCoordinationService coordinationService;
        ui16 coordinationPort = pm.GetPort();
        auto coordinationServer = StartGrpcServer(
                TStringBuilder() << "0.0.0.0:" << coordinationPort,
                coordinationService);

        TMockDiscoveryService discoveryService;
        {
            auto& dbResult = discoveryService.MockResults["/Root/My/DB"];
            auto* endpoint = dbResult.add_endpoints();
            endpoint->set_address("localhost");
            endpoint->set_port(coordinationPort);
        }

        ui16 discoveryPort = pm.GetPort();
        auto discoveryServer = StartGrpcServer(
                TStringBuilder() << "0.0.0.0:" << discoveryPort,
                discoveryService);

        auto config = TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << discoveryPort)
            .SetDatabase("/Root/My/DB");
        std::optional<TDriver> driver(std::in_place, config);
        std::optional<TClient> client(std::in_place, *driver);

        auto stoppedPromise = NThreading::NewPromise();
        auto stoppedFuture = stoppedPromise.GetFuture();
        auto settings = TSessionSettings()
            .OnStateChanged([](auto state) {
                std::cerr << "Session state: " << ToString(state) << std::endl;
            })
            .OnStopped([stoppedPromise]() mutable {
                stoppedPromise.SetValue();
            })
            .Timeout(TDuration::MilliSeconds(1000));

        auto startTimestamp = TInstant::Now();
        auto res = client->StartSession("/Some/Path", settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());

        auto session = res.ExtractResult();

        client.reset();
        driver->Stop();
        driver.reset();

        stoppedFuture.Wait();
        auto endTimestamp = TInstant::Now();
        auto elapsed = endTimestamp - startTimestamp;

        UNIT_ASSERT_C(elapsed < TDuration::Seconds(1), "Elapsed time too large: " << elapsed);

        auto res2 = session.Close().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(res2.GetStatus(), EStatus::CLIENT_CANCELLED, res2.GetIssues().ToString());
    }

}
