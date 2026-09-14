#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include "coordination_grpc_mock.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <atomic>
#include <memory>
#include <optional>

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
        UNIT_ASSERT(stoppedFuture.Wait(TDuration::Seconds(10)));
        auto endTimestamp = TInstant::Now();
        auto elapsed = endTimestamp - startTimestamp;

        UNIT_ASSERT_C(elapsed > TDuration::Seconds(1), "Elapsed time too short: " << elapsed);
        UNIT_ASSERT_C(elapsed < TDuration::Seconds(4), "Elapsed time too large: " << elapsed);

        auto res2 = session.Close().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(res2.GetStatus(), EStatus::TIMEOUT, res2.GetIssues().ToString());
    }

    Y_UNIT_TEST(DriverStopPreservesSessionAfterHandlesAreReleased) {
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
            .Timeout(TDuration::Seconds(30));

        auto started = client->StartSession("/Some/Path", settings);
        UNIT_ASSERT(started.Wait(TDuration::Seconds(10)));
        auto res = started.ExtractValue();
        UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());

        auto session = res.ExtractResult();

        client.reset();
        driver->Stop(true);
        driver.reset();
        UNIT_ASSERT(!stoppedFuture.IsReady());

        auto ping = session.Ping();
        UNIT_ASSERT(ping.Wait(TDuration::Seconds(10)));
        auto pingResult = ping.ExtractValue();
        UNIT_ASSERT_VALUES_EQUAL_C(pingResult.GetStatus(), EStatus::SUCCESS, pingResult.GetIssues().ToString());

        auto closed = session.Close();
        UNIT_ASSERT(closed.Wait(TDuration::Seconds(10)));
        auto closeResult = closed.ExtractValue();
        UNIT_ASSERT_VALUES_EQUAL_C(closeResult.GetStatus(), EStatus::SUCCESS, closeResult.GetIssues().ToString());
        UNIT_ASSERT(stoppedFuture.Wait(TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(SessionDropsDriverFromStateCallback) {
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

        auto droppedPromise = NThreading::NewPromise();
        auto droppedFuture = droppedPromise.GetFuture();
        auto settings = TSessionSettings()
            .OnStateChanged([&](auto state) mutable {
                if (state == ESessionState::ATTACHED) {
                    client.reset();
                    driver.reset();
                    droppedPromise.SetValue();
                }
            })
            .Timeout(TDuration::MilliSeconds(1000));

        auto res = client->StartSession("/Some/Path", settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());
        UNIT_ASSERT(droppedFuture.Wait(TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(SessionCallbacksSurviveDriverDestruction) {
        TPortManager pm;
        TMockCoordinationService service;
        const auto port = pm.GetPort();
        auto server = StartGrpcServer(TStringBuilder() << "0.0.0.0:" << port, service);
        std::optional<TDriver> driver(std::in_place, TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << port)
            .SetDiscoveryMode(EDiscoveryMode::Off));
        std::optional<TClient> client(std::in_place, *driver);
        auto detached = NThreading::NewPromise();
        auto reattached = NThreading::NewPromise();
        auto expired = NThreading::NewPromise();
        auto attachments = std::make_shared<std::atomic<unsigned>>(0);
        auto started = client->StartSession("/Some/Path", TSessionSettings()
            .Timeout(TDuration::Seconds(30))
            .OnStateChanged([detached, reattached, expired, attachments](ESessionState state) mutable {
                if (state == ESessionState::DETACHED) {
                    detached.TrySetValue();
                } else if (state == ESessionState::EXPIRED) {
                    expired.TrySetValue();
                } else if (state == ESessionState::ATTACHED && ++*attachments == 2) {
                    reattached.SetValue();
                }
            }));
        UNIT_ASSERT(started.Wait(TDuration::Seconds(10)));
        auto session = started.ExtractValue().ExtractResult();
        client.reset();
        driver->Stop(true);
        driver.reset();

        service.SendNextAcquirePending = true;
        auto accepted = NThreading::NewPromise();
        auto acquired = session.AcquireSemaphore("semaphore", TAcquireSemaphoreSettings()
            .Exclusive()
            .OnAccepted([accepted]() mutable { accepted.SetValue(); }));
        UNIT_ASSERT(accepted.GetFuture().Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(acquired.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(acquired.GetValue().GetResult());

        service.TriggerNextWatch = true;
        auto changed = NThreading::NewPromise<bool>();
        auto watched = session.DescribeSemaphore("semaphore", TDescribeSemaphoreSettings()
            .WatchData(true)
            .OnChanged([changed](bool triggered) mutable { changed.SetValue(triggered); }));
        UNIT_ASSERT(watched.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(watched.GetValue().IsSuccess());
        UNIT_ASSERT(changed.GetFuture().Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(changed.GetFuture().GetValue());

        service.FailNextPingStatus = Ydb::StatusIds::OVERLOADED;
        auto interrupted = session.Ping();
        UNIT_ASSERT(detached.GetFuture().Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(reattached.GetFuture().Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(interrupted.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(interrupted.GetValue().GetStatus(), EStatus::OVERLOADED);

        auto cancelled = NThreading::NewPromise<bool>();
        auto pendingWatch = session.DescribeSemaphore("semaphore", TDescribeSemaphoreSettings()
            .WatchData(true)
            .OnChanged([cancelled](bool triggered) mutable { cancelled.SetValue(triggered); }));
        UNIT_ASSERT(pendingWatch.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(pendingWatch.GetValue().IsSuccess());
        service.FailNextPingStatus = Ydb::StatusIds::SESSION_EXPIRED;
        auto failed = session.Ping();
        UNIT_ASSERT(expired.GetFuture().Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(failed.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(failed.GetValue().GetStatus(), EStatus::SESSION_EXPIRED);
        UNIT_ASSERT(cancelled.GetFuture().Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(!cancelled.GetFuture().GetValue());
        auto closed = session.Close();
        UNIT_ASSERT(closed.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(closed.GetValue().IsSuccess());
    }

}
