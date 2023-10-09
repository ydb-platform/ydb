#include <ydb/public/sdk/cpp/client/ydb_coordination/coordination.h>

#include <ydb/public/api/grpc/ydb_coordination_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NYdb;
using namespace NYdb::NCoordination;

namespace {

    class TMockDiscoveryService : public Ydb::Discovery::V1::DiscoveryService::Service {
    public:
        grpc::Status ListEndpoints(
                grpc::ServerContext* context,
                const Ydb::Discovery::ListEndpointsRequest* request,
                Ydb::Discovery::ListEndpointsResponse* response) override
        {
            Y_UNUSED(context);

            Cerr << "ListEndpoints: " << request->ShortDebugString() << Endl;

            const auto* result = MockResults.FindPtr(request->database());
            Y_ABORT_UNLESS(result, "Mock service doesn't have a result for database '%s'", request->database().c_str());

            auto* op = response->mutable_operation();
            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);
            op->mutable_result()->PackFrom(*result);
            return grpc::Status::OK;
        }

        // From database name to result
        THashMap<TString, Ydb::Discovery::ListEndpointsResult> MockResults;
    };

    class TMockCoordinationService : public Ydb::Coordination::V1::CoordinationService::Service {
    public:
        grpc::Status Session(
                grpc::ServerContext* context,
                grpc::ServerReaderWriter<
                    Ydb::Coordination::SessionResponse,
                    Ydb::Coordination::SessionRequest>* stream) override
        {
            Y_UNUSED(context);

            Cerr << "Session stream started" << Endl;

            Ydb::Coordination::SessionRequest request;

            // Process session start
            {
                if (!stream->Read(&request)) {
                    // Disconnected before the request was sent
                    return grpc::Status::OK;
                }
                Cerr << "Session request: " << request.ShortDebugString() << Endl;
                Y_ABORT_UNLESS(request.has_session_start(), "Expected session start");
                auto& start = request.session_start();
                uint64_t sessionId = start.session_id();
                if (!sessionId) {
                    sessionId = ++LastSessionId;
                }
                Ydb::Coordination::SessionResponse response;
                auto* started = response.mutable_session_started();
                started->set_session_id(sessionId);
                started->set_timeout_millis(start.timeout_millis());
                stream->Write(response);
                request.Clear();
            }

            size_t pings_received = 0;
            while (stream->Read(&request)) {
                Cerr << "Session request: " << request.ShortDebugString() << Endl;
                Y_ABORT_UNLESS(request.has_ping(), "Only ping requests are supported");
                if (++pings_received <= 2) {
                    // Only reply to the first 2 ping requests
                    Ydb::Coordination::SessionResponse response;
                    auto* pong = response.mutable_pong();
                    pong->set_opaque(request.ping().opaque());
                    stream->Write(response);
                }
                request.Clear();
            }

            return grpc::Status::OK;
        }

    private:
        std::atomic<uint64_t> LastSessionId{ 0 };
    };

    template<class TService>
    std::unique_ptr<grpc::Server> StartGrpcServer(const TString& address, TService& service) {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        return builder.BuildAndStart();
    }

} // namespace

Y_UNIT_TEST_SUITE(Coordination) {

    Y_UNIT_TEST(SessionStartTimeout) {
        TPortManager pm;

        // Start a fake endpoint on a random port
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

        // Fill mock discovery service with our fake database
        TMockDiscoveryService discoveryService;
        {
            auto& dbResult = discoveryService.MockResults["/Root/My/DB"];
            auto* endpoint = dbResult.add_endpoints();
            endpoint->set_address("localhost");
            endpoint->set_port(fakeEndpointPort);
        }

        // Start our mock discovery service
        ui16 discoveryPort = pm.GetPort();
        TString discoveryAddr = TStringBuilder() << "0.0.0.0:" << discoveryPort;
        auto discoveryServer = StartGrpcServer(discoveryAddr, discoveryService);

        auto config = TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << discoveryPort)
            .SetDatabase("/Root/My/DB");
        TDriver driver(config);
        TClient client(driver);

        auto settings = TSessionSettings()
            .Timeout(TDuration::MilliSeconds(500));

        // We expect either connection or session start timeout
        auto startTimestamp = TInstant::Now();
        auto res = client.StartSession("/Some/Path", settings).ExtractValueSync();
        auto endTimestamp = TInstant::Now();
        auto elapsed = endTimestamp - startTimestamp;

        Cerr << "Got: " << res.GetStatus() << ": " << res.GetIssues().ToString() << Endl;

        // Both connection and session timeout return EStatus::TIMEOUT
        UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::TIMEOUT, res.GetIssues().ToString());

        // Our timeout is 500ms, but allow up to 5 seconds of slack on very busy servers
        UNIT_ASSERT_C(elapsed < TDuration::Seconds(5), "Timeout after too much time: " << elapsed);
    }

    Y_UNIT_TEST(SessionPingTimeout) {
        TPortManager pm;

        // Start a fake coordination service
        TMockCoordinationService coordinationService;
        ui16 coordinationPort = pm.GetPort();
        auto coordinationServer = StartGrpcServer(
                TStringBuilder() << "0.0.0.0:" << coordinationPort,
                coordinationService);

        // Fill a fake discovery service
        TMockDiscoveryService discoveryService;
        {
            auto& dbResult = discoveryService.MockResults["/Root/My/DB"];
            auto* endpoint = dbResult.add_endpoints();
            endpoint->set_address("localhost");
            endpoint->set_port(coordinationPort);
        }

        // Start a fake discovery service
        ui16 discoveryPort = pm.GetPort();
        auto discoveryServer = StartGrpcServer(
                TStringBuilder() << "0.0.0.0:" << discoveryPort,
                discoveryService);

        // Create a driver and a client
        auto config = TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << discoveryPort)
            .SetDatabase("/Root/My/DB");
        TDriver driver(config);
        TClient client(driver);

        auto stoppedPromise = NThreading::NewPromise();
        auto stoppedFuture = stoppedPromise.GetFuture();
        auto settings = TSessionSettings()
            .OnStateChanged([](auto state) {
                Cerr << "Session state: " << state << Endl;
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

        // We expect first few pings to succeed
        UNIT_ASSERT_C(elapsed > TDuration::Seconds(1), "Elapsed time too short: " << elapsed);

        // We expect timeout to hit pretty soon afterwards
        UNIT_ASSERT_C(elapsed < TDuration::Seconds(4), "Elapsed time too large: " << elapsed);

        // Check the last failure stored in a session
        auto res2 = session.Close().ExtractValueSync();
        Cerr << "Close: " << res2.GetStatus() << ": " << res2.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(res2.GetStatus(), EStatus::TIMEOUT, res2.GetIssues().ToString());
    }

    Y_UNIT_TEST(SessionCancelByDriver) {
        TPortManager pm;

        // Start a fake coordination service
        TMockCoordinationService coordinationService;
        ui16 coordinationPort = pm.GetPort();
        auto coordinationServer = StartGrpcServer(
                TStringBuilder() << "0.0.0.0:" << coordinationPort,
                coordinationService);

        // Fill a fake discovery service
        TMockDiscoveryService discoveryService;
        {
            auto& dbResult = discoveryService.MockResults["/Root/My/DB"];
            auto* endpoint = dbResult.add_endpoints();
            endpoint->set_address("localhost");
            endpoint->set_port(coordinationPort);
        }

        // Start a fake discovery service
        ui16 discoveryPort = pm.GetPort();
        auto discoveryServer = StartGrpcServer(
                TStringBuilder() << "0.0.0.0:" << discoveryPort,
                discoveryService);

        // Create a driver and a client
        auto config = TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << discoveryPort)
            .SetDatabase("/Root/My/DB");
        std::optional<TDriver> driver(std::in_place, config);
        std::optional<TClient> client(std::in_place, *driver);

        auto stoppedPromise = NThreading::NewPromise();
        auto stoppedFuture = stoppedPromise.GetFuture();
        auto settings = TSessionSettings()
            .OnStateChanged([](auto state) {
                Cerr << "Session state: " << state << Endl;
            })
            .OnStopped([stoppedPromise]() mutable {
                stoppedPromise.SetValue();
            })
            .Timeout(TDuration::MilliSeconds(1000));

        auto startTimestamp = TInstant::Now();
        auto res = client->StartSession("/Some/Path", settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());

        auto session = res.ExtractResult();

        // Stop and destroy driver, forcing session to be cancelled
        client.reset();
        driver->Stop();
        driver.reset();

        stoppedFuture.Wait();
        auto endTimestamp = TInstant::Now();
        auto elapsed = endTimestamp - startTimestamp;

        // We expect session to be cancelled promptly
        UNIT_ASSERT_C(elapsed < TDuration::Seconds(1), "Elapsed time too large: " << elapsed);

        // Check the last failure stored in a session
        auto res2 = session.Close().ExtractValueSync();
        Cerr << "Close: " << res2.GetStatus() << ": " << res2.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(res2.GetStatus(), EStatus::CLIENT_CANCELLED, res2.GetIssues().ToString());
    }

}
