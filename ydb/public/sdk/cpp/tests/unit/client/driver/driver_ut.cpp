#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/constants.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_common.h>
#include <ydb/public/sdk/cpp/tests/common/fake_metric_registry.h>
#include <ydb/public/sdk/cpp/tests/common/fake_trace_provider.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/sdk_runtime/runtime.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections/grpc_connections.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_coordination_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/generic/mapfindptr.h>
#include <util/generic/scope.h>

#include <array>
#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <vector>

#include <google/protobuf/text_format.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

    constexpr const char LegacyV1Certificate[] = R"(-----BEGIN CERTIFICATE-----
MIIBbTCCARMCFBthJdWIg/H6ITeelffnCYoK8fDFMAoGCCqGSM49BAMCMDkxCzAJ
BgNVBAYTAlJVMQwwCgYDVQQKDANZREIxHDAaBgNVBAMME0xlZ2FjeSBUZXN0IFJv
b3QgQ0EwHhcNMjYwNzI3MDk0NDU2WhcNMzYwNzI0MDk0NDU2WjA5MQswCQYDVQQG
EwJSVTEMMAoGA1UECgwDWURCMRwwGgYDVQQDDBNMZWdhY3kgVGVzdCBSb290IENB
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE4zlS2ha5hOd20QJEh17FP/mjkzsO
PmwF7iY9zJ0HILwBjqxJSCGnNMMdT+A2d+Nry6de3WC6RkR72HTe6gffuTAKBggq
hkjOPQQDAgNIADBFAiEA/0rBKAconmtFcliTZ0i9HzIkQeG+E/zVMiUvlhwpylYC
IGfPhGBVwOMnr+uhwtpj4PAOIrlOQD/fBsaRtYuBRdg2
-----END CERTIFICATE-----)";

    std::string ReadBuildInfo(grpc::ServerContext* context) {
        const auto& metadata = context->client_metadata();
        const auto it = metadata.find(YDB_SDK_BUILD_INFO_HEADER);
        Y_ABORT_UNLESS(it != metadata.end());
        return {it->second.data(), it->second.length()};
    }

    class TMockDiscoveryService : public Ydb::Discovery::V1::DiscoveryService::Service {
    public:
        grpc::Status ListEndpoints(
                grpc::ServerContext* context,
                const Ydb::Discovery::ListEndpointsRequest* request,
                Ydb::Discovery::ListEndpointsResponse* response) override
        {
            BuildInfo = ReadBuildInfo(context);

            std::cerr << "ListEndpoints: " << request->ShortDebugString() << std::endl;

            const auto* result = MapFindPtr(MockResults, request->database());
            Y_ABORT_UNLESS(result, "Mock service doesn't have a result for database '%s'", request->database().c_str());

            auto* op = response->mutable_operation();
            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);
            op->mutable_result()->PackFrom(*result);
            return grpc::Status::OK;
        }

        // From database name to result
        std::unordered_map<std::string, Ydb::Discovery::ListEndpointsResult> MockResults;
        std::string BuildInfo;
    };

    class TMockTableService : public Ydb::Table::V1::TableService::Service {
    public:
        grpc::Status CreateSession(
                grpc::ServerContext* context,
                const Ydb::Table::CreateSessionRequest* request,
                Ydb::Table::CreateSessionResponse* response) override
        {
            BuildInfo = ReadBuildInfo(context);

            std::cerr << "CreateSession: " << request->ShortDebugString() << std::endl;

            Ydb::Table::CreateSessionResult result;
            result.set_session_id("my-session-id");

            auto* op = response->mutable_operation();
            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);
            op->mutable_result()->PackFrom(result);
            return grpc::Status::OK;
        }

        std::string BuildInfo;
    };

    template<class TService>
    std::unique_ptr<grpc::Server> StartGrpcServer(const std::string& address, TService& service) {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(TStringType{address}, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        return builder.BuildAndStart();
    }

    class TCountingCredentialsProvider final : public ICredentialsProvider {
    public:
        std::string GetAuthInfo() const override {
            return "token";
        }

        bool IsValid() const override {
            return true;
        }
    };

    class TCountingCredentialsProviderFactory final : public ICredentialsProviderFactory {
    public:
        explicit TCountingCredentialsProviderFactory(
            std::atomic_int& providerCount,
            std::function<void(int)> onCreate = {})
            : ProviderCount_(providerCount)
            , OnCreate_(std::move(onCreate))
        {}

        TCredentialsProviderPtr CreateProvider() const override {
            const auto count = ++ProviderCount_;
            if (OnCreate_) {
                OnCreate_(count);
            }
            return std::make_shared<TCountingCredentialsProvider>();
        }

        std::string GetClientIdentity() const override {
            return "same-credentials";
        }

    private:
        std::atomic_int& ProviderCount_;
        std::function<void(int)> OnCreate_;
    };

    class TDeferredAuthProvider final : public ICredentialsProvider {
    public:
        TDeferredAuthProvider()
            : AuthInfo_(NThreading::NewPromise<std::string>())
        {}

        std::string GetAuthInfo() const override {
            return AuthInfo_.GetFuture().GetValueSync();
        }

        NThreading::TFuture<std::string> GetAuthInfoAsync() const override {
            return AuthInfo_.GetFuture();
        }

        bool IsValid() const override {
            return true;
        }

        void SetReady() {
            AuthInfo_.SetValue("token");
        }

    private:
        NThreading::TPromise<std::string> AuthInfo_;
    };

    class TDeferredCredentialsFactory final : public ICredentialsProviderFactory {
    public:
        TDeferredCredentialsFactory()
            : Provider_(std::make_shared<TDeferredAuthProvider>())
        {}

        TCredentialsProviderPtr CreateProvider() const override {
            return Provider_;
        }

        void SetReady() {
            Provider_->SetReady();
        }

    private:
        std::shared_ptr<TDeferredAuthProvider> Provider_;
    };

    class TDeferredOperationService final: public Ydb::Operation::V1::OperationService::Service {
    public:
        grpc::Status GetOperation(grpc::ServerContext* context,
            const Ydb::Operations::GetOperationRequest* request,
            Ydb::Operations::GetOperationResponse* response) override
        {
            SawAuth.store(context->client_metadata().find(YDB_AUTH_TICKET_HEADER) != context->client_metadata().end());
            const auto call = ++Calls;
            if (call == 1) {
                Entered.TrySetValue();
                if (!Release.GetFuture().Wait(TDuration::Seconds(10))) {
                    return {grpc::StatusCode::DEADLINE_EXCEEDED, "Test did not release operation polling"};
                }
            }
            auto* operation = response->mutable_operation();
            operation->set_id(request->id());
            operation->set_ready(call > 1);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            context->AddInitialMetadata("poll-kind", "deferred");
            context->AddTrailingMetadata("poll-count", std::to_string(call));
            return grpc::Status::OK;
        }

        std::atomic_uint Calls = 0;
        std::atomic_bool SawAuth = false;
        NThreading::TPromise<void> Entered = NThreading::NewPromise();
        NThreading::TPromise<void> Release = NThreading::NewPromise();
    };

    class TControlledDiscoveryService final: public Ydb::Discovery::V1::DiscoveryService::Service {
    public:
        grpc::Status ListEndpoints(grpc::ServerContext*,
            const Ydb::Discovery::ListEndpointsRequest*,
            Ydb::Discovery::ListEndpointsResponse* response) override
        {
            Entered.TrySetValue();
            if (!Release.GetFuture().Wait(TDuration::Seconds(10))) {
                return {grpc::StatusCode::DEADLINE_EXCEEDED, "Test did not release discovery"};
            }
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Status);
            if (Status == Ydb::StatusIds::SUCCESS) {
                Ydb::Discovery::ListEndpointsResult result;
                auto* endpoint = result.add_endpoints();
                endpoint->set_address("127.0.0.1");
                endpoint->set_port(TablePort);
                operation->mutable_result()->PackFrom(result);
            } else {
                operation->add_issues()->set_message("Discovery denied by mock service");
            }
            return grpc::Status::OK;
        }

        ui16 TablePort = 0;
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        NThreading::TPromise<void> Entered = NThreading::NewPromise();
        NThreading::TPromise<void> Release = NThreading::NewPromise();
    };

    using TRpcOutcome = std::pair<bool, TPlainStatus>;

    NThreading::TFuture<TRpcOutcome> RunCreateSession(
        const std::shared_ptr<TGRpcConnectionsImpl>& connections, TDbDriverStatePtr state)
    {
        auto result = NThreading::NewPromise<TRpcOutcome>();
        TRpcRequestSettings settings;
        settings.Deadline = TDeadline::AfterDuration(TDuration::Seconds(10));
        connections->Run<Ydb::Table::V1::TableService,
            Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse>(
                Ydb::Table::CreateSessionRequest{},
                [result](Ydb::Table::CreateSessionResponse* response, TPlainStatus status) mutable {
                    result.SetValue({response != nullptr, std::move(status)});
                },
                &Ydb::Table::V1::TableService::Stub::AsyncCreateSession,
                std::move(state), settings);
        return result.GetFuture();
    }

    std::array<NThreading::TFuture<TRpcOutcome>, 2> StartStreams(
        const std::shared_ptr<TGRpcConnectionsImpl>& connections, const TDbDriverStatePtr& state,
        IQueueClientContextPtr context = {}, bool useAuth = true)
    {
        using TReadProcessor = NYdbGrpc::IStreamRequestReadProcessor<Ydb::Table::ReadTableResponse>;
        using TBidiProcessor = NYdbGrpc::IStreamRequestReadWriteProcessor<
            Ydb::Coordination::SessionRequest, Ydb::Coordination::SessionResponse>;
        auto read = NThreading::NewPromise<TRpcOutcome>();
        auto bidi = NThreading::NewPromise<TRpcOutcome>();
        TRpcRequestSettings settings;
        settings.UseAuth = useAuth;
        settings.Deadline = TDeadline::AfterDuration(TDuration::Seconds(10));
        connections->StartReadStream<Ydb::Table::V1::TableService,
            Ydb::Table::ReadTableRequest, Ydb::Table::ReadTableResponse>(
                Ydb::Table::ReadTableRequest{},
                [read](TPlainStatus status, TReadProcessor::TPtr processor) mutable {
                    if (processor) {
                        processor->Cancel();
                    }
                    read.SetValue({bool(processor), std::move(status)});
                }, &Ydb::Table::V1::TableService::Stub::AsyncStreamReadTable,
                state, settings, context);
        connections->StartBidirectionalStream<Ydb::Coordination::V1::CoordinationService,
            Ydb::Coordination::SessionRequest, Ydb::Coordination::SessionResponse>(
                [bidi](TPlainStatus status, TBidiProcessor::TPtr processor) {
                    if (processor) {
                        processor->Cancel();
                    }
                    auto result = bidi;
                    result.SetValue({bool(processor), std::move(status)});
                }, &Ydb::Coordination::V1::CoordinationService::Stub::AsyncSession,
                state, settings, std::move(context));
        return {read.GetFuture(), bidi.GetFuture()};
    }

    TDbDriverStatePtr MakeUndiscoveredState(const std::shared_ptr<TGRpcConnectionsImpl>& connections,
        const std::string& endpoint, EDiscoveryMode mode = EDiscoveryMode::Async)
    {
        auto state = std::make_shared<TDbDriverState>("/Root/runtime", endpoint, mode,
            TSslCredentials{}, connections);
        state->InitCredentials(CreateInsecureCredentialsProviderFactory());
        return state;
    }

} // namespace

Y_UNIT_TEST_SUITE(SdkRuntimeTest) {
    Y_UNIT_TEST(RuntimeIsProcessSingleton) {
        constexpr size_t ThreadCount = 8;
        std::array<TSdkRuntime*, ThreadCount> runtimes{};
        std::array<std::thread, ThreadCount> threads;

        for (size_t i = 0; i < ThreadCount; ++i) {
            threads[i] = std::thread([&, i] {
                runtimes[i] = &GetSdkRuntime();
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }

        for (auto* runtime : runtimes) {
            UNIT_ASSERT_VALUES_EQUAL(runtime, &GetSdkRuntime());
        }
    }

    Y_UNIT_TEST(RequestContextsCancelIndependently) {
        auto& network = GetSdkRuntime().GetNetwork(1);
        auto contextA = network.CreateContext();
        auto childA = contextA->CreateContext();
        auto contextB = network.CreateContext();
        contextA->Cancel();
        UNIT_ASSERT(contextA->IsCancelled());
        UNIT_ASSERT(childA->IsCancelled());
        UNIT_ASSERT(contextA->CreateContext()->IsCancelled());
        UNIT_ASSERT(!contextB->IsCancelled());
        UNIT_ASSERT(!network.CreateContext()->IsCancelled());
    }
}

Y_UNIT_TEST_SUITE(DeferredCredentialsTest) {
    Y_UNIT_TEST(RequestWaitsForAuthInfo) {
        auto factory = std::make_shared<TDeferredCredentialsFactory>();
        auto driver = TDriver(TDriverConfig()
            .SetEndpoint("localhost:100")
            .SetCredentialsProviderFactory(factory));
        auto result = TTableClient(driver).CreateSession();

        UNIT_ASSERT(!result.Wait(TDuration::MilliSeconds(100)));
        factory->SetReady();
        UNIT_ASSERT(result.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(result.GetValue().GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
    }

    Y_UNIT_TEST(RequestDeadlineWhileWaitingForCredentials) {
        auto factory = std::make_shared<TDeferredCredentialsFactory>();
        auto driver = TDriver(TDriverConfig()
            .SetEndpoint("localhost:100")
            .SetCredentialsProviderFactory(factory));
        auto result = TTableClient(driver).CreateSession(
            TCreateSessionSettings().ClientTimeout(TDuration::MilliSeconds(100))).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::CLIENT_DEADLINE_EXCEEDED);
    }

    Y_UNIT_TEST(DriverStopDoesNotCancelCredentialsWait) {
        auto factory = std::make_shared<TDeferredCredentialsFactory>();
        auto driver = TDriver(TDriverConfig()
            .SetEndpoint("localhost:100")
            .SetCredentialsProviderFactory(factory));
        auto result = TTableClient(driver).CreateSession();

        driver.Stop(true);
        UNIT_ASSERT(!result.IsReady());
        factory->SetReady();
        UNIT_ASSERT(result.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(result.GetValue().GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
    }

}

Y_UNIT_TEST_SUITE(DriverAsyncLifetimeTest) {
    Y_UNIT_TEST(DeferredOperationPollingOutlivesLastDriver) {
        TPortManager ports;
        TDeferredOperationService service;
        const std::string endpoint = TStringBuilder() << "127.0.0.1:" << ports.GetPort();
        auto server = StartGrpcServer(endpoint, service);
        UNIT_ASSERT(server);
        Y_SCOPE_EXIT(release = service.Release) {
            release.TrySetValue();
        };
        auto driver = std::make_unique<TDriver>(TDriverConfig()
            .SetEndpoint(endpoint).SetDiscoveryMode(EDiscoveryMode::Off));
        auto connections = CreateInternalInterface(*driver);
        std::weak_ptr<TGRpcConnectionsImpl> weak = connections;
        auto state = connections->GetDriverState({}, {}, {}, {}, {});
        auto result = NThreading::NewPromise<TRpcOutcome>();
        TDeferredAction action("poll-operation",
            [result](Ydb::Operations::Operation* operation, TPlainStatus status) mutable {
                result.SetValue({operation && operation->ready() && operation->id() == "poll-operation",
                    std::move(status)});
            }, connections.get(), connections->CreateContext(), std::chrono::milliseconds::zero(),
            TDeadline::AfterDuration(TDuration::Seconds(10)), state, endpoint);
        action.Start();
        UNIT_ASSERT(service.Entered.GetFuture().Wait(TDuration::Seconds(10)));
        driver->Stop(true);
        driver.reset();
        connections.reset();
        state.reset();
        UNIT_ASSERT(!weak.expired());
        service.Release.SetValue();
        UNIT_ASSERT(result.GetFuture().Wait(TDuration::Seconds(10)));
        const auto outcome = result.GetFuture().GetValue();
        UNIT_ASSERT(outcome.first);
        UNIT_ASSERT_VALUES_EQUAL(outcome.second.Status, EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(outcome.second.Endpoint, endpoint);
        const auto pollCount = outcome.second.Metadata.find("poll-count");
        UNIT_ASSERT(pollCount != outcome.second.Metadata.end());
        UNIT_ASSERT_VALUES_EQUAL(pollCount->second, "2");
        const auto pollKind = outcome.second.Metadata.find("poll-kind");
        UNIT_ASSERT(pollKind != outcome.second.Metadata.end());
        UNIT_ASSERT_VALUES_EQUAL(pollKind->second, "deferred");
        UNIT_ASSERT_VALUES_EQUAL(service.Calls.load(), 2);
    }

    Y_UNIT_TEST(DiscoveryEndpointRpcRetainsFacilityAndBypassesAuth) {
        TPortManager ports;
        TDeferredOperationService service;
        const std::string endpoint = TStringBuilder() << "127.0.0.1:" << ports.GetPort();
        auto server = StartGrpcServer(endpoint, service);
        UNIT_ASSERT(server);
        Y_SCOPE_EXIT(release = service.Release) {
            release.TrySetValue();
        };
        auto driver = std::make_unique<TDriver>(TDriverConfig()
            .SetEndpoint(endpoint).SetDiscoveryMode(EDiscoveryMode::Off));
        auto connections = CreateInternalInterface(*driver);
        auto state = MakeUndiscoveredState(connections, endpoint);
        state->InitCredentials(CreateOAuthCredentialsProviderFactory("invalid\ntoken"));
        std::weak_ptr<TGRpcConnectionsImpl> weak = connections;
        auto result = NThreading::NewPromise<TRpcOutcome>();
        Ydb::Operations::GetOperationRequest request;
        request.set_id("discovery-operation");
        TRpcRequestSettings settings;
        settings.Deadline = TDeadline::AfterDuration(TDuration::Seconds(10));
        TGRpcConnectionsImpl::RunOnDiscoveryEndpoint<Ydb::Operation::V1::OperationService,
            Ydb::Operations::GetOperationRequest, Ydb::Operations::GetOperationResponse>(
                state, std::move(request),
                [result](Ydb::Operations::GetOperationResponse* response, TPlainStatus status) mutable {
                    result.SetValue({response && response->operation().id() == "discovery-operation",
                        std::move(status)});
                }, &Ydb::Operation::V1::OperationService::Stub::AsyncGetOperation, settings);
        UNIT_ASSERT(service.Entered.GetFuture().Wait(TDuration::Seconds(10)));
        driver->Stop(true);
        driver.reset();
        connections.reset();
        state.reset();
        UNIT_ASSERT(!weak.expired());
        service.Release.SetValue();
        UNIT_ASSERT(result.GetFuture().Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(result.GetFuture().GetValue().first);
        UNIT_ASSERT_VALUES_EQUAL(result.GetFuture().GetValue().second.Status, EStatus::SUCCESS);
        UNIT_ASSERT(!service.SawAuth.load());
    }

    Y_UNIT_TEST(CancelledDeferredTimerReportsEndpointAfterDriverDestruction) {
        const std::string endpoint = "localhost:1";
        auto driver = std::make_unique<TDriver>(TDriverConfig()
            .SetEndpoint(endpoint).SetDiscoveryMode(EDiscoveryMode::Off));
        auto connections = CreateInternalInterface(*driver);
        auto context = connections->CreateContext();
        Y_SCOPE_EXIT(context) {
            context->Cancel();
        };
        auto result = NThreading::NewPromise<TRpcOutcome>();
        TDeferredAction action("cancelled-operation",
            [result](Ydb::Operations::Operation* operation, TPlainStatus status) mutable {
                result.SetValue({operation != nullptr, std::move(status)});
            }, connections.get(), context, std::chrono::hours(1), TDeadline::Max(),
            connections->GetDriverState({}, {}, {}, {}, {}), endpoint);
        action.Start();
        driver->Stop(false);
        driver.reset();
        connections.reset();
        UNIT_ASSERT(!result.GetFuture().IsReady());
        context->Cancel();
        UNIT_ASSERT(result.GetFuture().Wait(TDuration::Seconds(10)));
        const auto outcome = result.GetFuture().GetValue();
        UNIT_ASSERT(!outcome.first);
        UNIT_ASSERT_VALUES_EQUAL(outcome.second.Status, EStatus::CLIENT_INTERNAL_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(outcome.second.Endpoint, endpoint);
        UNIT_ASSERT_STRING_CONTAINS(outcome.second.Issues.ToString(), "Deferred timer interrupted");
        UNIT_ASSERT_STRING_CONTAINS(outcome.second.Issues.ToString(), "Grpc error response on endpoint " + endpoint);
    }

    Y_UNIT_TEST(AsyncDiscoveryRetainsStateAndEnforcesQueueLimit) {
        TPortManager ports;
        TMockTableService table;
        const auto tablePort = ports.GetPort();
        auto tableServer = StartGrpcServer(TStringBuilder() << "127.0.0.1:" << tablePort, table);
        UNIT_ASSERT(tableServer);
        TControlledDiscoveryService discovery;
        discovery.TablePort = tablePort;
        const std::string endpoint = TStringBuilder() << "127.0.0.1:" << ports.GetPort();
        auto server = StartGrpcServer(endpoint, discovery);
        UNIT_ASSERT(server);
        Y_SCOPE_EXIT(release = discovery.Release) {
            release.TrySetValue();
        };
        auto driver = std::make_unique<TDriver>(TDriverConfig().SetEndpoint(endpoint)
            .SetDiscoveryMode(EDiscoveryMode::Off).SetMaxQueuedRequests(1));
        auto connections = CreateInternalInterface(*driver);
        std::weak_ptr<TGRpcConnectionsImpl> weak = connections;
        auto state = MakeUndiscoveredState(connections, endpoint);
        auto first = RunCreateSession(connections, state);
        UNIT_ASSERT(discovery.Entered.GetFuture().Wait(TDuration::Seconds(10)));
        auto rejected = RunCreateSession(connections, state);
        UNIT_ASSERT(rejected.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(!rejected.GetValue().first);
        UNIT_ASSERT_VALUES_EQUAL(rejected.GetValue().second.Status, EStatus::CLIENT_LIMITS_REACHED);
        driver->Stop(true);
        driver.reset();
        connections.reset();
        state.reset();
        UNIT_ASSERT(!weak.expired());
        discovery.Release.SetValue();
        UNIT_ASSERT(first.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(first.GetValue().first);
        UNIT_ASSERT_VALUES_EQUAL(first.GetValue().second.Status, EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(first.GetValue().second.Endpoint,
            std::string(TStringBuilder() << "127.0.0.1:" << tablePort));
    }

    Y_UNIT_TEST(AsyncDiscoveryErrorReachesQueuedRequest) {
        TPortManager ports;
        TControlledDiscoveryService discovery;
        discovery.Status = Ydb::StatusIds::UNAUTHORIZED;
        const std::string endpoint = TStringBuilder() << "127.0.0.1:" << ports.GetPort();
        auto server = StartGrpcServer(endpoint, discovery);
        UNIT_ASSERT(server);
        Y_SCOPE_EXIT(release = discovery.Release) {
            release.TrySetValue();
        };
        TDriver driver(TDriverConfig().SetEndpoint(endpoint).SetDiscoveryMode(EDiscoveryMode::Off));
        auto connections = CreateInternalInterface(driver);
        auto pending = RunCreateSession(connections, MakeUndiscoveredState(connections, endpoint));
        UNIT_ASSERT(discovery.Entered.GetFuture().Wait(TDuration::Seconds(10)));
        driver.Stop(true);
        discovery.Release.SetValue();
        UNIT_ASSERT(pending.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(!pending.GetValue().first);
        UNIT_ASSERT_VALUES_EQUAL(pending.GetValue().second.Status, EStatus::UNAUTHORIZED);
        UNIT_ASSERT_STRING_CONTAINS(pending.GetValue().second.Issues.ToString(), "Discovery denied by mock service");
    }

    Y_UNIT_TEST(StreamsRejectInvalidTlsWithoutProcessor) {
        TDriver driver(TDriverConfig().SetEndpoint("localhost:1")
            .SetDiscoveryMode(EDiscoveryMode::Off).UseSecureConnection("not-a-certificate"));
        auto connections = CreateInternalInterface(driver);
        auto state = connections->GetDriverState({}, {}, {}, {}, {});
        driver.Stop(true);
        for (const auto& result : StartStreams(connections, state)) {
            UNIT_ASSERT(result.Wait(TDuration::Seconds(10)));
            UNIT_ASSERT(!result.GetValue().first);
            UNIT_ASSERT_VALUES_EQUAL(result.GetValue().second.Status, EStatus::TRANSPORT_UNAVAILABLE);
            UNIT_ASSERT_STRING_CONTAINS(result.GetValue().second.Issues.ToString(), "Client TLS credentials validation failed");
        }
    }

    Y_UNIT_TEST(StreamsPreserveDiscoveryFailureWithoutProcessor) {
        TDriver driver(TDriverConfig().SetEndpoint("localhost:1").SetDiscoveryMode(EDiscoveryMode::Off));
        auto connections = CreateInternalInterface(driver);
        auto state = MakeUndiscoveredState(connections, "localhost:1", EDiscoveryMode::Sync);
        state->LastDiscoveryStatus = TPlainStatus(EStatus::UNAUTHORIZED, "Discovery denied by mock service");
        driver.Stop(true);
        for (const auto& result : StartStreams(connections, state)) {
            UNIT_ASSERT(result.Wait(TDuration::Seconds(10)));
            UNIT_ASSERT(!result.GetValue().first);
            UNIT_ASSERT_VALUES_EQUAL(result.GetValue().second.Status, EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS(result.GetValue().second.Issues.ToString(), "Discovery denied by mock service");
        }
    }

    Y_UNIT_TEST(StreamsRejectInvalidTokenWithoutProcessor) {
        TDriver driver(TDriverConfig().SetEndpoint("localhost:1")
            .SetDiscoveryMode(EDiscoveryMode::Off).SetAuthToken("invalid\ntoken"));
        auto connections = CreateInternalInterface(driver);
        auto state = connections->GetDriverState({}, {}, {}, {}, {});
        driver.Stop(true);
        for (const auto& result : StartStreams(connections, state)) {
            UNIT_ASSERT(result.Wait(TDuration::Seconds(10)));
            UNIT_ASSERT(!result.GetValue().first);
            UNIT_ASSERT_VALUES_EQUAL(result.GetValue().second.Status, EStatus::CLIENT_UNAUTHENTICATED);
            UNIT_ASSERT_STRING_CONTAINS(result.GetValue().second.Issues.ToString(), "illegal characters");
        }
    }

    Y_UNIT_TEST(ExplicitCancellationRejectsStreamStart) {
        TDriver driver(TDriverConfig().SetEndpoint("127.0.0.1:0").SetDiscoveryMode(EDiscoveryMode::Off));
        auto connections = CreateInternalInterface(driver);
        auto state = connections->GetDriverState({}, {}, {}, {}, {});
        auto context = connections->CreateContext();
        context->Cancel();
        driver.Stop(true);
        for (const auto& result : StartStreams(connections, state, context, false)) {
            UNIT_ASSERT(result.Wait(TDuration::Seconds(10)));
            UNIT_ASSERT(!result.GetValue().first);
            const auto status = result.GetValue().second.Status;
            // Transport failure may finish before the low-level cancellation subscription runs.
            UNIT_ASSERT(status == EStatus::CLIENT_CANCELLED || status == EStatus::TRANSPORT_UNAVAILABLE);
        }
    }

    Y_UNIT_TEST(SyncDiscoveryWithoutEndpointsPreservesFailure) {
        TDriver driver(TDriverConfig().SetEndpoint("localhost:1").SetDiscoveryMode(EDiscoveryMode::Off));
        auto connections = CreateInternalInterface(driver);
        for (const auto previous : {EStatus::SUCCESS, EStatus::UNAUTHORIZED}) {
            auto state = MakeUndiscoveredState(connections, "localhost:1", EDiscoveryMode::Sync);
            state->LastDiscoveryStatus = TPlainStatus(previous, "Previous discovery status");
            auto result = RunCreateSession(connections, std::move(state));
            UNIT_ASSERT(result.Wait(TDuration::Seconds(10)));
            UNIT_ASSERT(!result.GetValue().first);
            UNIT_ASSERT_VALUES_EQUAL(result.GetValue().second.Status,
                previous == EStatus::SUCCESS ? EStatus::UNAVAILABLE : previous);
            UNIT_ASSERT_STRING_CONTAINS(result.GetValue().second.Issues.ToString(), "Endpoint list is empty");
        }
    }
}

Y_UNIT_TEST_SUITE(CppGrpcClientSimpleTest) {
    Y_UNIT_TEST(ConcurrentClientsShareCredentialsInitializationAndRetryFailure) {
        for (const bool failFirst : {false, true}) {
            std::atomic_int providerCount = 0;
            auto entered = NThreading::NewPromise<void>();
            auto release = NThreading::NewPromise<void>();
            auto factory = std::make_shared<TCountingCredentialsProviderFactory>(
                providerCount, [entered, release, failFirst](int count) mutable {
                    if (count == 1) {
                        entered.SetValue();
                        release.GetFuture().GetValueSync();
                        if (failFirst) {
                            ythrow yexception() << "Credentials initialization failed";
                        }
                    }
                });
            TDriver driver(TDriverConfig()
                .SetEndpoint("localhost:1")
                .SetDiscoveryMode(EDiscoveryMode::Off)
                .SetSocketIdleTimeout(TDuration::Max())
                .SetCredentialsProviderFactory(factory));
            auto first = std::async(std::launch::async, [&] {
                return std::make_shared<TTableClient>(driver);
            });
            std::future<std::shared_ptr<TTableClient>> second;
            Y_SCOPE_EXIT(&release, &first, &second) {
                release.TrySetValue();
                if (first.valid()) {
                    first.wait();
                }
                if (second.valid()) {
                    second.wait();
                }
            };
            UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(10)));
            auto secondStarted = std::make_shared<std::promise<void>>();
            auto secondStartedFuture = secondStarted->get_future();
            second = std::async(std::launch::async, [&, secondStarted] {
                secondStarted->set_value();
                return std::make_shared<TTableClient>(driver);
            });
            UNIT_ASSERT(secondStartedFuture.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
            UNIT_ASSERT(second.wait_for(std::chrono::seconds(0)) != std::future_status::ready);
            release.SetValue();
            std::shared_ptr<TTableClient> firstClient;
            if (failFirst) {
                UNIT_ASSERT_EXCEPTION_CONTAINS(first.get(), yexception, "Credentials initialization failed");
            } else {
                firstClient = first.get();
            }
            auto secondClient = second.get();
            UNIT_ASSERT(secondClient);
            UNIT_ASSERT_VALUES_EQUAL(providerCount.load(), failFirst ? 2 : 1);
        }
    }

    Y_UNIT_TEST(ReusesCredentialsProviderForSameIdentity) {
        std::atomic_int providerCount = 0;
        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint("localhost:1")
                .SetDatabase("/Root")
                .SetDiscoveryMode(EDiscoveryMode::Off));

        auto firstClient = TTableClient(driver, TClientSettings().CredentialsProviderFactory(
            std::make_shared<TCountingCredentialsProviderFactory>(providerCount)));
        auto secondClient = TTableClient(driver, TClientSettings().CredentialsProviderFactory(
            std::make_shared<TCountingCredentialsProviderFactory>(providerCount)));

        UNIT_ASSERT_VALUES_EQUAL(providerCount.load(), 1);
    }

    Y_UNIT_TEST(InvalidRootCertificatePemFailsFast) {
        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint("localhost:100")
                .UseSecureConnection("not-a-certificate"));
        auto client = NTable::TTableClient(driver);

        auto result = client.CreateSession().GetValueSync();

        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Client TLS credentials validation failed");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "root CA PEM:");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "failed to parse certificate #1");
    }

    Y_UNIT_TEST(LegacyV1TrustAnchorPassesValidation) {
        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint("localhost:100")
                .UseSecureConnection(LegacyV1Certificate));
        auto client = NTable::TTableClient(driver);

        auto result = client.CreateSession().GetValueSync();
        auto issues = result.GetIssues().ToString();

        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
        UNIT_ASSERT(issues.find("Client TLS credentials validation failed") == std::string::npos);
    }

    Y_UNIT_TEST(MalformedCertificateAfterValidRootPassesValidation) {
        const std::string rootBundle = std::string(LegacyV1Certificate) + R"(
-----BEGIN CERTIFICATE-----
not-base64
-----END CERTIFICATE-----)";
        grpc::SslCredentialsOptions sslOptions{
            .pem_root_certs = NYdb::TStringType{rootBundle},
        };
        std::string validationDetail;

        UNIT_ASSERT(NYdbGrpc::ValidateTlsCredentials(sslOptions, validationDetail));
        UNIT_ASSERT(validationDetail.empty());
    }

    Y_UNIT_TEST(EmptyRootCertificateWithoutClientCredentialsKeepsBehavior) {
        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint("localhost:100")
                .UseSecureConnection(""));
        auto client = NTable::TTableClient(driver);

        auto result = client.CreateSession().GetValueSync();
        auto issues = result.GetIssues().ToString();

        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
        UNIT_ASSERT(issues.find("Client TLS credentials validation failed") == std::string::npos);
    }

    Y_UNIT_TEST(InvalidClientCertificateFailsFast) {
        const std::string privateKeyOnly = "-----BEGIN PRIVATE KEY-----\ninvalid\n-----END PRIVATE KEY-----\n";

        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint("localhost:100")
                .UseClientCertificate("", privateKeyOnly));
        auto client = NTable::TTableClient(driver);

        auto result = client.CreateSession().GetValueSync();

        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Client TLS credentials validation failed");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "client TLS:");
    }

    Y_UNIT_TEST(ConnectWrongPort) {
        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint("localhost:100"));
        auto client = NTable::TTableClient(driver);
        auto sessionFuture = client.CreateSession();

        UNIT_ASSERT(sessionFuture.Wait(TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(ConnectWrongPortRetry) {
        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint("localhost:100"));
        auto client = NTable::TTableClient(driver);

        std::atomic_int counter = 0;
        std::function<void(const NTable::TAsyncCreateSessionResult& future)> handler =
            [&handler, &counter, client] (const NTable::TAsyncCreateSessionResult& future) mutable {
                UNIT_ASSERT_EQUAL(future.GetValue().GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
                UNIT_ASSERT_EXCEPTION(future.GetValue().GetSession(), NYdb::TContractViolation);
                ++counter;
                if (counter.load() > 4) {
                    return;
                }

                auto f = client.CreateSession();

                f.Apply(handler).GetValueSync();
            };

        client.CreateSession().Apply(handler).GetValueSync();
        UNIT_ASSERT_EQUAL(counter, 5);
    }

    Y_UNIT_TEST(TokenCharacters) {
        auto checkToken = [](const std::string& token) {
            auto driver = TDriver(
                TDriverConfig()
                    .SetEndpoint("localhost:100")
                    .SetAuthToken(token));
            auto client = NTable::TTableClient(driver);

            auto result = client.CreateSession().GetValueSync();

            return result.GetStatus();
        };

        std::vector<std::string> InvalidTokens = {
            std::string("\t"),
            std::string("\n"),
            std::string("\r")
        };
        for (auto& t : InvalidTokens) {
            UNIT_ASSERT_EQUAL(checkToken(t), EStatus::CLIENT_UNAUTHENTICATED);
        }

        std::vector<std::string> ValidTokens = {
            std::string("qwerty 1234 <>,.?/:;\"'\\|}{~`!@#$%^&*()_+=-"),
            std::string()
        };
        for (auto& t : ValidTokens) {
            UNIT_ASSERT_EQUAL(checkToken(t), EStatus::TRANSPORT_UNAVAILABLE);
        }
    }

    Y_UNIT_TEST(UsingIpAddresses) {
        TPortManager pm;

        // Start our mock table service
        TMockTableService tableService;
        ui16 tablePort = pm.GetPort();
        auto tableServer = StartGrpcServer(
                TStringBuilder() << "127.0.0.1:" << tablePort,
                tableService);

        // Start our mock discovery service
        TMockDiscoveryService discoveryService;
        {
            auto& dbResult = discoveryService.MockResults["/Root/My/DB"];
            auto* endpoint = dbResult.add_endpoints();
            endpoint->set_address("this.dns.name.is.not.reachable");
            endpoint->set_port(tablePort);
            endpoint->add_ip_v4("127.0.0.1");
        }
        ui16 discoveryPort = pm.GetPort();
        auto discoveryServer = StartGrpcServer(
                TStringBuilder() << "0.0.0.0:" << discoveryPort,
                discoveryService);

        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << discoveryPort)
                .SetDatabase("/Root/My/DB")
                .SetTraceProvider(std::make_shared<NTests::TFakeTraceProvider>())
                .SetMetricRegistry(std::make_shared<NTests::TFakeMetricRegistry>())
                .AppendBuildInfo("test-client/1.2.3"));
        auto client = NTable::TTableClient(driver);
        auto sessionFuture = client.CreateSession();

        UNIT_ASSERT(sessionFuture.Wait(TDuration::Seconds(10)));
        auto sessionResult = sessionFuture.ExtractValueSync();
        UNIT_ASSERT(sessionResult.IsSuccess());
        auto session = sessionResult.GetSession();
        UNIT_ASSERT_VALUES_EQUAL(session.GetId(), "my-session-id");

        const auto baseBuildInfo = "ydb-cpp-sdk/" + GetSdkSemver();
        UNIT_ASSERT_VALUES_EQUAL(
            discoveryService.BuildInfo,
            baseBuildInfo
                + " ydb-sdk-tracing/" + std::string(NObservability::kTracingChainVersion)
                + " ydb-sdk-metrics/" + std::string(NObservability::kMetricsChainVersion)
                + ";test-client/1.2.3");
        UNIT_ASSERT_VALUES_EQUAL(tableService.BuildInfo, baseBuildInfo + ";test-client/1.2.3");
    }

    Y_UNIT_TEST(WithoutDiscoveryDriverLevel) {
        TPortManager pm;

        // Start our mock table service
        TMockTableService tableService;
        ui16 tablePort = pm.GetPort();
        auto tableServer = StartGrpcServer(
                TStringBuilder() << "127.0.0.1:" << tablePort,
                tableService);

        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << tablePort)
                .SetDiscoveryMode(EDiscoveryMode::Off)
                .SetDatabase("/Root/My/DB"));
        auto client = NTable::TTableClient(driver);
        auto sessionFuture = client.CreateSession();

        UNIT_ASSERT(sessionFuture.Wait(TDuration::Seconds(10)));
        auto sessionResult = sessionFuture.ExtractValueSync();
        UNIT_ASSERT(sessionResult.IsSuccess());
        auto session = sessionResult.GetSession();
        UNIT_ASSERT_VALUES_EQUAL(session.GetId(), "my-session-id");
    }

    Y_UNIT_TEST(WithoutDiscoveryClientLevel) {
        TPortManager pm;

        // Start our mock table service
        TMockTableService tableService;
        ui16 tablePort = pm.GetPort();
        auto tableServer = StartGrpcServer(
                TStringBuilder() << "127.0.0.1:" << tablePort,
                tableService);

        auto driver = TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << tablePort)
                .SetDatabase("/Root/My/DB"));
        auto client = NTable::TTableClient(driver, TClientSettings().DiscoveryMode(EDiscoveryMode::Off));
        auto sessionFuture = client.CreateSession();

        UNIT_ASSERT(sessionFuture.Wait(TDuration::Seconds(10)));
        auto sessionResult = sessionFuture.ExtractValueSync();
        UNIT_ASSERT(sessionResult.IsSuccess());
        auto session = sessionResult.GetSession();
        UNIT_ASSERT_VALUES_EQUAL(session.GetId(), "my-session-id");
    }

}
