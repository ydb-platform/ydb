#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <grpcpp/server_builder.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/src/client/impl/endpoints/endpoints.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <sstream>
#include <set>
#include <memory>
#include <string>
#include <vector>

using namespace NYdb;

namespace {

    std::string Address(ui16 port) {
        return "127.0.0.1:" + std::to_string(port);
    }

    std::unique_ptr<grpc::Server> StartServer(ui16 port, grpc::Service& service, std::unique_ptr<grpc::ServerBuilderOption> option = {}) {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("0.0.0.0:" + ToString(port), grpc::InsecureServerCredentials());
        if (option) {
            builder.SetOption(std::move(option));
        }
        builder.RegisterService(&service);
        return builder.BuildAndStart();
    }

#if defined(_linux_)
    bool HasEstablishedConnection(ui16 port) {
        for (const auto* path : {"/proc/net/tcp", "/proc/net/tcp6"}) {
            std::ifstream table(path);
            for (std::string line; std::getline(table, line);) {
                std::istringstream columns(line);
                std::string index, local, remote, state;
                if (columns >> index >> local >> remote >> state && state == "01"
                    && std::stoul(remote.substr(remote.find(':') + 1), nullptr, 16) == port) {
                    return true;
                }
            }
        }
        return false;
    }

    bool WaitUntilDisconnected(ui16 port) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(3);
        while (HasEstablishedConnection(port) && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(10));
        }
        return !HasEstablishedConnection(port);
    }
#endif

    std::string RpcPeer(const TStatus& status) {
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        const auto& headers = status.GetResponseMetadata();
        UNIT_ASSERT(headers.contains("test-peer"));
        return headers.find("test-peer")->second;
    }

    class TTableService: public Ydb::Table::V1::TableService::Service {
    public:
        grpc::Status BulkUpsert(grpc::ServerContext* context, const Ydb::Table::BulkUpsertRequest*,
                                Ydb::Table::BulkUpsertResponse* response) override {
            context->AddInitialMetadata("test-peer", context->peer());
            ++Calls;
            if (HoldNext.exchange(false)) {
                Held.TrySetValue();
                ReleaseHeld.GetFuture().Wait(TDuration::Seconds(10));
            }
            if (FailNext.exchange(false)) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "data connection lost");
            }
            auto* op = response->mutable_operation();
            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);
            return grpc::Status::OK;
        }

        std::atomic_uint Calls = 0;
        std::atomic_bool FailNext = false;
        std::atomic_bool HoldNext = false;
        NThreading::TPromise<void> Held = NThreading::NewPromise<void>();
        NThreading::TPromise<void> ReleaseHeld = NThreading::NewPromise<void>();
    };

    class TDiscovery : public Ydb::Discovery::V1::DiscoveryService::Service {
    public:
        std::function<grpc::Status(grpc::ServerContext*, Ydb::Discovery::ListEndpointsResult&)> Lookup;

        grpc::Status ListEndpoints(grpc::ServerContext* context, const Ydb::Discovery::ListEndpointsRequest*,
                                   Ydb::Discovery::ListEndpointsResponse* response) override {
            Ydb::Discovery::ListEndpointsResult result;
            auto status = Lookup(context, result);
            auto* op = response->mutable_operation();
            op->set_status(Ydb::StatusIds::SUCCESS);
            op->set_ready(true);
            op->mutable_result()->PackFrom(result);
            return status;
        }
    };
} // namespace

Y_UNIT_TEST_SUITE(EndpointRediscovery) {
    Y_UNIT_TEST(RetiredEndpointHandleCannotDispatchAnotherRpc) {
        TPortManager ports;
        const auto port = ports.GetPort();
        TTableService table;
        auto live = StartServer(port, table);
        NYdbGrpc::TGRpcClientLow client;
        using TService = Ydb::Table::V1::TableService;
        auto connection = client.CreateGRpcServiceConnection<TService>(NYdbGrpc::TGRpcClientConfig(Address(port)));
        auto request = [&](const NYdbGrpc::TCallMeta& meta) {
            auto completed = NThreading::NewPromise<int>();
            connection->DoAdvancedRequest<Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>({},
                [completed](const auto&, NYdbGrpc::TGrpcStatus&& status, auto&&) mutable {
                    completed.SetValue(status.GRpcStatusCode);
                }, &TService::Stub::AsyncBulkUpsert, meta);
            return completed.GetFuture().GetValueSync();
        };
        NYdbGrpc::TCallMeta stale;
        stale.EndpointAdmission = std::make_shared<NYdbGrpc::TEndpointAdmission>();
        stale.EndpointAdmission->Retire();
        UNIT_ASSERT_VALUES_EQUAL(request(stale), int(grpc::StatusCode::UNAVAILABLE));
        UNIT_ASSERT_VALUES_EQUAL(table.Calls.load(), 0);
        NYdbGrpc::TCallMeta replacement;
        replacement.EndpointAdmission = std::make_shared<NYdbGrpc::TEndpointAdmission>();
        UNIT_ASSERT_VALUES_EQUAL(request(replacement), int(grpc::StatusCode::OK));
        UNIT_ASSERT_VALUES_EQUAL(request(stale), int(grpc::StatusCode::UNAVAILABLE));
        UNIT_ASSERT_VALUES_EQUAL(table.Calls.load(), 1);
    }

    Y_UNIT_TEST(SimpleUnaryRejectsRetiredEndpointHandles) {
        TPortManager ports;
        const auto port = ports.GetPort();
        TTableService table;
        auto server = StartServer(port, table);
        NYdbGrpc::TGRpcClientLow client;
        using TService = Ydb::Table::V1::TableService;
        auto connection = client.CreateGRpcServiceConnection<TService>(NYdbGrpc::TGRpcClientConfig(Address(port)));
        NYdbGrpc::TCallMeta meta;
        meta.Timeout = std::chrono::seconds(3);
        meta.EndpointAdmission = std::make_shared<NYdbGrpc::TEndpointAdmission>();
        auto call = [&] {
            auto status = NThreading::NewPromise<int>();
            connection->DoRequest<Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>({},
                [status](NYdbGrpc::TGrpcStatus&& response, auto&&) mutable {
                    status.SetValue(response.GRpcStatusCode);
                }, &TService::Stub::AsyncBulkUpsert, meta);
            return status.GetFuture().GetValueSync();
        };
        UNIT_ASSERT_VALUES_EQUAL(call(), int(grpc::StatusCode::OK));
        meta.EndpointAdmission->Retire();
        UNIT_ASSERT_VALUES_EQUAL(call(), int(grpc::StatusCode::UNAVAILABLE));
        UNIT_ASSERT_VALUES_EQUAL(table.Calls.load(), 1);
    }

    Y_UNIT_TEST(RetiringAnEndpointPreservesHeldQueryStreams) {
        using TService = Ydb::Query::V1::QueryService;
        using TRequest = Ydb::Query::AttachSessionRequest;
        using TResponse = Ydb::Query::SessionState;
        using TReader = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
        class TQueryService : public TService::Service {
        public:
            NThreading::TPromise<void> Release = NThreading::NewPromise<void>();

        private:
            grpc::Status AttachSession(grpc::ServerContext* context, const TRequest*, grpc::ServerWriter<TResponse>* stream) override {
                context->AddInitialMetadata("replica", "source");
                TResponse state;
                state.set_status(Ydb::StatusIds::SUCCESS);
                stream->Write(state);
                if (Release.GetFuture().Wait(TDuration::Seconds(10))) {
                    stream->Write(state);
                }
                return grpc::Status::OK;
            }
        } service;
        TPortManager ports;
        const auto port = ports.GetPort();
        auto server = StartServer(port, service);
        NYdbGrpc::TGRpcClientLow client;
        auto channel = NYdbGrpc::CreateChannelInterface(NYdbGrpc::TGRpcClientConfig(Address(port)));
        auto holder = std::make_unique<NYdbGrpc::TStubsHolder>(std::move(channel));
        auto conn = client.CreateGRpcServiceConnection<TService>(*holder);
        auto open = [&](const NYdbGrpc::TCallMeta& meta) {
            auto started = NThreading::NewPromise<TReader>();
            conn->DoStreamRequest<TRequest, TResponse>({}, [started](auto, TReader reader) mutable {
                started.SetValue(std::move(reader));
            }, &TService::Stub::AsyncAttachSession, meta);
            auto reader = started.GetFuture().GetValueSync();
            UNIT_ASSERT(reader);
            TResponse response;
            auto read = NThreading::NewPromise<bool>();
            reader->Read(&response, [read](auto status) mutable { read.SetValue(status.Ok()); });
            UNIT_ASSERT(read.GetFuture().GetValueSync());
            return reader;
        };
        NYdbGrpc::TCallMeta meta;
        meta.EndpointAdmission = std::make_shared<NYdbGrpc::TEndpointAdmission>();
        auto reader = open(meta);
        conn.reset();
        holder.reset();
        meta.EndpointAdmission->Retire();
        service.Release.SetValue();
        TResponse response;
        auto read = NThreading::NewPromise<bool>();
        reader->Read(&response, [read](auto status) mutable { read.SetValue(status.Ok()); });
        UNIT_ASSERT(read.GetFuture().Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(read.GetFuture().GetValueSync());
        UNIT_ASSERT_VALUES_EQUAL(int(response.status()), int(Ydb::StatusIds::SUCCESS));
        reader->Cancel();
    }

    Y_UNIT_TEST(StoppingDuringInitialPollCompletesPendingRequests) {
        TPortManager ports;
        const auto port = ports.GetPort();
        TDiscovery bootstrap;
        auto held = NThreading::NewPromise<void>();
        bootstrap.Lookup = [&](auto* context, auto&) {
            held.TrySetValue();
            while (!context->IsCancelled()) {
                Sleep(TDuration::MilliSeconds(10));
            }
            return grpc::Status::CANCELLED;
        };
        auto server = StartServer(port, bootstrap);
        TDriver driver(TDriverConfig().SetEndpoint(Address(port)).SetDatabase("/Root")
            .SetDiscoveryMode(EDiscoveryMode::LongPoll));
        NTable::TTableClient client(driver);
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        auto result = client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings);
        UNIT_ASSERT(held.GetFuture().Wait(TDuration::Seconds(5)));
        driver.Stop(true);
        UNIT_ASSERT(result.Wait(TDuration::Seconds(3)));
        UNIT_ASSERT_VALUES_EQUAL(result.GetValueSync().GetStatus(), EStatus::CLIENT_CANCELLED);
    }

    Y_UNIT_TEST(IndependentDriversKeepSharingDataSockets) {
        TPortManager ports;
        const auto bootPort = ports.GetPort(), dataPort = ports.GetPort();
        TTableService table;
        auto node = StartServer(dataPort, table);
        TDiscovery discovery;
        discovery.Lookup = [&](auto*, auto& result) {
            auto* endpoint = result.add_endpoints();
            endpoint->set_address("127.0.0.1");
            endpoint->set_port(dataPort);
            return grpc::Status::OK;
        };
        auto bootstrap = StartServer(bootPort, discovery);
        auto config = TDriverConfig().SetEndpoint(Address(bootPort)).SetDatabase("/Root")
            .SetDiscoveryMode(EDiscoveryMode::LongPoll);
        TDriver first(config), second(config);
        NTable::TTableClient firstClient(first), secondClient(second);
        const auto dataPeer = [](NTable::TTableClient& client) {
            auto status = client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
            const auto& headers = status.GetResponseMetadata();
            UNIT_ASSERT(headers.contains("test-peer"));
            return headers.find("test-peer")->second;
        };
        UNIT_ASSERT_VALUES_EQUAL(dataPeer(firstClient), dataPeer(secondClient));
    }

    Y_UNIT_TEST(LastClientCancelsDatabasePoll) {
        TPortManager ports;
        const auto port = ports.GetPort();
        TDiscovery bootstrap;
        auto held = NThreading::NewPromise<void>();
        auto cancelled = NThreading::NewPromise<void>();
        std::atomic_uint openings = 0;
        auto reopened = NThreading::NewPromise<void>();
        bootstrap.Lookup = [&](auto* context, auto& result) {
            if (context->client_metadata().contains("x-ydb-if-route-generation")) {
                held.TrySetValue();
                while (!context->IsCancelled()) {
                    Sleep(TDuration::MilliSeconds(10));
                }
                cancelled.TrySetValue();
                return grpc::Status::CANCELLED;
            }
            if (++openings == 2) {
                reopened.TrySetValue();
            }
            context->AddInitialMetadata("x-ydb-route-generation", "1");
            auto* node = result.add_endpoints();
            node->set_address("127.0.0.1");
            node->set_port(port);
            return grpc::Status::OK;
        };
        auto server = StartServer(port, bootstrap);
        TDriver driver(TDriverConfig().SetEndpoint(Address(port)).SetDiscoveryMode(EDiscoveryMode::Off));
        {
            NTable::TTableClient client(driver, NTable::TClientSettings()
                .Database("/Root/temporary").DiscoveryMode(EDiscoveryMode::LongPoll));
            UNIT_ASSERT(held.GetFuture().Wait(TDuration::Seconds(5)));
        }
        UNIT_ASSERT_C(cancelled.GetFuture().Wait(TDuration::Seconds(3)), "poll outlived its database client");
        NTable::TTableClient replacement(driver, NTable::TClientSettings()
            .Database("/Root/temporary").DiscoveryMode(EDiscoveryMode::LongPoll));
        UNIT_ASSERT(reopened.GetFuture().Wait(TDuration::Seconds(3)));
    }

#if defined(_linux_)
    Y_UNIT_TEST(OrdinarySelectionCannotReclaimRetiredSharedChannel) {
        using TService = Ydb::Table::V1::TableService;
        TPortManager ports;
        const auto port = ports.GetPort();
        const auto address = Address(port);
        TTableService service;
        auto server = StartServer(port, service);
        NYdbGrpc::TGRpcClientLow client;
        NYdbGrpc::TChannelPool channels({}, TDuration::Max());
        NYdbGrpc::TGRpcClientConfig config(address);
        auto ordinary = std::make_shared<int>(), watched = std::make_shared<int>();
        TEndpointElectorSafe elector;
        elector.SetNewState({TEndpointRecord(address, 0)});
        auto select = [&](const std::shared_ptr<int>& owner, std::function<bool()> current = {}) {
            std::unique_ptr<NYdbGrpc::TServiceConnection<TService>> connection;
            channels.GetStubsHolderLocked(address, config, [&](auto& holder) {
                holder.AddOwner(owner);
                connection = client.CreateGRpcServiceConnection<TService>(holder);
            }, std::move(current));
            return connection;
        };
        auto release = [&](const auto& owner) {
            channels.DeleteChannel(address, [&](auto& holder) { return holder.ReleaseOwner(owner.get()); });
        };
        select(ordinary);
        auto live = select(watched);
        auto completed = NThreading::NewPromise<int>();
        live->DoAdvancedRequest<Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>({},
            [completed](const auto&, NYdbGrpc::TGrpcStatus&& status, auto&&) mutable {
                completed.SetValue(status.GRpcStatusCode);
            }, &TService::Stub::AsyncBulkUpsert);
        UNIT_ASSERT_VALUES_EQUAL(completed.GetFuture().GetValueSync(), int(grpc::StatusCode::OK));
        live.reset();
        UNIT_ASSERT(HasEstablishedConnection(port));

        // Interleave an ordinary refresh between its endpoint selection and channel acquisition.
        const auto selected = elector.GetEndpoint({}, false);
        UNIT_ASSERT_VALUES_EQUAL(selected.Endpoint, address);
        elector.SetNewState({});
        release(ordinary);
        const bool reacquired = bool(select(ordinary, [&] {
            return bool(elector.GetEndpoint(TEndpointKey(selected.Endpoint, 0), true));
        }));
        release(watched);
        UNIT_ASSERT_C(WaitUntilDisconnected(port), "The late ordinary selection retained the old data socket");
        UNIT_ASSERT(!reacquired);
    }
#endif

    Y_UNIT_TEST(DirectEndpointVersionsPreserveCommonChannelsAndDrainRemovedEndpoints) {
        TPortManager ports;
        const auto bootstrapPort = ports.GetPort(), sourcePort = ports.GetPort(), targetPort = ports.GetPort();
        TTableService source, target;
        auto sourceServer = StartServer(sourcePort, source);
        auto targetServer = StartServer(targetPort, target);
        auto firstPoll = NThreading::NewPromise<void>();
        auto updateSource = NThreading::NewPromise<void>();
        auto secondPoll = NThreading::NewPromise<void>();
        auto removeSource = NThreading::NewPromise<void>();
        auto unexpectedPoll = NThreading::NewPromise<void>();
        TDiscovery discovery;
        discovery.Lookup = [&](auto* context, auto& result) {
            const auto& headers = context->client_metadata();
            auto mode = headers.find("x-ydb-discovery-long-poll");
            if (mode == headers.end() || mode->second != "1" || headers.contains("x-ydb-if-route-generation")) {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid direct poll");
            }
            auto* endpoint = result.add_endpoints();
            endpoint->set_address("127.0.0.1");
            endpoint->set_port(sourcePort);
            auto it = headers.find("x-ydb-if-endpoint-version");
            if (it == headers.end()) {
                if (firstPoll.GetFuture().HasValue()) {
                    unexpectedPoll.TrySetValue();
                }
                context->AddInitialMetadata("x-ydb-endpoint-version", "v1/source");
                return grpc::Status::OK;
            }
            const auto remaining = context->deadline() - std::chrono::system_clock::now();
            if (remaining < std::chrono::seconds(35) || remaining > std::chrono::seconds(47)) {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "missing direct long poll deadline");
            }
            if (it->second == "v1/source") {
                firstPoll.TrySetValue();
                if (!updateSource.GetFuture().Wait(TDuration::Seconds(5))) {
                    return grpc::Status::CANCELLED;
                }
                endpoint->set_location("local");
                context->AddTrailingMetadata("x-ydb-endpoint-version", "opaque-v2");
            } else if (it->second == "opaque-v2") {
                secondPoll.TrySetValue();
                if (!removeSource.GetFuture().Wait(TDuration::Seconds(5))) {
                    return grpc::Status::CANCELLED;
                }
                // A request routed to an older discovery server gets an ordinary snapshot.
                endpoint->set_port(targetPort);
            } else {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "unexpected endpoint version");
            }
            return grpc::Status::OK;
        };
        auto bootstrap = StartServer(bootstrapPort, discovery);
        TDriver driver(TDriverConfig().SetEndpoint(Address(bootstrapPort)).SetDatabase("/Root")
            .SetDiscoveryMode(EDiscoveryMode::LongPoll).SetSocketIdleTimeout(TDuration::Max()));
        NTable::TTableClient client(driver);
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        auto upsert = [&] { return client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings); };
        UNIT_ASSERT_C(firstPoll.GetFuture().Wait(TDuration::Seconds(3)), "no conditional poll to direct YDB");
        const auto peer = RpcPeer(upsert().GetValueSync());
        updateSource.SetValue();
        UNIT_ASSERT(secondPoll.GetFuture().Wait(TDuration::Seconds(3)));
        UNIT_ASSERT_VALUES_EQUAL(RpcPeer(upsert().GetValueSync()), peer);
        source.HoldNext = true;
        auto held = upsert();
        UNIT_ASSERT(source.Held.GetFuture().Wait(TDuration::Seconds(3)));
        removeSource.SetValue();
        // Synchronize on the new assignment by observing the destination of a real request.
        const auto deadline = TInstant::Now() + TDuration::Seconds(3);
        while (!target.Calls.load() && TInstant::Now() < deadline) {
            RpcPeer(upsert().GetValueSync());
        }
        UNIT_ASSERT(target.Calls.load());
        UNIT_ASSERT(!held.HasValue());
        source.ReleaseHeld.SetValue();
        UNIT_ASSERT_VALUES_EQUAL(RpcPeer(held.GetValueSync()), peer);
        UNIT_ASSERT_C(!unexpectedPoll.GetFuture().Wait(TDuration::MilliSeconds(300)),
            "a server without endpoint version support triggered immediate polling");
    }

    Y_UNIT_TEST(DirectPollRecoversFromAnEmptyAssignmentWithIpv6Endpoints) {
        TPortManager ports;
        const auto bootstrapPort = ports.GetPort(), dataPort = ports.GetPort();
        TTableService table;
        grpc::ServerBuilder builder;
        builder.AddListeningPort("[::1]:" + ToString(dataPort), grpc::InsecureServerCredentials());
        builder.RegisterService(&table);
        auto node = builder.BuildAndStart();
        UNIT_ASSERT(node);

        TDiscovery discovery;
        std::atomic_uint replies = 0;
        auto installed = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        discovery.Lookup = [&](auto* context, auto& result) {
            if (++replies == 1) {
                // YDB may have no advertised nodes while a tenant starts up.
                return grpc::Status::OK;
            }
            auto* endpoint = result.add_endpoints();
            endpoint->set_address("undiscoverable.invalid");
            endpoint->set_port(dataPort);
            endpoint->set_bridge_pile_name("local");
            result.add_pile_states()->set_pile_name("local");
            endpoint->add_ip_v6("");
            endpoint->add_ip_v6("::1");
            endpoint->add_ip_v6("[::1]");
            endpoint->add_ip_v4("");
            context->AddTrailingMetadata("x-ydb-endpoint-version", "ready");
            if (context->client_metadata().contains("x-ydb-if-endpoint-version")) {
                installed.TrySetValue();
                if (!release.GetFuture().Wait(TDuration::Seconds(5))) {
                    return grpc::Status::CANCELLED;
                }
            }
            return grpc::Status::OK;
        };
        auto bootstrap = StartServer(bootstrapPort, discovery);
        TDriver driver(TDriverConfig().SetEndpoint(Address(bootstrapPort)).SetDatabase("/Root")
            .SetDiscoveryMode(EDiscoveryMode::LongPoll));
        NTable::TTableClient client(driver);
        UNIT_ASSERT(installed.GetFuture().Wait(TDuration::Seconds(4)));
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        const auto response = client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings).GetValueSync();
        release.TrySetValue();
        UNIT_ASSERT_C(response.IsSuccess(), response.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(table.Calls.load(), 1);
    }

    Y_UNIT_TEST(DirectPollThrottlesServersThatReturnUnchangedVersionsImmediately) {
        TPortManager ports;
        auto port = ports.GetPort();
        TDiscovery discovery;
        std::atomic_uint calls = 0;
        auto firstConditional = NThreading::NewPromise<TInstant>();
        auto elapsed = NThreading::NewPromise<TDuration>();
        discovery.Lookup = [&](auto* context, auto& result) {
            auto* endpoint = result.add_endpoints();
            endpoint->set_address("127.0.0.1");
            endpoint->set_port(port);
            context->AddInitialMetadata("x-ydb-endpoint-version", "unchanged");
            if (context->client_metadata().contains("x-ydb-if-endpoint-version")) {
                if (++calls == 1) {
                    firstConditional.SetValue(TInstant::Now());
                } else {
                    elapsed.TrySetValue(TInstant::Now() - firstConditional.GetFuture().GetValueSync());
                    while (!context->IsCancelled()) {
                        Sleep(TDuration::MilliSeconds(10));
                    }
                    return grpc::Status::CANCELLED;
                }
            }
            return grpc::Status::OK;
        };
        auto server = StartServer(port, discovery);
        TDriver driver(TDriverConfig().SetEndpoint(Address(port)).SetDatabase("/Root")
            .SetDiscoveryMode(EDiscoveryMode::LongPoll));
        UNIT_ASSERT(elapsed.GetFuture().Wait(TDuration::Seconds(4)));
        UNIT_ASSERT(elapsed.GetFuture().GetValueSync() >= TDuration::MilliSeconds(500));
    }

    Y_UNIT_TEST(EndpointVersionCannotOverrideManagedRouteFence) {
        TPortManager ports;
        auto bootPort = ports.GetPort(), dataPort = ports.GetPort();
        TTableService table;
        auto node = StartServer(dataPort, table);
        TDiscovery discovery;
        std::atomic_uint calls = 0;
        auto retried = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        discovery.Lookup = [&](auto* context, auto& result) {
            auto* endpoint = result.add_endpoints();
            endpoint->set_address("127.0.0.1");
            endpoint->set_port(dataPort);
            const auto call = ++calls;
            const auto& headers = context->client_metadata();
            if (call == 1) {
                // Mixed response metadata can come from the worker and server.
                context->AddTrailingMetadata("x-ydb-route-generation", "11");
                context->AddInitialMetadata("x-ydb-endpoint-version", "ambiguous");
                context->AddTrailingMetadata("x-ydb-endpoint-version", "duplicate");
            } else {
                const auto route = headers.find("x-ydb-if-route-generation");
                if (route == headers.end() || route->second != "11" || headers.contains("x-ydb-if-endpoint-version")) {
                    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "unsafe discovery condition");
                }
                if (call == 2) {
                    context->AddInitialMetadata("x-ydb-route-generation", "10");
                } else if (call > 3) {
                    // The preceding response omitted route metadata entirely.
                    retried.TrySetValue();
                    if (!release.GetFuture().Wait(TDuration::Seconds(5))) {
                        return grpc::Status::CANCELLED;
                    }
                    context->AddInitialMetadata("x-ydb-route-generation", "11");
                }
                context->AddTrailingMetadata("x-ydb-endpoint-version", "valid-version");
            }
            return grpc::Status::OK;
        };
        auto bootstrap = StartServer(bootPort, discovery);
        TDriver driver(TDriverConfig().SetEndpoint(Address(bootPort)).SetDatabase("/Root")
            .SetDiscoveryMode(EDiscoveryMode::LongPoll));
        NTable::TTableClient client(driver);
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        UNIT_ASSERT(retried.GetFuture().Wait(TDuration::Seconds(3)));
        auto rejected = client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings);
        UNIT_ASSERT(rejected.Wait(TDuration::Seconds(3)));
        UNIT_ASSERT(!rejected.GetValueSync().IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(table.Calls.load(), 0);
        release.SetValue();
    }

    Y_UNIT_TEST(LongPollDrainsInFlightRequestsAndReusesBootstrap) {
        TPortManager ports;
        const auto sourcePort = ports.GetPort(), bootstrapPort = ports.GetPort();
        TTableService source;
        auto a = StartServer(sourcePort, source);
        TDiscovery discovery;
        auto switched = NThreading::NewPromise<void>();
        auto waiting = NThreading::NewPromise<void>();
        auto installed = NThreading::NewPromise<void>();
        std::set<TString> bootstrapPeers;
        std::mutex mutex;
        discovery.Lookup = [&](auto* context, auto& result) {
            auto& headers = context->client_metadata();
            if (!headers.contains("x-ydb-discovery-long-poll") || !headers.contains("x-ydb-database")) {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "missing long poll or database");
            }
            {
                std::lock_guard guard(mutex);
                if (!headers.contains("x-ydb-if-route-generation")) {
                    bootstrapPeers.insert(context->peer());
                } else if (!bootstrapPeers.contains(context->peer())) {
                    return grpc::Status(grpc::StatusCode::INTERNAL, "bootstrap changed");
                }
            }
            auto it = headers.find("x-ydb-if-route-generation");
            if (it != headers.end()) {
                auto version = headers.find("x-ydb-if-endpoint-version");
                if (version == headers.end() || version->second != "same-endpoints") {
                    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "missing backend endpoint version");
                }
                if (it->second == "1") {
                    waiting.TrySetValue();
                    if (!switched.GetFuture().Wait(TDuration::Seconds(5))) { return grpc::Status::CANCELLED; }
                } else {
                    installed.TrySetValue();
                    while (!context->IsCancelled()) { Sleep(TDuration::MilliSeconds(10)); }
                    return grpc::Status::CANCELLED;
                }
            }
            context->AddInitialMetadata("x-ydb-endpoint-version", "same-endpoints");
            context->AddTrailingMetadata("x-ydb-route-generation", switched.GetFuture().HasValue() ? "2" : "1");
            auto* endpoint = result.add_endpoints();
            endpoint->set_address("127.0.0.1");
            endpoint->set_port(sourcePort);
            endpoint->set_node_id(1);
            return grpc::Status::OK;
        };
        const auto otherPort = ports.GetPort();
        TDiscovery otherDiscovery;
        otherDiscovery.Lookup = [&](auto*, auto& result) {
            auto* endpoint = result.add_endpoints();
            endpoint->set_address("127.0.0.1");
            endpoint->set_port(sourcePort);
            return grpc::Status::OK;
        };
        auto otherBootstrap = StartServer(otherPort, otherDiscovery);
        auto boot = StartServer(bootstrapPort, discovery);
        const auto config = TDriverConfig().SetEndpoint(Address(bootstrapPort)).SetDatabase("/Root")
            .SetDiscoveryMode(EDiscoveryMode::LongPoll).SetSocketIdleTimeout(TDuration::Max());
        TDriver driver(config);
        NTable::TTableClient client(driver);
        std::optional<NTable::TTableClient> other(std::in_place, driver, NTable::TClientSettings().Database("/Other")
            .DiscoveryEndpoint(Address(otherPort)).DiscoveryMode(EDiscoveryMode::Sync));
        auto otherPeer = [&] {
            return RpcPeer(other->BulkUpsert("/Other/T", NTable::EDataFormat::CSV, "1", {}).GetValueSync());
        };
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        const auto upsert = [&] { return client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings); };
        UNIT_ASSERT(waiting.GetFuture().Wait(TDuration::Seconds(5)));
        const auto peerBefore = otherPeer();
        source.HoldNext = true;
        auto old = upsert();
        UNIT_ASSERT(source.Held.GetFuture().Wait(TDuration::Seconds(5)));
        switched.SetValue();
        UNIT_ASSERT(installed.GetFuture().Wait(TDuration::Seconds(5)));
        const auto freshPeer = RpcPeer(upsert().GetValueSync());
        TDriver second(config);
        NTable::TTableClient cold(second);
        UNIT_ASSERT_VALUES_EQUAL(RpcPeer(cold.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings).GetValueSync()), freshPeer);
        source.ReleaseHeld.SetValue();
        UNIT_ASSERT_UNEQUAL(freshPeer, RpcPeer(old.GetValueSync()));
        const auto peerAfter = otherPeer();
        UNIT_ASSERT_VALUES_EQUAL(source.Calls.load(), 5);
#if defined(_linux_)
        UNIT_ASSERT(HasEstablishedConnection(sourcePort));
        other.reset();
#endif
        driver.Stop(true);
        UNIT_ASSERT_VALUES_EQUAL(peerAfter, peerBefore);
    }
}
