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
#include <fstream>
#include <sstream>
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

    class TTableService: public Ydb::Table::V1::TableService::Service {
    public:
        grpc::Status BulkUpsert(grpc::ServerContext* context, const Ydb::Table::BulkUpsertRequest*,
                                Ydb::Table::BulkUpsertResponse* response) override {
            context->AddInitialMetadata("test-peer", context->peer());
            ++Calls;
            if (HoldNext.exchange(false)) {
                Held.TrySetValue();
                ReleaseHeld.GetFuture().Wait(TDuration::Seconds(10));
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "in-flight data connection lost");
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

    Y_UNIT_TEST(RetiringAnEndpointReleasesHeldQueryStreams) {
        using TService = Ydb::Query::V1::QueryService;
        using TRequest = Ydb::Query::AttachSessionRequest;
        using TResponse = Ydb::Query::SessionState;
        using TReader = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
        class TQueryService : public TService::Service {
            grpc::Status AttachSession(grpc::ServerContext* context, const TRequest*, grpc::ServerWriter<TResponse>* stream) override {
                context->AddInitialMetadata("replica", "source");
                TResponse state;
                state.set_status(Ydb::StatusIds::SUCCESS);
                stream->Write(state);
                const auto deadline = TInstant::Now() + TDuration::Seconds(10);
                while (!context->IsCancelled() && TInstant::Now() < deadline) {
                    Sleep(TDuration::MilliSeconds(5));
                }
                return grpc::Status::OK;
            }
        } service;
        TPortManager ports;
        const auto port = ports.GetPort();
        auto server = StartServer(port, service);
        NYdbGrpc::TGRpcClientLow client;
        auto channel = NYdbGrpc::CreateChannelInterface(NYdbGrpc::TGRpcClientConfig(Address(port)));
        std::weak_ptr<grpc::ChannelInterface> transport = channel;
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
            auto finished = NThreading::NewPromise<int>();
            reader->AddFinishedCallback([finished](auto status) mutable { finished.SetValue(status.GRpcStatusCode); });
            return std::pair{reader, finished.GetFuture()};
        };
        NYdbGrpc::TCallMeta first, second;
        first.EndpointAdmission = std::make_shared<NYdbGrpc::TEndpointAdmission>();
        second.EndpointAdmission = std::make_shared<NYdbGrpc::TEndpointAdmission>();
        auto [firstReader, firstDone] = open(first);
        auto [secondReader, secondDone] = open(second);
        conn.reset();
        holder.reset();
        first.EndpointAdmission->Retire();
        const bool firstRetired = firstDone.Wait(TDuration::Seconds(3));
        const bool secondStillActive = !secondDone.HasValue() && !transport.expired();
        second.EndpointAdmission->Retire();
        const bool secondRetired = secondDone.Wait(TDuration::Seconds(3));
        // Keep teardown bounded if retirement regresses.
        firstReader->Cancel();
        secondReader->Cancel();
        UNIT_ASSERT(firstRetired);
        UNIT_ASSERT(secondStillActive);
        UNIT_ASSERT(secondRetired);
        UNIT_ASSERT_VALUES_EQUAL(firstDone.GetValueSync(), int(grpc::StatusCode::CANCELLED));
        UNIT_ASSERT_VALUES_EQUAL(secondDone.GetValueSync(), int(grpc::StatusCode::CANCELLED));
        UNIT_ASSERT_C(transport.expired(), "the retained session readers still own the retired gRPC channel");
        std::unordered_multimap<std::string, std::string> metadata;
        auto headers = NThreading::NewPromise<bool>();
        firstReader->ReadInitialMetadata(&metadata, [headers](auto status) mutable { headers.SetValue(status.Ok()); });
        UNIT_ASSERT(headers.GetFuture().GetValueSync());
        UNIT_ASSERT(metadata.contains("replica"));
        UNIT_ASSERT_VALUES_EQUAL(metadata.find("replica")->second, "source");
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

    Y_UNIT_TEST(LongPollCutsInFlightRequestsAndReusesBootstrap) {
        TPortManager ports;
        const auto sourcePort = ports.GetPort(), targetPort = ports.GetPort(), bootstrapPort = ports.GetPort();
        TTableService source, target;
        auto a = StartServer(sourcePort, source);
        auto b = StartServer(targetPort, target);
        TDiscovery discovery;
        auto switched = NThreading::NewPromise<void>();
        auto waiting = NThreading::NewPromise<void>();
        auto installed = NThreading::NewPromise<void>();
        std::string peer;
        std::mutex mutex;
        discovery.Lookup = [&](auto* context, auto& result) {
            auto& headers = context->client_metadata();
            if (!headers.contains("x-ydb-discovery-long-poll") || !headers.contains("x-ydb-database")) {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "missing long poll or database");
            }
            {
                std::lock_guard guard(mutex);
                if (peer.empty()) { peer = context->peer(); }
                if (peer != context->peer()) { return grpc::Status(grpc::StatusCode::INTERNAL, "bootstrap changed"); }
            }
            auto it = headers.find("x-ydb-if-route-generation");
            if (it != headers.end()) {
                if (it->second == "1") {
                    waiting.TrySetValue();
                    if (!switched.GetFuture().Wait(TDuration::Seconds(5))) { return grpc::Status::CANCELLED; }
                } else {
                    installed.TrySetValue();
                    while (!context->IsCancelled()) { Sleep(TDuration::MilliSeconds(10)); }
                    return grpc::Status::CANCELLED;
                }
            }
            context->AddTrailingMetadata("x-ydb-route-generation", it == headers.end() ? "1" : "2");
            auto* endpoint = result.add_endpoints();
            endpoint->set_address("127.0.0.1");
            endpoint->set_port(it == headers.end() ? sourcePort : targetPort);
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
        TDriver driver(TDriverConfig().SetEndpoint(Address(bootstrapPort)).SetDatabase("/Root").SetDiscoveryMode(EDiscoveryMode::LongPoll)
            .SetUsePerChannelTcpConnection(true).SetSocketIdleTimeout(TDuration::Max()));
        NTable::TTableClient client(driver);
        std::optional<NTable::TTableClient> other(std::in_place, driver, NTable::TClientSettings().Database("/Other")
            .DiscoveryEndpoint(Address(otherPort)).DiscoveryMode(EDiscoveryMode::Sync));
        auto otherPeer = [&] {
            auto status = other->BulkUpsert("/Other/T", NTable::EDataFormat::CSV, "1", {}).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
            const auto& headers = status.GetResponseMetadata();
            UNIT_ASSERT(headers.contains("test-peer"));
            return headers.find("test-peer")->second;
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
        const bool retired = old.Wait(TDuration::Seconds(3));
        source.ReleaseHeld.SetValue();
        UNIT_ASSERT(retired);
        UNIT_ASSERT(!old.GetValueSync().IsSuccess());
        UNIT_ASSERT(upsert().GetValueSync().IsSuccess());
        const auto peerAfter = otherPeer();
        UNIT_ASSERT_VALUES_EQUAL(source.Calls.load(), 3);
        UNIT_ASSERT_VALUES_EQUAL(target.Calls.load(), 1);
#if defined(_linux_)
        UNIT_ASSERT(HasEstablishedConnection(sourcePort));
        other.reset();
        const auto closedAfterLastOwner = WaitUntilDisconnected(sourcePort);
#endif
        driver.Stop(true);
        UNIT_ASSERT_VALUES_EQUAL(peerAfter, peerBefore);
#if defined(_linux_)
        UNIT_ASSERT_C(closedAfterLastOwner, "The retired source remained connected after its final owner was destroyed");
#endif
    }
}
