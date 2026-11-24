#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <grpcpp/server_builder.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <ydb/public/api/grpc/draft/xds/codec/codec.h>
#include <ydb/public/api/grpc/draft/xds/eds.grpc.pb.h>
#if defined(_unix_)
#include <contrib/libs/grpc/src/core/lib/iomgr/socket_mutator.h>
#include <util/network/socket.h>
#include <util/system/file.h>
#endif
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

using namespace NYdb;

namespace {

    std::string Address(ui16 port) {
        return "127.0.0.1:" + std::to_string(port);
    }

#if defined(_unix_)
    // Retain a descriptor for each accepted connection so a test can cut the
    // existing transports while leaving the very same listener in service.
    struct TAcceptedSockets : grpc_socket_mutator {
        TAcceptedSockets() {
            static const grpc_socket_mutator_vtable vtable = {
                nullptr,
                [](auto* a, auto* b) { return a == b ? 0 : std::less<>{}(a, b) ? -1 : 1; },
                [](auto* self) { delete static_cast<TAcceptedSockets*>(self); },
                [](const grpc_mutate_socket_info* info, grpc_socket_mutator* mutator) {
                    if (info->usage == GRPC_FD_SERVER_CONNECTION_USAGE) {
                        auto* self = static_cast<TAcceptedSockets*>(mutator);
                        std::lock_guard lock(self->Mutex);
                        self->Connections.push_back(Duplicate(info->fd));
                    }
                    return true;
                },
            };
            grpc_socket_mutator_init(this, &vtable);
        }

        void Disconnect() {
            std::lock_guard lock(Mutex);
            UNIT_ASSERT(!Connections.empty());
            for (const auto& connection : Connections) {
                ShutDown(connection.GetHandle(), SHUT_RDWR);
            }
            Connections.clear();
        }

        std::mutex Mutex;
        std::vector<TFile> Connections;
    };

    class TSocketOption : public grpc::ServerBuilderOption {
    public:
        explicit TSocketOption(TAcceptedSockets* sockets)
            : Sockets(sockets) {}

        void UpdateArguments(grpc::ChannelArguments* args) override {
            args->SetSocketMutator(Sockets);
        }
        void UpdatePlugins(std::vector<std::unique_ptr<grpc::ServerBuilderPlugin>>*) override {}

    private:
        TAcceptedSockets* Sockets;
    };
#endif

    std::unique_ptr<grpc::Server> StartServer(ui16 port, grpc::Service& service, std::unique_ptr<grpc::ServerBuilderOption> option = {}) {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("0.0.0.0:" + ToString(port), grpc::InsecureServerCredentials());
        if (option) {
            builder.SetOption(std::move(option));
        }
        builder.RegisterService(&service);
        return builder.BuildAndStart();
    }

    class TDiscovery: public Ydb::Discovery::V1::DiscoveryService::Service {
    public:
        explicit TDiscovery(ui16 sourcePort)
            : SourcePort(sourcePort)
            , Port(sourcePort)
        {
        }

        grpc::Status ListEndpoints(grpc::ServerContext*, const Ydb::Discovery::ListEndpointsRequest*,
                                   Ydb::Discovery::ListEndpointsResponse* response) override {
            ++Calls;
            Ydb::Discovery::ListEndpointsResult result;
            const auto port = Port.load();
            if (Calls.load() > 1) {
                RefreshStarted.TrySetValue();
                ReleaseDiscovery.GetFuture().Wait(TDuration::Seconds(10));
            }
            if (port == SourcePort) {
                // Losing one selected address is below the periodic pessimization threshold.
                for (const auto* host : {"127.0.0.1", "127.0.0.2", "127.0.0.3"}) {
                    auto* endpoint = result.add_endpoints();
                    endpoint->set_address(host);
                    endpoint->set_port(port);
                    endpoint->set_load_factor(std::string_view(host) == "127.0.0.1" ? 0 : 1);
                }
            } else {
                if (!port) {
                    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "bootstrap unavailable");
                }
                auto* endpoint = result.add_endpoints();
                endpoint->set_address("127.0.0.1");
                endpoint->set_port(port);
            }
            auto* op = response->mutable_operation();
            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);
            op->mutable_result()->PackFrom(result);
            return grpc::Status::OK;
        }

        const ui16 SourcePort;
        std::atomic_uint Port;
        std::atomic_uint Calls = 0;
        NThreading::TPromise<void> RefreshStarted = NThreading::NewPromise<void>();
        NThreading::TPromise<void> ReleaseDiscovery = NThreading::NewPromise<void>();
    };

    template <class TService>
    struct TSwitch {
        explicit TSwitch(std::unique_ptr<grpc::ServerBuilderOption> option = {})
            : SourceServer(StartServer(SourcePort, Source, std::move(option)))
        {}

        TPortManager Ports;
        const ui16 SourcePort = Ports.GetPort();
        const ui16 DestinationPort = Ports.GetPort();
        const ui16 DiscoveryPort = Ports.GetPort();
        TService Source, Destination;
        TDiscovery Discovery{SourcePort};
        std::unique_ptr<grpc::Server> SourceServer;
        std::unique_ptr<grpc::Server> DestinationServer = StartServer(DestinationPort, Destination);
        std::unique_ptr<grpc::Server> DiscoveryServer = StartServer(DiscoveryPort, Discovery);
        TDriver Driver{TDriverConfig().SetEndpoint(Address(DiscoveryPort)).SetDatabase("/Root").SetMaxQueuedRequests(1)};
    };

    class TWatchService : public envoy::service::endpoint::v3::YdbDraft::EndpointDiscoveryService::Service {
    public:
        using TStream = grpc::ServerReaderWriter<Ydb::Xds::DiscoveryResponse, Ydb::Xds::DiscoveryRequest>;
        using THandler = std::function<grpc::Status(grpc::ServerContext*, TStream*)>;

        explicit TWatchService(THandler handler)
            : Handler(std::move(handler))
        {}

        grpc::Status StreamEndpoints(grpc::ServerContext* context, TStream* stream) override {
            ++Attempts;
            return Handler(context, stream);
        }

        THandler Handler;
        std::atomic_uint Attempts = 0;
    };

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

    class TTopicService: public Ydb::Topic::V1::TopicService::Service {
    public:
        using TRead = Ydb::Topic::StreamReadMessage;
        using TWrite = Ydb::Topic::StreamWriteMessage;

        grpc::Status StreamRead(grpc::ServerContext*, grpc::ServerReaderWriter<TRead::FromServer, TRead::FromClient>* stream) override {
            return Serve<TRead>(stream);
        }

        grpc::Status StreamWrite(grpc::ServerContext*, grpc::ServerReaderWriter<TWrite::FromServer, TWrite::FromClient>* stream) override {
            return Serve<TWrite>(stream);
        }

        template <class TMessage>
        grpc::Status Serve(grpc::ServerReaderWriter<typename TMessage::FromServer, typename TMessage::FromClient>* stream) {
            ++Attempts;
            typename TMessage::FromClient init;
            typename TMessage::FromServer response;
            response.set_status(Ydb::StatusIds::SUCCESS);
            response.mutable_init_response()->set_session_id("session");
            if (!stream->Read(&init) || !init.has_init_request() || !stream->Write(response)) {
                return grpc::Status::CANCELLED;
            }
            Established.TrySetValue();
            Finish.GetFuture().Wait(TDuration::Seconds(10));
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "source transport closed");
        }

        std::atomic_uint Attempts = 0;
        NThreading::TPromise<void> Established = NThreading::NewPromise<void>();
        NThreading::TPromise<void> Finish = NThreading::NewPromise<void>();
    };

    template <class TStart>
    void CheckTopicReconnect(TStart start) {
        TSwitch<TTopicService> servers;
        NTopic::TTopicClient client(servers.Driver);
        auto retry = NTopic::IRetryPolicy::GetFixedIntervalPolicy(TDuration::MilliSeconds(1), TDuration::MilliSeconds(1), 1);
        auto session = start(client, retry);
        UNIT_ASSERT(servers.Source.Established.GetFuture().Wait(TDuration::Seconds(10)));
        const auto initialDiscoveries = servers.Discovery.Calls.load();
        servers.Discovery.Port = servers.DestinationPort;
        servers.Discovery.ReleaseDiscovery.SetValue();
        servers.Source.Finish.SetValue();
        const auto reconnected = servers.Destination.Established.GetFuture().Wait(TDuration::Seconds(3));
        servers.Destination.Finish.SetValue();
        session->Close(TDuration::Zero());
        UNIT_ASSERT_C(reconnected, "The established Topic stream did not reconnect to the newly advertised endpoint");
        UNIT_ASSERT_VALUES_EQUAL(servers.Source.Attempts.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(servers.Discovery.Calls.load(), initialDiscoveries + 1);
    }

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

    Y_UNIT_TEST(WatchPermissionDeniedDoesNotFallBackToListEndpoints) {
        TPortManager ports;
        const auto port = ports.GetPort();
        const auto dataPort = ports.GetPort();
        TDiscovery discovery(dataPort);
        TTableService table;
        auto data = StartServer(dataPort, table);
        TWatchService eds([](auto*, auto* stream) {
            Ydb::Xds::DiscoveryRequest request;
            stream->Read(&request);
            return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "database access denied");
        });
        grpc::ServerBuilder builder;
        builder.AddListeningPort(Address(port), grpc::InsecureServerCredentials());
        builder.RegisterService(&eds);
        builder.RegisterService(&discovery);
        auto bootstrap = builder.BuildAndStart();
        TDriver driver(TDriverConfig().SetEndpoint(Address(port)).SetDatabase("/Root")
            .SetDiscoveryMode(EDiscoveryMode::Eds));
        NTable::TTableClient client(driver);
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        auto status = client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(status.GetStatus(), EStatus::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(discovery.Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(table.Calls.load(), 0);
        UNIT_ASSERT(eds.Attempts.load() > 0);
    }

    Y_UNIT_TEST(WatchFallsBackOnMissingMethod) {
        TPortManager ports;
        const auto port = ports.GetPort();
        const auto dataPort = ports.GetPort();
        TDiscovery discovery(dataPort);
        TTableService table;
        auto data = StartServer(dataPort, table);
        auto bootstrap = StartServer(port, discovery);
        TDriver driver(TDriverConfig().SetEndpoint(Address(port)).SetDatabase("/Root").SetDiscoveryMode(EDiscoveryMode::Eds));
        NTable::TTableClient client(driver);
        auto result = client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(discovery.Calls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(table.Calls.load(), 1);
    }

    Y_UNIT_TEST(IdleWatchReplacesSourceWithoutClosingItsListener) {
        TPortManager ports;
        const auto port = ports.GetPort();
        const auto sourcePort = ports.GetPort();
        const auto targetPort = ports.GetPort();
        const auto otherPort = ports.GetPort();
        TDiscovery otherDiscovery(sourcePort);
        auto otherBootstrap = StartServer(otherPort, otherDiscovery);
        TTableService source, target;
        auto liveSource = StartServer(sourcePort, source);
        auto liveTarget = StartServer(targetPort, target);
        auto switched = NThreading::NewPromise<void>();
        auto acknowledged = NThreading::NewPromise<void>();
        auto firstAck = NThreading::NewPromise<void>();
        TWatchService eds([&](auto* context, auto* stream) {
            Ydb::Xds::DiscoveryRequest request;
            if (!stream->Read(&request) || request.resource_names_size() != 1 || request.resource_names(0) != "/Root"
                || request.type_url() != NEds::TypeUrl || !request.version_info().empty() || !request.response_nonce().empty()
                || !context->client_metadata().contains("x-ydb-database")) {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid subscription");
            }
            for (const auto member : {sourcePort, targetPort}) {
                Ydb::Discovery::ListEndpointsResult result;
                auto* endpoint = result.add_endpoints();
                endpoint->set_address("127.0.0.1");
                endpoint->set_port(member);
                Ydb::Xds::DiscoveryResponse response;
                NEds::Encode(result, "/Root", ToString(member), "nonce-" + ToString(member), response);
                if (!stream->Write(response) || !stream->Read(&request) || request.version_info() != response.version_info()
                    || request.response_nonce() != response.nonce() || request.has_error_detail()) {
                    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "missing assignment ACK");
                }
                if (member == sourcePort) {
                    firstAck.TrySetValue();
                    if (!switched.GetFuture().Wait(TDuration::Seconds(10))) {
                        return grpc::Status::CANCELLED;
                    }
                }
            }
            acknowledged.TrySetValue();
            stream->Read(&request); // driver teardown cancels the idle watch
            return grpc::Status::OK;
        });
        auto bootstrap = StartServer(port, eds);
        TDriver driver(TDriverConfig().SetEndpoint(Address(port)).SetDatabase("/Root").SetDiscoveryMode(EDiscoveryMode::Eds)
            .SetUsePerChannelTcpConnection(true));
        NTable::TTableClient client(driver);
        NTable::TTableClient other(driver, NTable::TClientSettings().Database("/Other")
            .DiscoveryEndpoint(Address(otherPort)).DiscoveryMode(EDiscoveryMode::Sync));
        const auto otherPeer = [&] {
            auto status = other.BulkUpsert("/Other/T", NTable::EDataFormat::CSV, "1", {}).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
            const auto& headers = status.GetResponseMetadata();
            UNIT_ASSERT(headers.contains("test-peer"));
            return headers.find("test-peer")->second;
        };
        const auto upsert = [&] { return client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}).GetValueSync(); };
        UNIT_ASSERT(firstAck.GetFuture().Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(upsert().IsSuccess());
        const auto peerBefore = otherPeer();
        source.HoldNext = true;
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        auto inFlight = client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "2", {}, settings);
        UNIT_ASSERT(source.Held.GetFuture().Wait(TDuration::Seconds(5)));
        switched.SetValue();
        UNIT_ASSERT(acknowledged.GetFuture().Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(upsert().IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(target.Calls.load(), 1);
        source.ReleaseHeld.SetValue();
        UNIT_ASSERT(inFlight.GetValueSync().IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(eds.Attempts.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(otherPeer(), peerBefore);
        UNIT_ASSERT_VALUES_EQUAL(source.Calls.load(), 4);
        UNIT_ASSERT_VALUES_EQUAL(target.Calls.load(), 1);
    }

    Y_UNIT_TEST(ReopenedWatchGetsNewDataPeerAtSameAddress) {
        TPortManager ports;
        const auto dataPort = ports.GetPort();
        const auto bootstrapPort = ports.GetPort();
        TTableService data;
        auto dataServer = StartServer(dataPort, data);
        auto switched = NThreading::NewPromise<void>();
        auto installed = NThreading::NewPromise<void>();
        std::atomic_uint attempt = 0;
        TWatchService eds([&](auto* context, auto* stream) {
            const auto generation = ToString(++attempt);
            context->AddInitialMetadata("x-ydb-route-generation", generation);
            context->AddInitialMetadata("x-ydb-route-mode", "managed");
            Ydb::Xds::DiscoveryRequest request;
            if (!stream->Read(&request)) {
                return grpc::Status::CANCELLED;
            }
            Ydb::Discovery::ListEndpointsResult result;
            auto* endpoint = result.add_endpoints();
            endpoint->set_address("127.0.0.1");
            endpoint->set_port(dataPort);
            endpoint->set_node_id(1);
            Ydb::Xds::DiscoveryResponse response;
            NEds::Encode(result, "/Root", generation, generation, response);
            if (!stream->Write(response) || !stream->Read(&request) || request.response_nonce() != generation) {
                return grpc::Status::CANCELLED;
            }
            if (generation == "1") {
                switched.GetFuture().Wait(TDuration::Seconds(10));
                context->AddTrailingMetadata("x-ydb-route-generation", "2");
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "replica switched");
            }
            installed.TrySetValue();
            stream->Read(&request);
            return grpc::Status::OK;
        });
        auto bootstrap = StartServer(bootstrapPort, eds);
        TDriver driver(TDriverConfig().SetEndpoint(Address(bootstrapPort)).SetDatabase("/Root").SetDiscoveryMode(EDiscoveryMode::Eds));
        NTable::TTableClient client(driver);
        const auto upsert = [&] { return client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}); };
        data.HoldNext = true;
        auto old = upsert();
        UNIT_ASSERT(data.Held.GetFuture().Wait(TDuration::Seconds(5)));
        switched.SetValue();
        UNIT_ASSERT(installed.GetFuture().Wait(TDuration::Seconds(5)));
        const auto freshPeer = RpcPeer(upsert().GetValueSync());
        data.ReleaseHeld.SetValue();
        UNIT_ASSERT_UNEQUAL(freshPeer, RpcPeer(old.GetValueSync()));
    }

    Y_UNIT_TEST(BulkUpsertTransportLoss) {
        TSwitch<TTableService> servers;
        NTable::TTableClient client(servers.Driver);
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        const auto upsert = [&] { return client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings); };

        UNIT_ASSERT(upsert().GetValueSync().IsSuccess());
        const auto initialDiscoveries = servers.Discovery.Calls.load();
        servers.Discovery.Port = servers.DestinationPort;
        servers.Source.FailNext = true;
        UNIT_ASSERT_VALUES_EQUAL(upsert().GetValueSync().GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);

        auto shortRequest = client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {},
                                              NTable::TBulkUpsertSettings(settings).ClientTimeout(TDuration::MilliSeconds(100)));
        const bool deadlineObserved = shortRequest.Wait(TDuration::Seconds(2));

        std::vector<NTable::TAsyncBulkUpsertResult> next;
        for (unsigned i = 0; i < 4; ++i) {
            next.push_back(upsert());
        }
        servers.Discovery.ReleaseDiscovery.SetValue();
        UNIT_ASSERT_C(deadlineObserved, "A short RPC deadline must interrupt the discovery wait");
        UNIT_ASSERT_VALUES_EQUAL(shortRequest.GetValueSync().GetStatus(), EStatus::CLIENT_DEADLINE_EXCEEDED);
        for (const auto& future : next) {
            const auto& status = future.GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
        UNIT_ASSERT_VALUES_EQUAL(servers.Discovery.Calls.load(), initialDiscoveries + 1);
        UNIT_ASSERT_VALUES_EQUAL(servers.Source.Calls.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(servers.Destination.Calls.load(), next.size());
    }

    Y_UNIT_TEST(FailedDiscoveryKeepsHealthyEndpoints) {
        TSwitch<TTableService> servers;
        NTable::TTableClient client(servers.Driver);
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        const auto upsert = [&] { return client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings); };

        const auto initialDiscoveries = servers.Discovery.Calls.load();
        servers.Discovery.Port = 0;
        servers.Source.FailNext = true;
        const auto failed = upsert().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(failed.GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
        const auto next = upsert();
        servers.Discovery.ReleaseDiscovery.SetValue();
        const auto& status = next.GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        UNIT_ASSERT_UNEQUAL(status.GetEndpoint(), failed.GetEndpoint());
        UNIT_ASSERT_VALUES_EQUAL(servers.Discovery.Calls.load(), initialDiscoveries + 1);
    }


#if defined(_unix_)
    Y_UNIT_TEST(IdleSourceTransportClosesWhileListenerKeepsServing) {
        auto* sockets = new TAcceptedSockets;
        TSwitch<TTableService> servers(std::make_unique<TSocketOption>(sockets));
        NTable::TTableClient client(servers.Driver);
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        const auto upsert = [&] { return client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings); };

        UNIT_ASSERT(upsert().GetValueSync().IsSuccess());
        servers.Discovery.Port = servers.DestinationPort;
        sockets->Disconnect();
        UNIT_ASSERT_C(servers.Discovery.RefreshStarted.GetFuture().Wait(TDuration::Seconds(3)),
                      "An idle data connection closed, but the driver never queried bootstrap");
        TDriver direct(TDriverConfig().SetEndpoint(Address(servers.SourcePort)).SetDatabase("/Root").SetDiscoveryMode(EDiscoveryMode::Off));
        NTable::TTableClient directClient(direct);
        UNIT_ASSERT(directClient.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings).GetValueSync().IsSuccess());

        auto next = upsert();
        servers.Discovery.ReleaseDiscovery.SetValue();
        UNIT_ASSERT(next.GetValueSync().IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(servers.Source.Calls.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(servers.Destination.Calls.load(), 1);
    }
#endif

    Y_UNIT_TEST(SecondFailureAfterUnchangedDiscovery) {
        TSwitch<TTableService> servers;
        NTable::TTableClient client(servers.Driver);
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        const auto upsert = [&] { return client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings); };

        servers.Source.FailNext = true;
        const auto first = upsert().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(first.GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
        servers.Discovery.ReleaseDiscovery.SetValue();
        const auto peer = upsert().GetValueSync();
        UNIT_ASSERT(peer.IsSuccess());
        UNIT_ASSERT_UNEQUAL(first.GetEndpoint(), peer.GetEndpoint());

        servers.Discovery.Port = servers.DestinationPort;
        servers.Source.FailNext = true;
        UNIT_ASSERT_VALUES_EQUAL(upsert().GetValueSync().GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
        UNIT_ASSERT(upsert().GetValueSync().IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(servers.Destination.Calls.load(), 1);
    }

    Y_UNIT_TEST(DiscoverySampledBeforeLastFailure) {
        TSwitch<TTableService> servers;
        NTable::TTableClient client(servers.Driver);
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        const auto upsert = [&] { return client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings); };

        servers.Source.HoldNext = true;
        const auto oldRequest = upsert();
        UNIT_ASSERT(servers.Source.Held.GetFuture().Wait(TDuration::Seconds(3)));
        servers.Source.FailNext = true;
        UNIT_ASSERT_VALUES_EQUAL(upsert().GetValueSync().GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);
        UNIT_ASSERT(servers.Discovery.RefreshStarted.GetFuture().Wait(TDuration::Seconds(3)));
        servers.Discovery.Port = servers.DestinationPort;
        servers.Source.FailNext = true;
        servers.Source.ReleaseHeld.SetValue();
        UNIT_ASSERT_VALUES_EQUAL(oldRequest.GetValueSync().GetStatus(), EStatus::TRANSPORT_UNAVAILABLE);

        const auto next = upsert();
        servers.Discovery.ReleaseDiscovery.SetValue();
        UNIT_ASSERT(next.GetValueSync().IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(servers.Destination.Calls.load(), 1);
    }

    Y_UNIT_TEST(EstablishedTopicReader) {
        CheckTopicReconnect([](auto& client, auto retry) {
            return client.CreateReadSession(NTopic::TReadSessionSettings().ConsumerName("consumer").AppendTopics(std::string("/Root/T")).RetryPolicy(retry));
        });
    }

    Y_UNIT_TEST(EstablishedTopicWriter) {
        CheckTopicReconnect([](auto& client, auto retry) {
            return client.CreateWriteSession(NTopic::TWriteSessionSettings().Path("/Root/T").ProducerId("producer").DirectWriteToPartition(false).RetryPolicy(retry));
        });
    }
} // Y_UNIT_TEST_SUITE(EndpointRediscovery)
