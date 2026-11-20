#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <grpcpp/server_builder.h>
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

    std::unique_ptr<grpc::Server> StartServer(ui16 port, grpc::Service& service) {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("0.0.0.0:" + ToString(port), grpc::InsecureServerCredentials());
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
            if (port == SourcePort) {
                // Losing one selected address is below the periodic pessimization threshold.
                for (const auto* host : {"127.0.0.1", "127.0.0.2", "127.0.0.3"}) {
                    auto* endpoint = result.add_endpoints();
                    endpoint->set_address(host);
                    endpoint->set_port(port);
                }
            } else {
                ReleaseDiscovery.GetFuture().Wait(TDuration::Seconds(10));
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
        NThreading::TPromise<void> ReleaseDiscovery = NThreading::NewPromise<void>();
    };

    template <class TService>
    struct TSwitch {
        TPortManager Ports;
        const ui16 SourcePort = Ports.GetPort();
        const ui16 DestinationPort = Ports.GetPort();
        const ui16 DiscoveryPort = Ports.GetPort();
        TService Source, Destination;
        TDiscovery Discovery{SourcePort};
        std::unique_ptr<grpc::Server> SourceServer = StartServer(SourcePort, Source);
        std::unique_ptr<grpc::Server> DestinationServer = StartServer(DestinationPort, Destination);
        std::unique_ptr<grpc::Server> DiscoveryServer = StartServer(DiscoveryPort, Discovery);
        TDriver Driver{TDriverConfig().SetEndpoint(Address(DiscoveryPort)).SetDatabase("/Root")};
    };

    class TTableService: public Ydb::Table::V1::TableService::Service {
    public:
        grpc::Status BulkUpsert(grpc::ServerContext*, const Ydb::Table::BulkUpsertRequest*,
                                Ydb::Table::BulkUpsertResponse* response) override {
            ++Calls;
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
    Y_UNIT_TEST(BulkUpsertTransportLoss) {
        TSwitch<TTableService> servers;
        NTable::TTableClient client(servers.Driver);
        NTable::TBulkUpsertSettings settings;
        settings.RetrySettings(NTable::TRetryOperationSettings().MaxRetries(0));
        const auto upsert = [&] { return client.BulkUpsert("/Root/T", NTable::EDataFormat::CSV, "1", {}, settings); };

        UNIT_ASSERT(upsert().GetValueSync().IsSuccess());
        const auto initialDiscoveries = servers.Discovery.Calls.load();
        servers.Discovery.Port = servers.DestinationPort;
        servers.SourceServer->Shutdown();
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
        UNIT_ASSERT_VALUES_EQUAL(servers.Source.Calls.load(), 1);
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
