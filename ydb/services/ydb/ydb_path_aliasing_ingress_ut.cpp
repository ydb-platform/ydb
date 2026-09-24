#include "ydb_common_ut.h"

#include <ydb/services/keyvalue/grpc_service_v1.h>

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <array>
#include <chrono>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

namespace NKikimr::NGRpcService {
    namespace {

        using TDiscovery = Ydb::Discovery::V1::DiscoveryService::Stub;
        using TKeyValue = Ydb::KeyValue::V1::KeyValueService::Stub;
        using TTable = Ydb::Table::V1::TableService::Stub;
        using TTopic = Ydb::Topic::V1::TopicService::Stub;

        void AddRule(NKikimrConfig::TAppConfig& config, const char* src, const char* dst) {
            auto* rule = config.MutableResourcePathPrefixMapping()->AddRules();
            rule->SetSrc(src);
            rule->SetDst(dst);
        }

        NKikimrConfig::TAppConfig MakeConfig(bool useSimpleProxy = false, bool enablePathAliasing = true) {
            NKikimrConfig::TAppConfig config;
            config.MutableGRpcConfig()->SetSkipSchemeCheck(useSimpleProxy);
            if (!enablePathAliasing) {
                return config;
            }
            AddRule(config, "/alias", "/Root/kfront");
            AddRule(config, "/discovery-alias", "/Root/kfront");
            AddRule(config, "/discovery-boundary", "/Root/k");
            AddRule(config, "/volume-alias", "/Root/kfront/Volume");
            AddRule(config, "/volume-inspect", "/Root/kfront/Volume");
            AddRule(config, "/virtual/", "/Root");
            AddRule(config, "/Root/kfront/Volume", "/Root/kfront/Wrong");
            AddRule(config, "/Root/kfront", "/Root/missing");
            return config;
        }

        template <class TStub, class TRequest, class TResponse>
        TResponse Call(TStub& stub,
                       grpc::Status (TStub::*method)(grpc::ClientContext*, const TRequest&, TResponse*),
                       TRequest request, TStringBuf database,
                       bool addDatabaseHeader = true, bool addAuthTicket = true)
        {
            grpc::ClientContext context;
            if (addDatabaseHeader) {
                context.AddMetadata("x-ydb-database", std::string(database.data(), database.size()));
            }
            if (addAuthTicket) {
                context.AddMetadata("x-ydb-auth-ticket", "root@builtin");
            }
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
            if constexpr (requires { request.mutable_operation_params(); }) {
                request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::SYNC);
            }
            TResponse response;
            const auto status = (stub.*method)(&context, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_message());
            return response;
        }

        template <class TResponse>
        Ydb::StatusIds::StatusCode Status(const TResponse& response) {
            if constexpr (requires { response.operation(); }) {
                UNIT_ASSERT_C(response.operation().ready(), response.DebugString());
                return response.operation().status();
            } else {
                return response.status();
            }
        }

        template <class TResponse>
        void Success(const TResponse& response) {
            UNIT_ASSERT_VALUES_EQUAL_C(Status(response), Ydb::StatusIds::SUCCESS, response.DebugString());
        }

        template <class TResult, class TResponse>
        TResult Result(const TResponse& response) {
            Success(response);
            TResult result;
            UNIT_ASSERT_C(response.operation().result().UnpackTo(&result), response.DebugString());
            return result;
        }

        struct TFixture {
            NYdb::TKikimrWithGrpcAndRootSchema Server;
            std::shared_ptr<grpc::Channel> Channel;

            explicit TFixture(bool useSimpleProxy = false, bool enablePathAliasing = true)
                : Server(MakeConfig(useSimpleProxy, enablePathAliasing), {}, {}, false, nullptr, [](Tests::TServerSettings& settings) {
                    settings.StoragePoolTypes.clear();
                    settings.AddStoragePool("hdd");
                    settings.StoragePoolTypes.at("hdd").SetStoragePoolId(0);
                    settings.RegisterGrpcService<TKeyValueGRpcServiceV1>("keyvalue");
                })
                , Channel(grpc::CreateChannel(TStringBuilder() << "localhost:" << Server.GetPort(), grpc::InsecureChannelCredentials()))
            {
                Ydb::Cms::CreateDatabaseRequest request;
                request.set_path("/Root/kfront");
                auto* storage = request.mutable_resources()->add_storage_units();
                storage->set_unit_kind("hdd");
                storage->set_count(1);
                Server.Tenants_->CreateTenant(std::move(request));
                for (const auto node : Server.Tenants_->List("/Root/kfront")) {
                    Server.GetServer().EnableGRpc(Server.GetPortManager().GetPort(), node, "/Root/kfront");
                }
            }
        };

        std::set<std::tuple<TString, ui32, ui32>> EndpointIdentities(
            const Ydb::Discovery::ListEndpointsResult& result)
        {
            std::set<std::tuple<TString, ui32, ui32>> endpoints;
            for (const auto& endpoint : result.endpoints()) {
                endpoints.emplace(endpoint.address(), endpoint.port(), endpoint.node_id());
            }
            return endpoints;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(YdbPathAliasingIngress) {
        Y_UNIT_TEST(DeferredDatabaseOnlyRequestRewritesTheHeaderOnce) {
            TFixture fixture;
            auto stub = Ydb::Table::V1::TableService::NewStub(fixture.Channel);

            // The first request for this tenant is deferred while its database info
            // is fetched, then re-enters ingress. The physical-name decoy rule must
            // not see the cached result on replay.
            const auto session = Result<Ydb::Table::CreateSessionResult>(
                Call(*stub, &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}, "/alias/"));
            UNIT_ASSERT(!session.session_id().empty());

            Ydb::Table::KeepAliveRequest keepAlive;
            keepAlive.set_session_id(session.session_id());
            Success(Call(*stub, &TTable::KeepAlive, keepAlive, "/alias"));

            Ydb::Table::DeleteSessionRequest close;
            close.set_session_id(session.session_id());
            Success(Call(*stub, &TTable::DeleteSession, close, "/alias"));
        }

        Y_UNIT_TEST(SimpleProxyInitializesDatabaseNormalization) {
            TFixture fixture(true);
            auto stub = Ydb::Table::V1::TableService::NewStub(fixture.Channel);

            const auto session = Result<Ydb::Table::CreateSessionResult>(
                Call(*stub, &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}, "/alias"));
            UNIT_ASSERT(!session.session_id().empty());

            Ydb::Table::DeleteSessionRequest close;
            close.set_session_id(session.session_id());
            Success(Call(*stub, &TTable::DeleteSession, close, "/alias"));
        }

        Y_UNIT_TEST(DiscoveryKeepsHeaderAndBodySeparate) {
            TFixture fixture;
            auto stub = Ydb::Discovery::V1::DiscoveryService::NewStub(fixture.Channel);

            auto list = [&](TStringBuf header, TStringBuf body) {
                Ydb::Discovery::ListEndpointsRequest request;
                request.set_database(std::string(body.data(), body.size()));
                return Result<Ydb::Discovery::ListEndpointsResult>(
                    Call(*stub, &TDiscovery::ListEndpoints, request, header));
            };

            const auto expected = EndpointIdentities(list("/alias", "/discovery-alias"));
            UNIT_ASSERT(!expected.empty());
            UNIT_ASSERT(EndpointIdentities(list("/virtual/kfront", "/virtual/kfront")) == expected);
            UNIT_ASSERT(EndpointIdentities(list("/alias", "/alias")) == expected);
            UNIT_ASSERT(EndpointIdentities(list("/alias/", "/discovery-alias/")) == expected);

            Ydb::Discovery::ListEndpointsRequest missing;
            missing.set_database("/Root/missing");
            UNIT_ASSERT_VALUES_EQUAL(
                Status(Call(*stub, &TDiscovery::ListEndpoints, missing, "/alias")), Ydb::StatusIds::NOT_FOUND);
            UNIT_ASSERT(EndpointIdentities(list("/discovery-alias", "/alias")) == expected);

            // A byte-prefix-only match would incorrectly resolve this to the existing tenant.
            missing.set_database("/discovery-boundaryfront");
            UNIT_ASSERT(Status(Call(*stub, &TDiscovery::ListEndpoints, missing, "/alias")) != Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(UnmatchedInputsPreserveDisabledIngressBehavior) {
            auto observe = [](bool enablePathAliasing) {
                TFixture fixture(false, enablePathAliasing);
                auto discovery = Ydb::Discovery::V1::DiscoveryService::NewStub(fixture.Channel);
                auto keyValue = Ydb::KeyValue::V1::KeyValueService::NewStub(fixture.Channel);

                auto discoveryStatus = [&](bool addDatabaseHeader, bool addAuthTicket) {
                    Ydb::Discovery::ListEndpointsRequest request;
                    request.set_database("/Root");
                    return Status(Call(*discovery, &TDiscovery::ListEndpoints, request, "",
                        addDatabaseHeader, addAuthTicket));
                };

                Ydb::KeyValue::CreateVolumeRequest relative;
                relative.set_path("relative-volume");
                relative.set_partition_count(1);
                for (ui32 index = 0; index < 3; ++index) {
                    relative.mutable_storage_config()->add_channel()->set_media("hdd");
                }

                auto slashless = relative;
                slashless.set_path("Root/legacy-volume");

                return std::array{
                    discoveryStatus(false, false),
                    discoveryStatus(true, false),
                    discoveryStatus(false, true),
                    discoveryStatus(true, true),
                    Status(Call(*keyValue, &TKeyValue::CreateVolume, relative, "/Root")),
                    Status(Call(*keyValue, &TKeyValue::CreateVolume, slashless, "/Root")),
                };
            };

            const auto disabled = observe(false);
            const auto unmatched = observe(true);
            UNIT_ASSERT(disabled == unmatched);
            UNIT_ASSERT_VALUES_EQUAL(disabled[0], Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(disabled[1], Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(disabled[2], Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(disabled[3], Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(disabled[4], Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(disabled[5], Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(NativeStorageRewritesOnlyExplicitResourcePaths) {
            TFixture fixture;
            auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(fixture.Channel);

            Ydb::KeyValue::CreateVolumeRequest create;
            create.set_path("/volume-alias/");
            create.set_partition_count(1);
            for (ui32 index = 0; index < 3; ++index) {
                create.mutable_storage_config()->add_channel()->set_media("hdd");
            }
            Success(Call(*stub, &TKeyValue::CreateVolume, create, "/alias"));

            Ydb::KeyValue::ExecuteTransactionRequest write;
            write.set_path("/volume-inspect");
            auto* command = write.add_commands()->mutable_write();
            command->set_key("/volume-alias/key");
            command->set_value("/volume-alias/value");
            command->set_storage_channel(1);
            Success(Call(*stub, &TKeyValue::ExecuteTransaction, write, "/alias"));

            Ydb::KeyValue::ReadRequest read;
            read.set_path("/volume-inspect");
            read.set_key("/volume-alias/key");
            const auto result = Result<Ydb::KeyValue::ReadResult>(
                Call(*stub, &TKeyValue::Read, read, "/alias"));
            UNIT_ASSERT_VALUES_EQUAL(result.requested_key(), "/volume-alias/key");
            UNIT_ASSERT_VALUES_EQUAL(result.value(), "/volume-alias/value");

            Ydb::KeyValue::DescribeVolumeRequest describe;
            describe.set_path("/volume-inspect");
            UNIT_ASSERT_VALUES_EQUAL(
                Result<Ydb::KeyValue::DescribeVolumeResult>(
                    Call(*stub, &TKeyValue::DescribeVolume, describe, "/alias"))
                    .partition_count(),
                1);
        }

        Y_UNIT_TEST(AlterTableRewritesAbsoluteSequenceDefaults) {
            TFixture fixture;
            auto stub = Ydb::Table::V1::TableService::NewStub(fixture.Channel);
            const TString table = "/alias/sequence_table";
            const TString sequence = table + "/seq";

            Ydb::Table::CreateTableRequest create;
            create.set_path(table);
            auto* key = create.add_columns();
            key->set_name("key");
            key->mutable_type()->set_type_id(Ydb::Type::INT64);
            key->mutable_from_sequence()->set_name("seq");
            create.add_primary_key("key");
            auto* existing = create.add_columns();
            existing->set_name("existing");
            existing->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::INT64);
            Success(Call(*stub, &TTable::CreateTable, create, "/alias"));

            Ydb::Table::AlterTableRequest absolute;
            absolute.set_path(table);
            auto* added = absolute.add_add_columns();
            added->set_name("added_absolute");
            added->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::INT64);
            added->mutable_from_sequence()->set_name(sequence);
            auto* altered = absolute.add_alter_columns();
            altered->set_name("existing");
            altered->mutable_from_sequence()->set_name(sequence);
            Success(Call(*stub, &TTable::AlterTable, absolute, "/alias"));

            Ydb::Table::AlterTableRequest relative;
            relative.set_path(table);
            auto* relativeColumn = relative.add_add_columns();
            relativeColumn->set_name("added_relative");
            relativeColumn->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::INT64);
            relativeColumn->mutable_from_sequence()->set_name("sequence_table/seq");
            auto* relativeAlter = relative.add_alter_columns();
            relativeAlter->set_name("existing");
            relativeAlter->mutable_from_sequence()->set_name("sequence_table/seq");
            Success(Call(*stub, &TTable::AlterTable, relative, "/alias"));
        }

        Y_UNIT_TEST(TopicRewritesNestedDlqPathsOnly) {
            TFixture fixture;
            auto stub = Ydb::Topic::V1::TopicService::NewStub(fixture.Channel);

            auto createTopic = [&](const TString& path) {
                Ydb::Topic::CreateTopicRequest request;
                request.set_path(path);
                request.mutable_partitioning_settings()->set_min_active_partitions(1);
                Success(Call(*stub, &TTopic::CreateTopic, request, "/alias"));
            };
            createTopic("/alias/dlq");

            Ydb::Topic::CreateTopicRequest create;
            create.set_path("/alias/source");
            create.mutable_partitioning_settings()->set_min_active_partitions(1);
            auto* aliasConsumer = create.add_consumers();
            aliasConsumer->set_name("alias_consumer");
            auto* aliasPolicy = aliasConsumer->mutable_shared_consumer_type()->mutable_dead_letter_policy();
            aliasPolicy->set_enabled(true);
            aliasPolicy->mutable_move_action()->set_dead_letter_queue("/alias/dlq");
            Success(Call(*stub, &TTopic::CreateTopic, create, "/alias"));

            Ydb::Topic::AlterTopicRequest alter;
            alter.set_path("/alias/source");
            auto* added = alter.add_add_consumers();
            added->set_name("sqs_consumer");
            auto* addedPolicy = added->mutable_shared_consumer_type()->mutable_dead_letter_policy();
            addedPolicy->set_enabled(true);
            addedPolicy->mutable_move_action()->set_dead_letter_queue("sqs://account/queue");
            auto* changed = alter.add_alter_consumers();
            changed->set_name("alias_consumer");
            changed->mutable_alter_shared_consumer_type()->mutable_alter_dead_letter_policy()
                ->mutable_alter_move_action()->set_set_dead_letter_queue("/alias/dlq");
            Success(Call(*stub, &TTopic::AlterTopic, alter, "/alias"));

            Ydb::Topic::AlterTopicRequest setMove;
            setMove.set_path("/alias/source");
            auto* changedMove = setMove.add_alter_consumers();
            changedMove->set_name("alias_consumer");
            changedMove->mutable_alter_shared_consumer_type()->mutable_alter_dead_letter_policy()
                ->mutable_set_move_action()->set_dead_letter_queue("/alias/dlq");
            Success(Call(*stub, &TTopic::AlterTopic, setMove, "/alias"));

            Ydb::Topic::DescribeTopicRequest describe;
            describe.set_path("/alias/source");
            const auto result = Result<Ydb::Topic::DescribeTopicResult>(
                Call(*stub, &TTopic::DescribeTopic, describe, "/alias"));
            const std::array<std::pair<std::string, std::string>, 2> expected{{
                {"alias_consumer", "/Root/kfront/dlq"},
                {"sqs_consumer", "sqs://account/queue"},
            }};
            for (const auto& [name, dlq] : expected) {
                bool found = false;
                for (const auto& consumer : result.consumers()) {
                    if (consumer.name() == name) {
                        UNIT_ASSERT_VALUES_EQUAL(consumer.shared_consumer_type()
                            .dead_letter_policy().move_action().dead_letter_queue(), dlq);
                        found = true;
                    }
                }
                UNIT_ASSERT_C(found, name);
            }
        }
    } // Y_UNIT_TEST_SUITE(YdbPathAliasingIngress)

} // namespace NKikimr::NGRpcService
