#include "ydb_common_ut.h"

#include <ydb/services/keyvalue/grpc_service_v1.h>

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
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

        void AddRule(NKikimrConfig::TAppConfig& config, const char* pattern, const char* replacement) {
            auto* rule = config.MutablePathRewriteConfig()->AddRules();
            rule->SetPattern(pattern);
            rule->SetReplacement(replacement);
        }

        NKikimrConfig::TAppConfig MakeConfig(bool useSimpleProxy = false, bool enablePathAliasing = true) {
            NKikimrConfig::TAppConfig config;
            config.MutableGRpcConfig()->SetSkipSchemeCheck(useSimpleProxy);
            if (!enablePathAliasing) {
                return config;
            }
            AddRule(config, "^/alias$", "/Root/kfront");
            AddRule(config, "^/discovery-alias$", "/Root/kfront");
            AddRule(config, "^/Root/kfront$", "/Root/missing");
            AddRule(config, "^/volume-(alias|inspect)$", "/Root/kfront/Volume");
            AddRule(config, "^/Root/kfront/Volume$", "/Root/kfront/Wrong");
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
                    settings.AddStoragePoolType("hdd");
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
                Call(*stub, &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}, "/alias"));
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
            UNIT_ASSERT(EndpointIdentities(list("/alias", "/alias")) == expected);

            Ydb::Discovery::ListEndpointsRequest missing;
            missing.set_database("/Root/missing");
            UNIT_ASSERT_VALUES_EQUAL(
                Status(Call(*stub, &TDiscovery::ListEndpoints, missing, "/alias")), Ydb::StatusIds::NOT_FOUND);
            UNIT_ASSERT(EndpointIdentities(list("/discovery-alias", "/alias")) == expected);
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
            create.set_path("/volume-alias");
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
    } // Y_UNIT_TEST_SUITE(YdbPathAliasingIngress)

} // namespace NKikimr::NGRpcService
