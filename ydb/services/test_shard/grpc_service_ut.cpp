#include "grpc_service.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_test_shard_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/logger/backend.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <util/string/builder.h>


#define UNIT_ASSERT_CHECK_STATUS(got, exp) \
    UNIT_ASSERT_C(got.status() == exp, "exp# " << Ydb::StatusIds::StatusCode_Name(exp) \
            << " got# " << Ydb::StatusIds::StatusCode_Name(got.status()) << " issues# "  << got.issues()) \
// UNIT_ASSERT_CHECK_STATUS

namespace NKikimr::NGRpcService {

class TKikimrWithGrpcAndRootSchema {
public:
    TKikimrWithGrpcAndRootSchema(
            NKikimrConfig::TAppConfig appConfig = {},
            TAutoPtr<TLogBackend> logBackend = {})
    {
        ui16 port = PortManager.GetPort(2134);
        ui16 grpc = PortManager.GetPort(2135);
        ServerSettings = new Tests::TServerSettings(port);
        ServerSettings->SetGrpcPort(grpc);
        ServerSettings->SetLogBackend(logBackend);
        ServerSettings->SetDomainName("Root");
        ServerSettings->SetDynamicNodeCount(1);
        ServerSettings->AddStoragePool("ssd", "ssd-pool");
        ServerSettings->AddStoragePool("hdd", "hdd-pool");
        ServerSettings->Formats = new TFormatFactory;
        ServerSettings->FeatureFlags = appConfig.GetFeatureFlags();
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TTestShardGRpcService>("test_shard");

        Server_.Reset(new Tests::TServer(*ServerSettings));
        Tenants_.Reset(new Tests::TTenants(Server_));

        Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_PROXY, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_DEBUG);

        NYdbGrpc::TServerOptions grpcOption;
        grpcOption.SetPort(grpc);
        Server_->EnableGRpc(grpcOption);

        Tests::TClient annoyingClient(*ServerSettings);
        if (ServerSettings->AppConfig->GetDomainsConfig().GetSecurityConfig().GetEnforceUserTokenRequirement()) {
            annoyingClient.SetSecurityToken("root@builtin");
        }
        annoyingClient.InitRootScheme("Root");
        GRpcPort_ = grpc;
    }

    ui16 GetPort() {
        return GRpcPort_;
    }

    TPortManager& GetPortManager() {
        return PortManager;
    }

    TTestActorRuntime* GetRuntime() {
        return Server_->GetRuntime();
    }

    Tests::TServer& GetServer() {
        return *Server_;
    }

    Tests::TServerSettings::TPtr ServerSettings;
    Tests::TServer::TPtr Server_;
    THolder<Tests::TTenants> Tenants_;
private:
    TPortManager PortManager;
    ui16 GRpcPort_;
};


Y_UNIT_TEST_SUITE(TestShardGRPCService) {

    template <typename TCtx>
    void AdjustCtxForDB(TCtx &ctx) {
        ctx.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, "root@builtin");
    }

    void MakeDirectory(auto &channel, const TString &path) {
        std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> stub;
        stub = Ydb::Scheme::V1::SchemeService::NewStub(channel);

        Ydb::Scheme::MakeDirectoryRequest makeDirectoryRequest;
        makeDirectoryRequest.set_path(path);
        Ydb::Scheme::MakeDirectoryResponse makeDirectoryResponse;
        grpc::ClientContext makeDirectoryCtx;
        AdjustCtxForDB(makeDirectoryCtx);
        stub->MakeDirectory(&makeDirectoryCtx, makeDirectoryRequest, &makeDirectoryResponse);
        UNIT_ASSERT_CHECK_STATUS(makeDirectoryResponse.operation(), Ydb::StatusIds::SUCCESS);
    }

    Ydb::Scheme::ListDirectoryResult ListDirectory(auto &channel, const TString &path) {
        std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> stub;
        stub = Ydb::Scheme::V1::SchemeService::NewStub(channel);
        Ydb::Scheme::ListDirectoryRequest listDirectoryRequest;
        listDirectoryRequest.set_path(path);

        Ydb::Scheme::ListDirectoryResult listDirectoryResult;
        Ydb::Scheme::ListDirectoryResponse listDirectoryResponse;

        grpc::ClientContext listDirectoryCtx;
        AdjustCtxForDB(listDirectoryCtx);
        stub->ListDirectory(&listDirectoryCtx, listDirectoryRequest, &listDirectoryResponse);

        UNIT_ASSERT_CHECK_STATUS(listDirectoryResponse.operation(), Ydb::StatusIds::SUCCESS);
        listDirectoryResponse.operation().result().UnpackTo(&listDirectoryResult);
        return listDirectoryResult;
    }

    void CreateTestShard(auto &channel, const TString &path, ui16 port) {
        std::unique_ptr<Ydb::TestShard::V1::TestShardService::Stub> stub;
        stub = Ydb::TestShard::V1::TestShardService::NewStub(channel);

        Ydb::TestShard::CreateTestShardRequest request;
        request.set_path(path);
        request.set_count(1);
        request.add_channels("ssd");
        request.add_channels("ssd");
        request.add_channels("ssd");

        TString config = R"(
workload:
  sizes:
    - {weight: 9, min: 128, max: 2048, inline: false}
    - {weight: 2, min: 1, max: 128, inline: true}
    - {weight: 1, min: 524288, max: 8388608, inline: false}
  write:
    - frequency: 10.0
      max_interval_ms: 1000
  restart:
    - frequency: 0.01
      max_interval_ms: 120000
  patch_fraction_ppm: 100000

limits:
  data:
    min: 750000000
    max: 1000000000
  concurrency:
    writes: 3
    reads: 1

timing:
  delay_start: 5
  reset_on_full: true
  stall_counter: 1000
tracing:
  put_fraction_ppm: 1000
  verbosity: 15

validation:
  server: "localhost:__TSSERVER_PORT__"
  after_bytes: 5000000000
)";
        SubstGlobal(config, "__TSSERVER_PORT__", ToString(port));
        request.set_config(config);

        Ydb::TestShard::CreateTestShardResponse response;
        Ydb::TestShard::CreateTestShardResult result;

        grpc::ClientContext ctx;
        AdjustCtxForDB(ctx);
        stub->CreateTestShard(&ctx, request, &response);
        UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
        response.operation().result().UnpackTo(&result);
        UNIT_ASSERT_VALUES_EQUAL(result.tablet_ids_size(), 1);
    }

    void DeleteTestShard(auto &channel, const TString &path) {
        std::unique_ptr<Ydb::TestShard::V1::TestShardService::Stub> stub;
        stub = Ydb::TestShard::V1::TestShardService::NewStub(channel);

        Ydb::TestShard::DeleteTestShardRequest request;
        request.set_path(path);

        Ydb::TestShard::DeleteTestShardResponse response;
        Ydb::TestShard::DeleteTestShardResult result;

        grpc::ClientContext ctx;
        AdjustCtxForDB(ctx);
        stub->DeleteTestShard(&ctx, request, &response);
        UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
        response.operation().result().UnpackTo(&result);
    }

    Y_UNIT_TEST(SimpleCreateDeleteTestShard) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString path = "/Root/mydb/";
        TString testShardPath = "/Root/mydb/mytestshard";
        MakeDirectory(channel, path);
        CreateTestShard(channel, testShardPath, grpc);

        Ydb::Scheme::ListDirectoryResult listDirectoryResult = ListDirectory(channel, path);
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.self().name(), "mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.children(0).name(), "mytestshard");

        DeleteTestShard(channel, testShardPath);
        listDirectoryResult = ListDirectory(channel, path);
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.self().name(), "mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.children_size(), 0);
    }

    void CreateTestShardWithInvalidConfig(auto &channel, const TString &path, const TString & config, Ydb::StatusIds::StatusCode expectedStatus, const TString& expectedErrorSubstring = "") {
        std::unique_ptr<Ydb::TestShard::V1::TestShardService::Stub> stub;
        stub = Ydb::TestShard::V1::TestShardService::NewStub(channel);

        Ydb::TestShard::CreateTestShardRequest request;
        request.set_path(path);
        request.set_count(1);
        request.set_config(config);

        Ydb::TestShard::CreateTestShardResponse response;

        grpc::ClientContext ctx;
        AdjustCtxForDB(ctx);
        stub->CreateTestShard(&ctx, request, &response);
        
        UNIT_ASSERT_CHECK_STATUS(response.operation(), expectedStatus);
        
        if (!expectedErrorSubstring.empty()) {
            UNIT_ASSERT_STRING_CONTAINS(response.operation().issues(0).message(), expectedErrorSubstring);
        }
    }

    Y_UNIT_TEST(InvalidYamlConfig) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString path = "/Root/mydb/";
        TString testShardPath = "/Root/mydb/mytestshard";
        MakeDirectory(channel, path);

        {
            TString invalidConfig = R"(
workload:
  sizes:
    - {weight: 9, min: 128, max: 2048, inline: false
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "Failed to parse YAML config");
        }

        {
            TString invalidConfig = R"(
workload:
  sizes:
    - {weight: 9, min: 128, max: 2048, inline: false}
invalid_section:
  some_key: some_value
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "Unknown key in 'config root': 'invalid_section'");
        }

        {
            TString invalidConfig = R"(
workload:
  sizes:
    - {weight: 9, min: 128, max: 2048, inline: false}
  invalid_key: some_value
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "Unknown key in 'workload': 'invalid_key'");
        }

        {
            TString invalidConfig = R"(
workload: "not_a_map"
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "'workload' must be a map");
        }

        {
            TString invalidConfig = R"(
validation: "not_a_map"
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "'validation' must be a map");
        }

        {
            TString invalidConfig = R"(
validation:
  server: "localhost_without_port"
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "'validation.server' must be in 'host:port' format");
        }

        {
            TString invalidConfig = R"("not_a_map")";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "Config root must be a YAML map");
        }

        {
            TString invalidConfig = R"(
workload:
  sizes: "not_a_list"
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "'workload.sizes' must be a list");
        }

        {
            TString invalidConfig = R"(
workload:
  write: "not_a_list"
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "'workload.write' must be a list");
        }

        {
            TString invalidConfig = R"(
workload:
  restart: "not_a_list"
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "'workload.restart' must be a list");
        }

        {
            TString invalidConfig = R"(
limits:
  data: "not_a_map"
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "'limits.data' must be a map");
        }

        {
            TString invalidConfig = R"(
limits:
  concurrency: "not_a_map"
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "'limits.concurrency' must be a map");
        }

        {
            TString invalidConfig = R"(
limits:
  data:
    min: 750000000
    max: 1000000000
  invalid_key: some_value
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "Unknown key in 'limits': 'invalid_key'");
        }

        {
            TString invalidConfig = R"(
timing:
  delay_start: 5
  invalid_key: some_value
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "Unknown key in 'timing': 'invalid_key'");
        }

        {
            TString invalidConfig = R"(
validation:
  server: "localhost:2135"
  invalid_key: some_value
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "Unknown key in 'validation': 'invalid_key'");
        }

        {
            TString invalidConfig = R"(
tracing:
  put_fraction_ppm: 1000
  invalid_key: some_value
)";
            CreateTestShardWithInvalidConfig(channel, testShardPath, invalidConfig,
                Ydb::StatusIds::BAD_REQUEST, "Unknown key in 'tracing': 'invalid_key'");
        }
    }

} // Y_UNIT_TEST_SUITE(TestShardGRPCService)

} // NKikimr::NGRpcService
