#include "grpc_service.h"

#include <ydb/library/yaml_config/public/yaml_config.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>

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

namespace NKikimr::NGRpcService {

struct TKikimrTestSettings {
    static constexpr bool SSL = false;
    static constexpr bool AUTH = false;
    static constexpr bool PrecreatePools = true;
    static constexpr bool EnableSystemViews = true;
};

struct TKikimrTestWithAuth : TKikimrTestSettings {
    static constexpr bool AUTH = true;
};

struct TKikimrTestWithAuthAndSsl : TKikimrTestWithAuth {
    static constexpr bool SSL = true;
};

struct TKikimrTestNoSystemViews : TKikimrTestSettings {
    static constexpr bool EnableSystemViews = false;
};

template <typename TestSettings = TKikimrTestSettings>
class TBasicKikimrWithGrpcAndRootSchema {
public:
    TBasicKikimrWithGrpcAndRootSchema(
            NKikimrConfig::TAppConfig appConfig = {},
            TAutoPtr<TLogBackend> logBackend = {})
    {
        ui16 port = PortManager.GetPort(2134);
        ui16 grpc = PortManager.GetPort(2135);
        ServerSettings = new NKikimr::Tests::TServerSettings(port);
        ServerSettings->SetGrpcPort(grpc);
        ServerSettings->SetLogBackend(logBackend);
        ServerSettings->SetDomainName("Root");
        ServerSettings->SetDynamicNodeCount(1);
        if (TestSettings::PrecreatePools) {
            ServerSettings->AddStoragePool("ssd");
            ServerSettings->AddStoragePool("hdd");
            ServerSettings->AddStoragePool("hdd1");
            ServerSettings->AddStoragePool("hdd2");
        } else {
            ServerSettings->AddStoragePoolType("ssd");
            ServerSettings->AddStoragePoolType("hdd");
            ServerSettings->AddStoragePoolType("hdd1");
            ServerSettings->AddStoragePoolType("hdd2");
        }
        ServerSettings->Formats = new NKikimr::TFormatFactory;
        ServerSettings->FeatureFlags = appConfig.GetFeatureFlags();
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TConfigGRpcService>("bsconfig");

        Server_.Reset(new NKikimr::Tests::TServer(*ServerSettings));
        Tenants_.Reset(new NKikimr::Tests::TTenants(Server_));

        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NActors::NLog::PRI_TRACE);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_PROXY, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::BSCONFIG, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::STATESTORAGE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SAUSAGE_BIO, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_FLATBOOT, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_OLAPSHARD, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        NYdbGrpc::TServerOptions grpcOption;
        if (TestSettings::AUTH) {
            grpcOption.SetUseAuth(true);
        }
        grpcOption.SetPort(grpc);
        Server_->EnableGRpc(grpcOption);

        NKikimr::Tests::TClient annoyingClient(*ServerSettings);
        if (ServerSettings->AppConfig->GetDomainsConfig().GetSecurityConfig().GetEnforceUserTokenRequirement()) {
            annoyingClient.SetSecurityToken("root@builtin");
        }
        annoyingClient.InitRootScheme("Root");
        GRpcPort_ = grpc;

        Channel_ = grpc::CreateChannel(TStringBuilder() << "localhost:" << GetPort(), grpc::InsecureChannelCredentials());
    }

    ui16 GetPort() {
        return GRpcPort_;
    }

    TPortManager& GetPortManager() {
        return PortManager;
    }

    std::shared_ptr<grpc::Channel>& GetChannel() {
        return Channel_;
    }

    void ResetSchemeCache(TString path, ui32 nodeIndex = 0) {
        NActors::TTestActorRuntime* runtime = Server_->GetRuntime();
        NKikimr::Tests::TClient annoyingClient(*ServerSettings);
        annoyingClient.RefreshPathCache(runtime, path, nodeIndex);
    }

    NActors::TTestActorRuntime* GetRuntime() {
        return Server_->GetRuntime();
    }

    NKikimr::Tests::TServer& GetServer() {
        return *Server_;
    }

    NKikimr::Tests::TServerSettings::TPtr ServerSettings;
    NKikimr::Tests::TServer::TPtr Server_;
    THolder<NKikimr::Tests::TTenants> Tenants_;
    std::shared_ptr<grpc::Channel> Channel_;
private:
    TPortManager PortManager;
    ui16 GRpcPort_;
};

using TKikimrWithGrpcAndRootSchema = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestSettings>;

TString NormalizeYaml(const TString& yaml) {
    TStringStream normalized;
    auto doc = NKikimr::NFyaml::TDocument::Parse(yaml);
    normalized << doc;
    return normalized.Str();
}

Y_UNIT_TEST_SUITE(ConfigGRPCService) {

    template <typename TCtx>
    void AdjustCtxForDB(TCtx &ctx) {    
        ctx.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, "root@builtin");
    }

    void ReplaceConfig(
            auto &channel,
            std::optional<TString> mainConfig,
            std::optional<TString> storageConfig,
            std::optional<bool> switchDedicatedStorageSection,
            bool dedicatedConfigMode) {

        std::unique_ptr<Ydb::Config::V1::ConfigService::Stub> stub;
        stub = Ydb::Config::V1::ConfigService::NewStub(channel);

        Ydb::Config::ReplaceConfigRequest request;

        if (!dedicatedConfigMode && !switchDedicatedStorageSection) {
            if (mainConfig) {
                request.set_replace(*mainConfig);
            }
        } else if (dedicatedConfigMode && !switchDedicatedStorageSection) {
            auto& replace = *request.mutable_replace_with_dedicated_storage_section();
            if (mainConfig) {
                replace.set_main_config(*mainConfig);
            }
            if (storageConfig) {
                replace.set_storage_config(*storageConfig);
            }
        } else if (switchDedicatedStorageSection && *switchDedicatedStorageSection) {
            auto& replace = *request.mutable_replace_enable_dedicated_storage_section();
            if (mainConfig) {
                replace.set_main_config(*mainConfig);
            }
            if (storageConfig) {
                replace.set_storage_config(*storageConfig);
            }
        } else if (switchDedicatedStorageSection && !*switchDedicatedStorageSection) {
            if (mainConfig) {
                request.set_replace_disable_dedicated_storage_section(*mainConfig);
            }
        } else {
            Y_ABORT("invariant violation");
        }

        Ydb::Config::ReplaceConfigResponse response;
        Ydb::Config::ReplaceConfigResult result;

        grpc::ClientContext replaceConfigCtx;
        AdjustCtxForDB(replaceConfigCtx);
        stub->ReplaceConfig(&replaceConfigCtx, request, &response);
        UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
        Cerr << "response: " << response.operation().result().DebugString() << Endl;
        response.operation().result().UnpackTo(&result);
    }

    void FetchConfig(
            auto& channel,
            bool dedicatedStorageSection,
            bool dedicatedClusterSection,
            std::optional<TString>& mainConfig,
            std::optional<TString>& storageConfig) {
        std::unique_ptr<Ydb::Config::V1::ConfigService::Stub> stub;
        stub = Ydb::Config::V1::ConfigService::NewStub(channel);

        Ydb::Config::FetchConfigRequest request;

        auto& all = *request.mutable_all();

        if (dedicatedStorageSection || dedicatedClusterSection) {
            all.mutable_detach_storage_config_section();
        }

        Ydb::Config::FetchConfigResponse response;
        Ydb::Config::FetchConfigResult result;

        grpc::ClientContext fetchConfigCtx;
        AdjustCtxForDB(fetchConfigCtx);
        stub->FetchConfig(&fetchConfigCtx, request, &response);
        UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
        response.operation().result().UnpackTo(&result);

        std::optional<TString> rcvMainConfig;
        std::optional<TString> rcvStorageConfig;

        for (auto& entry : result.config()) {
            if (entry.identity().type_case() == Ydb::Config::ConfigIdentity::TypeCase::kMain) {
                rcvMainConfig = entry.config();
            }

            if (entry.identity().type_case() == Ydb::Config::ConfigIdentity::TypeCase::kStorage) {
                rcvStorageConfig = entry.config();
            }
        }

        mainConfig = rcvMainConfig;
        storageConfig = rcvStorageConfig;
    }   

    Y_UNIT_TEST(ReplaceConfig) {
        TKikimrWithGrpcAndRootSchema server;
        TString yamlConfig = R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 0

allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string

selector_config: []

config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: SectorMap:1:64
      type: SSD
      expected_slot_count: 9
    - path: SectorMap:2:64
      type: SSD
      expected_slot_count: 9
  - host_config_id: 2
    drive:
    - path: SectorMap:3:64
      type: SSD
      expected_slot_count: 9
  hosts:
  - host: ::1
    port: 12001
    host_config_id: 2
)";
        TString yamlConfigExpected = SubstGlobalCopy(yamlConfig, "version: 0", "version: 1");
        ReplaceConfig(server.GetChannel(), yamlConfig, std::nullopt, std::nullopt, false);
        std::optional<TString> yamlConfigFetched, storageYamlConfigFetched;
        FetchConfig(server.GetChannel(), false, false, yamlConfigFetched, storageYamlConfigFetched);
        UNIT_ASSERT(yamlConfigFetched);
        UNIT_ASSERT(!storageYamlConfigFetched);
        UNIT_ASSERT_VALUES_EQUAL(yamlConfigExpected, *yamlConfigFetched);
    }

    Y_UNIT_TEST(FetchConfig) {
        TKikimrWithGrpcAndRootSchema server;
        std::optional<TString> yamlConfigFetched, storageYamlConfigFetched;
        FetchConfig(server.GetChannel(), false, false, yamlConfigFetched, storageYamlConfigFetched);
        UNIT_ASSERT(!yamlConfigFetched);
        UNIT_ASSERT(!storageYamlConfigFetched);
    }
}

} // NKikimr::NGRpcService
