#include "grpc_service.h"

#include <ydb/library/yaml_config/public/yaml_config.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/base/blobstorage_console_events.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/draft/ydb_dynamic_config_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/services/dynamic_config/grpc_service.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/logger/backend.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/thread.h>

#include <functional>

#define UNIT_ASSERT_CHECK_STATUS(got, exp) \
    UNIT_ASSERT_C(got.status() == exp, "exp# " << Ydb::StatusIds::StatusCode_Name(exp) \
            << " got# " << Ydb::StatusIds::StatusCode_Name(got.status()) << " issues# "  << got.issues()) \

namespace NKikimr::NGRpcService {

struct TKikimrTestSettings {
    static constexpr bool SSL = false;
    static constexpr bool AUTH = false;
    static constexpr bool PrecreatePools = true;
};

struct TKikimrTestWithAuth : TKikimrTestSettings {
    static constexpr bool AUTH = true;
};

struct TKikimrTestWithAuthAndSsl : TKikimrTestWithAuth {
    static constexpr bool SSL = true;
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
        ServerSettings->SetEnableFeatureFlagsConfigurator(true);
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TConfigGRpcService>("bsconfig");
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TGRpcDynamicConfigService>("dynconfig", std::nullopt, true);

        Server_.Reset(new NKikimr::Tests::TServer(*ServerSettings));
        Tenants_.Reset(new NKikimr::Tests::TTenants(Server_));

        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NActors::NLog::PRI_TRACE);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NActors::NLog::PRI_DEBUG);
        // Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        // Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
        // Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_PROXY, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::BS_CONTROLLER, NActors::NLog::PRI_DEBUG);
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
            bool dedicatedConfigMode,
            bool dryRun,
            const std::function<void(const Ydb::Config::ReplaceConfigResponse&)>& checker) {

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
        request.set_dry_run(dryRun);

        Ydb::Config::ReplaceConfigResponse response;

        grpc::ClientContext replaceConfigCtx;
        AdjustCtxForDB(replaceConfigCtx);
        stub->ReplaceConfig(&replaceConfigCtx, request, &response);
        Cerr << "response: " << response.operation().DebugString() << Endl;
        checker(response);
    }

    void EnableConfigV2(auto& channel) {
        TString enableV2Config = R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  feature_flags:
    switch_to_config_v2: true
)";

        std::unique_ptr<Ydb::DynamicConfig::V1::DynamicConfigService::Stub> stub;
        stub = Ydb::DynamicConfig::V1::DynamicConfigService::NewStub(channel);

        Ydb::DynamicConfig::ReplaceConfigRequest request;
        request.set_config(enableV2Config);

        Ydb::DynamicConfig::ReplaceConfigResponse response;
        grpc::ClientContext context;
        AdjustCtxForDB(context);

        stub->ReplaceConfig(&context, request, &response);
        UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
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
        EnableConfigV2(server.GetChannel());
        TString pdiskPath = server.GetRuntime()->GetTempDir() + "pdisk_1.dat";
        TString yamlConfig = Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 1

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
    - path: %s
      type: SSD
  hosts:
  - host: ::1
    port: 12001
    host_config_id: 2
)", pdiskPath.c_str());
        ReplaceConfig(server.GetChannel(), yamlConfig, std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
            });
        std::optional<TString> yamlConfigFetched, storageYamlConfigFetched;
        FetchConfig(server.GetChannel(), false, false, yamlConfigFetched, storageYamlConfigFetched);
        UNIT_ASSERT(yamlConfigFetched);
        UNIT_ASSERT(!storageYamlConfigFetched);
        UNIT_ASSERT_VALUES_EQUAL(yamlConfig, *yamlConfigFetched);
    }

    Y_UNIT_TEST(ReplaceConfigWithInvalidHostConfig) {
        TKikimrWithGrpcAndRootSchema server;
        EnableConfigV2(server.GetChannel());
        TString pdiskPath = server.GetRuntime()->GetTempDir() + "pdisk_1.dat";
        TString yamlConfig = Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 1
config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: %s
      type: SSD
    - path: %s
      type: SSD
  hosts:
  - host: ::1
    port: 12001
    host_config_id: 1
)", pdiskPath.c_str(), pdiskPath.c_str());
        ReplaceConfig(server.GetChannel(), yamlConfig, std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::INTERNAL_ERROR);
                TString opDebugString = resp.operation().DebugString();
                UNIT_ASSERT_C(opDebugString.Contains("duplicate path"), opDebugString);
            });
    }

    Y_UNIT_TEST(FetchConfig) {
        TKikimrWithGrpcAndRootSchema server;

        EnableConfigV2(server.GetChannel());

        auto* runtime = server.GetRuntime();
        auto sender = runtime->AllocateEdgeActor();
        ui64 bscId = MakeBSControllerID();
        ForwardToTablet(*runtime, bscId, sender, new TEvents::TEvPoisonPill(), 0, true);

        bool switchToConfigV2 = false;
        for (int i = 0; i < 10; ++i) {
            auto& appData = runtime->GetAppData(0);
            if (appData.FeatureFlags.GetSwitchToConfigV2()) {
                switchToConfigV2 = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT(switchToConfigV2);

        std::unique_ptr<Ydb::Config::V1::ConfigService::Stub> stub;
        stub = Ydb::Config::V1::ConfigService::NewStub(server.GetChannel());

        Ydb::Config::FetchConfigRequest request;
        request.mutable_all();

        Ydb::Config::FetchConfigResponse response;
        grpc::ClientContext fetchConfigCtx;
        AdjustCtxForDB(fetchConfigCtx);
        stub->FetchConfig(&fetchConfigCtx, request, &response);
        const auto status = response.operation().status();
        UNIT_ASSERT_C(
            status == Ydb::StatusIds::UNSUPPORTED,
            "unexpected FetchConfig status after restart: " << Ydb::StatusIds::StatusCode_Name(status)
                << " issues# " << response.operation().issues()
        );
    }

    Y_UNIT_TEST(CheckV1IsBlocked) {
        NKikimr::TTestActorRuntimeBase::ResetFirstNodeId();

        TKikimrWithGrpcAndRootSchema server;
        EnableConfigV2(server.GetChannel());
        TString pdiskPath = server.GetRuntime()->GetTempDir() + "pdisk_1.dat";
        TString yamlConfig = Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 1

config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: SectorMap:1:64
      type: SSD
    - path: SectorMap:2:64
      type: SSD
  - host_config_id: 2
    drive:
    - path: %s
      type: SSD
  hosts:
  - host: ::1
    port: 12001
    host_config_id: 2
  feature_flags:
    switch_to_config_v2: true
)", pdiskPath.c_str());

        ReplaceConfig(server.GetChannel(), yamlConfig, std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
            });

        auto* runtime = server.GetRuntime();
        bool switchToConfigV2 = false;
        for (int i = 0; i < 10; ++i) {
            auto& appData = runtime->GetAppData(0);
            if (appData.FeatureFlags.GetSwitchToConfigV2()) {
                switchToConfigV2 = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT(switchToConfigV2);

        std::optional<TString> yamlConfigFetched, storageYamlConfigFetched;
        FetchConfig(server.GetChannel(), false, false, yamlConfigFetched, storageYamlConfigFetched);
        UNIT_ASSERT(yamlConfigFetched);
        UNIT_ASSERT(!storageYamlConfigFetched);
        UNIT_ASSERT_VALUES_EQUAL(yamlConfig, *yamlConfigFetched);

        std::unique_ptr<Ydb::DynamicConfig::V1::DynamicConfigService::Stub> stub;
        stub = Ydb::DynamicConfig::V1::DynamicConfigService::NewStub(server.GetChannel());

        Ydb::DynamicConfig::ReplaceConfigRequest request;
        request.set_config(yamlConfig);

        Ydb::DynamicConfig::ReplaceConfigResponse response;
        grpc::ClientContext context;
        AdjustCtxForDB(context);

        stub->ReplaceConfig(&context, request, &response);

        UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(response.operation().issues(0).message(), "Dynamic Config V1 is disabled. Use V2 API.");
    }

    Y_UNIT_TEST(CheckDryRun) {
        NKikimr::TTestActorRuntimeBase::ResetFirstNodeId();
        TKikimrWithGrpcAndRootSchema server;
        EnableConfigV2(server.GetChannel());

        TString pdiskPath = server.GetRuntime()->GetTempDir() + "pdisk_1.dat";

        TString yamlConfig = Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 1

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
    - path: %s
      type: SSD
  hosts:
  - host: ::1
    port: 12001
    host_config_id: 2
  feature_flags:
    switch_to_config_v2: true
)", pdiskPath.c_str());

        ReplaceConfig(server.GetChannel(), yamlConfig, std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
            });

        auto* runtime = server.GetRuntime();
        bool switchToConfigV2 = false;
        for (int i = 0; i < 10; ++i) {
            auto& appData = runtime->GetAppData(0);
            if (appData.FeatureFlags.GetSwitchToConfigV2()) {
                switchToConfigV2 = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT(switchToConfigV2);

        std::optional<TString> yamlConfigFetched, storageYamlConfigFetched;
        FetchConfig(server.GetChannel(), false, false, yamlConfigFetched, storageYamlConfigFetched);
        UNIT_ASSERT(yamlConfigFetched);
        UNIT_ASSERT(!storageYamlConfigFetched);
        TString originalConfig = *yamlConfigFetched;

        TString yamlConfigV2 = Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 2

config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: SectorMap:1:64
      type: SSD
      expected_slot_count: 10
    - path: SectorMap:2:64
      type: SSD
      expected_slot_count: 10
  - host_config_id: 2
    drive:
    - path: %s
      type: SSD
  hosts:
  - host: ::1
    port: 12001
    host_config_id: 2
  feature_flags:
    switch_to_config_v2: true
)", pdiskPath.c_str());

        ReplaceConfig(server.GetChannel(), yamlConfigV2, std::nullopt, std::nullopt, false, true,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
            });

        FetchConfig(server.GetChannel(), false, false, yamlConfigFetched, storageYamlConfigFetched);
        UNIT_ASSERT(yamlConfigFetched);
        UNIT_ASSERT(!storageYamlConfigFetched);
        UNIT_ASSERT_VALUES_EQUAL(originalConfig, *yamlConfigFetched);

        TString invalidYamlConfig = Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 2
config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: %s
      type: SSD
    - path: %s
      type: SSD
  hosts:
  - host: ::1
    port: 12001
    host_config_id: 1
  feature_flags:
    switch_to_config_v2: true
)", pdiskPath.c_str(), pdiskPath.c_str());

        ReplaceConfig(server.GetChannel(), invalidYamlConfig, std::nullopt, std::nullopt, false, true,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::INTERNAL_ERROR);
                TString opDebugString = resp.operation().DebugString();
                UNIT_ASSERT_C(opDebugString.Contains("duplicate path"), opDebugString);
            });

        FetchConfig(server.GetChannel(), false, false, yamlConfigFetched, storageYamlConfigFetched);
        UNIT_ASSERT(yamlConfigFetched);
        UNIT_ASSERT(!storageYamlConfigFetched);
        UNIT_ASSERT_VALUES_EQUAL(originalConfig, *yamlConfigFetched);
    }

}

Y_UNIT_TEST_SUITE(ConfigGRPCServiceAuth) {

    using TServerWithAuth = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth>;

    struct TKikimrTestWithAuthDynamicPools : TKikimrTestWithAuth {
        static constexpr bool PrecreatePools = false;
    };

    using TServerWithAuthDynamicPools = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuthDynamicPools>;

    template <class TServer>
    void ConfigureAuth(TServer& server, const TVector<TString>& adminSids, bool enableDatabaseAdmin) {
        auto& appData = server.GetRuntime()->GetAppData(0);
        appData.AdministrationAllowedSIDs = adminSids;
        appData.FeatureFlags.SetEnableDatabaseAdmin(enableDatabaseAdmin);
    }

    template <class TServer>
    TString CreateDatabaseWithOwner(TServer& server, const TString& name, const TString& owner) {
        const TString path = "/Root/" + name;

        Ydb::Cms::CreateDatabaseRequest request;
        request.set_path(path);
        auto& storage = *request.mutable_resources()->add_storage_units();
        storage.set_unit_kind("ssd");
        storage.set_count(1);
        server.Tenants_->CreateTenant(std::move(request));

        NKikimr::Tests::TClient client(*server.ServerSettings);
        client.TestModifyOwner("/Root", name, owner);
        server.ResetSchemeCache(path);

        return path;
    }

    Ydb::Config::FetchConfigResponse DoFetchConfig(
            auto& channel,
            std::optional<TString> token,
            std::optional<TString> database) {
        auto stub = Ydb::Config::V1::ConfigService::NewStub(channel);

        Ydb::Config::FetchConfigRequest request;
        request.mutable_all();

        Ydb::Config::FetchConfigResponse response;
        grpc::ClientContext ctx;
        if (token) {
            ctx.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, *token);
        }
        if (database) {
            ctx.AddMetadata(NYdb::YDB_DATABASE_HEADER, *database);
        }
        stub->FetchConfig(&ctx, request, &response);
        return response;
    }

    Ydb::Config::ReplaceConfigResponse DoReplaceConfig(
            auto& channel,
            std::optional<TString> token,
            std::optional<TString> database,
            const TString& yaml) {
        auto stub = Ydb::Config::V1::ConfigService::NewStub(channel);

        Ydb::Config::ReplaceConfigRequest request;
        request.set_replace(yaml);

        Ydb::Config::ReplaceConfigResponse response;
        grpc::ClientContext ctx;
        if (token) {
            ctx.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, *token);
        }
        if (database) {
            ctx.AddMetadata(NYdb::YDB_DATABASE_HEADER, *database);
        }
        stub->ReplaceConfig(&ctx, request, &response);
        return response;
    }

    TString MakeClusterConfigYaml() {
        return R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 1
config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: SectorMap:1:64
      type: SSD
  hosts:
  - host: ::1
    port: 12001
    host_config_id: 1
)";
    }

    TString MakeDatabaseConfigYaml(const TString& database) {
        return TStringBuilder() << R"(
metadata:
  kind: DatabaseConfig
  database: ")" << database << R"("
  version: 0
config:
  feature_flags: {}
)";
    }

    void AssertAllowed(const auto& op, const TString& what) {
        UNIT_ASSERT_C(op.status() != Ydb::StatusIds::UNAUTHORIZED,
            what << ": expected the authorization gate to pass, but got UNAUTHORIZED, issues# " << op.issues());
    }

    static constexpr const char* NOT_CLUSTER_ADMIN_MSG = "User is not a cluster administrator.";
    static constexpr const char* NOT_DATABASE_ADMIN_MSG = "User is not a database administrator.";

    void AssertDenied(const auto& op, const TString& what, std::optional<TString> expectedMessage = std::nullopt) {
        UNIT_ASSERT_C(op.status() == Ydb::StatusIds::UNAUTHORIZED,
            what << ": expected UNAUTHORIZED, but got " << Ydb::StatusIds::StatusCode_Name(op.status())
                << ", issues# " << op.issues());
        if (expectedMessage) {
            UNIT_ASSERT_C(op.issues_size() > 0, what << ": expected a denial issue, but issues are empty");
            UNIT_ASSERT_STRING_CONTAINS_C(op.issues(0).message(), *expectedMessage, what);
        }
    }

    void CheckClusterConfigAccess(auto& channel, std::optional<TString> token, bool allowed) {
        auto fetch = DoFetchConfig(channel, token, std::nullopt);
        auto replace = DoReplaceConfig(channel, token, std::nullopt, MakeClusterConfigYaml());
        if (allowed) {
            AssertAllowed(fetch.operation(), "FetchConfig");
            AssertAllowed(replace.operation(), "ReplaceConfig");
        } else {
            AssertDenied(fetch.operation(), "FetchConfig", NOT_CLUSTER_ADMIN_MSG);
            AssertDenied(replace.operation(), "ReplaceConfig", NOT_CLUSTER_ADMIN_MSG);
        }
    }

    void CheckDatabaseConfigAccess(auto& channel, std::optional<TString> token, const TString& database, bool allowed,
                                   std::optional<TString> expectedDeniedMessage = std::nullopt) {
        auto fetch = DoFetchConfig(channel, token, database);
        auto replace = DoReplaceConfig(channel, token, std::nullopt, MakeDatabaseConfigYaml(database));
        if (allowed) {
            AssertAllowed(fetch.operation(), "FetchConfig");
            AssertAllowed(replace.operation(), "ReplaceConfig");
        } else {
            AssertDenied(fetch.operation(), "FetchConfig", expectedDeniedMessage);
            AssertDenied(replace.operation(), "ReplaceConfig", expectedDeniedMessage);
        }
    }

    Y_UNIT_TEST(AnonymousClusterConfigAllowedWhenAdminSidsEmpty) {
        TServerWithAuth server;
        // empty AdministrationAllowedSIDs, so there is no admin restriction and everyone is a cluster admin
        ConfigureAuth(server, /*adminSids*/ {}, /*enableDatabaseAdmin*/ false);
        CheckClusterConfigAccess(server.GetChannel(), std::nullopt, /*allowed*/ true);
    }

    Y_UNIT_TEST(UserClusterConfigAllowedWhenAdminSidsEmpty) {
        TServerWithAuth server;
        // empty AdministrationAllowedSIDs, so there is no admin restriction and everyone is a cluster admin
        ConfigureAuth(server, /*adminSids*/ {}, /*enableDatabaseAdmin*/ false);
        CheckClusterConfigAccess(server.GetChannel(), "user@builtin", /*allowed*/ true);
    }

    Y_UNIT_TEST(AnonymousClusterConfigDeniedWhenAdminSidsNotEmpty) {
        TServerWithAuth server;
        ConfigureAuth(server, /*adminSids*/ {"root@builtin"}, /*enableDatabaseAdmin*/ false);
        CheckClusterConfigAccess(server.GetChannel(), std::nullopt, /*allowed*/ false);
    }

    Y_UNIT_TEST(UserClusterConfigDeniedWhenNotInAdminSids) {
        TServerWithAuth server;
        ConfigureAuth(server, /*adminSids*/ {"root@builtin"}, /*enableDatabaseAdmin*/ false);
        CheckClusterConfigAccess(server.GetChannel(), "user@builtin", /*allowed*/ false);
    }

    Y_UNIT_TEST(UserClusterConfigAllowedWhenInAdminSids) {
        TServerWithAuth server;
        ConfigureAuth(server, /*adminSids*/ {"user@builtin"}, /*enableDatabaseAdmin*/ false);
        CheckClusterConfigAccess(server.GetChannel(), "user@builtin", /*allowed*/ true);
    }

    Y_UNIT_TEST(DatabaseConfigDeniedForOwnerWhenDatabaseAdminDisabled) {
        TServerWithAuthDynamicPools server;
        const TString database = CreateDatabaseWithOwner(server, "db_owner_user", "user@builtin");
        ConfigureAuth(server, /*adminSids*/ {"root@builtin"}, /*enableDatabaseAdmin*/ false);
        CheckDatabaseConfigAccess(server.GetChannel(), "user@builtin", database, /*allowed*/ false,
            NOT_CLUSTER_ADMIN_MSG);
    }

    Y_UNIT_TEST(DatabaseConfigAllowedForOwnerWhenDatabaseAdminEnabled) {
        TServerWithAuthDynamicPools server;
        const TString database = CreateDatabaseWithOwner(server, "db_owner_user", "user@builtin");
        ConfigureAuth(server, /*adminSids*/ {"root@builtin"}, /*enableDatabaseAdmin*/ true);
        CheckDatabaseConfigAccess(server.GetChannel(), "user@builtin", database, /*allowed*/ true);
    }

    Y_UNIT_TEST(DatabaseConfigDeniedForNonOwnerWhenDatabaseAdminDisabled) {
        TServerWithAuthDynamicPools server;
        const TString database = CreateDatabaseWithOwner(server, "db_owner_root", "root@builtin");
        ConfigureAuth(server, /*adminSids*/ {"root@builtin"}, /*enableDatabaseAdmin*/ false);
        CheckDatabaseConfigAccess(server.GetChannel(), "user@builtin", database, /*allowed*/ false, NOT_CLUSTER_ADMIN_MSG);
    }

    Y_UNIT_TEST(DatabaseConfigDeniedForNonOwnerWhenDatabaseAdminEnabled) {
        TServerWithAuthDynamicPools server;
        const TString database = CreateDatabaseWithOwner(server, "db_owner_root", "root@builtin");
        ConfigureAuth(server, /*adminSids*/ {"root@builtin"}, /*enableDatabaseAdmin*/ true);
        CheckDatabaseConfigAccess(server.GetChannel(), "user@builtin", database, /*allowed*/ false, NOT_DATABASE_ADMIN_MSG);
    }

    Y_UNIT_TEST(DatabaseConfigAllowedForClusterAdminWhenDatabaseAdminDisabled) {
        TServerWithAuthDynamicPools server;
        const TString database = CreateDatabaseWithOwner(server, "db_owner_root", "root@builtin");
        ConfigureAuth(server, /*adminSids*/ {"user@builtin"}, /*enableDatabaseAdmin*/ false);
        CheckDatabaseConfigAccess(server.GetChannel(), "user@builtin", database, /*allowed*/ true);
    }

    Y_UNIT_TEST(DatabaseConfigAllowedForClusterAdminWhenDatabaseAdminEnabled) {
        TServerWithAuthDynamicPools server;
        const TString database = CreateDatabaseWithOwner(server, "db_owner_root", "root@builtin");
        ConfigureAuth(server, /*adminSids*/ {"user@builtin"}, /*enableDatabaseAdmin*/ true);
        CheckDatabaseConfigAccess(server.GetChannel(), "user@builtin", database, /*allowed*/ true);
    }

}

} // NKikimr::NGRpcService
