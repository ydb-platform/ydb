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

#include <atomic>
#include <mutex>
#include <thread>
#include <library/cpp/threading/future/future.h>
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

namespace {

    // Run blocking calls on a separate thread while the test thread dispatches the simulated runtime.
    class TAsyncCall {
    public:
        explicit TAsyncCall(std::function<void()> func)
            : Promise(NThreading::NewPromise<void>())
            , Thread([promise = Promise, func = std::move(func)]() mutable {
                try {
                    func();
                    promise.SetValue();
                } catch (...) {
                    promise.SetException(std::current_exception());
                }
            })
        {}

        ~TAsyncCall() {
            if (Thread.joinable()) {
                Thread.join();
            }
        }

        // dispatches events until the call is done, and rethrows its failure
        void Wait(NActors::TTestActorRuntime& runtime) {
            runtime.WaitFuture(Promise.GetFuture());
        }

    private:
        NThreading::TPromise<void> Promise;
        std::thread Thread;
    };

    void Dispatched(NActors::TTestActorRuntime& runtime, std::function<void()> func) {
        TAsyncCall(std::move(func)).Wait(runtime);
    }

} // anonymous namespace

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
    // useRealThreads=false makes the runtime simulate the actor system, which observers of events need
    TBasicKikimrWithGrpcAndRootSchema(
            NKikimrConfig::TAppConfig appConfig = {},
            TAutoPtr<TLogBackend> logBackend = {},
            bool useRealThreads = true)
    {
        ui16 port = PortManager.GetPort(2134);
        ui16 grpc = PortManager.GetPort(2135);
        ServerSettings = new NKikimr::Tests::TServerSettings(port);
        ServerSettings->SetGrpcPort(grpc);
        ServerSettings->SetLogBackend(logBackend);
        ServerSettings->SetDomainName("Root");
        ServerSettings->SetUseRealThreads(useRealThreads);
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
        if (useRealThreads) {
            annoyingClient.InitRootScheme("Root");
        } else {
            // the client waits for the replies, which come only while the runtime dispatches events
            Dispatched(*Server_->GetRuntime(), [&] {
                annoyingClient.InitRootScheme("Root");
            });
        }
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

    void EnableConfigV2(TKikimrWithGrpcAndRootSchema* server) {
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
        stub = Ydb::DynamicConfig::V1::DynamicConfigService::NewStub(server->GetChannel());

        Ydb::DynamicConfig::ReplaceConfigRequest request;
        request.set_config(enableV2Config);

        Ydb::DynamicConfig::ReplaceConfigResponse response;
        grpc::ClientContext context;
        AdjustCtxForDB(context);

        stub->ReplaceConfig(&context, request, &response);
        UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);

        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        while (!server->GetRuntime()->GetAppData(0).FeatureFlags.GetSwitchToConfigV2()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline,
                "SwitchToConfigV2 was not applied on the gRPC node");
            Sleep(TDuration::MilliSeconds(10));
        }
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
        EnableConfigV2(&server);
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
        EnableConfigV2(&server);
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

        EnableConfigV2(&server);

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
        EnableConfigV2(&server);
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
        EnableConfigV2(&server);

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

// replacements of the configuration overlapping with each other and with the replies of Console
Y_UNIT_TEST_SUITE(ConfigGRPCServiceReplaceRaces) {
    using namespace NTestSuiteConfigGRPCService;

    // BSC parses the main YAML with the hosts of the cluster, so each YAML of these tests has the host of the test node.
    // Its drive keeps the type of the static PDisk, ROT: a new type changes the device type of the static group without
    // a new generation of the group, and BSC sends the group to the cache of distconf anew when it restarts.
    TString HostsYaml(TTestActorRuntime& runtime) {
        return Sprintf(R"(  host_configs:
  - host_config_id: 1
    drive:
    - path: %s
      type: ROT
  hosts:
  - host: ::1
    port: 12001
    host_config_id: 1
)", (runtime.GetTempDir() + "pdisk_1.dat").c_str());
    }

    // dispatches events until the condition holds
    void WaitFor(TTestActorRuntime& runtime, std::function<bool()> condition) {
        TDispatchOptions options;
        options.CustomFinalCondition = std::move(condition);
        UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Minutes(10)));
    }

    Ydb::Config::ReplaceConfigResponse Replace(TTestActorRuntime& runtime, TKikimrWithGrpcAndRootSchema& server,
            const TString& mainConfig, bool dryRun = false) {
        Ydb::Config::ReplaceConfigResponse response;
        Dispatched(runtime, [&] {
            ReplaceConfig(server.GetChannel(), mainConfig, std::nullopt, std::nullopt, false, dryRun,
                [&](const auto& resp) { response = resp; });
        });
        return response;
    }

    // a replacement whose validation or application is held: it is answered when the test lets it go
    std::unique_ptr<TAsyncCall> ReplaceAsync(TKikimrWithGrpcAndRootSchema& server, const TString& mainConfig,
            Ydb::Config::ReplaceConfigResponse& response) {
        return std::make_unique<TAsyncCall>([&server, &response, mainConfig] {
            ReplaceConfig(server.GetChannel(), mainConfig, std::nullopt, std::nullopt, false, false,
                [&](const auto& resp) { response = resp; });
        });
    }

    Ydb::Config::FetchConfigResponse Fetch(TTestActorRuntime& runtime, TKikimrWithGrpcAndRootSchema& server) {
        Ydb::Config::FetchConfigResponse response;
        Dispatched(runtime, [&] {
            auto stub = Ydb::Config::V1::ConfigService::NewStub(server.GetChannel());
            Ydb::Config::FetchConfigRequest request;
            request.mutable_all();
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            stub->FetchConfig(&ctx, request, &response);
        });
        return response;
    }

    std::optional<TString> FetchMainConfig(TTestActorRuntime& runtime, TKikimrWithGrpcAndRootSchema& server) {
        const auto response = Fetch(runtime, server);
        UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
        Ydb::Config::FetchConfigResult result;
        response.operation().result().UnpackTo(&result);
        for (const auto& entry : result.config()) {
            if (entry.identity().type_case() == Ydb::Config::ConfigIdentity::TypeCase::kMain) {
                return entry.config();
            }
        }
        return std::nullopt;
    }

    // the settings of the static PDisk of the test node can't change, so a change of the storage settings is a drive more
    TString ExtraDrives(ui32 count) {
        TStringBuilder drives;
        for (ui32 i = 1; i <= count; ++i) {
            drives << "    - path: SectorMap:" << i << ":64\n      type: SSD\n";
        }
        return drives;
    }

    // the node ids start from 1 again, so that the host of the YAML is the node with the PDisk of the test
    struct TFirstNodeIdReset {
        TFirstNodeIdReset() {
            NKikimr::TTestActorRuntimeBase::ResetFirstNodeId();
        }
    };

    // the server of these tests: the runtime simulates the actor system, and the gRPC node has configuration v2 on;
    // BSC turns it on with the first replacement
    struct TRaceServer : TFirstNodeIdReset, TKikimrWithGrpcAndRootSchema {
        TRaceServer()
            : TKikimrWithGrpcAndRootSchema({}, {}, false)
        {
            auto& runtime = *GetRuntime();
            runtime.SetScheduledLimit(10'000'000); // the timers of the whole server over minutes of simulated time
            Ydb::DynamicConfig::ReplaceConfigResponse response;
            Dispatched(runtime, [&] {
                auto stub = Ydb::DynamicConfig::V1::DynamicConfigService::NewStub(GetChannel());
                Ydb::DynamicConfig::ReplaceConfigRequest request;
                request.set_config(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  feature_flags:
    switch_to_config_v2: true
)");
                grpc::ClientContext context;
                AdjustCtxForDB(context);
                stub->ReplaceConfig(&context, request, &response);
            });
            UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
            WaitFor(runtime, [&] { return runtime.GetAppData(0).FeatureFlags.GetSwitchToConfigV2(); });
        }
    };

    // The reply of Console validating the main YAML for BSC is held until the replacement times out. It says then that
    // the YAML is not valid; BSC has no request for it any more and must drop the reply, then serve the next request.
    Y_UNIT_TEST(LateConsoleValidationReplyIsDropped) {
        TRaceServer server;
        auto& runtime = *server.GetRuntime();
        const TString yamlConfig = R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 1
config:
  log_config:
    cluster_name: late-validation
)" + HostsYaml(runtime);
        TAutoPtr<IEventHandle> heldReply;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerValidateConfigResponse && !heldReply) {
                heldReply = ev;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        const auto timedOut = Replace(runtime, server, yamlConfig);
        runtime.SetObserverFunc(prevObserver);
        UNIT_ASSERT_C(timedOut.operation().status() != Ydb::StatusIds::SUCCESS, timedOut.operation().DebugString());
        UNIT_ASSERT(heldReply);
        heldReply->Get<TEvBlobStorage::TEvControllerValidateConfigResponse>()->Record.SetStatus(
            NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigNotValid);
        runtime.Send(heldReply.Release());

        // the replacement timed out with nothing applied, so the same YAML is checked again
        const auto checked = Replace(runtime, server, yamlConfig, /*dryRun=*/ true);
        UNIT_ASSERT_CHECK_STATUS(checked.operation(), Ydb::StatusIds::SUCCESS);
    }

    // Replacement A is validated and done; the timer of its validation stays scheduled. Replacement B comes a minute
    // later, and the reply of Console validating it is held. The timer of A fires in between; B must still get its
    // validation timeout instead of waiting forever and keeping later replacements out.
    Y_UNIT_TEST(ValidationTimeoutSurvivesTimerOfFinishedReplacement) {
        TRaceServer server;
        auto& runtime = *server.GetRuntime();
        auto makeYaml = [&](ui32 version) {
            return Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: %u
config:
  log_config:
    cluster_name: %s-%u
)", version, "timer", version) + HostsYaml(runtime);
        };
        const auto first = Replace(runtime, server, makeYaml(1));
        UNIT_ASSERT_CHECK_STATUS(first.operation(), Ydb::StatusIds::SUCCESS);
        runtime.SimulateSleep(TDuration::Minutes(1));

        bool holdReplies = true;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            return holdReplies && ev->GetTypeRewrite() == TEvBlobStorage::EvControllerValidateConfigResponse
                ? TTestActorRuntime::EEventAction::DROP
                : TTestActorRuntime::EEventAction::PROCESS;
        });
        grpc::Status status;
        Ydb::Config::ReplaceConfigResponse response;
        Dispatched(runtime, [&] {
            std::unique_ptr<Ydb::Config::V1::ConfigService::Stub> stub = Ydb::Config::V1::ConfigService::NewStub(
                server.GetChannel());
            Ydb::Config::ReplaceConfigRequest request;
            request.set_replace(makeYaml(2));
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::minutes(4));
            status = stub->ReplaceConfig(&ctx, request, &response);
        });
        holdReplies = false;
        runtime.SetObserverFunc(prevObserver);
        UNIT_ASSERT_C(status.ok(), "no reply to the replacement whose validation is held: " << status.error_message());
        UNIT_ASSERT_C(response.operation().status() != Ydb::StatusIds::SUCCESS, response.operation().DebugString());
    }

    // Replacement A is done, and replacement B comes right after it, while the timer of A is still scheduled; the reply
    // of Console validating B is held. The timer of A comes late, after the deadline of B has passed: B must get its
    // validation timeout then instead of waiting forever.
    Y_UNIT_TEST(ValidationTimeoutAfterLateTimerOfFinishedReplacement) {
        TRaceServer server;
        auto& runtime = *server.GetRuntime();
        auto makeYaml = [&](ui32 version) {
            return Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: %u
config:
  log_config:
    cluster_name: %s-%u
)", version, "late-timer", version) + HostsYaml(runtime);
        };

        // BSC gets the validation of A from Console; its timer of the validation is the private event 0 without a
        // body coming to it
        TActorId controller;
        bool holdReplies = false;
        bool holdTimer = false;
        TAutoPtr<IEventHandle> heldTimer;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerValidateConfigResponse) {
                controller = ev->GetRecipientRewrite();
                if (holdReplies) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            if (holdTimer && !heldTimer && controller && ev->GetRecipientRewrite() == controller
                    && ev->GetTypeRewrite() == 0 && !ev->HasEvent() && !ev->HasBuffer()) {
                heldTimer = ev;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        const auto first = Replace(runtime, server, makeYaml(1));
        UNIT_ASSERT_CHECK_STATUS(first.operation(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(controller);

        holdReplies = true;
        holdTimer = true;
        grpc::Status status;
        Ydb::Config::ReplaceConfigResponse response;
        TAsyncCall replace([&] {
            std::unique_ptr<Ydb::Config::V1::ConfigService::Stub> stub = Ydb::Config::V1::ConfigService::NewStub(
                server.GetChannel());
            Ydb::Config::ReplaceConfigRequest request;
            request.set_replace(makeYaml(2));
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::minutes(5));
            status = stub->ReplaceConfig(&ctx, request, &response);
        });
        // the timer of A fires two minutes after A, and B, started right after A, has about the same deadline
        WaitFor(runtime, [&] { return bool(heldTimer); });
        runtime.SimulateSleep(TDuration::Seconds(30));
        runtime.Send(heldTimer.Release());
        replace.Wait(runtime);
        holdReplies = false;
        runtime.SetObserverFunc(prevObserver);
        UNIT_ASSERT_C(status.ok(), "no reply to the replacement whose validation is held: " << status.error_message());
        UNIT_ASSERT_C(response.operation().status() != Ydb::StatusIds::SUCCESS, response.operation().DebugString());
    }

    // Replacement A waits for the reply of Console validating it, and replacement B comes then. B is refused as there
    // is a replacement going on; A must go on as it was and apply its YAML.
    Y_UNIT_TEST(RefusedConcurrentReplacementKeepsOngoingOne) {
        TRaceServer server;
        auto& runtime = *server.GetRuntime();
        auto makeYaml = [&](ui32 version, const char *name) {
            return Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: %u
config:
  log_config:
    cluster_name: %s
)", version, name) + HostsYaml(runtime);
        };

        TAutoPtr<IEventHandle> heldReply;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerValidateConfigResponse && !heldReply) {
                heldReply = ev;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Ydb::Config::ReplaceConfigResponse responseA;
        auto replaceA = ReplaceAsync(server, makeYaml(1, "replacement-a"), responseA);
        WaitFor(runtime, [&] { return bool(heldReply); });

        const auto refused = Replace(runtime, server, makeYaml(1, "replacement-b"));
        UNIT_ASSERT_C(refused.operation().status() != Ydb::StatusIds::SUCCESS, refused.operation().DebugString());

        runtime.SetObserverFunc(prevObserver);
        runtime.Send(heldReply.Release());
        replaceA->Wait(runtime);
        UNIT_ASSERT_CHECK_STATUS(responseA.operation(), Ydb::StatusIds::SUCCESS);

        const auto mainConfig = FetchMainConfig(runtime, server);
        UNIT_ASSERT(mainConfig);
        UNIT_ASSERT_C(mainConfig->Contains("replacement-a"), *mainConfig);
    }

    // With the dedicated storage YAML, the storage YAML alone is replaced: there is no main YAML for Console to
    // validate. The replacement must still go on in BSC and apply, and the next one must not be refused as ongoing.
    Y_UNIT_TEST(StorageOnlyReplacementApplies) {
        TRaceServer server;
        auto& runtime = *server.GetRuntime();
        const TString pdiskPath = runtime.GetTempDir() + "pdisk_1.dat";
        auto makeStorageYaml = [&](ui32 version, ui32 extraDrives) {
            return Sprintf(R"(
metadata:
  kind: StorageConfig
  cluster: ""
  version: %u
config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: %s
      type: ROT
%s  hosts:
  - host: ::1
    port: 12001
    host_config_id: 1
)", version, pdiskPath.c_str(), ExtraDrives(extraDrives).c_str());
        };
        auto makeMainYaml = [&](ui32 version, const char *name) {
            return Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: %u
config:
  log_config:
    cluster_name: %s
)", version, name) + HostsYaml(runtime);
        };
        auto replace = [&](std::optional<TString> mainConfig, std::optional<TString> storageConfig,
                std::optional<bool> switchDedicatedStorageSection, bool dedicatedConfigMode) {
            Ydb::Config::ReplaceConfigResponse response;
            Dispatched(runtime, [&] {
                ReplaceConfig(server.GetChannel(), mainConfig, storageConfig, switchDedicatedStorageSection,
                    dedicatedConfigMode, false, [&](const auto& resp) { response = resp; });
            });
            UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
        };
        auto fetchMain = [&] {
            const auto edge = runtime.AllocateEdgeActor();
            auto request = std::make_unique<TEvBlobStorage::TEvControllerFetchConfigRequest>();
            request->Record.SetDedicatedClusterSection(true);
            runtime.SendToPipe(MakeBSControllerID(), edge, request.release());
            const auto response = runtime.GrabEdgeEvent<TEvBlobStorage::TEvControllerFetchConfigResponse>(edge);
            const auto& record = response->Get()->Record;
            UNIT_ASSERT_C(!record.HasErrorReason(), record.GetErrorReason());
            UNIT_ASSERT(record.HasClusterYaml());
            return record.GetClusterYaml();
        };
        // BSC turns configuration v2 on with the first replacement, which can't switch the dedicated storage mode too
        replace(makeMainYaml(1, "single"), std::nullopt, std::nullopt, false);
        replace(makeMainYaml(2, "storage-only"), makeStorageYaml(0, 0),
            /*switchDedicatedStorageSection=*/ true, false);
        const auto mainBefore = fetchMain();
        for (ui32 version : {1, 2}) {
            replace(std::nullopt, makeStorageYaml(version, version), std::nullopt, /*dedicatedConfigMode=*/ true);
            UNIT_ASSERT_VALUES_EQUAL(fetchMain(), mainBefore);
        }
        // version 2 is accepted only if version 1 has applied and the replacement isn't seen as ongoing any more

        TActorId controller;
        bool reconnected = false;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerValidateConfigResponse) {
                controller = ev->GetRecipientRewrite();
            }
            if (controller && ev->GetRecipientRewrite() == controller
                    && ev->GetTypeRewrite() == TEvBlobStorage::EvControllerProposeConfigResponse) {
                const auto status = ev->Get<TEvBlobStorage::TEvControllerProposeConfigResponse>()->Record.GetStatus();
                UNIT_ASSERT_C(status == NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNotNeeded,
                    NKikimrBlobStorage::TEvControllerProposeConfigResponse::EStatus_Name(status));
                reconnected = true;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        // Storage-only commits must leave Console expecting main version 3, just as BSC does.
        replace(makeMainYaml(3, "main-after-storage"), std::nullopt, std::nullopt, /*dedicatedConfigMode=*/ true);
        UNIT_ASSERT(controller);
        const auto mainAfter = fetchMain();
        UNIT_ASSERT_C(mainAfter.Contains("main-after-storage"), mainAfter);

        // BSC schedules GetBlock before it reconnects; the simulated runtime otherwise drops this event.
        runtime.SetScheduledEventFilter([](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& ev, TDuration delay,
                TInstant& deadline) {
            return ev->GetTypeRewrite() != TEvBlobStorage::EvGetBlock
                && TTestActorRuntime::DefaultScheduledFilterFunc(r, ev, delay, deadline);
        });
        RebootTablet(runtime, MakeConsoleID(), runtime.AllocateEdgeActor());
        WaitFor(runtime, [&] { return reconnected; });
        runtime.SetObserverFunc(prevObserver);
        replace(makeMainYaml(4, "main-after-reconnect"), std::nullopt, std::nullopt, /*dedicatedConfigMode=*/ true);
        const auto mainAfterReconnect = fetchMain();
        UNIT_ASSERT_C(mainAfterReconnect.Contains("main-after-reconnect"), mainAfterReconnect);
    }

    // The new YAML puts into the box a host that is in its node list, but not in the node list BSC has now. BSC checks
    // the box against the node list of the new YAML, and then applies it against its own list. It must not go down
    // there; the YAML with the known host only is the control and applies.
    Y_UNIT_TEST(BoxHostUnknownToControllerIsNotApplied) {
        NKikimr::TTestActorRuntimeBase::ResetFirstNodeId();
        TKikimrWithGrpcAndRootSchema server;
        EnableConfigV2(&server);
        const TString pdiskPath = server.GetRuntime()->GetTempDir() + "pdisk_1.dat";
        auto makeYaml = [&](ui32 version, const char *extraHost) {
            return Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: %u
config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: %s
      type: ROT
  hosts:
  - host: ::1
    port: 12001
    host_config_id: 1
%s)", version, pdiskPath.c_str(), extraHost);
        };
        ReplaceConfig(server.GetChannel(), makeYaml(1, R"(  - host: ::1
    port: 12099
    host_config_id: 1
)"), std::nullopt, std::nullopt, false, false, [](const auto& resp) {
            UNIT_ASSERT_C(resp.operation().status() != Ydb::StatusIds::SUCCESS, resp.operation().DebugString());
        });

        // nothing was persisted: the same version applies with the known host only
        ReplaceConfig(server.GetChannel(), makeYaml(1, ""), std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
            });
    }

    // The dedicated storage YAML is turned off, and the single YAML coming instead of it has the storage settings now.
    // BSC must check them as in the single-config mode: the host config listing one drive twice is refused, the same
    // YAML without it applies.
    Y_UNIT_TEST(DisablingDedicatedStorageChecksStorageOfMainYaml) {
        NKikimr::TTestActorRuntimeBase::ResetFirstNodeId();
        TKikimrWithGrpcAndRootSchema server;
        EnableConfigV2(&server);
        const TString pdiskPath = server.GetRuntime()->GetTempDir() + "pdisk_1.dat";
        const TString storageYaml = Sprintf(R"(
metadata:
  kind: StorageConfig
  cluster: ""
  version: 0
config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: %s
      type: SSD
  hosts:
  - host: ::1
    port: 12001
    host_config_id: 1
)", pdiskPath.c_str());
        auto makeMainYaml = [&](ui32 version, const char *name) {
            return Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: %u
config:
  log_config:
    cluster_name: %s
)", version, name) + HostsYaml(*server.GetRuntime());
        };
        auto success = [](const auto& resp) {
            UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
        };
        // BSC turns configuration v2 on with the first replacement, which can't switch the dedicated storage mode too
        ReplaceConfig(server.GetChannel(), makeMainYaml(1, "single"), std::nullopt, std::nullopt, false, false, success);
        ReplaceConfig(server.GetChannel(), makeMainYaml(2, "dedicated"), storageYaml,
            /*switchDedicatedStorageSection=*/ true, false, false, success);

        auto makeSingleYaml = [&](const TString& drives) {
            return Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 3
config:
  log_config:
    cluster_name: single
  host_configs:
  - host_config_id: 1
    drive:
%s  hosts:
  - host: ::1
    port: 12001
    host_config_id: 1
)", drives.c_str());
        };
        const TString drive = Sprintf("    - path: %s\n      type: SSD\n", pdiskPath.c_str());
        ReplaceConfig(server.GetChannel(), makeSingleYaml(drive + drive), std::nullopt,
            /*switchDedicatedStorageSection=*/ false, false, false, [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::INTERNAL_ERROR);
                const TString opDebugString = resp.operation().DebugString();
                UNIT_ASSERT_C(opDebugString.Contains("duplicate path"), opDebugString);
            });

        ReplaceConfig(server.GetChannel(), makeSingleYaml(drive), std::nullopt,
            /*switchDedicatedStorageSection=*/ false, false, false, success);
        std::optional<TString> mainConfig, storageConfig;
        FetchConfig(server.GetChannel(), false, false, mainConfig, storageConfig);
        UNIT_ASSERT(mainConfig);
        UNIT_ASSERT(!storageConfig);
        UNIT_ASSERT_C(mainConfig->Contains("cluster_name: single"), *mainConfig);
    }

    // An unchanged main YAML still becomes the source of storage settings when the dedicated section is turned off.
    // Check the host config in BSC itself: dropping the dedicated YAML alone does not apply the extra drive from main.
    Y_UNIT_TEST(DisablingDedicatedStorageAppliesUnchangedMainYaml) {
        NKikimr::TTestActorRuntimeBase::ResetFirstNodeId();
        TKikimrWithGrpcAndRootSchema server;
        EnableConfigV2(&server);
        auto& runtime = *server.GetRuntime();
        const TString pdiskPath = runtime.GetTempDir() + "pdisk_1.dat";
        auto makeYaml = [&](const char *kind, ui32 version, ui32 extraDrives) {
            return Sprintf(R"(
metadata:
  kind: %s
  cluster: ""
  version: %u
config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: %s
      type: ROT
%s  hosts:
  - host: ::1
    port: 12001
    host_config_id: 1
)", kind, version, pdiskPath.c_str(), ExtraDrives(extraDrives).c_str());
        };
        auto success = [](const auto& resp) {
            UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
        };
        auto checkExtraDrive = [&](bool expected) {
            const TActorId sender = runtime.AllocateEdgeActor();
            auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
            request->Record.MutableRequest()->AddCommand()->MutableReadHostConfig()->AddHostConfigId(1);
            runtime.SendToPipe(MakeBSControllerID(), sender, request.Release());
            const auto event = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(sender);
            const auto& response = event->Get()->Record.GetResponse();
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 1);
            const auto& status = response.GetStatus(0);
            UNIT_ASSERT_C(status.GetSuccess(), status.GetErrorDescription());
            UNIT_ASSERT_VALUES_EQUAL(status.HostConfigSize(), 1);
            const auto& hostConfig = status.GetHostConfig(0);
            UNIT_ASSERT_VALUES_EQUAL(hostConfig.DriveSize(), expected ? 2 : 1);
            bool found = false;
            for (const auto& drive : hostConfig.GetDrive()) {
                found |= drive.GetPath() == "SectorMap:1:64";
            }
            UNIT_ASSERT_VALUES_EQUAL(found, expected);
        };

        ReplaceConfig(server.GetChannel(), makeYaml("MainConfig", 1, 0), std::nullopt, std::nullopt,
            false, false, success);
        const TString mainYaml = makeYaml("MainConfig", 2, 1);
        ReplaceConfig(server.GetChannel(), mainYaml, makeYaml("StorageConfig", 0, 0),
            /*switchDedicatedStorageSection=*/ true, false, false, success);
        checkExtraDrive(false);

        ReplaceConfig(server.GetChannel(), mainYaml, std::nullopt,
            /*switchDedicatedStorageSection=*/ false, false, /*dryRun=*/ true, success);
        checkExtraDrive(false);
        ReplaceConfig(server.GetChannel(), mainYaml, std::nullopt,
            /*switchDedicatedStorageSection=*/ false, false, false, success);
        checkExtraDrive(true);

        std::optional<TString> mainConfig, storageConfig;
        FetchConfig(server.GetChannel(), false, false, mainConfig, storageConfig);
        UNIT_ASSERT(mainConfig);
        UNIT_ASSERT_VALUES_EQUAL(*mainConfig, mainYaml);
        UNIT_ASSERT(!storageConfig);
    }
    // The validation of the replacement passes, but the config command applying its storage settings after the commit
    // fails. The replacement must not be reported as a success; the next one, applying fine, is.
    Y_UNIT_TEST(FailedStorageApplicationIsNotSuccess) {
        TRaceServer server;
        auto& runtime = *server.GetRuntime();
        const TString pdiskPath = runtime.GetTempDir() + "pdisk_1.dat";
        auto makeYaml = [&](ui32 version, ui32 extraDrives) {
            return Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: %u
config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: %s
      type: ROT
%s  hosts:
  - host: ::1
    port: 12001
    host_config_id: 1
)", version, pdiskPath.c_str(), ExtraDrives(extraDrives).c_str());
        };

        // BSC sends the config commands to itself; the one without rollback is the application, which fails here
        ui32 numFailed = 0;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerConfigResponse && ev->Sender == ev->Recipient) {
                auto *response = ev->Get<TEvBlobStorage::TEvControllerConfigResponse>()->Record.MutableResponse();
                if (response->GetSuccess() && !response->GetRollbackSuccess()) {
                    response->SetSuccess(false);
                    response->SetErrorDescription("application failed by the test");
                    ++numFailed;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        const auto failed = Replace(runtime, server, makeYaml(1, 1));
        runtime.SetObserverFunc(prevObserver);
        UNIT_ASSERT_C(failed.operation().status() != Ydb::StatusIds::SUCCESS, failed.operation().DebugString());
        UNIT_ASSERT(numFailed);

        const auto applied = Replace(runtime, server, makeYaml(2, 2));
        UNIT_ASSERT_CHECK_STATUS(applied.operation(), Ydb::StatusIds::SUCCESS);
    }

    // The application of replacement A is held after Console commits it, and Console restarts, ending its session.
    // A's application then comes back either successfully before B's validation, or failed after B's commit.
    // B must get the result of its own application and persist its own YAML in either order.
    void CheckApplicationOfEarlierCommitIsNotTakenForLaterOne(bool beforeValidation) {
        TRaceServer server;
        auto& runtime = *server.GetRuntime();
        const TString pdiskPath = runtime.GetTempDir() + "pdisk_1.dat";
        auto makeYaml = [&](ui32 version, ui32 extraDrives) {
            return Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: %u
config:
  log_config:
    cluster_name: replacement-%u
  host_configs:
  - host_config_id: 1
    drive:
    - path: %s
      type: ROT
%s  hosts:
  - host: ::1
    port: 12001
    host_config_id: 1
)", version, version, pdiskPath.c_str(), ExtraDrives(extraDrives).c_str());
        };

        // BSC sends the config commands to itself; the one without rollback is the application
        std::vector<TAutoPtr<IEventHandle>> held;
        bool hold = true;
        bool holdValidation = false;
        TAutoPtr<IEventHandle> heldValidation;
        TActorId controller;
        ui32 consoleCommits = 0;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerValidateConfigResponse) {
                controller = ev->GetRecipientRewrite();
                if (holdValidation && !heldValidation) {
                    heldValidation = ev;
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerConsoleCommitResponse
                    && ev->GetRecipientRewrite() == controller
                    && ev->Get<TEvBlobStorage::TEvControllerConsoleCommitResponse>()->Record.GetStatus()
                        == NKikimrBlobStorage::TEvControllerConsoleCommitResponse::Committed) {
                ++consoleCommits;
            }
            if (hold && ev->GetTypeRewrite() == TEvBlobStorage::EvControllerConfigResponse && ev->Sender == ev->Recipient) {
                const auto& response = ev->Get<TEvBlobStorage::TEvControllerConfigResponse>()->Record.GetResponse();
                if (response.GetSuccess() && !response.GetRollbackSuccess()) {
                    held.push_back(ev);
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // BSC asks blob storage for its block before it connects to Console again, with a scheduled event, which the
        // runtime drops by default
        runtime.SetScheduledEventFilter([](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& ev, TDuration delay,
                TInstant& deadline) {
            return ev->GetTypeRewrite() != TEvBlobStorage::EvGetBlock
                && TTestActorRuntime::DefaultScheduledFilterFunc(r, ev, delay, deadline);
        });

        Ydb::Config::ReplaceConfigResponse responseA, responseB;
        auto a = ReplaceAsync(server, makeYaml(1, 1), responseA);
        WaitFor(runtime, [&] { return held.size() == 1 && consoleCommits; });
        const ui64 applicationCookieA = held[0]->Cookie;
        RebootTablet(runtime, MakeConsoleID(), runtime.AllocateEdgeActor()); // the session of A ends
        a->Wait(runtime);
        UNIT_ASSERT_C(responseA.operation().status() != Ydb::StatusIds::SUCCESS, responseA.operation().DebugString());

        // B is refused until the session of BSC with Console comes back, so it is sent again then
        holdValidation = true;
        std::unique_ptr<TAsyncCall> b;
        std::atomic<bool> answered = false;
        for (ui32 attempt = 0; !heldValidation; ++attempt) {
            UNIT_ASSERT_C(attempt < 30, "the validation of B was not held: " << responseB.operation().DebugString());
            answered = false;
            b = std::make_unique<TAsyncCall>([&] {
                ReplaceConfig(server.GetChannel(), makeYaml(2, 2), std::nullopt, std::nullopt, false, false,
                    [&](const auto& resp) { responseB = resp; });
                answered = true;
            });
            WaitFor(runtime, [&] { return bool(heldValidation) || answered; });
            if (!heldValidation) {
                b->Wait(runtime);
                runtime.SimulateSleep(TDuration::Seconds(1));
            }
        }
        const ui32 commitsBeforeB = consoleCommits;
        if (beforeValidation) {
            // A's successful application must not acknowledge B while its YAML has not even been validated yet
            runtime.Send(held[0].Release());
        }
        holdValidation = false;
        runtime.Send(heldValidation.Release());
        WaitFor(runtime, [&] {
            return answered || (held.size() == 2 && (beforeValidation || consoleCommits > commitsBeforeB));
        });

        const bool ownApplicationHeld = held.size() == 2;
        const bool ownApplicationCookie = ownApplicationHeld && held[1]->Cookie != applicationCookieA;
        hold = false;
        if (!beforeValidation) {
            auto *failed = held[0]->Get<TEvBlobStorage::TEvControllerConfigResponse>()->Record.MutableResponse();
            failed->SetSuccess(false);
            failed->SetErrorDescription("application of A failed by the test");
            runtime.Send(held[0].Release());
        }
        if (ownApplicationHeld) {
            runtime.Send(held[1].Release());
        }
        b->Wait(runtime);
        runtime.SetObserverFunc(prevObserver);
        UNIT_ASSERT_C(ownApplicationHeld, "B replied without its own application: " << responseB.operation().DebugString());
        UNIT_ASSERT_C(ownApplicationCookie, "B reused the application cookie of A");
        UNIT_ASSERT_C(responseB.operation().status() == Ydb::StatusIds::SUCCESS,
            "B got the result of the application of A: " << responseB.operation().DebugString());
        const auto mainConfig = FetchMainConfig(runtime, server);
        UNIT_ASSERT(mainConfig);
        UNIT_ASSERT_C(mainConfig->Contains("replacement-2"), *mainConfig);
    }

    Y_UNIT_TEST(ApplicationOfEarlierCommitIsNotTakenForLaterOne) {
        CheckApplicationOfEarlierCommitIsNotTakenForLaterOne(/*beforeValidation=*/ false);
    }

    Y_UNIT_TEST(ApplicationOfEarlierCommitBeforeValidationIsNotTakenForLaterOne) {
        CheckApplicationOfEarlierCommitIsNotTakenForLaterOne(/*beforeValidation=*/ true);
    }

    // The node serving the request asks to switch back to configuration v1, and the YAML of the replacement is the
    // stored one. Nothing is to be written but the mode, and the mode must be switched: the next replacement through
    // configuration v2 is refused then.
    Y_UNIT_TEST(SwitchToV1WithUnchangedYaml) {
        NKikimr::TTestActorRuntimeBase::ResetFirstNodeId();
        TKikimrWithGrpcAndRootSchema server;
        EnableConfigV2(&server);
        auto *runtime = server.GetRuntime();
        auto makeYaml = [&](ui32 version) {
            return Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: %u
config:
  feature_flags:
    switch_to_config_v1: true
    switch_to_config_v2: false
)", version) + HostsYaml(*runtime);
        };
        ReplaceConfig(server.GetChannel(), makeYaml(1), std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
            });
        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        while (!runtime->GetAppData(0).FeatureFlags.GetSwitchToConfigV1()
                || runtime->GetAppData(0).FeatureFlags.GetSwitchToConfigV2()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "SwitchToConfigV1 was not applied on the gRPC node");
            Sleep(TDuration::MilliSeconds(10));
        }

        // the same YAML once more, now with the request to switch to configuration v1
        ReplaceConfig(server.GetChannel(), makeYaml(1), std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
            });

        ReplaceConfig(server.GetChannel(), makeYaml(2), std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_C(resp.operation().status() != Ydb::StatusIds::SUCCESS, resp.operation().DebugString());
            });
    }

    void CheckUnchangedYamlDoesNotEnableV2WithChangedConsoleConfig(bool reuseConsoleVersion) {
        NKikimr::TTestActorRuntimeBase::ResetFirstNodeId();
        TKikimrWithGrpcAndRootSchema server;
        EnableConfigV2(&server);
        auto *runtime = server.GetRuntime();
        auto makeYaml = [&](ui32 version, bool enableV2) {
            return Sprintf(R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: %u
config:
  feature_flags:
    switch_to_config_v1: %s
    switch_to_config_v2: %s
)", version, enableV2 ? "false" : "true", enableV2 ? "true" : "false") + HostsYaml(*runtime);
        };
        const TString oldYaml = makeYaml(1, false);
        ReplaceConfig(server.GetChannel(), oldYaml, std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
            });
        auto waitForMode = [&](bool enableV2) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            while (runtime->GetAppData(0).FeatureFlags.GetSwitchToConfigV2() != enableV2
                    || runtime->GetAppData(0).FeatureFlags.GetSwitchToConfigV1() == enableV2) {
                UNIT_ASSERT_C(TInstant::Now() < deadline, "Configuration mode was not applied on the gRPC node");
                Sleep(TDuration::MilliSeconds(10));
            }
        };
        waitForMode(false);

        // BSC retains oldYaml when the unchanged replacement only switches its mode to v1.
        ReplaceConfig(server.GetChannel(), oldYaml, std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
            });

        auto stub = Ydb::DynamicConfig::V1::DynamicConfigService::NewStub(server.GetChannel());
        auto replaceInV1 = [&](ui32 version, bool enableV2) {
            Ydb::DynamicConfig::ReplaceConfigRequest request;
            request.set_config(makeYaml(version, enableV2));
            Ydb::DynamicConfig::ReplaceConfigResponse response;
            grpc::ClientContext context;
            AdjustCtxForDB(context);
            stub->ReplaceConfig(&context, request, &response);
            UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
        };
        ui32 expectedVersion = 3;
        if (reuseConsoleVersion) {
            // Public DropConfig resets Console's version; recreating it can reuse BSC's retained version.
            Ydb::DynamicConfig::DropConfigRequest request;
            request.mutable_identity()->set_cluster("");
            request.mutable_identity()->set_version(2);
            Ydb::DynamicConfig::DropConfigResponse response;
            grpc::ClientContext context;
            AdjustCtxForDB(context);
            stub->DropConfig(&context, request, &response);
            UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
            replaceInV1(0, false);
            waitForMode(false);
            expectedVersion = 2;
        }
        // Console changes independently in v1 and asks the gRPC node to enable v2 again.
        replaceInV1(expectedVersion - 1, true);
        waitForMode(true);

        // Equality with BSC's retained YAML must not bypass the version returned by Console.
        ReplaceConfig(server.GetChannel(), oldYaml, std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_C(resp.operation().status() != Ydb::StatusIds::SUCCESS, resp.operation().DebugString());
                UNIT_ASSERT_C(resp.operation().issues_size(), resp.operation().DebugString());
                UNIT_ASSERT_STRING_CONTAINS(resp.operation().issues(0).message(), "cluster YAML config version mismatch");
            });

        ReplaceConfig(server.GetChannel(), makeYaml(expectedVersion, true), std::nullopt, std::nullopt, false, false,
            [](const auto& resp) {
                UNIT_ASSERT_CHECK_STATUS(resp.operation(), Ydb::StatusIds::SUCCESS);
            });
        std::optional<TString> mainConfig, storageConfig;
        FetchConfig(server.GetChannel(), false, false, mainConfig, storageConfig);
        UNIT_ASSERT(mainConfig);
        UNIT_ASSERT(!storageConfig);
        UNIT_ASSERT_VALUES_EQUAL(NormalizeYaml(*mainConfig), NormalizeYaml(makeYaml(expectedVersion, true)));
    }

    Y_UNIT_TEST(UnchangedYamlDoesNotEnableV2WithNewerConsoleConfig) {
        CheckUnchangedYamlDoesNotEnableV2WithChangedConsoleConfig(/*reuseConsoleVersion=*/ false);
    }

    Y_UNIT_TEST(UnchangedYamlDoesNotEnableV2WithRecreatedConsoleConfig) {
        CheckUnchangedYamlDoesNotEnableV2WithChangedConsoleConfig(/*reuseConsoleVersion=*/ true);
    }

    // The YAML is right but its metadata is not: a wrong version and a wrong cluster. Without bypass_checks the
    // replacement is refused; with it the metadata is replaced with the right one and the YAML applies.
    Y_UNIT_TEST(BypassChecksReplacesWrongMetadata) {
        NKikimr::TTestActorRuntimeBase::ResetFirstNodeId();
        TKikimrWithGrpcAndRootSchema server;
        EnableConfigV2(&server);
        const TString yaml = R"(
metadata:
  kind: MainConfig
  cluster: "some-other-cluster"
  version: 7
config:
  log_config:
    cluster_name: bypass-checks
)" + HostsYaml(*server.GetRuntime());
        auto replace = [&](bool bypassChecks) {
            auto stub = Ydb::Config::V1::ConfigService::NewStub(server.GetChannel());
            Ydb::Config::ReplaceConfigRequest request;
            request.set_replace(yaml);
            request.set_bypass_checks(bypassChecks);
            Ydb::Config::ReplaceConfigResponse response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            stub->ReplaceConfig(&ctx, request, &response);
            Cerr << "response: " << response.operation().DebugString() << Endl;
            return response.operation();
        };

        const auto refused = replace(false);
        UNIT_ASSERT_C(refused.status() != Ydb::StatusIds::SUCCESS, refused.DebugString());

        const auto applied = replace(true);
        UNIT_ASSERT_CHECK_STATUS(applied, Ydb::StatusIds::SUCCESS);
        std::optional<TString> mainConfig, storageConfig;
        FetchConfig(server.GetChannel(), false, false, mainConfig, storageConfig);
        UNIT_ASSERT(mainConfig);
        UNIT_ASSERT_C(mainConfig->Contains("cluster_name: bypass-checks"), *mainConfig);
        UNIT_ASSERT_C(!mainConfig->Contains("some-other-cluster"), *mainConfig);
    }

}

} // NKikimr::NGRpcService
