#pragma once
#include "config.h"
#include "factories.h"
#include "service_initializer.h"

#include <ydb/core/memory_controller/memory_controller.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log_settings.h>
#include <ydb/library/actors/interconnect/poller/poller_tcp.h>
#include <ydb/library/actors/util/should_continue.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/scheme_types/scheme_types.h>
#include <ydb/core/formats/factory.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/client/server/grpc_server.h>
#include <ydb/core/fq/libs/shared_resources/interface/shared_resources.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/tablet/node_tablet_monitor.h>
#include <ydb/core/tablet/tablet_setup.h>
#include <ydb/core/ymq/http/http.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {

using TGRpcServers = TVector<std::pair<TString, TAutoPtr<NYdbGrpc::TGRpcServer>>>;
using TGRpcServersFactory = std::function<TGRpcServers()>;

struct TGRpcServersWrapper {
    TGRpcServers Servers;
    TGRpcServersFactory GrpcServersFactory;
    TMutex Mutex;
    std::atomic<bool> IsDisabled = false;

    TGuard<TMutex> Guard() {
        return TGuard<TMutex>(Mutex);
    }
};

class TKikimrRunner : public virtual TThrRefBase, private IGlobalObjectStorage {
protected:
    static TProgramShouldContinue KikimrShouldContinue;
    static void OnTerminate(int);

    std::shared_ptr<TModuleFactories> ModuleFactories;
    TIntrusivePtr<NScheme::TTypeRegistry> TypeRegistry;
    TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<TFormatFactory> FormatFactory;
    NFq::IYqSharedResources::TPtr YqSharedResources;

    TAutoPtr<TMon> Monitoring;
    ::NMonitoring::TDynamicCounterPtr Counters;
    NMonitoring::TIndexMonPage *ActorsMonPage = nullptr;

    bool EnabledGrpcService = false;
    bool GracefulShutdownSupported = false;
    TDuration MinDelayBeforeShutdown;
    TDuration DrainTimeout;
    TDuration CheckForStopInterval;
    THolder<NSQS::TAsyncHttpServer> SqsHttp;

    THolder<NYdb::TDriver> YdbDriver;

    std::vector<std::shared_ptr<void>> GlobalObjects;

    THolder<NKqp::TKqpShutdownController> KqpShutdownController;

    TIntrusivePtr<NInterconnect::TPollerThreads> PollerThreads;
    TAutoPtr<TAppData> AppData;

    TIntrusivePtr<NActors::NLog::TSettings> LogSettings;
    std::shared_ptr<TLogBackend> LogBackend;
    TAutoPtr<TActorSystem> ActorSystem;

    TIntrusivePtr<NMemory::IProcessMemoryInfoProvider> ProcessMemoryInfoProvider;

    TVector<NYdb::NGlobalPlugins::IPlugin::TPtr> Plugins;

    TKikimrRunner(std::shared_ptr<TModuleFactories> factories = {});

    std::shared_ptr<TGRpcServersWrapper> GRpcServersWrapper;
    TActorId GRpcServersManager;
    TDuration GRpcWarmupTimeout;

    virtual ~TKikimrRunner();

    virtual void InitializeRegistries(const TKikimrRunConfig& runConfig);

    /**
     * Initializes the XDS bootstrap configuration for the runner.
     * This method should be called during the startup sequence, before any components
     * that depend on XDS configuration are initialized. It reads the relevant settings
     * from the provided runConfig and sets up the XDS bootstrap environment accordingly.
     * This is necessary for enabling gRPC XDS features such as dynamic service discovery
     * and load balancing.
     */
    void InitializeXdsBootstrapConfig(const TKikimrRunConfig& runConfig);

    void InitializeAllocator(const TKikimrRunConfig& runConfig);

    void InitializeLogSettings(const TKikimrRunConfig& runConfig);

    void ApplyLogSettings(const TKikimrRunConfig& runConfig);

    void InitializeMonitoring(const TKikimrRunConfig& runConfig, bool includeHostName = true);

    void InitializeControlBoard(const TKikimrRunConfig& runConfig);

    void InitializeMonitoringLogin(const TKikimrRunConfig& runConfig);

    void InitializeGRpc(const TKikimrRunConfig& runConfig);
    TGRpcServers CreateGRpcServers(const TKikimrRunConfig& runConfig);

    void InitializeKqpController(const TKikimrRunConfig& runConfig);

    void InitializeGracefulShutdown(const TKikimrRunConfig& runConfig);

    void InitializeAppData(const TKikimrRunConfig& runConfig);

    void InitializePlugins(const TKikimrRunConfig& runConfig);

    void InitializeActorSystem(
        const TKikimrRunConfig& runConfig,
        TIntrusivePtr<TServiceInitializersList> serviceInitializers,
        const TBasicKikimrServicesMask& serviceMask = {});

    TIntrusivePtr<TServiceInitializersList> CreateServiceInitializersList(
        const TKikimrRunConfig& runConfig,
        const TBasicKikimrServicesMask& serviceMask = {});

private:
    void AddGlobalObject(std::shared_ptr<void> object) override;

public:
    static void SetSignalHandlers();

    virtual void KikimrStart();
    virtual void BusyLoop();
    virtual void KikimrStop(bool graceful);

    static TIntrusivePtr<TKikimrRunner> CreateKikimrRunner(
            const TKikimrRunConfig& runConfig,
            std::shared_ptr<TModuleFactories> factories);
};

int MainRun(const TKikimrRunConfig &runConfig, std::shared_ptr<TModuleFactories> factories);

}
