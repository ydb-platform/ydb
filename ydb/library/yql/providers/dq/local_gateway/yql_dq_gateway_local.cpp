#include "yql_dq_gateway_local.h"

#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_local.h>

#include <ydb/library/yql/providers/dq/service/interconnect_helpers.h>
#include <ydb/library/yql/providers/dq/service/service_node.h>

#include <ydb/library/yql/providers/dq/stats_collector/pool_stats_collector.h>
#include <ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h>
#include <ydb/library/yql/dq/actors/spilling/spilling_file.h>

#include <ydb/library/yql/utils/range_walker.h>
#include <ydb/library/yql/utils/bind_in_range.h>

#include <library/cpp/messagebus/network.h>

#include <util/system/env.h>
#include <util/generic/size_literals.h>
#include <util/folder/dirut.h>

namespace NYql {

using namespace NActors;
using NDqs::MakeWorkerManagerActorID;

class TLocalServiceHolder {
public:
    TLocalServiceHolder(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
        TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories, NBus::TBindResult interconnectPort, NBus::TBindResult grpcPort,
        NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, int threads,
        IMetricsRegistryPtr metricsRegistry,
        const std::function<IActor*(void)>& metricsPusherFactory,
        bool withSpilling)
        : MetricsRegistry(metricsRegistry
            ? metricsRegistry
            : CreateMetricsRegistry(GetSensorsGroupFor(NSensorComponent::kDq))
        )
    {
        ui32 nodeId = 1;

        TString hostName = "localhost";
        TString localAddress = "::1";

        NDqs::TServiceNodeConfig config = {
            nodeId,
            localAddress,
            hostName,
            static_cast<ui16>(interconnectPort.Addr.GetPort()),
            static_cast<ui16>(grpcPort.Addr.GetPort()),
            0, // mbus
            interconnectPort.Socket.Get()->Release(),
            grpcPort.Socket.Get()->Release(),
            1
        };

        ServiceNode = MakeHolder<TServiceNode>(
            config,
            threads,
            MetricsRegistry);

        auto lwmGroup = MetricsRegistry->GetSensors()->GetSubgroup("component", "lwm");
        auto patternCache = std::make_shared<NKikimr::NMiniKQL::TComputationPatternLRUCache>(NKikimr::NMiniKQL::TComputationPatternLRUCache::Config(200_MB, 200_MB));
        NDqs::TLocalWorkerManagerOptions lwmOptions;
        lwmOptions.Factory = NTaskRunnerProxy::CreateFactory(functionRegistry, compFactory, taskTransformFactory, patternCache, true);
        lwmOptions.AsyncIoFactory = std::move(asyncIoFactory);
        lwmOptions.FunctionRegistry = functionRegistry;
        lwmOptions.TaskRunnerInvokerFactory = new NDqs::TTaskRunnerInvokerFactory();
        lwmOptions.TaskRunnerActorFactory = NDq::NTaskRunnerActor::CreateLocalTaskRunnerActorFactory(
            [factory=lwmOptions.Factory](std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const NDq::TDqTaskSettings& task, NDqProto::EDqStatsMode statsMode, const NDq::TLogFunc& )
                {
                    return factory->Get(alloc, task, statsMode);
                });
        lwmOptions.Counters = NDqs::TWorkerManagerCounters(lwmGroup);
        lwmOptions.DropTaskCountersOnFinish = false;
        auto resman = NDqs::CreateLocalWorkerManager(lwmOptions);

        ServiceNode->AddLocalService(
            MakeWorkerManagerActorID(nodeId),
            TActorSetupCmd(resman, TMailboxType::Simple, 0));

        if (withSpilling) {
            char tempDir[MAX_PATH];
            if (MakeTempDir(tempDir, nullptr) != 0)
                ythrow yexception() << "LocalServiceHolder: Can't create temporary directory " << tempDir;

            auto spillingActor = NDq::CreateDqLocalFileSpillingService(NDq::TFileSpillingServiceConfig{.Root = tempDir, .CleanupOnShutdown = true}, MakeIntrusive<NDq::TSpillingCounters>(lwmGroup));

            ServiceNode->AddLocalService(
                NDq::MakeDqLocalFileSpillingServiceID(nodeId),
                TActorSetupCmd(spillingActor, TMailboxType::Simple, 0));
        }

        auto statsCollector = CreateStatsCollector(1, *ServiceNode->GetSetup(), MetricsRegistry->GetSensors());

        auto actorSystem = ServiceNode->StartActorSystem();
        if (metricsPusherFactory) {
            actorSystem->Register(metricsPusherFactory());
        }

        actorSystem->Register(statsCollector);

        ServiceNode->StartService(dqTaskPreprocessorFactories);
    }

    ~TLocalServiceHolder()
    {
        ServiceNode->Stop();
    }

private:
    IMetricsRegistryPtr MetricsRegistry;
    THolder<TServiceNode> ServiceNode;
};

class TDqGatewayLocalImpl: public std::enable_shared_from_this<TDqGatewayLocalImpl>
{
    struct TRequest {
        TString SessionId;
        NDqs::TPlan Plan;
        TVector<TString> Columns;
        THashMap<TString, TString> SecureParams;
        THashMap<TString, TString> GraphParams;
        TDqSettings::TPtr Settings;
        IDqGateway::TDqProgressWriter ProgressWriter;
        THashMap<TString, TString> ModulesMapping;
        bool Discard;
        NThreading::TPromise<IDqGateway::TResult> Result;
        ui64 ExecutionTimeout;
    };

public:
    TDqGatewayLocalImpl(THolder<TLocalServiceHolder>&& localService, const IDqGateway::TPtr& gateway)
        : LocalService(std::move(localService))
        , Gateway(gateway)
        , DeterministicMode(!!GetEnv("YQL_DETERMINISTIC_MODE"))
    { }

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) {
        return Gateway->OpenSession(sessionId, username);
    }

    NThreading::TFuture<void> CloseSession(const TString& sessionId) {
        return Gateway->CloseSessionAsync(sessionId);
    }

    NThreading::TFuture<IDqGateway::TResult>
    ExecutePlan(const TString& sessionId, NDqs::TPlan&& plan, const TVector<TString>& columns,
                const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& graphParams,
                const TDqSettings::TPtr& settings,
                const IDqGateway::TDqProgressWriter& progressWriter, const THashMap<TString, TString>& modulesMapping,
                bool discard, ui64 executionTimeout)
    {

        NThreading::TFuture<IDqGateway::TResult> result;
        {
            TGuard<TMutex> lock(Mutex);
            Queue.emplace_back(TRequest{sessionId, std::move(plan), columns, secureParams, graphParams, settings, progressWriter, modulesMapping, discard, NThreading::NewPromise<IDqGateway::TResult>(), executionTimeout});
            result = Queue.back().Result;
        }

        TryExecuteNext();

        return result;
    }

    void Stop() {
        Gateway->Stop();
    }

private:
    void TryExecuteNext() {
        TGuard<TMutex> lock(Mutex);
        if (!Queue.empty() && (!DeterministicMode || Inflight == 0)) {
            auto request = std::move(Queue.front()); Queue.pop_front();
            Inflight++;
            lock.Release();

            auto weak = weak_from_this();

            Gateway->ExecutePlan(request.SessionId, std::move(request.Plan), request.Columns, request.SecureParams, request.GraphParams, request.Settings, request.ProgressWriter, request.ModulesMapping, request.Discard, request.ExecutionTimeout)
                .Apply([promise=request.Result, weak](const NThreading::TFuture<IDqGateway::TResult>& result) mutable {
                    try {
                        promise.SetValue(result.GetValue());
                    } catch (...) {
                        promise.SetException(std::current_exception());
                    }

                    if (auto ptr = weak.lock()) {
                        {
                            TGuard<TMutex> lock(ptr->Mutex);
                            ptr->Inflight--;
                        }

                        ptr->TryExecuteNext();
                    }
                });
        }
    }

    THolder<TLocalServiceHolder> LocalService;
    IDqGateway::TPtr Gateway;
    const bool DeterministicMode;
    TMutex Mutex;
    TList<TRequest> Queue;
    int Inflight = 0;
};

class TDqGatewayLocal : public IDqGateway {
public:
    TDqGatewayLocal(THolder<TLocalServiceHolder>&& localService, const IDqGateway::TPtr& gateway)
        : Impl(std::make_shared<TDqGatewayLocalImpl>(std::move(localService), gateway))
    {}

    ~TDqGatewayLocal() {
        Impl->Stop();
    }

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) override {
        return Impl->OpenSession(sessionId, username);
    }

    NThreading::TFuture<void> CloseSessionAsync(const TString& sessionId) override {
        return Impl->CloseSession(sessionId);
    }

    NThreading::TFuture<TResult>
    ExecutePlan(const TString& sessionId, NDqs::TPlan&& plan, const TVector<TString>& columns,
        const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& graphParams,
        const TDqSettings::TPtr& settings,
        const TDqProgressWriter& progressWriter, const THashMap<TString, TString>& modulesMapping,
        bool discard, ui64 executionTimeout) override
    {
        return Impl->ExecutePlan(sessionId, std::move(plan), columns, secureParams, graphParams,
            settings, progressWriter, modulesMapping, discard, executionTimeout);
    }

    void Stop() override {
        Impl->Stop();
    }

private:
    std::shared_ptr<TDqGatewayLocalImpl> Impl;
};

THolder<TLocalServiceHolder> CreateLocalServiceHolder(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories,
    NBus::TBindResult interconnectPort, NBus::TBindResult grpcPort,
    NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, int threads,
    IMetricsRegistryPtr metricsRegistry,
    const std::function<IActor*(void)>& metricsPusherFactory, bool withSpilling)
{
    return MakeHolder<TLocalServiceHolder>(functionRegistry,
        compFactory,
        taskTransformFactory,
        dqTaskPreprocessorFactories,
        interconnectPort,
        grpcPort,
        std::move(asyncIoFactory),
        threads,
        metricsRegistry,
        metricsPusherFactory,
        withSpilling);
}

TIntrusivePtr<IDqGateway> CreateLocalDqGateway(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories,
    bool withSpilling, NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, int threads,
    IMetricsRegistryPtr metricsRegistry,
    const std::function<IActor*(void)>& metricsPusherFactory)
{
    int startPort = 31337;
    TRangeWalker<int> portWalker(startPort, startPort+100);
    auto interconnectPort = BindInRange(portWalker)[1];
    auto grpcPort = BindInRange(portWalker)[1];

    return new TDqGatewayLocal(
        CreateLocalServiceHolder(
            functionRegistry,
            compFactory,
            taskTransformFactory,
            dqTaskPreprocessorFactories,
            interconnectPort,
            grpcPort,
            std::move(asyncIoFactory),
            threads,
            metricsRegistry,
            metricsPusherFactory,
            withSpilling),
        CreateDqGateway("[::1]", grpcPort.Addr.GetPort()));
}

} // namespace NYql
