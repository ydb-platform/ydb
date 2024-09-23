#pragma once

#include "yql_dq_gateway.h"

#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

#include <library/cpp/threading/future/async_semaphore.h>
#include <util/generic/ptr.h>

namespace NYql {

using namespace NDqs; // TODO: remove this namespace;

struct TDqState: public TThrRefBase {
    using TPtr = TIntrusivePtr<TDqState>;

    IDqGateway::TPtr DqGateway;
    const TGatewaysConfig* GatewaysConfig;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    NKikimr::NMiniKQL::TComputationNodeFactory ComputationFactory;
    TTaskTransformFactory TransformFactory;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TTypeAnnotationContext* TypeCtx;
    const TOperationProgressWriter ProgressWriter;
    const TYqlOperationOptions OperationOptions;
    const TString SessionId;
    const IMetricsRegistryPtr Metrics;
    const TFileStoragePtr FileStorage;
    const TString VanillaJobPath;
    const TString VanillaJobMd5;
    TDqConfiguration::TPtr Settings = MakeIntrusive<TDqConfiguration>();
    bool ExternalUser;

    TMutex Mutex;
    THashMap<ui32, TOperationStatistics> Statistics;
    std::atomic<ui32> MetricId = 1;

    std::function<void()> AbortHidden = [](){};
    NThreading::TAsyncSemaphore::TPtr OperationSemaphore = nullptr;  // pragmas are not yet parsed, so we initialize it later
    TAdaptiveLock Mutex_;

    TDqState(
        const IDqGateway::TPtr& dqGateway,
        const TGatewaysConfig* gatewaysConfig,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
        TTaskTransformFactory transformFactory,
        const TIntrusivePtr<IRandomProvider>& randomProvider,
        TTypeAnnotationContext* typeCtx,
        const TOperationProgressWriter& progressWriter,
        const TYqlOperationOptions& operationOptions,
        const TString& sessionId,
        const IMetricsRegistryPtr& metrics,
        const TFileStoragePtr& fileStorage,
        const TString& vanillaJobPath,
        const TString& vanillaJobMd5,
        bool externalUser,
        THiddenQueryAborter&& hiddenAborter)
        : DqGateway(dqGateway)
        , GatewaysConfig(gatewaysConfig)
        , FunctionRegistry(functionRegistry)
        , ComputationFactory(compFactory)
        , TransformFactory(transformFactory)
        , RandomProvider(randomProvider)
        , TypeCtx(typeCtx)
        , ProgressWriter(progressWriter)
        , OperationOptions(operationOptions)
        , SessionId(sessionId)
        , Metrics(metrics)
        , FileStorage(fileStorage)
        , VanillaJobPath(vanillaJobPath)
        , VanillaJobMd5(vanillaJobMd5)
        , ExternalUser(externalUser)
        , AbortHidden(std::move(hiddenAborter))
    { }

    NThreading::TFuture<IDqGateway::TResult>
    ExecutePlan(const TString& sessionId, NDqs::TPlan&& plan, const TVector<TString>& columns,
                const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& graphParams,
                const TDqSettings::TPtr& settings,
                const IDqGateway::TDqProgressWriter& progressWriter, const THashMap<TString, TString>& modulesMapping,
                bool discard, ui64 executionTimeout) {
        with_lock(Mutex_) {
            if (!OperationSemaphore) {
                const auto parallelOperationsLimit = Settings->ParallelOperationsLimit.Get().GetOrElse(TDqSettings::TDefault::ParallelOperationsLimit);
                OperationSemaphore = NThreading::TAsyncSemaphore::Make(parallelOperationsLimit);
            }
        }
        return OperationSemaphore->AcquireAsync().Apply([this_=TIntrusivePtr<TDqState>(this), sessionId, plan=std::move(plan), columns, secureParams, graphParams, settings, progressWriter, modulesMapping, discard, executionTimeout](const auto& f) mutable {
            auto lock = f.GetValue()->MakeAutoRelease();
            return this_->DqGateway->ExecutePlan(sessionId, std::move(plan), columns, secureParams, graphParams, settings, progressWriter, modulesMapping, discard, executionTimeout).Apply([unlock = lock.DeferRelease()](const auto& f) {
                unlock(NThreading::MakeFuture());
                return f;
            });
        });
    }
};

using TDqStatePtr = TIntrusivePtr<TDqState>;

} // namespace
