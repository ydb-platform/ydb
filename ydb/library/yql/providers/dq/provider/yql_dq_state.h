#pragma once

#include "yql_dq_gateway.h"

#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

#include <util/generic/ptr.h>

namespace NYql {

using namespace NDqs; // TODO: remove this namespace;

struct TDqState: public TThrRefBase {
    IDqGateway::TPtr DqGateway;
    const TGatewaysConfig* GatewaysConfig;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    NKikimr::NMiniKQL::TComputationNodeFactory ComputationFactory;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TTypeAnnotationContext* TypeCtx;
    const TOperationProgressWriter ProgressWriter;
    const TYqlOperationOptions OperationOptions;
    const TString SessionId;
    const IMetricsRegistryPtr Metrics;
    const TFileStoragePtr FileStorage;
    const TString VanillaJobPath;
    const TString VanillaJobMd5;
    TString YtToken;
    TDqConfiguration::TPtr Settings = MakeIntrusive<TDqConfiguration>();
    bool ExternalUser;

    TMutex Mutex;
    THashMap<ui32, TOperationStatistics> Statistics;
    std::atomic<ui32> MetricId = 1;

    TDqState(
        const IDqGateway::TPtr& dqGateway,
        const TGatewaysConfig* gatewaysConfig,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
        const TIntrusivePtr<IRandomProvider>& randomProvider,
        TTypeAnnotationContext* typeCtx,
        const TOperationProgressWriter& progressWriter,
        const TYqlOperationOptions& operationOptions,
        const TString& sessionId,
        const IMetricsRegistryPtr& metrics,
        const TFileStoragePtr& fileStorage,
        const TString& vanillaJobPath,
        const TString& vanillaJobMd5,
        bool externalUser)
        : DqGateway(dqGateway)
        , GatewaysConfig(gatewaysConfig)
        , FunctionRegistry(functionRegistry)
        , ComputationFactory(compFactory)
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
    { }
};

using TDqStatePtr = TIntrusivePtr<TDqState>;

} // namespace
