#pragma once

#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <contrib/ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.h>
#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>

namespace NActors {
class IActor;
}

namespace NYql {

TIntrusivePtr<IDqGateway> CreateLocalDqGateway(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories,
    bool withSpilling,
    NDq::IDqAsyncIoFactory::TPtr = nullptr, int threads = 16,
    IMetricsRegistryPtr metricsRegistry = {},
    const std::function<NActors::IActor*(void)>& metricsPusherFactory = {},
    TVector<std::pair<NActors::TActorId, NActors::TActorSetupCmd>>&& additionalLocalServices = {});

} // namespace NYql
