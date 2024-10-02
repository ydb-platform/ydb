#pragma once

#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>
#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

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
    NFq::NConfig::TConfig fqConfig = NFq::NConfig::TConfig{},
    const NYql::IPqGateway::TPtr& pqGateway = {});

} // namespace NYql
