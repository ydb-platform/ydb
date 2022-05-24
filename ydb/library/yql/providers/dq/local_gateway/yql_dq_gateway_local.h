#pragma once

#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NYql {

TIntrusivePtr<IDqGateway> CreateLocalDqGateway(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories,
    NDq::IDqSourceFactory::TPtr = nullptr, NDq::IDqSinkFactory::TPtr = nullptr, NDq::IDqOutputTransformFactory::TPtr = nullptr);

} // namespace NYql
