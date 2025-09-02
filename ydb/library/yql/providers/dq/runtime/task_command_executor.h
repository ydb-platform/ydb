#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_stats_registry.h>
#include <yql/essentials/core/dq_integration/transform/yql_dq_task_transform.h>

namespace NYql {
namespace NTaskRunnerProxy {

int CreateTaskCommandExecutor(NKikimr::NMiniKQL::TComputationNodeFactory compFactory, TTaskTransformFactory taskTransformFactory, NKikimr::NMiniKQL::IStatsRegistry* jobStats, bool terminateOnError = false);

} // namespace NTaskRunnerProxy
} // namespace NYql
