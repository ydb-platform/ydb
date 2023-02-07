#pragma once

#include "tasks_runner_proxy.h"

#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>

namespace NYql::NTaskRunnerProxy {

IProxyFactory::TPtr CreateFactory(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory, TTaskTransformFactory taskTransformFactory,
    bool terminateOnError
);

} // namespace NYql::NTaskRunnerProxy
