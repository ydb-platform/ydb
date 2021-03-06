#pragma once

#include "tasks_runner_proxy.h"

#include <ydb/library/yql/providers/dq/interface/yql_dq_task_transform.h>

namespace NYql::NTaskRunnerProxy {

IProxyFactory::TPtr CreateFactory(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory, TTaskTransformFactory taskTransformFactory,
    bool terminateOnError
);

} // namespace NYql::NTaskRunnerProxy
