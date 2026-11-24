#pragma once

#include "actor_helpers.h"
#include "events.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/dq/actors/task_runner/task_runner_actor.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>

namespace NYql {
    struct TWorkerRuntimeData;
}

namespace NYql::NDqs {

    NActors::IActor* CreateWorkerActor(
        TWorkerRuntimeData* runtimeData,
        const TString& traceId,
        const NDq::NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
        const NDq::IDqAsyncIoFactory::TPtr& asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry);

} // namespace NYql::NDqs
