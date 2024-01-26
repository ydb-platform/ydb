#include "dq_compute_actor_impl.h"
#include "dq_compute_actor.h"
#include "dq_task_runner_exec_ctx.h"

#include <ydb/library/yql/dq/common/dq_common.h>

namespace NYql {
namespace NDq {

using namespace NActors;

namespace {
TDqExecutionSettings ExecutionSettings;


} // anonymous namespace

const TDqExecutionSettings& GetDqExecutionSettings() {
    return ExecutionSettings;
}

TDqExecutionSettings& GetDqExecutionSettingsForTests() {
    return ExecutionSettings;
}

} // namespace NDq
} // namespace NYql
