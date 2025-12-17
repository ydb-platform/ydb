#include "dq_task_runner_exec_ctx.h"

#include <ydb/library/yql/dq/actors/spilling/channel_storage.h>


namespace NYql {
namespace NDq {

TDqTaskRunnerExecutionContext::TDqTaskRunnerExecutionContext(TTxId txId, TWakeUpCallback&& wakeUpCallback, TErrorCallback&& errorCallback)
    : TxId_(txId)
    , WakeUpCallback_(std::move(wakeUpCallback))
    , ErrorCallback_(std::move(errorCallback))
    , SpillingTaskCounters_(MakeIntrusive<TSpillingTaskCounters>())
{
}

TWakeUpCallback TDqTaskRunnerExecutionContext::GetWakeupCallback() const {
    return WakeUpCallback_;
}

TErrorCallback TDqTaskRunnerExecutionContext::GetErrorCallback() const {
    return ErrorCallback_;
}

TIntrusivePtr<TSpillingTaskCounters> TDqTaskRunnerExecutionContext::GetSpillingTaskCounters() const {
    return SpillingTaskCounters_;
}

TTxId TDqTaskRunnerExecutionContext::GetTxId() const {
    return TxId_;
}

} // namespace NDq
} // namespace NYql
