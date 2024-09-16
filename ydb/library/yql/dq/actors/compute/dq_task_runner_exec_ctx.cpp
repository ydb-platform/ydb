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

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 channelId, bool withSpilling) const {
    return CreateChannelStorage(channelId, withSpilling, NActors::TlsActivationContext->ActorSystem());
}

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 channelId, bool withSpilling,  NActors::TActorSystem* actorSystem) const {
    if (withSpilling) {
        return CreateDqChannelStorage(TxId_, channelId, WakeUpCallback_, ErrorCallback_, SpillingTaskCounters_, actorSystem);
    } else {
        return nullptr;
    }
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
