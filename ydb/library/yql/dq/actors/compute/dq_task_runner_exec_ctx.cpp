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
    return CreateChannelStorage(channelId, withSpilling, nullptr, NActors::TlsActivationContext->ActorSystem());
}

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 channelId, bool withSpilling, NActors::TActorSystem* actorSystem) const {
    return CreateChannelStorage(channelId, withSpilling, nullptr, actorSystem);
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

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 channelId, bool withSpilling, NKikimr::NMiniKQL::ISpiller::TPtr sharedSpiller) const {
    return CreateChannelStorage(channelId, withSpilling, sharedSpiller, NActors::TlsActivationContext->ActorSystem());
}

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 channelId, bool withSpilling, NKikimr::NMiniKQL::ISpiller::TPtr sharedSpiller, NActors::TActorSystem* actorSystem) const {
    Y_UNUSED(actorSystem);
    if (withSpilling) {
        if (!sharedSpiller) {
            Y_ABORT("SharedSpiller is required for channel storage with spilling enabled");
        }
        return CreateDqChannelStorageWithSharedSpiller(channelId, sharedSpiller, SpillingTaskCounters_);
    } else {
        return nullptr;
    }
}

} // namespace NDq
} // namespace NYql
