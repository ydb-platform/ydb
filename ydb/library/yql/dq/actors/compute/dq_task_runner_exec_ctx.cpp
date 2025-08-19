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
    if (withSpilling) {
        if (sharedSpiller) {
            // Use new shared spiller approach
            return CreateDqChannelStorageWithSharedSpiller(channelId, sharedSpiller, SpillingTaskCounters_);
        } else {
            // Fallback to old approach when no shared spiller provided
            return CreateDqChannelStorage(TxId_, channelId, WakeUpCallback_, ErrorCallback_, SpillingTaskCounters_, actorSystem);
        }
    } else {
        return nullptr;
    }
}

} // namespace NDq
} // namespace NYql
