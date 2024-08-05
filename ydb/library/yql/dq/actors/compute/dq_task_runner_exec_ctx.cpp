#include "dq_task_runner_exec_ctx.h"

#include <ydb/library/yql/dq/actors/spilling/channel_storage.h>


namespace NYql {
namespace NDq {

TDqTaskRunnerExecutionContext::TDqTaskRunnerExecutionContext(TTxId txId, IDqChannelStorage::TWakeUpCallback&& wakeUpCallback, std::function<void(const TString& error)>&& errorCallback)
    : TxId_(txId)
    , WakeUpCallback_(std::move(wakeUpCallback))
    , ErrorCallback_(std::move(errorCallback))
{
}

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 channelId, bool withSpilling) const {
    return CreateChannelStorage(channelId, withSpilling, NActors::TlsActivationContext->ActorSystem());
}

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 channelId, bool withSpilling, NActors::TActorSystem* actorSystem) const {
    if (withSpilling) {
        return CreateDqChannelStorage(TxId_, channelId, WakeUpCallback_, actorSystem);
    } else {
        return nullptr;
    }
}

std::function<void()> TDqTaskRunnerExecutionContext::GetWakeupCallback() const {
    return WakeUpCallback_;
}

std::function<void(const TString&)> TDqTaskRunnerExecutionContext::GetErrorCallback() const {
    return ErrorCallback_;
}

TTxId TDqTaskRunnerExecutionContext::GetTxId() const {
    return TxId_;
}

} // namespace NDq
} // namespace NYql
