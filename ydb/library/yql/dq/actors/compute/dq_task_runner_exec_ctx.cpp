#include "dq_task_runner_exec_ctx.h"

#include <ydb/library/yql/dq/actors/spilling/channel_storage.h>


namespace NYql {
namespace NDq {

TDqTaskRunnerExecutionContext::TDqTaskRunnerExecutionContext(TTxId txId, IDqChannelStorage::TWakeUpCallback&& wakeUp)
    : TxId_(txId)
    , WakeUp_(std::move(wakeUp))
{
}

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 channelId, bool withSpilling) const {
    return CreateChannelStorage(channelId, withSpilling, nullptr, false);
}

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 channelId, bool withSpilling, NActors::TActorSystem* actorSystem, bool isConcurrent) const {
    if (withSpilling) {
        return CreateDqChannelStorage(TxId_, channelId, WakeUp_, actorSystem, isConcurrent);
    } else {
        return nullptr;
    }
}

std::function<void()> TDqTaskRunnerExecutionContext::GetWakeupCallback() const {
    return WakeUp_;
}

} // namespace NDq
} // namespace NYql
