#include "dq_task_runner_exec_ctx.h"

#include <ydb/library/yql/dq/actors/spilling/channel_storage.h>


namespace NYql {
namespace NDq {

TDqTaskRunnerExecutionContext::TDqTaskRunnerExecutionContext(TTxId txId, bool withSpilling, IDqChannelStorage::TWakeUpCallback&& wakeUp)
    : TxId_(txId)
    , WakeUp_(std::move(wakeUp))
    , WithSpilling_(withSpilling)
{
}

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 channelId) const {
    return CreateChannelStorage(channelId, nullptr, false);
}

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 channelId, NActors::TActorSystem* actorSystem, bool isConcurrent) const {
    if (WithSpilling_) {
        return CreateDqChannelStorage(TxId_, channelId, WakeUp_, actorSystem, isConcurrent);
    } else {
        return nullptr;
    }
}

} // namespace NDq
} // namespace NYql
