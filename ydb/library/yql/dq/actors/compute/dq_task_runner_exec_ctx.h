#pragma once

#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/actors/core/actor.h>

namespace NYql {
namespace NDq {

class TDqTaskRunnerExecutionContext : public TDqTaskRunnerExecutionContextBase {
public:
    TDqTaskRunnerExecutionContext(TTxId txId, TWakeUpCallback&& WakeUpCallback_, TErrorCallback&& ErrorCallback_);

    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling) const override;
    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling, NActors::TActorSystem* actorSystem) const override;

    TWakeUpCallback GetWakeupCallback() const override;
    TErrorCallback GetErrorCallback() const override;
    TTxId GetTxId() const override;

private:
    const TTxId TxId_;
    const TWakeUpCallback WakeUpCallback_;
    const TErrorCallback ErrorCallback_;
};

} // namespace NDq
} // namespace NYql
