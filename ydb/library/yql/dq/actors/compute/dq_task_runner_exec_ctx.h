#pragma once

#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <library/cpp/actors/core/actor.h>


namespace NYql {
namespace NDq {

class TDqTaskRunnerExecutionContext : public TDqTaskRunnerExecutionContextBase {
public:
    TDqTaskRunnerExecutionContext(TTxId txId, bool withSpilling, IDqChannelStorage::TWakeUpCallback&& wakeUp, const NActors::TActorContext& ctx);

    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId) const override;

private:
    const TTxId TxId_;
    const IDqChannelStorage::TWakeUpCallback WakeUp_;
    const NActors::TActorContext& Ctx_;
    const bool WithSpilling_;
};

} // namespace NDq
} // namespace NYql
