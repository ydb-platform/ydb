#pragma once

#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/actors/core/actor.h>

namespace NYql {
namespace NDq {

class TDqTaskRunnerExecutionContext : public TDqTaskRunnerExecutionContextBase {
public:
    TDqTaskRunnerExecutionContext(TTxId txId, IDqChannelStorage::TWakeUpCallback&& WakeUpCallback_, std::function<void(const TString& error)>&& ErrorCallback_);

    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling) const override;
    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling, NActors::TActorSystem* actorSystem) const override;

    std::function<void()> GetWakeupCallback() const override;
    std::function<void(const TString& error)> GetErrorCallback() const override;
    TTxId GetTxId() const override;

private:
    const TTxId TxId_;
    const IDqChannelStorage::TWakeUpCallback WakeUpCallback_;
    const std::function<void(const TString& error)> ErrorCallback_;
};

} // namespace NDq
} // namespace NYql
