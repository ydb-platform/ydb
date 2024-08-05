#pragma once

#include "kqp_compute_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_task_runner_exec_ctx.h>
#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>


namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;

class TKqpTaskRunnerExecutionContext : public TDqTaskRunnerExecutionContext {
public:
    TKqpTaskRunnerExecutionContext(ui64 txId, bool withSpilling, IDqChannelStorage::TWakeUpCallback&& wakeUpCallback, std::function<void(const TString& error)>&& errorCallback)
        : TDqTaskRunnerExecutionContext(txId, std::move(wakeUpCallback), std::move(errorCallback))
        , WithSpilling_(withSpilling)
    {
    }

    IDqOutputConsumer::TPtr CreateOutputConsumer(const NDqProto::TTaskOutput& outputDesc,
        const NMiniKQL::TType* type, NUdf::IApplyContext* applyCtx, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        TVector<IDqOutput::TPtr>&& outputs) const override
    {
        return KqpBuildOutputConsumer(outputDesc, type, applyCtx, typeEnv, holderFactory, std::move(outputs));
    }

    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling) const override {
        return TDqTaskRunnerExecutionContext::CreateChannelStorage(channelId, WithSpilling_ || withSpilling);
    }

private:
    bool WithSpilling_;
};

} // namespace NKqp
} // namespace NKikimr
