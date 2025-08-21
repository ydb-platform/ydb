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
    TKqpTaskRunnerExecutionContext(ui64 txId, bool withSpilling, TMaybe<ui8> minFillPercentage, TWakeUpCallback&& wakeUpCallback, TErrorCallback&& errorCallback)
        : TDqTaskRunnerExecutionContext(txId, std::move(wakeUpCallback), std::move(errorCallback))
        , WithSpilling_(withSpilling)
        , MinFillPercentage_(minFillPercentage)
    {
    }

    IDqOutputConsumer::TPtr CreateOutputConsumer(const NDqProto::TTaskOutput& outputDesc,
        const NMiniKQL::TType* type, NUdf::IApplyContext* applyCtx, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        TVector<IDqOutput::TPtr>&& outputs, NUdf::IPgBuilder* /* pgBuilder */) const override
    {
        return KqpBuildOutputConsumer(outputDesc, type, applyCtx, typeEnv, holderFactory, std::move(outputs), MinFillPercentage_);
    }

    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling) const override {
        return TDqTaskRunnerExecutionContext::CreateChannelStorage(channelId, WithSpilling_ || withSpilling);
    }

    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling, NActors::TActorSystem* actorSystem) const override {
        return TDqTaskRunnerExecutionContext::CreateChannelStorage(channelId, WithSpilling_ || withSpilling, actorSystem);
    }

    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling, std::shared_ptr<IDqSpiller> spiller) const override {
        return TDqTaskRunnerExecutionContext::CreateChannelStorage(channelId, WithSpilling_ || withSpilling, spiller);
    }

    std::shared_ptr<IDqSpiller> GetSpiller() const override {
        return TDqTaskRunnerExecutionContext::GetSpiller();
    }

private:
    const bool WithSpilling_;
    const TMaybe<ui8> MinFillPercentage_;
};

} // namespace NKqp
} // namespace NKikimr
