#pragma once

#include "kqp_compute_actor.h"

#include <ydb/core/kqp/runtime/kqp_channel_storage.h>
#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>


namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;

class TKqpTaskRunnerExecutionContext : public IDqTaskRunnerExecutionContext {
public:
    TKqpTaskRunnerExecutionContext(ui64 txId, bool withSpilling, IDqChannelStorage::TWakeUpCallback&& wakeUp,
        const TActorContext& ctx)
        : TxId(txId)
        , WakeUp(std::move(wakeUp))
        , Ctx(ctx)
        , WithSpilling(withSpilling) {}

    IDqOutputConsumer::TPtr CreateOutputConsumer(const NDqProto::TTaskOutput& outputDesc,
        const NMiniKQL::TType* type, NUdf::IApplyContext* applyCtx, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        TVector<IDqOutput::TPtr>&& outputs) const override
    {
        return KqpBuildOutputConsumer(outputDesc, type, applyCtx, typeEnv, holderFactory, std::move(outputs));
    }

    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId) const override {
        if (WithSpilling) {
            return CreateKqpChannelStorage(TxId, channelId, WakeUp, Ctx);
        } else {
            return nullptr;
        }
    }

private:
    const ui64 TxId;
    const IDqChannelStorage::TWakeUpCallback WakeUp;
    const TActorContext& Ctx;
    const bool WithSpilling;
};

} // namespace NKqp
} // namespace NKikimr
