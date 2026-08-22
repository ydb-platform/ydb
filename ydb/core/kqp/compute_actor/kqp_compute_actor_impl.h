#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_task_runner_exec_ctx.h>

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
        const NMiniKQL::TType* type, NUdf::IApplyContext*, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        TVector<IDqOutput::TPtr>&& outputs, NUdf::IPgBuilder* /* pgBuilder */) const override;

    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling) const override {
        return TDqTaskRunnerExecutionContext::CreateChannelStorage(channelId, WithSpilling_ || withSpilling);
    }

private:
    const bool WithSpilling_;
    const TMaybe<ui8> MinFillPercentage_;
};

} // namespace NKqp
} // namespace NKikimr
