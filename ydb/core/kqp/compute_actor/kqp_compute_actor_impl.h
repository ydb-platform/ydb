#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_task_runner_exec_ctx.h>
#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;

class TKqpTaskRunnerExecutionContext : public TDqTaskRunnerExecutionContext {
public:
    TKqpTaskRunnerExecutionContext(ui64 txId, TMaybe<ui8> minFillPercentage, TWakeUpCallback&& wakeUpCallback, TErrorCallback&& errorCallback)
        : TDqTaskRunnerExecutionContext(txId, std::move(wakeUpCallback), std::move(errorCallback))
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

private:
    const TMaybe<ui8> MinFillPercentage_;
};

} // namespace NKqp
} // namespace NKikimr
