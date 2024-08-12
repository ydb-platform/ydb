#include "empty_gateway.h"

#include <ydb/core/fq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/tasks_packer/tasks_packer.h>

#include <ydb/library/actors/core/actor.h>

namespace NFq {

class TEmptyGateway : public NYql::IDqGateway {
public:
    explicit TEmptyGateway(NActors::TActorId runActorId) : RunActorId(runActorId) {
    }

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) override {
        Y_UNUSED(sessionId);
        Y_UNUSED(username);
        auto result = NThreading::NewPromise<void>();
        result.SetValue();
        return result;
    }

    NThreading::TFuture<void> CloseSessionAsync(const TString& action) override {
        Y_UNUSED(action);
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<TResult> ExecutePlan(
        const TString& sessionId,
        NYql::NDqs::TPlan&& plan,
        const TVector<TString>& columns,
        const THashMap<TString, TString>& secureParams,
        const THashMap<TString, TString>& queryParams,
        const NYql::TDqSettings::TPtr& settings,
        const TDqProgressWriter& progressWriter,
        const THashMap<TString, TString>& modulesMapping,
        bool discard,
        ui64 executionTimeout) override
    {
        Y_UNUSED(progressWriter);
        Y_UNUSED(modulesMapping); // TODO: support.
        Y_UNUSED(discard);
        Y_UNUSED(executionTimeout);

        NProto::TGraphParams params;
        THashMap<i64, TString> stagePrograms;
        NTasksPacker::Pack(plan.Tasks, stagePrograms);
        for (auto&& task : plan.Tasks) {
            *params.AddTasks() = std::move(task);
        }
        for (const auto& [id, program]: stagePrograms) {
            (*params.MutableStageProgram())[id] = program;
        }
        for (auto&& col : columns) {
            *params.AddColumns() = col;
        }
        for (auto&& [k, v] : secureParams) {
            (*params.MutableSecureParams())[k] = v;
        }
        settings->Save(params);
        if (plan.SourceID) {
            params.SetSourceId(plan.SourceID.NodeId() - 1);
            params.SetResultType(plan.ResultType);
        }
        params.SetSession(sessionId);

        auto result = NThreading::NewPromise<NYql::IDqGateway::TResult>();
        auto event = MakeHolder<TEvents::TEvGraphParams>(params);
        event->IsEvaluation = FromString<bool>(queryParams.Value("Evaluation", "false")) || FromString<bool>(queryParams.Value("Precompute", "false"));

        // result promise should be resolved here
        // in the same thread which execute yql facade functions
        // to avoid data races for TExprNode
        if (!event->IsEvaluation) {
            NYql::IDqGateway::TResult gatewayResult;
            // fake it till you make it
            // generate dummy result for YQL facade now, remove this gateway completely
            // when top-level YQL facade call like Preprocess() is implemented
            if (params.GetResultType()) {
                // for resultable graphs return dummy "select 1" result (it is not used and is required to satisfy YQL facade only)
                gatewayResult.SetSuccess();
                gatewayResult.Data = "[[\001\0021]]";
                gatewayResult.Truncated = true;
                gatewayResult.RowsCount = 0;
            } else {
                // for resultless results expect infinite INSERT FROM SELECT and just return "nothing"
            }
            result.SetValue(gatewayResult);
        }

        event->Result = result;
        NActors::TActivationContext::Send(new NActors::IEventHandle(RunActorId, {}, event.Release()));

        return result;
    }

private:
    NActors::TActorId RunActorId;
};

TIntrusivePtr<NYql::IDqGateway> CreateEmptyGateway(NActors::TActorId runActorId) {
    return MakeIntrusive<TEmptyGateway>(runActorId);
}

} // namespace NFq
