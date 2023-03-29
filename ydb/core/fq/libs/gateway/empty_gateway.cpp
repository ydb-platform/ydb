#include "empty_gateway.h"

#include <ydb/core/fq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/tasks_packer/tasks_packer.h>

#include <library/cpp/actors/core/actor.h>

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

    void CloseSession(const TString& action) override {
        Y_UNUSED(action);
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
        bool discard) override
    {
        Y_UNUSED(progressWriter);
        Y_UNUSED(modulesMapping); // TODO: support.
        Y_UNUSED(discard);

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
