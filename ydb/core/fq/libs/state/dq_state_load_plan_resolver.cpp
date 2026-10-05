#include "dq_state_load_plan.h"
#include "dq_state_load_plan_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/fq/libs/checkpointing/events/events.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_checkpoints.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <yql/essentials/public/issue/protos/issue_id.pb.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/threading/future/wait/wait.h>

#include <algorithm>

namespace NFq {

namespace {

class TStateLoadPlanResolverActor final : public NActors::TActorBootstrapped<TStateLoadPlanResolverActor>, public NActors::IActorExceptionHandler {
    struct TEvPrivate {
        enum EEv : ui32 {
            EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvSourcesPrepared = EvBegin,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE));

        struct TEvSourcesPrepared : NActors::TEventLocal<TEvSourcesPrepared, EvSourcesPrepared> {
            explicit TEvSourcesPrepared(NThreading::TFuture<NYql::TIssues> result)
                : Result(std::move(result))
            {}

            NThreading::TFuture<NYql::TIssues> Result;
        };
    };

public:
    TStateLoadPlanResolverActor(NProto::TGraphParams src, NProto::TGraphParams dst, TStateLoadPlanResolverSettings settings, ui64 cookie)
        : Src(std::move(src))
        , Dst(std::move(dst))
        , Settings(std::move(settings))
        , Cookie(cookie)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        const TGraphStateContext context;
        const TGraphStateInfo next(Dst, context);

        // Recovery from explicit output event time

        if (Settings.OutputStartTimeUs) {
            if (!MakeOutputStartTimeReplayPlan(next, *Settings.OutputStartTimeUs, Settings.UseSourceDisposition, Plan, Issues)) {
                Finish(/* success */ false);
                co_return;
            }

            Finish(co_await PrepareSources(/* selected */ nullptr));
            co_return;
        }

        // Try to recalculate state if any of previous or new graph has hopping operators

        const TGraphStateInfo previous(Src, context);
        if (NKikimr::AppData()->FeatureFlags.GetEnableStreamingQueryStateRecompute() && (previous.HasHopping() || next.HasHopping())) {
            std::vector<ui64> taskIds;
            taskIds.reserve(previous.GetStages().size());
            for (const auto& stage : previous.GetStages()) {
                for (const auto* task : stage.Tasks) {
                    taskIds.push_back(task->GetId());
                }
            }

            if (taskIds.empty()) {
                Issues.AddIssue("Previous query has no streaming inputs to restore");
                Finish(/* success */ false);
                co_return;
            }

            std::sort(taskIds.begin(), taskIds.end());

            if (!co_await LoadStates(std::move(taskIds))) {
                Finish(/* success */ false);
                co_return;
            }

            if (MakeHistoryReplayPlan(previous, next, States, Plan, Issues) && co_await PrepareSources(/* selected */ nullptr)) {
                Finish(/* success */ true);
                co_return;
            }

            if (!Settings.Force) {
                Finish(/* success */ false);
                co_return;
            }

            WarnAndContinue("History replay is unavailable, FORCE=true resumes from streaming offsets");
        }

        // Directly transfer state and offsets from old graph to new one

        TSourceRecoverySet sourcesToPrepare;
        if (!MakeContinueFromStreamingOffsetsPlan(previous, next, Settings.Force, Plan, sourcesToPrepare, Issues)) {
            Finish(/* success */ false);
            co_return;
        }

        THashSet<ui64> taskIds;
        for (const auto& [taskId, taskPlan] : Plan) {
            for (const auto& source : taskPlan.GetSources()) {
                if (sourcesToPrepare.contains(std::pair<ui64, ui64>{taskId, source.GetInputIndex()}) && !source.HasState()) {
                    for (const auto& foreign : source.GetForeignTasksSources()) {
                        if (const auto foreignTaskId = foreign.GetTaskId(); !States.contains(foreignTaskId)) {
                            taskIds.insert(foreignTaskId);
                        }
                    }
                }
            }
        }

        if (!co_await LoadStates(std::vector<ui64>(taskIds.begin(), taskIds.end()))) {
            Finish(/* success */ false);
            co_return;
        }

        const bool prepared = co_await PrepareSources(&sourcesToPrepare);
        if (!prepared && Settings.Force) {
            WarnAndContinue("Source recovery preparation failed, FORCE=true continues from streaming offsets");
        }

        Finish(prepared || Settings.Force);
    }

private:
    void Registered(NActors::TActorSystem* system, const NActors::TActorId& owner) final {
        TActorBootstrapped::Registered(system, owner);
        Owner = owner;
    }

    bool OnUnhandledException(const std::exception& e) final {
        Issues.AddIssue(NYql::TIssue(TStringBuilder() << "Cannot prepare source recovery: " << e.what()));
        Finish(/* success */ false);
        return true;
    }

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == NActors::TEvents::TEvPoison::EventType) {
            PassAway();
        }
    }

    NActors::async<bool> LoadStates(std::vector<ui64> taskIds) {
        if (taskIds.empty()) {
            co_return true;
        }

        const auto ev = co_await NActors::ActorRequest<NYql::NDq::TEvDqCompute::TEvGetTaskStateResult>(
            Settings.StorageProxy,
            new NYql::NDq::TEvDqCompute::TEvGetTaskState(Settings.GraphId, taskIds, Settings.Checkpoint, Settings.CoordinatorGeneration)
        );
        const auto& result = *ev->Get();
        YQL_ENSURE(result.Generation == Settings.CoordinatorGeneration
            && result.Checkpoint.GetId() == Settings.Checkpoint.GetId()
            && result.Checkpoint.GetGeneration() == Settings.Checkpoint.GetGeneration(), "Unexpected checkpoint response while preparing recovery");

        if (!result.Issues.Empty()) {
            AddIssueWithSubIssues("Failed to load checkpoint task states", result.Issues);
            co_return false;
        }

        if (result.States.size() != taskIds.size()) {
            Issues.AddIssue("Incomplete checkpoint while preparing source recovery");
            co_return false;
        }

        for (size_t i = 0; i < taskIds.size(); ++i) {
            States.emplace(taskIds[i], std::move(ev->Get()->States[i]));
        }

        co_return true;
    }

    NActors::async<bool> PrepareSources(const TSourceRecoverySet* selected) {
        using namespace NYql::NDqProto::NDqStateLoadPlan;

        THashMap<TString, ICheckpointProviderIntegration::TPtr> integrations;
        integrations.reserve(Settings.ProviderIntegrations.size());
        for (const auto& [_, integration] : Settings.ProviderIntegrations) {
            const auto& name = integration->GetSourceName();
            Y_VALIDATE(integrations.emplace(name, integration).second, "Duplicate source recovery integration: " << name);
        }

        THashMap<std::pair<ui32, ui64>, ICheckpointProviderIntegration::TPrepareSource> stageSources;
        for (const auto& task : Dst.GetTasks()) {
            const auto* taskPlan = Plan.FindPtr(task.GetId());
            if (!taskPlan) {
                continue;
            }

            for (const auto& sourcePlan : taskPlan->GetSources()) {
                if (sourcePlan.GetStateType() != STATE_TYPE_FOREIGN || (selected && !selected->contains(std::pair<ui64, ui64>{task.GetId(), sourcePlan.GetInputIndex()}))) {
                    continue;
                }

                const auto& source = task.GetInputs(sourcePlan.GetInputIndex()).GetSource();

                auto [it, inserted] = stageSources.try_emplace(std::make_pair(task.GetStageId(), sourcePlan.GetInputIndex()));
                auto& request = it->second;
                if (inserted) {
                    request.Source = source;
                    request.SecureParams = {task.GetSecureParams().begin(), task.GetSecureParams().end()};
                    request.RequestContext = {task.GetRequestContext().begin(), task.GetRequestContext().end()};
                } else {
                    Y_VALIDATE(request.Source.SerializeAsString() == source.SerializeAsString(), "Source settings must be equal for stage tasks");
                    Y_VALIDATE((request.SecureParams == THashMap<TString, TString>(task.GetSecureParams().begin(), task.GetSecureParams().end())), "Secure params must be equal for stage tasks");
                    Y_VALIDATE((request.RequestContext == THashMap<TString, TString>(task.GetRequestContext().begin(), task.GetRequestContext().end())), "Request context must be equal for stage tasks");
                }

                auto& prepared = request.Tasks.emplace_back();
                prepared.TaskId = task.GetId();
                prepared.Meta = task.GetMeta();
                prepared.ReadRanges = {task.GetReadRanges().begin(), task.GetReadRanges().end()};
                prepared.State.InputIndex = sourcePlan.GetInputIndex();

                if (sourcePlan.HasState()) {
                    prepared.State.Data.emplace_back(sourcePlan.GetState(), sourcePlan.GetStateVersion());
                } else {
                    for (const auto& foreign : sourcePlan.GetForeignTasksSources()) {
                        const auto* state = States.FindPtr(foreign.GetTaskId());
                        YQL_ENSURE(state, "Missing source checkpoint for task " << foreign.GetTaskId());
                        const auto it = std::find_if(state->Sources.begin(), state->Sources.end(), [&](const auto& item) { return item.InputIndex == foreign.GetInputIndex(); });
                        YQL_ENSURE(it != state->Sources.end(), "Missing source input in checkpoint");
                        prepared.State.Data.insert(prepared.State.Data.end(), it->Data.begin(), it->Data.end());
                    }
                }
            }
        }

        THashMap<TString, TVector<ICheckpointProviderIntegration::TPrepareSource>> requests;
        for (auto& [_, request] : stageSources) {
            requests[request.Source.GetType()].emplace_back(std::move(request));
        }

        TVector<NThreading::TFuture<NYql::TIssues>> futures;
        futures.reserve(requests.size());
        for (auto& [type, sources] : requests) {
            try {
                const auto it = integrations.find(type);
                YQL_ENSURE(it != integrations.end(), "Source recovery preparation is unavailable for " << type);
                futures.emplace_back(it->second->PrepareSourceRecovery(std::move(sources)));
            } catch (const std::exception& e) {
                futures.emplace_back(NThreading::MakeFuture(NYql::TIssues{NYql::TIssue(e.what())}));
            }
        }

        if (futures.empty()) {
            co_return true;
        }

        const auto cookie = NActors::AllocateWaitCookie();
        NThreading::WaitAll(futures).Apply([futures](const auto&) {
            NYql::TIssues issues;
            for (const auto& future : futures) {
                try {
                    issues.AddIssues(future.GetValue());
                } catch (const std::exception& e) {
                    issues.AddIssue(NYql::TIssue(e.what()));
                }
            }

            return issues;
        }).Subscribe([system = ActorContext().ActorSystem(), self = SelfId(), cookie](const auto& result) {
            system->Send(new NActors::IEventHandle(self, {}, new TEvPrivate::TEvSourcesPrepared(result), /* flags */ 0, cookie));
        });

        const auto ev = co_await NActors::ActorWaitForEvent<TEvPrivate::TEvSourcesPrepared>(cookie);
        if (const auto& issues = ev->Get()->Result.GetValue(); !issues.Empty()) {
            AddIssueWithSubIssues("Failed to prepare sources for recovery", issues);
            co_return false;
        }

        co_return true;
    }

    void WarnAndContinue(const TString& message) {
        NYql::TIssue warning(message);
        warning.SetCode(NYql::TIssuesIds::WARNING, NYql::TSeverityIds::S_WARNING);
        const auto demote = [](auto&& self, NYql::TIssue& issue) -> void {
            issue.SetCode(NYql::TIssuesIds::WARNING, NYql::TSeverityIds::S_WARNING);

            for (const auto& child : issue.GetSubIssues()) {
                self(self, *child);
            }
        };

        for (const auto& issue : Issues) {
            auto detail = MakeIntrusive<NYql::TIssue>(issue);
            demote(demote, *detail);
            warning.AddSubIssue(detail);
        }

        Issues.Clear();
        Issues.AddIssue(std::move(warning));
    }

    void Finish(bool success) {
        if (!success) {
            Plan.clear();
        }

        Send(Owner, new TEvCheckpointCoordinator::TEvPrepareStateLoadPlanResult(success, std::move(Plan), std::move(Issues)), /* flags */ 0, Cookie);
        PassAway();
    }

    void AddIssueWithSubIssues(const TString& message, const NYql::TIssues& issues) {
        NYql::TIssue root(message);
        for (const auto& issue : issues) {
            root.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
        }
        Issues.AddIssue(root);
    }

    const NProto::TGraphParams Src;
    const NProto::TGraphParams Dst;
    const TStateLoadPlanResolverSettings Settings;
    const ui64 Cookie = 0;
    NActors::TActorId Owner;
    TCheckpointTaskStates States;
    TStateLoadPlan Plan;
    NYql::TIssues Issues;
};

} // anonymous namespace

NActors::IActor* CreateStateLoadPlanResolver(NProto::TGraphParams src, NProto::TGraphParams dst, TStateLoadPlanResolverSettings settings, ui64 cookie) {
    return new TStateLoadPlanResolverActor(std::move(src), std::move(dst), std::move(settings), cookie);
}

} // namespace NFq
