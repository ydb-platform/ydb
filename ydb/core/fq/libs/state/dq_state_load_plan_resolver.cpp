#include "dq_state_load_plan.h"
#include "dq_state_load_plan_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/fq/libs/checkpointing/events/events.h>
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
            EvReplayFailed,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE));

        struct TEvReplayFailed : NActors::TEventLocal<TEvReplayFailed, EvReplayFailed> {};

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

        if (Settings.OutputStartTimeUs) {
            if (MakeOutputStartTimeReplayPlan(Dst, *Settings.OutputStartTimeUs, Settings.UseSourceDisposition, Plan, Issues)) {
                PrepareSources();
            } else {
                Finish(/* success */ false);
            }
            return;
        }

        Fallback = !NKikimr::AppData()->FeatureFlags.GetEnableStreamingQueryStateRecompute();
        if (Fallback) {
            Finish(MakeFallbackPlan());
            return;
        }

        for (const auto& task : Src.GetTasks()) {
            if (NYql::NDq::GetTaskCheckpointingMode(task) != NYql::NDqProto::CHECKPOINTING_MODE_DISABLED) {
                TaskIds.push_back(task.GetId());
            }
        }

        if (TaskIds.empty()) {
            Issues.AddIssue("Previous query has no streaming inputs to restore");
            Finish(/* success */ false);
            return;
        }

        std::sort(TaskIds.begin(), TaskIds.end());
        Send(Settings.StorageProxy, new NYql::NDq::TEvDqCompute::TEvGetTaskState(Settings.GraphId, TaskIds, Settings.Checkpoint, Settings.CoordinatorGeneration));
    }

private:
    void Registered(NActors::TActorSystem* system, const NActors::TActorId& owner) final {
        TActorBootstrapped::Registered(system, owner);
        Owner = owner;
    }

    bool OnUnhandledException(const std::exception& e) final {
        Issues.AddIssue(NYql::TIssue(TStringBuilder() << "Cannot prepare source recovery: " << e.what()));

        if (Fallback || !Settings.Force || Settings.OutputStartTimeUs) {
            Finish(/* success */ false);
        } else {
            Send(SelfId(), new TEvPrivate::TEvReplayFailed());
        }

        return true;
    }

    STRICT_STFUNC(StateWork,
        hFunc(NYql::NDq::TEvDqCompute::TEvGetTaskStateResult, Handle)
        hFunc(TEvPrivate::TEvSourcesPrepared, Handle)
        sFunc(TEvPrivate::TEvReplayFailed, ReplayFailed)
        sFunc(NActors::TEvents::TEvPoison, PassAway)
    )

    void Handle(NYql::NDq::TEvDqCompute::TEvGetTaskStateResult::TPtr& ev) {
        if (!ev->Get()->Issues.Empty()) {
            AddIssueWithSubIssues("Failed to load checkpoint task states", ev->Get()->Issues);
            Finish(/* success */ false);
            return;
        }

        if (ev->Get()->States.size() != TaskIds.size()) {
            Issues.AddIssue("Incomplete checkpoint while preparing source recovery");
            Finish(/* success */ false);
            return;
        }

        for (size_t i = 0; i < TaskIds.size(); ++i) {
            States.emplace(TaskIds[i], std::move(ev->Get()->States[i]));
        }

        if (MakeHistoryReplayPlan(Src, Dst, States, Plan, Issues)) {
            PrepareSources();
        } else {
            ReplayFailed();
        }
    }

    void Handle(TEvPrivate::TEvSourcesPrepared::TPtr& ev) {
        if (const auto& issues = ev->Get()->Result.GetValue(); issues.Empty()) {
            Finish(/* success */ true);
        } else {
            AddIssueWithSubIssues("Failed to prepare sources for recovery", issues);
            ReplayFailed();
        }
    }

    void ReplayFailed() {
        if (Fallback || !Settings.Force || Settings.OutputStartTimeUs) {
            Finish(/* success */ false);
            return;
        }
        Fallback = true;

        NYql::TIssue warning("History replay is unavailable; FORCE=true resumes from streaming offsets");
        warning.SetCode(NYql::TIssuesIds::WARNING, NYql::TSeverityIds::S_WARNING);
        for (const auto& issue : Issues) {
            auto detail = MakeIntrusive<NYql::TIssue>(issue);
            detail->SetCode(NYql::TIssuesIds::WARNING, NYql::TSeverityIds::S_WARNING);
            warning.AddSubIssue(detail);
        }
        Issues.Clear();
        Issues.AddIssue(std::move(warning));

        Finish(MakeFallbackPlan());
    }

    void PrepareSources() {
        using namespace NYql::NDqProto::NDqStateLoadPlan;

        THashMap<TString, ICheckpointProviderIntegration::TPtr> integrations;
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
                if (sourcePlan.GetStateType() != STATE_TYPE_FOREIGN) {
                    continue;
                }

                const auto& source = task.GetInputs(sourcePlan.GetInputIndex()).GetSource();
                YQL_ENSURE(integrations.contains(source.GetType()), "Source recovery preparation is unavailable for " << source.GetType());

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
            futures.emplace_back(integrations.at(type)->PrepareSourceRecovery(std::move(sources)));
        }

        NThreading::WaitAll(futures).Apply([futures](const auto&) {
            NYql::TIssues issues;
            for (const auto& future : futures) {
                issues.AddIssues(future.GetValue());
            }
            return issues;
        }).Subscribe([system = ActorContext().ActorSystem(), self = SelfId()](const auto& result) {
            system->Send(self, new TEvPrivate::TEvSourcesPrepared(result));
        });
    }

    bool MakeFallbackPlan() {
        Plan.clear();
        return MakeContinueFromStreamingOffsetsPlan(Src.GetTasks(), Dst.GetTasks(), Settings.Force, Plan, Issues);
    }

    void Finish(bool success) {
        if (!success) {
            Plan.clear();
        }

        Send(Owner, new TEvCheckpointCoordinator::TEvPrepareStateLoadPlanResult(success, std::move(Plan), std::move(Issues)), 0, Cookie);
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
    const ui64 Cookie;
    NActors::TActorId Owner;
    std::vector<ui64> TaskIds;
    TCheckpointTaskStates States;
    TStateLoadPlan Plan;
    NYql::TIssues Issues;
    bool Fallback = false;
};

} // anonymous namespace

NActors::IActor* CreateStateLoadPlanResolver(NProto::TGraphParams src, NProto::TGraphParams dst, TStateLoadPlanResolverSettings settings, ui64 cookie) {
    return new TStateLoadPlanResolverActor(std::move(src), std::move(dst), std::move(settings), cookie);
}

} // namespace NFq
