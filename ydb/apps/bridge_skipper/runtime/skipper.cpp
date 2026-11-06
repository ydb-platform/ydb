#include "skipper.h"
#include "events.h"

#include <ydb/apps/bridge_skipper/domain/mutation.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/create_message.h>

#include <library/cpp/json/json_writer.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <algorithm>

namespace NBridgeSkipper {

void TSnapshotStore::Publish(std::shared_ptr<const TSkipperSnapshot> snapshot) {
    // Destruction of the previous snapshot is outside the critical section too.
    {
        std::lock_guard lock(Mutex);
        Snapshot.swap(snapshot);
    }
}

std::shared_ptr<const TSkipperSnapshot> TSnapshotStore::Read() const {
    std::lock_guard lock(Mutex);
    return Snapshot;
}

NJson::TJsonValue UpdateJson(const TExactUpdate& update) {
    NJson::TJsonValue result(NJson::JSON_MAP);
    for (const auto& [pile, state] : update) {
        result[pile] = ToString(state);
    }
    return result;
}

NJson::TJsonValue PlanJson(const TPlan& plan) {
    NJson::TJsonValue result(NJson::JSON_MAP);
    result["expected_generation"] = ::ToString(plan.ExpectedGeneration);
    result["update"] = UpdateJson(plan.Update);
    result["endpoint"] = plan.Endpoint;
    return result;
}

namespace {

using TSlot = std::pair<TString, EYdbMethod>;

struct TEndpointData {
    TEndpointSnapshot Snapshot;
    // Request sequence of Snapshot.View; it orders replies sent at one instant.
    ui64 ViewSequence = 0;
    // The last usable connectivity report, binding or not. A successful but unusable
    // reply never replaces it; evaluation still checks generation and TTL.
    std::optional<TConnectivity> Report;
    // The last usable report sent while not binding; it carries the negative claims
    // behind Lost until its TTL.
    std::optional<TConnectivity> SettledReport;
    bool RepollConnectivity = false;
    // Methods whose round sample found the slot busy; started when the slot settles.
    std::set<EYdbMethod> Due;
    std::map<EYdbMethod, TString> Errors;
};

void SetEndpointError(TEndpointData& endpoint, EYdbMethod method, TString error) {
    if (error.empty()) {
        endpoint.Errors.erase(method);
    } else {
        endpoint.Errors[method] = std::move(error);
    }
    endpoint.Snapshot.LastError.clear();
    for (const auto& [failedMethod, message] : endpoint.Errors) {
        (void)failedMethod;
        if (!endpoint.Snapshot.LastError.empty()) {
            endpoint.Snapshot.LastError += "; ";
        }
        endpoint.Snapshot.LastError += message;
    }
}

TString ConnectivityError(const std::optional<TConnectivity>& report, ui64 generation) {
    if (!report) {
        return "CONNECTIVITY_REPORT_ABSENT";
    }
    if (report->Generation != generation) {
        return "CONNECTIVITY_GENERATION_MISMATCH";
    }
    return {};
}

TDecision Blocked(EBlockerReason reason) {
    TDecision decision;
    decision.Kind = EDecisionKind::Blocked;
    decision.Blocker = reason;
    decision.Reason = ToString(reason);
    return decision;
}

// A decision is logged when its kind, reason or plan changes.
bool IsTransition(const std::optional<TDecision>& previous, const TDecision& next) {
    return !previous || previous->Kind != next.Kind || previous->Reason != next.Reason || previous->Plan != next.Plan;
}

TString MetricStatus(const TRpcResult& rpc) {
    if (rpc.SdkStatus) {
        return SdkStatusName(*rpc.SdkStatus);
    }
    for (const TStringBuf known : {"SDK_START_ERROR", "SDK_RESULT_ERROR", "INVALID_RPC_REQUEST"}) {
        if (rpc.Status == known) {
            return TString(known);
        }
    }
    return "OTHER";
}

TString ExplanationName(ETransitionExplanation explanation) {
    return explanation == ETransitionExplanation::Explained ? "Explained" : "Unexplained";
}

TString Compact(const NJson::TJsonValue& value) {
    return NJson::WriteJson(value, false, true);
}

class TSkipperActor final : public NActors::TActorBootstrapped<TSkipperActor> {
public:
    static constexpr char ActorName[] = "BridgeSkipper";

    explicit TSkipperActor(TSkipperOptions options)
        : Options(std::move(options))
        , Desired(Options.StateFile->GetState()) {
        for (const auto& [pile, endpoints] : Options.Config.Endpoints) {
            for (const auto& endpoint : endpoints) {
                auto& data = Endpoints[endpoint.Uri];
                data.Snapshot.Uri = endpoint.Uri;
                data.Snapshot.Pile = pile;
            }
        }
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Tick();
    }

private:
    void HandleTick(TEvSkipper::TEvTick::TPtr& ev) {
        Tick();
        if (ev->Sender) {
            Send(ev->Sender, new TEvSkipper::TEvTickComplete());
        }
    }

    void Tick() {
        const auto now = NActors::TActivationContext::Monotonic();
        if (!Stopping) {
            ScheduleCalls(now);
        }
        Evaluate(now);
        Publish(now);
        if (!Stopping && Options.ScheduleTicks) {
            Schedule(TDuration::MilliSeconds(250), new TEvSkipper::TEvTick());
        }
    }

    bool Begin(const TRpcRequest& request, EYdbMethod method) {
        return Outstanding.insert(TSlot{request.Endpoint, method}).second;
    }

    // A slot is reused only after it settles, so the slot identifies the call.
    bool Settle(const TRpcResult& rpc) {
        const auto slot = Outstanding.find({rpc.Request.Endpoint, rpc.Method});
        if (slot == Outstanding.end()) {
            return false;
        }
        Outstanding.erase(slot);
        ++Counters.RpcTotal[{rpc.Method, MetricStatus(rpc)}];
        YDB_LOG_DEBUG_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "RPC result",
            {"event", "rpc_result"}, {"category", "sdk"}, {"method", ToString(rpc.Method)},
            {"endpoint", rpc.Request.Endpoint}, {"sequence", rpc.Request.Sequence},
            {"status", rpc.Status}, {"issues", rpc.Issues});
        return true;
    }

    // Starts a read; false when the endpoint's slot for this method is busy.
    bool Start(TEndpointData& endpoint, EYdbMethod method) {
        TRpcRequest request{endpoint.Snapshot.Uri, endpoint.Snapshot.Pile, ++NextSequence};
        if (!Begin(request, method)) {
            return false;
        }
        auto* system = NActors::TActivationContext::ActorSystem();
        const auto recipient = SelfId();
        if (method == EYdbMethod::SelfCheck) {
            Options.Clients->SelfCheck(std::move(request), [system, recipient](TSelfCheckResult result) {
                system->Send(new NActors::IEventHandle(recipient, {}, new TEvSkipper::TEvSelfCheckReply(std::move(result))));
            });
            return true;
        }
        if (method == EYdbMethod::ConnectivityState) {
            endpoint.RepollConnectivity = false;
        }
        Options.Clients->GetState(std::move(request), method, [system, recipient](TStateResult result) {
            system->Send(new NActors::IEventHandle(recipient, {}, new TEvSkipper::TEvStateReply(std::move(result))));
        });
        return true;
    }

    void SampleMethod(
        const TVector<TEndpoint>& endpoints,
        EYdbMethod method,
        size_t& rotation)
    {
        const size_t start = rotation;
        size_t scanned = 0;
        size_t chosen = 0;
        std::vector<TEndpointData*> busy;
        while (scanned < endpoints.size() && chosen < Options.Config.HealthEndpointsPerPile) {
            auto& endpoint = Endpoints.at(endpoints[(start + scanned) % endpoints.size()].Uri);
            ++scanned;
            if (endpoint.Snapshot.Identity == EEndpointIdentity::Mismatch) {
                continue;
            }
            if (Start(endpoint, method)) {
                ++chosen;
            } else {
                busy.push_back(&endpoint);
            }
        }
        for (size_t i = 0; i < busy.size() && chosen + i < Options.Config.HealthEndpointsPerPile; ++i) {
            busy[i]->Due.insert(method);
        }
        rotation = (start + scanned) % endpoints.size();
    }

    void ScheduleCalls(TMonotonic now) {
        if (now >= NextStatePoll) {
            NextStatePoll = now + Options.Config.Timing.StatePollInterval;
            // Keep preferring alternatives across this poll; once none was
            // selectable at the previous evaluation, allow retrying them all.
            // This bounds NotAppliedHere resends to about one per endpoint per poll.
            if (Decision.Blocker == EBlockerReason::NoMutationEndpoint) {
                NotAppliedEndpoints.clear();
            }
            for (auto& [uri, endpoint] : Endpoints) {
                (void)uri;
                Start(endpoint, EYdbMethod::PlainState);
                if (endpoint.RepollConnectivity) {
                    Start(endpoint, EYdbMethod::ConnectivityState);
                }
            }
        }
        if (now < NextHealthRound) {
            return;
        }
        NextHealthRound = now + Options.Config.Timing.RoundInterval;
        for (const auto& [pile, endpoints] : Options.Config.Endpoints) {
            for (const auto& configured : endpoints) {
                auto& endpoint = Endpoints.at(configured.Uri);
                if (endpoint.Snapshot.Identity == EEndpointIdentity::Mismatch) {
                    // Recovery probes are separate from normal evidence budgets.
                    Start(endpoint, EYdbMethod::SelfCheck);
                }
            }
            if (PreviousView && !PreviousView->Piles.contains(pile)) {
                continue;
            }
            // A busy slot for one method must not consume another endpoint's
            // budget for that method or couple the two independent samplers.
            SampleMethod(endpoints, EYdbMethod::SelfCheck, HealthRotation[pile]);
            SampleMethod(endpoints, EYdbMethod::ConnectivityState, ConnectivityRotation[pile]);
        }
    }

    void HandleState(TEvSkipper::TEvStateReply::TPtr& ev) {
        const auto& result = ev->Get()->Result;
        if (!Settle(result.Rpc)) {
            return;
        }
        auto& endpoint = Endpoints.at(result.Rpc.Request.Endpoint);
        if (endpoint.Due.erase(result.Rpc.Method) && !Stopping) {
            Start(endpoint, result.Rpc.Method);
        }
        TString error;
        if (!result.Rpc.Success) {
            // A failed attempt keeps the last valid view and report as they are.
            error = result.Rpc.Status + ": " + result.Rpc.Issues;
        } else if (!result.View) {
            error = result.Error.empty() ? TString("INVALID_STATE_REPLY") : result.Error;
        } else {
            const auto& view = *result.View;
            auto& latest = endpoint.Snapshot.View;
            if (!latest || view.SentAt > latest->SentAt
                || (view.SentAt == latest->SentAt && result.Rpc.Request.Sequence > endpoint.ViewSequence))
            {
                latest = view;
                endpoint.ViewSequence = result.Rpc.Request.Sequence;
            }
            if (result.Rpc.Method == EYdbMethod::ConnectivityState) {
                error = ConnectivityError(result.Connectivity, view.Generation);
                if (error.empty()) {
                    endpoint.Report = result.Connectivity;
                    if (!result.Connectivity->BindingInProgress) {
                        endpoint.SettledReport = result.Connectivity;
                    }
                }
                // Binding may be brief; ask again at the next state poll.
                endpoint.RepollConnectivity = !error.empty() || result.Connectivity->BindingInProgress;
            }
        }
        SetEndpointError(endpoint, result.Rpc.Method, std::move(error));
    }

    void HandleHealth(TEvSkipper::TEvSelfCheckReply::TPtr& ev) {
        auto& result = ev->Get()->Result;
        if (!Settle(result.Rpc)) {
            return;
        }
        auto& endpoint = Endpoints.at(result.Rpc.Request.Endpoint);
        if (endpoint.Due.erase(EYdbMethod::SelfCheck) && !Stopping) {
            Start(endpoint, EYdbMethod::SelfCheck);
        }
        auto& snapshot = endpoint.Snapshot;
        TString error;
        if (!result.Rpc.Success) {
            // A failed attempt keeps the last reply until its TTL.
            error = result.Rpc.Status + ": " + result.Rpc.Issues;
        } else if (!result.Reply) {
            error = "INVALID_SELF_CHECK_REPLY";
        } else {
            // A reply without a pile (storage config not ready) keeps the
            // identity and is not evidence (EvaluatePiles).
            const auto& pile = result.Reply->ResponderPile;
            if (!pile) {
                error = "ENDPOINT_PILE_UNVERIFIED";
            } else if (*pile == snapshot.Pile) {
                snapshot.Identity = EEndpointIdentity::Verified;
            } else {
                snapshot.Identity = EEndpointIdentity::Mismatch;
                error = "ENDPOINT_PILE_MISMATCH";
            }
            // A pile-less success is not new evidence. Keep the last named
            // observation with its original TTL, as for an unsuccessful call.
            // Named fallback/truncation and mismatch replies still replace it.
            if (pile) {
                snapshot.SelfCheck = std::move(result.Reply);
            }
        }
        SetEndpointError(endpoint, EYdbMethod::SelfCheck, std::move(error));
    }

    void ResetContinuity() {
        ActiveGeneration.reset();
        // A missing view breaks evidence continuity, but not knowledge of the
        // last roles or their reset boundary. Recovery must still detect a BSC
        // move and exclude cached replies sent before an observed role change.
        HealthState.CleanSince.clear();
        HealthState.EndpointModes.clear();
        Confirmations = {};
    }

    bool Persist(TPersistentState next) {
        const auto before = Options.StateFile->GetState();
        Desired = std::move(next);
        const auto error = Options.WriteState ? Options.WriteState(Desired) : Options.StateFile->Write(Desired);
        Dirty = !error.empty();
        if (Dirty) {
            if (StateWriteError.empty()) {
                YDB_LOG_ERROR_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "State write failed",
                    {"event", "state_write_failed"}, {"category", "state_write"}, {"reason", error});
            }
            StateWriteError = error;
            return false;
        }
        StateWriteError.clear();
        if (before.PendingMutation != Desired.PendingMutation) {
            const auto& pending = Desired.PendingMutation ? Desired.PendingMutation : before.PendingMutation;
            YDB_LOG_INFO_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "Pending mutation changed",
                {"event", "pending_mutation"}, {"action", Desired.PendingMutation ? "set" : "cleared"},
                {"expected_generation", pending->ExpectedGeneration}, {"update", Compact(UpdateJson(pending->Update))});
        }
        return true;
    }

    void ResetResends() {
        LastSendAt.reset();
        LastOutcome.reset();
        AmbiguousResends = 0;
        InFlight.reset();
        NotAppliedEndpoints.clear();
    }

    void LogViewChange(const TClusterView& view) {
        if (PreviousView && PreviousView->Generation == view.Generation && PreviousView->Piles == view.Piles) {
            return;
        }
        auto priority = NActors::NLog::PRI_INFO;
        TString explanation = "Initial";
        if (PreviousView) {
            // Once our own success clears the pending record, it explains the change.
            const auto& pending = Desired.PendingMutation && Desired.PendingMutation->ExpectedGeneration == PreviousView->Generation
                ? Desired.PendingMutation : LastSuccess;
            const auto result = ExplainTransition(*PreviousView, view, pending);
            explanation = ExplanationName(result);
            if (result == ETransitionExplanation::Unexplained) {
                ++Counters.UnexplainedStateChanges;
                priority = NActors::NLog::PRI_WARN;
            }
        }
        YDB_LOG_CTX_COMP(*NActors::TlsActivationContext, priority, SkipperLogComponent, "View changed",
            {"event", "view_change"}, {"generation", view.Generation},
            {"states", Compact(UpdateJson(view.Piles))}, {"explanation", explanation});
        PreviousView = view;
    }

    void Evaluate(TMonotonic now) {
        if (Dirty) {
            Persist(Desired);
        }
        const auto& timing = Options.Config.Timing;
        std::vector<TClusterView> views;
        for (const auto& [uri, endpoint] : Endpoints) {
            (void)uri;
            const auto& snapshot = endpoint.Snapshot;
            // Exclude known pile mismatches and views missing the configured
            // pile. This attributes piles, not remote cluster identity;
            // independently trusted endpoint inventory is required.
            if (snapshot.Identity != EEndpointIdentity::Mismatch && snapshot.View
                && snapshot.View->Piles.contains(snapshot.Pile))
            {
                views.push_back(*snapshot.View);
            }
        }
        Selection = SelectView(views, Desired.GenerationHighWater, now, timing.EvidenceTtl);
        Health = {};
        std::vector<TEndpointState> endpoints;
        if (Selection.View) {
            const auto& view = *Selection.View;
            if (ActiveGeneration != view.Generation) {
                ActiveGeneration = view.Generation;
                // A new generation starts a health round at once.
                NextHealthRound = TMonotonic::Zero();
            }
            LogViewChange(view);
            auto next = Desired;
            next.GenerationHighWater = std::max(next.GenerationHighWater, view.Generation);
            if (next.PendingMutation && ResolvedBy(*next.PendingMutation, view)) {
                next.PendingMutation.reset();
                ResetResends();
            }
            if (next != Desired) {
                Persist(std::move(next));
            }
            THealthReplies replies;
            for (auto& [uri, endpoint] : Endpoints) {
                auto& snapshot = endpoint.Snapshot;
                replies.Endpoints.push_back({uri, snapshot.Pile, snapshot.Identity, Selection.CurrentEndpoints.contains(uri)});
                snapshot.Ignored = !view.Piles.contains(snapshot.Pile);
                if (snapshot.Ignored && WarnedAbsent.insert(uri).second) {
                    YDB_LOG_WARN_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "Configured endpoint ignored",
                        {"event", "configured_endpoint_ignored"}, {"endpoint", uri}, {"pile", snapshot.Pile});
                } else if (!snapshot.Ignored) {
                    WarnedAbsent.erase(uri);
                }
                if (snapshot.SelfCheck) {
                    replies.SelfChecks.push_back(*snapshot.SelfCheck);
                }
                if (endpoint.Report) {
                    replies.Connectivity.push_back({uri, *endpoint.Report});
                    if (endpoint.SettledReport && endpoint.Report->BindingInProgress) {
                        replies.Connectivity.push_back({uri, *endpoint.SettledReport});
                    }
                }
            }
            const THealthParams params{timing.EvidenceTtl, timing.BscWarmup};
            Health = EvaluatePiles(view, replies, now, params, HealthState);
            Confirmations.Update(view, Health.Piles, now);
            endpoints = std::move(replies.Endpoints);
        } else {
            ResetContinuity();
        }
        Decision = Decide(Selection, Health, Confirmations, Desired.PendingMutation,
            endpoints, timing.Confirm, NotAppliedEndpoints);
        if (Dirty) {
            Decision = Blocked(EBlockerReason::StateWriteFailed);
        }
        if (Decision.Kind == EDecisionKind::Ready && InFlight
            && InFlight->second.ExpectedGeneration == Decision.Plan->ExpectedGeneration
            && InFlight->second.Update == Decision.Plan->Update)
        {
            // Publish the actual route of the outstanding invocation.
            Decision.Plan = InFlight->second;
            Decision.CliCommand = RenderCliCommand(InFlight->second);
        }
        if (!Stopping && !Dirty && Options.Config.Mode == EMode::Auto && Decision.Kind == EDecisionKind::Ready) {
            if (RejectedGeneration == Decision.Plan->ExpectedGeneration) {
                // B10: the server refused an update at this generation; an operator
                // must act, or the generation must move, before Auto sends again.
                Decision.Reason = "REJECTED_BY_SERVER";
            } else {
                MaybeSend(endpoints, now);
            }
        }
        if (IsTransition(PreviousDecision, Decision)) {
            YDB_LOG_INFO_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "Decision changed",
                {"event", "decision"}, {"kind", ToString(Decision.Kind)}, {"reason", Decision.Reason},
                {"plan", Decision.Plan ? Compact(PlanJson(*Decision.Plan)) : TString("null")});
            PreviousDecision = Decision;
        }
    }

    void MaybeSend(const std::vector<TEndpointState>& endpoints, TMonotonic now) {
        // Decide admits at the pending generation only the identical update or
        // a D2 superset replacement, which replaces the record before it is sent.
        const bool replacing = Desired.PendingMutation && Desired.PendingMutation->Update != Decision.Plan->Update;
        if (Desired.PendingMutation && !replacing && !MayResend(LastSendAt, LastOutcome, now,
            Options.Config.Timing.MutationClientTimeout, AmbiguousResends))
        {
            return;
        }
        auto exclude = NotAppliedEndpoints;
        for (const auto& slot : Outstanding) {
            if (slot.second == EYdbMethod::Update) {
                exclude.insert(slot.first);
            }
        }
        auto plan = *Decision.Plan;
        const auto endpoint = SelectMutationEndpoint(*Selection.View, endpoints, plan.Update, Health.Connectivity, exclude);
        if (!endpoint) {
            Decision = Blocked(EBlockerReason::NoMutationEndpoint);
            return;
        }
        plan.Endpoint = *endpoint;
        if (!Desired.PendingMutation || replacing) {
            ResetResends();
            auto next = Desired;
            next.PendingMutation = TPendingMutation{plan.ExpectedGeneration, plan.Update};
            if (!Persist(next)) {
                // Memory follows the file. A record the disk never acknowledged does
                // not exist, so the next plan at this generation is not blocked by it;
                // one the rename made visible is kept and rewritten until durable.
                Desired = Options.StateFile->GetState();
                if (Desired.PendingMutation != next.PendingMutation) {
                    Dirty = false;
                    StateWriteError.clear();
                }
                Decision = Blocked(EBlockerReason::StateWriteFailed);
                return;
            }
        }
        const TUpdateAuthorization authorization{std::min(Selection.ValidUntil, Health.ValidUntil)};
        const auto expired = [&] {
            Decision = {};
            Decision.Kind = EDecisionKind::Unknown;
            Decision.Reason = "EVIDENCE_EXPIRED";
        };
        // Includes pending writes above and Dirty/high-water writes in Evaluate.
        if (NActors::TActivationContext::Monotonic() > authorization.ValidUntil) {
            expired();
            return;
        }
        TRpcRequest request{plan.Endpoint, Endpoints.at(plan.Endpoint).Snapshot.Pile, ++NextSequence};
        if (!Begin(request, EYdbMethod::Update)) {
            return;
        }
        const auto sequence = request.Sequence;
        auto* system = NActors::TActivationContext::ActorSystem();
        const auto recipient = SelfId();
        if (!Options.Clients->Update(std::move(request), plan, authorization, [system, recipient](TUpdateResult result) {
            system->Send(new NActors::IEventHandle(recipient, {}, new TEvSkipper::TEvUpdateReply(std::move(result))));
        })) {
            // Local preparation used up the authorization: no callback will
            // arrive and no invocation may be counted or own this slot.
            Outstanding.erase(TSlot{plan.Endpoint, EYdbMethod::Update});
            expired();
            return;
        }
        const auto sentAt = NActors::TActivationContext::Monotonic();
        if (LastOutcome == EMutationOutcome::Ambiguous) {
            ++AmbiguousResends;
        }
        InFlight.emplace(sequence, plan);
        LastSendAt = sentAt;
        LastOutcome.reset();
        Decision.Plan = plan;
        Decision.CliCommand = RenderCliCommand(plan);
        LastMutation = TMutationInvocation{sequence, sentAt, plan, {}, {}, AmbiguousResends};
        YDB_LOG_INFO_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "Mutation sent",
            {"event", "mutation_sent"}, {"invocation_id", sequence},
            {"expected_generation", plan.ExpectedGeneration}, {"endpoint", plan.Endpoint},
            {"update", Compact(UpdateJson(plan.Update))});
    }

    void HandleUpdate(TEvSkipper::TEvUpdateReply::TPtr& ev) {
        const auto& result = ev->Get()->Result;
        if (!Settle(result.Rpc)) {
            return;
        }
        const auto sequence = result.Rpc.Request.Sequence;
        const auto& plan = result.Plan;
        const auto outcome = result.Outcome;
        ++Counters.MutationTotal[outcome];
        if (LastMutation && LastMutation->Id == sequence) {
            LastMutation->SentAt = result.Rpc.SentAt;
            LastMutation->Status = result.Rpc.Status;
            LastMutation->Outcome = outcome;
        }
        YDB_LOG_INFO_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "Mutation completed",
            {"event", "mutation_outcome"}, {"invocation_id", sequence},
            {"expected_generation", plan.ExpectedGeneration}, {"endpoint", plan.Endpoint},
            {"status", result.Rpc.Status}, {"outcome", ToString(outcome)}, {"issues", result.Rpc.Issues});
        // An older invocation, or one whose record was resolved, only reports.
        if (!InFlight || InFlight->first != sequence || !Desired.PendingMutation) {
            return;
        }
        InFlight.reset();
        LastOutcome = outcome;
        LastSendAt = result.Rpc.SentAt;
        if (outcome == EMutationOutcome::Success || outcome == EMutationOutcome::GenerationMoved) {
            LastSuccess = Desired.PendingMutation;
            Persist(ApplyOutcome(Desired, outcome));
            if (!Desired.PendingMutation) {
                ResetResends();
            }
        } else if (outcome == EMutationOutcome::NotAppliedHere) {
            // Not applied before distconf: resend the identical update elsewhere.
            NotAppliedEndpoints.insert(plan.Endpoint);
        } else if (outcome == EMutationOutcome::Rejected) {
            YDB_LOG_ERROR_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "Mutation rejected",
                {"event", "mutation_rejected"}, {"category", "sdk"}, {"endpoint", plan.Endpoint},
                {"status", result.Rpc.Status}, {"issues", result.Rpc.Issues});
            RejectedGeneration = plan.ExpectedGeneration;
            // A rejection resolves only this invocation; the durable overlay
            // still constrains earlier attempts and decisions after restart.
        }
    }

    void Publish(TMonotonic now) {
        auto snapshot = std::make_shared<TSkipperSnapshot>();
        snapshot->ClusterId = Options.Config.ClusterId;
        snapshot->Mode = Options.Config.Mode;
        snapshot->LastTickAt = now;
        snapshot->Selection = Selection;
        snapshot->GenerationHighWater = Desired.GenerationHighWater;
        for (const auto& [uri, endpoint] : Endpoints) {
            (void)uri;
            snapshot->Endpoints.push_back(endpoint.Snapshot);
        }
        for (const auto& [pile, verdict] : Health.Piles) {
            auto& item = snapshot->Piles[pile];
            item.Verdict = verdict;
            for (const auto& connectivity : Health.Connectivity) {
                if (connectivity.Report.Piles.contains(pile)) {
                    item.Connectivity.push_back(connectivity.Report);
                }
            }
            if (const auto it = Confirmations.Piles.find(pile); it != Confirmations.Piles.end()) {
                item.DownSince = it->second.DownSince;
            }
            item.Confirmed = Confirmations.IsConfirmed(pile, Options.Config.Timing.Confirm);
        }
        snapshot->Decision = Decision;
        snapshot->PersistentState = Options.StateFile->GetState();
        snapshot->StateWriteError = StateWriteError;
        snapshot->LastMutation = LastMutation;
        snapshot->Counters = Counters;
        Options.Snapshots->Publish(std::move(snapshot));
    }

    void HandleStop(TEvSkipper::TEvStop::TPtr& ev) {
        Stopping = true;
        Publish(NActors::TActivationContext::Monotonic());
        Send(ev->Sender, new TEvSkipper::TEvStopped());
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvSkipper::TEvTick, HandleTick);
        hFunc(TEvSkipper::TEvStateReply, HandleState);
        hFunc(TEvSkipper::TEvSelfCheckReply, HandleHealth);
        hFunc(TEvSkipper::TEvUpdateReply, HandleUpdate);
        hFunc(TEvSkipper::TEvStop, HandleStop);
    )

    TSkipperOptions Options;
    TPersistentState Desired;
    bool Dirty = false;
    TString StateWriteError;
    bool Stopping = false;
    TMonotonic NextStatePoll;
    TMonotonic NextHealthRound;
    ui64 NextSequence = 0;
    std::map<TString, TEndpointData> Endpoints;
    std::map<TString, size_t> HealthRotation;
    std::map<TString, size_t> ConnectivityRotation;
    std::set<TString> WarnedAbsent;
    std::set<TSlot> Outstanding;
    TViewSelection Selection;
    std::optional<TClusterView> PreviousView;
    std::optional<ui64> ActiveGeneration;
    THealthState HealthState;
    THealthEvaluation Health;
    TConfirmations Confirmations;
    TDecision Decision;
    std::optional<TDecision> PreviousDecision;
    std::optional<TPendingMutation> LastSuccess;
    std::optional<TMonotonic> LastSendAt;
    std::optional<EMutationOutcome> LastOutcome;
    // Resends after ambiguous outcomes for the current pending record.
    ui64 AmbiguousResends = 0;
    std::optional<ui64> RejectedGeneration;
    // The pending record's latest invocation while it is outstanding.
    std::optional<std::pair<ui64, TPlan>> InFlight;
    // Endpoints that answered NotAppliedHere. They accumulate and are skipped
    // until a state poll that follows an evaluation with no selectable endpoint.
    std::set<TString> NotAppliedEndpoints;
    std::optional<TMutationInvocation> LastMutation;
    TSkipperCounters Counters;
};

} // namespace

NActors::IActor* CreateSkipperActor(TSkipperOptions options) {
    Y_ENSURE(options.Clients && options.StateFile && options.Snapshots, "incomplete skipper options");
    Y_ENSURE(options.StateFile->GetState().ClusterId == options.Config.ClusterId, "skipper state cluster_id mismatch");
    return new TSkipperActor(std::move(options));
}

} // namespace NBridgeSkipper
