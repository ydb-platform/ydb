#include "skipper.h"
#include "events.h"

#include <ydb/apps/bridge_skipper/domain/mutation.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/create_message.h>

#include <library/cpp/json/json_writer.h>

#include <util/generic/yexception.h>
#include <util/generic/guid.h>
#include <deque>
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
    bool Discovered = false;
    // One extra client can search for Config/Discovery without consuming a
    // health slot. Its node may also have a separate Bridge/Monitoring port.
    bool InventoryOnly = false;
    bool Retiring = false;
    // A fresh Bridge reply alone cannot keep an unverified address forever.
    std::optional<TMonotonic> VerificationDeadline;
    // Mismatching replies cannot renew the last successfully verified identity.
    std::optional<TMonotonic> VerifiedSelfCheckAt;
    std::optional<ui32> ExpectedNodeId;
    std::optional<ui32> NodeId;
    TMonotonic NextBootstrapPoll;

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

bool ShouldPollPile(const TClusterView& view, const TString& name) {
    const auto pile = view.Piles.find(name);
    return pile != view.Piles.end() && pile->second != EPileState::Inactive
        && pile->second != EPileState::Disconnected;
}

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
        BootId = Options.BootId.empty() ? CreateGuidAsString() : Options.BootId;
        if (!Options.Diagnostics) {
            Options.Diagnostics = std::make_shared<TDiagnosticStore>(BootId, Options.Config.Logging.Level);
        }
        MayApplyElsewhere = Desired.PendingMutation.has_value();
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
        BootAt = NActors::TActivationContext::Monotonic();
        BootUtc = NActors::TActivationContext::Now();
        Tick();
    }

private:
    void HandleTick(TEvSkipper::TEvTick::TPtr& ev) {
        Tick();
        if (ev->Sender) {
            Send(ev->Sender, new TEvSkipper::TEvTickComplete());
        }
    }

    bool IsStopping() const {
        return Stopping || (Options.ShutdownRequested && Options.ShutdownRequested->load());
    }

    void Tick() {
        const auto now = NActors::TActivationContext::Monotonic();
        if (!IsStopping()) {
            ScheduleCalls(now);
        }
        Evaluate(now);
        Publish(now);
        if (!IsStopping() && Options.ScheduleTicks) {
            Schedule(TDuration::MilliSeconds(250), new TEvSkipper::TEvTick());
        }
    }

    bool Begin(const TRpcRequest& request, EYdbMethod method) {
        const TSlot slot{request.Endpoint, method};
        return Outstanding.emplace(slot, request.Sequence).second;
    }

    // Match the invocation sequence before releasing a reused endpoint slot.
    bool Settle(const TRpcResult& rpc) {
        const auto slot = Outstanding.find({rpc.Request.Endpoint, rpc.Method});
        if (slot == Outstanding.end() || slot->second != rpc.Request.Sequence) {
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
        if (IsStopping() || endpoint.Retiring || !Begin(request, method)) {
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

    bool MayResetNotAppliedEndpoints() const {
        if (!Desired.PendingMutation || Desired.PendingMutation->Kind == EOperationKind::Failover) {
            return Decision.Blocker == EBlockerReason::NoMutationEndpoint;
        }
        if (Desired.Mode != EMode::Manual || !Selection.View || NotAppliedEndpoints.empty()) {
            return false;
        }
        const auto excluded = LifecycleEndpointExclusions(Desired.PendingMutation->Kind,
            Desired.PendingMutation->Update, CurrentEndpoints, NotAppliedEndpoints);
        return !SelectMutationEndpoint(*Selection.View, CurrentEndpoints, Desired.PendingMutation->Update,
            Health.Connectivity, excluded, Health.Participation);
    }

    bool HasCalls(const TString& uri) const {
        const auto it = Outstanding.lower_bound({uri, EYdbMethod::PlainState});
        return it != Outstanding.end() && it->first.first == uri;
    }

    void ReapEndpoints() {
        for (auto it = Endpoints.begin(); it != Endpoints.end();) {
            auto& endpoint = it->second;
            if (!endpoint.Retiring || HasCalls(it->first)) {
                ++it;
                continue;
            }
            const auto uri = it->first;
            const auto pile = endpoint.Snapshot.Pile;
            Options.Clients->RemoveEndpoint(uri);
            auto& configured = Options.Config.Endpoints.at(pile);
            std::erase_if(configured, [&](const TEndpoint& item) { return item.Uri == uri; });
            if (configured.empty()) {
                Options.Config.Endpoints.erase(pile);
                HealthRotation.erase(pile);
                ConnectivityRotation.erase(pile);
            }
            HealthState.EndpointModes.erase(uri);
            WarnedAbsent.erase(uri);
            it = Endpoints.erase(it);
        }
    }

    bool IsVerifiedCurrentEndpoint(const TString& uri, const TEndpointData& endpoint) const {
        return !endpoint.Retiring && !endpoint.Snapshot.Ignored
            && endpoint.Snapshot.Identity == EEndpointIdentity::Verified
            && Selection.CurrentEndpoints.contains(uri);
    }

    TDuration HealthSamplingPeriod(const TString& pile, size_t extra = 0) const {
        const auto count = Options.Config.Endpoints.at(pile).size() + extra;
        const auto limit = Options.Config.HealthEndpointsPerPile;
        return Options.Config.Timing.RoundInterval * ((count + limit - 1) / limit);
    }

    void ScheduleInventory(TMonotonic now) {
        if (now < NextInventory || InventoryInFlight || !Selection.View) {
            return;
        }
        std::vector<TString> eligible;
        std::optional<TString> probe;
        for (const auto& [uri, endpoint] : Endpoints) {
            if (IsVerifiedCurrentEndpoint(uri, endpoint)) {
                eligible.push_back(uri);
                if (endpoint.InventoryOnly) {
                    probe = uri;
                }
            }
        }
        if (eligible.empty()) {
            return;
        }
        // The independent probe must get a turn even when many usable health
        // ports do not serve either Config or Discovery.
        const TString uri = probe ? *probe : eligible[InventoryRotation++ % eligible.size()];
        TRpcRequest request{uri, Endpoints.at(uri).Snapshot.Pile, ++NextSequence};
        if (!Begin(request, EYdbMethod::Inventory)) {
            return;
        }
        NextInventory = now + TDuration::Seconds(5);
        InventoryInFlight = true;
        auto* system = NActors::TActivationContext::ActorSystem();
        const auto recipient = SelfId();
        Options.Clients->Discover(std::move(request), [system, recipient](TInventoryResult result) {
            system->Send(new NActors::IEventHandle(recipient, {}, new TEvSkipper::TEvInventoryReply(std::move(result))));
        });
    }

    void AddDiscoveredEndpoint(const TDiscoveredEndpoint& item, bool inventoryOnly, TMonotonic now) {
        Options.Clients->AddEndpoint(item.Endpoint, item.Pile);
        auto& endpoint = Endpoints[item.Endpoint.Uri];
        endpoint.Snapshot.Uri = item.Endpoint.Uri;
        endpoint.Snapshot.Pile = item.Pile;
        endpoint.Discovered = true;
        endpoint.InventoryOnly = inventoryOnly;
        endpoint.ExpectedNodeId = item.NodeId;
        Options.Config.Endpoints[item.Pile].push_back(item.Endpoint);
        const auto& timing = Options.Config.Timing;
        // Reserve scheduling room for the independent inventory probe too;
        // adding it later must not shorten this client's first attempt.
        endpoint.VerificationDeadline = now + std::max(
            timing.ReadTimeout + timing.StatePollInterval,
            timing.SelfCheckClientTimeout + HealthSamplingPeriod(item.Pile, inventoryOnly ? 0 : 1));
        NextStatePoll = TMonotonic::Zero();
        NextHealthRound = TMonotonic::Zero();
        YDB_LOG_INFO_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "Endpoint discovered",
            {"event", "endpoint_discovered"}, {"pile", item.Pile}, {"endpoint", item.Endpoint.Uri});
    }

    void RefreshEndpoints(TMonotonic now) {
        if (now < NextEndpointRefresh || !PreviousView) {
            return;
        }
        // Last accepted membership only scopes bootstrap probes. Requiring a
        // fresh view here would prevent cached locators from restoring one.
        // Evidence and mutation authorization still use Selection and its TTL.
        const auto& view = *PreviousView;
        NextEndpointRefresh = now + TDuration::Seconds(5);
        // Keep clients through their complete sampling/renewal period. This
        // owns addresses only: observation and routing TTLs remain unchanged.
        for (auto& [uri, endpoint] : Endpoints) {
            (void)uri;
            if (!endpoint.Discovered) {
                continue;
            }
            const auto& snapshot = endpoint.Snapshot;
            const auto& timing = Options.Config.Timing;
            const auto healthBudget = std::max(timing.EvidenceTtl, HealthSamplingPeriod(snapshot.Pile))
                + timing.SelfCheckClientTimeout;
            const bool responsive = snapshot.View
                && now - snapshot.View->SentAt < timing.EvidenceTtl + timing.ReadTimeout + timing.StatePollInterval
                && endpoint.VerifiedSelfCheckAt && now - *endpoint.VerifiedSelfCheckAt < healthBudget;
            if (responsive) {
                endpoint.VerificationDeadline.reset();
            } else if (!endpoint.VerificationDeadline || now >= *endpoint.VerificationDeadline) {
                endpoint.Retiring = true;
            }
        }
        // A URI can be reused by a new node or pile. Discovery supplies the
        // proposed identity; drain old calls, then verify it through SelfCheck.
        for (const auto& item : InventoryCandidates) {
            const auto it = Endpoints.find(item.Endpoint.Uri);
            if (it == Endpoints.end() || !ShouldPollPile(view, item.Pile)) {
                continue;
            }
            auto& endpoint = it->second;
            const bool reusedSeed = endpoint.Snapshot.Ignored && endpoint.Snapshot.Pile != item.Pile;
            const bool replacedLearned = endpoint.Discovered && endpoint.Snapshot.Identity == EEndpointIdentity::Mismatch
                && (endpoint.Snapshot.Pile != item.Pile || endpoint.ExpectedNodeId != item.NodeId);
            if (reusedSeed || replacedLearned) {
                endpoint.Retiring = true;
            }
        }
        ReapEndpoints();
        if (InventorySource) {
            const auto it = Endpoints.find(*InventorySource);
            if (it == Endpoints.end() || !IsVerifiedCurrentEndpoint(it->first, it->second)) {
                InventorySource.reset();
            }
        }
        bool hasProbe = false;
        std::map<TString, size_t> learned;
        std::set<ui32> nodes;
        for (auto& [uri, endpoint] : Endpoints) {
            if (endpoint.InventoryOnly) {
                hasProbe = true; // a retiring invocation still owns the extra slot
                if (InventorySource && *InventorySource != uri) {
                    endpoint.Retiring = true;
                }
                continue;
            }
            learned[endpoint.Snapshot.Pile] += endpoint.Discovered;
            if (endpoint.Discovered && !endpoint.Retiring && !endpoint.Snapshot.Ignored) {
                // Reserve the advertised node across refreshes, including the
                // first pending verification. A failed locator retires above.
                nodes.insert(*endpoint.ExpectedNodeId);
            } else if (endpoint.NodeId && IsVerifiedCurrentEndpoint(uri, endpoint)) {
                nodes.insert(*endpoint.NodeId);
            }
        }
        std::map<TString, std::vector<const TDiscoveredEndpoint*>> candidates;
        for (const auto& item : InventoryCandidates) {
            if (ShouldPollPile(view, item.Pile)) {
                candidates[item.Pile].push_back(&item);
            }
        }
        std::erase_if(DiscoveryRotation, [&](const auto& entry) { return !view.Piles.contains(entry.first); });
        for (const auto& [pile, choices] : candidates) {
            auto& rotation = DiscoveryRotation[pile];
            const size_t start = rotation % choices.size();
            for (size_t i = 0; i < choices.size() && learned[pile] < Options.Config.HealthEndpointsPerPile
                && Endpoints.size() < 16384; ++i)
            {
                const auto& item = *choices[(start + i) % choices.size()];
                if (nodes.contains(item.NodeId) || Endpoints.contains(item.Endpoint.Uri)) {
                    continue;
                }
                AddDiscoveredEndpoint(item, false, now);
                ++learned[pile];
                nodes.insert(item.NodeId);
                rotation = (start + i + 1) % choices.size();
            }
        }
        // Advertisements are bootstrap candidates, not evidence. Keep the last
        // complete bounded list so a failed inventory API cannot prevent trying
        // a different advertised port after every configured seed is removed.
        if (!InventorySource && !hasProbe && !InventoryCandidates.empty() && Endpoints.size() < 16384) {
            const size_t start = InventoryDiscoveryRotation % InventoryCandidates.size();
            for (size_t i = 0; i < InventoryCandidates.size(); ++i) {
                const auto& item = InventoryCandidates[(start + i) % InventoryCandidates.size()];
                if (!ShouldPollPile(view, item.Pile) || Endpoints.contains(item.Endpoint.Uri)) {
                    continue;
                }
                AddDiscoveredEndpoint(item, true, now);
                InventoryDiscoveryRotation = (start + i + 1) % InventoryCandidates.size();
                break;
            }
        }
    }

    void HandleInventory(TEvSkipper::TEvInventoryReply::TPtr& ev) {
        const auto& result = ev->Get()->Result;
        if (!Settle(result.Rpc)) {
            return;
        }
        InventoryInFlight = false;
        auto& source = Endpoints.at(result.Rpc.Request.Endpoint);
        SetEndpointError(source, EYdbMethod::Inventory, !result.Rpc.Success
            ? result.Rpc.Status + ": " + result.Rpc.Issues : result.Error);
        if (!result.Rpc.Success || !result.Error.empty()) {
            NextEndpointRefresh = TMonotonic::Zero();
            if (InventorySource == result.Rpc.Request.Endpoint) {
                InventorySource.reset();
            }
            if (source.InventoryOnly) {
                source.Retiring = true;
            }
            return;
        }
        if (IsStopping() || !Selection.View || !IsVerifiedCurrentEndpoint(result.Rpc.Request.Endpoint, source)
            || NActors::TActivationContext::Monotonic() >= Selection.ValidUntil)
        {
            return;
        }
        InventorySource = result.Rpc.Request.Endpoint;
        InventoryCandidates = result.Endpoints;
        NextEndpointRefresh = TMonotonic::Zero();
        RefreshEndpoints(NActors::TActivationContext::Monotonic());
    }

    void ScheduleCalls(TMonotonic now) {
        RefreshEndpoints(now);
        ScheduleInventory(now);
        if (now >= NextStatePoll) {
            NextStatePoll = now + Options.Config.Timing.StatePollInterval;
            // Keep preferring alternatives across this poll; once none was
            // selectable at the previous evaluation, allow retrying them all.
            // This bounds NotAppliedHere resends to about one per endpoint per poll.
            if (MayResetNotAppliedEndpoints()) {
                // Lifecycle reviews do not replace the failover decision, so
                // check their pending overlay for eligible alternatives too.
                NotAppliedEndpoints.clear();
            }
            for (auto& [uri, endpoint] : Endpoints) {
                (void)uri;
                if (endpoint.Snapshot.Ignored) {
                    if (endpoint.Discovered || now < endpoint.NextBootstrapPoll) {
                        continue;
                    }
                    endpoint.NextBootstrapPoll = now + TDuration::Seconds(5);
                }
                Start(endpoint, EYdbMethod::PlainState);
                if (!endpoint.Snapshot.Ignored && endpoint.RepollConnectivity) {
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
            if (PreviousView && !ShouldPollPile(*PreviousView, pile)) {
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
        if (endpoint.Retiring) {
            return;
        }
        if (endpoint.Due.erase(result.Rpc.Method) && !IsStopping()) {
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
        if (endpoint.Retiring) {
            return;
        }
        if (endpoint.Due.erase(EYdbMethod::SelfCheck) && !IsStopping()) {
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
                if (endpoint.ExpectedNodeId && result.ResponderNodeId != endpoint.ExpectedNodeId) {
                    snapshot.Identity = EEndpointIdentity::Mismatch;
                    error = "ENDPOINT_NODE_MISMATCH";
                } else {
                    snapshot.Identity = EEndpointIdentity::Verified;
                    endpoint.NodeId = result.ResponderNodeId;
                    endpoint.VerifiedSelfCheckAt = result.Reply->SentAt;
                }
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
        if (!Desired.PendingMutation) {
            PendingAttempts = 0;
            PendingFirstSendUtc.reset();
            PendingLastSendUtc.reset();
        }
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
            // Refresh sample time and pending/conversion metadata even when no
            // public transition is recorded. This is the last observed view.
            PreviousView = view;
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
        auto event = PlanJson(TPlan{view.Generation, view.Piles, view.Endpoint});
        event["generation"] = ::ToString(view.Generation);
        const bool gap = PreviousView && view.Generation > PreviousView->Generation
            && view.Generation - PreviousView->Generation > 1;
        event["before_generation"] = PreviousView ? NJson::TJsonValue(::ToString(PreviousView->Generation)) : NJson::TJsonValue();
        event["coverage_gap"] = gap;
        event["coverage_gap_text"] = NJson::TJsonValue();
        if (gap) {
            const auto first = PreviousView->Generation + 1;
            const auto last = view.Generation - 1;
            event["coverage_gap_text"] = first == last ? "Generation " + ::ToString(first) + " not observed."
                : "Generations " + ::ToString(first) + "–" + ::ToString(last) + " not observed.";
        }
        event["before"] = PreviousView ? UpdateJson(PreviousView->Piles) : NJson::TJsonValue();
        event["after"] = UpdateJson(view.Piles);
        event["message"] = !PreviousView ? TString("Observation baseline")
            : PreviousView->Piles == view.Piles ? TString("Generation changed; no public state change")
            : TString("Observed pile state transition");
        event["origin"] = explanation == "Initial" ? "baseline" : explanation == "Explained" ? "inferred" : "external";
        Options.Diagnostics->History("view", std::move(event), NActors::TActivationContext::Now());
        PreviousView = view;
    }

    void Evaluate(TMonotonic now, bool dispatch = true) {
        if (Dirty) {
            Persist(Desired);
        }
        const auto& timing = Options.Config.Timing;
        ReapEndpoints();
        std::vector<TClusterView> views;
        for (const auto& [uri, endpoint] : Endpoints) {
            (void)uri;
            const auto& snapshot = endpoint.Snapshot;
            // Exclude known pile mismatches and views missing the configured
            // pile. This attributes piles, not remote cluster identity;
            // independently trusted endpoint inventory is required.
            if (!endpoint.Retiring && snapshot.Identity != EEndpointIdentity::Mismatch && snapshot.View
                && (!endpoint.Discovered || snapshot.Identity == EEndpointIdentity::Verified)
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
            if (RejectedGeneration && view.Generation > *RejectedGeneration) {
                RejectedGeneration.reset();
            }
            if (LifecycleRejectedGeneration && view.Generation > *LifecycleRejectedGeneration) {
                LifecycleRejectedGeneration.reset();
            }
            auto next = Desired;
            next.GenerationHighWater = std::max(next.GenerationHighWater, view.Generation);
            if (next.PendingMutation && ResolvedBy(*next.PendingMutation, view)) {
                next.PendingMutation.reset();
                MayApplyElsewhere = false;
                ResetResends();
            }
            if (next != Desired) {
                Persist(std::move(next));
            }
            THealthReplies replies;
            for (auto& [uri, endpoint] : Endpoints) {
                auto& snapshot = endpoint.Snapshot;
                replies.Endpoints.push_back({uri, snapshot.Pile, snapshot.Identity, Selection.CurrentEndpoints.contains(uri)});
                const bool ignored = !ShouldPollPile(view, snapshot.Pile);
                if (ignored && !snapshot.Ignored && (endpoint.InventoryOnly || InventorySource == uri)) {
                    NextEndpointRefresh = TMonotonic::Zero();
                }
                snapshot.Ignored = ignored;
                if (snapshot.Ignored && endpoint.Discovered) {
                    endpoint.Retiring = true;
                }
                if (endpoint.Retiring) {
                    continue;
                }
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
        CurrentEndpoints = endpoints;
        if (!IsStopping() && !Dirty && Decision.Kind == EDecisionKind::Ready) {
            if (RejectedGeneration == Decision.Plan->ExpectedGeneration) {
                // B10: rejection blocks every dispatch at this generation,
                // including non-sending command evaluations and mode changes.
                Decision.Reason = "REJECTED_BY_SERVER";
            } else if (Desired.Mode == EMode::Auto) {
                MaybeSend(endpoints, now, dispatch);
            }
        }
        if (IsTransition(PreviousDecision, Decision)) {
            NJson::TJsonValue event(NJson::JSON_MAP);
            event["kind"] = ToString(Decision.Kind);
            event["reason"] = Decision.Reason;
            Options.Diagnostics->History("decision", std::move(event), NActors::TActivationContext::Now());
            YDB_LOG_INFO_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "Decision changed",
                {"event", "decision"}, {"kind", ToString(Decision.Kind)}, {"reason", Decision.Reason},
                {"plan", Decision.Plan ? Compact(PlanJson(*Decision.Plan)) : TString("null")});
            PreviousDecision = Decision;
        }
    }

    void MaybeSend(const std::vector<TEndpointState>& endpoints, TMonotonic now, bool dispatch) {
        // Decide admits at the pending generation only the identical update or
        // a D2 superset replacement, which replaces the record before it is sent.
        const bool replacing = Desired.PendingMutation && Desired.PendingMutation->Update != Decision.Plan->Update;
        if (Desired.PendingMutation && !replacing && !MayResend(LastSendAt, LastOutcome, now,
            Options.Config.Timing.MutationClientTimeout, AmbiguousResends))
        {
            return;
        }
        auto exclude = NotAppliedEndpoints;
        for (const auto& [slot, sequence] : Outstanding) {
            if (slot.second == EYdbMethod::Update) {
                exclude.insert(slot.first);
            }
        }
        auto plan = *Decision.Plan;
        const auto endpoint = SelectMutationEndpoint(*Selection.View, endpoints, plan.Update, Health.Connectivity, exclude, Health.Participation);
        if (!endpoint) {
            Decision = Blocked(EBlockerReason::NoMutationEndpoint);
            return;
        }
        plan.Endpoint = *endpoint;
        Decision.Plan = plan;
        Decision.CliCommand = RenderCliCommand(plan);
        // Commands evaluate the same route and blocker without handing off.
        if (dispatch) {
            Dispatch(plan, EOperationKind::Failover, EDispatchOrigin::Auto);
        }
    }

    bool Dispatch(TPlan plan, EOperationKind kind, EDispatchOrigin origin) {
        if (IsStopping() || Dirty || Outstanding.contains({plan.Endpoint, EYdbMethod::Update})) {
            return false;
        }
        const bool replacing = Desired.PendingMutation && Desired.PendingMutation->Update != plan.Update;
        const bool overlap = HasInFlightAttempt();
        auto next = Desired;
        next.PendingMutation = TPendingMutation{plan.ExpectedGeneration, plan.Update, kind, origin};
        // Dirty was rejected above; an identical record is already durable.
        if (next != Desired && !Persist(next)) {
            // Before rename preserve the old record; after rename follow the visible one.
            // Keep the error latched and retry the visible record before any dispatch.
            const auto visible = Options.StateFile->GetState();
            if (visible.PendingMutation == next.PendingMutation && replacing) {
                // Rename installed the superset even though it was not sent.
                // Retire the predecessor's callback authority and retry timing.
                ResetResends();
                MayApplyElsewhere = MayApplyElsewhere || overlap;
            }
            const auto effectiveMode = Desired.Mode;
            Desired = visible;
            Desired.Mode = effectiveMode;
            Dirty = true;
            Decision = Blocked(EBlockerReason::StateWriteFailed);
            return false;
        }
        if (replacing) {
            ResetResends();
        }
        MayApplyElsewhere = MayApplyElsewhere || overlap;
        if (IsStopping()) {
            return false; // keep the durable prepared record; shutdown cannot hand it off
        }
        const TUpdateAuthorization authorization{std::min(Selection.ValidUntil, Health.ValidUntil)};
        const auto expired = [&] {
            Decision = {};
            Decision.Kind = EDecisionKind::Unknown;
            Decision.Reason = "EVIDENCE_EXPIRED";
        };
        // Includes pending writes above and Dirty/high-water writes in Evaluate.
        if (NActors::TActivationContext::Monotonic() >= authorization.ValidUntil) {
            expired();
            return false;
        }
        TRpcRequest request{plan.Endpoint, Endpoints.at(plan.Endpoint).Snapshot.Pile, ++NextSequence};
        if (!Begin(request, EYdbMethod::Update)) {
            return false;
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
            return false;
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
        LastMutation = TMutationInvocation{};
        LastMutation->Id = sequence;
        LastMutation->SentAt = sentAt;
        LastMutation->Plan = plan;
        LastMutation->SentUtc = NActors::TActivationContext::Now();
        ++PendingAttempts;
        if (!PendingFirstSendUtc) {
            PendingFirstSendUtc = LastMutation->SentUtc;
        }
        PendingLastSendUtc = LastMutation->SentUtc;
        LastMutation->Kind = kind;
        LastMutation->Origin = origin;
        LastMutation->AmbiguousResends = AmbiguousResends;
        auto operation = PlanJson(plan);
        operation["kind"] = ToString(kind);
        operation["origin"] = ToString(origin);
        operation["sent_at"] = LastMutation->SentUtc.ToString();
        operation["status"] = "IN_FLIGHT";
        operation["may_still_complete"] = true;
        const auto id = BootId + ":operation:" + ::ToString(sequence);
        Options.Diagnostics->Operation(id, operation, true);
        Options.Diagnostics->History("operation_sent", std::move(operation), LastMutation->SentUtc);
        YDB_LOG_INFO_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "Mutation sent",
            {"event", "mutation_sent"}, {"invocation_id", sequence},
            {"expected_generation", plan.ExpectedGeneration}, {"endpoint", plan.Endpoint},
            {"update", Compact(UpdateJson(plan.Update))});
        return true;
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
        const auto id = BootId + ":operation:" + ::ToString(sequence);
        auto operation = Options.Diagnostics->ReadOperation(id).value_or(PlanJson(plan));
        operation["status"] = result.Rpc.Status;
        operation["outcome"] = ToString(outcome);
        operation["finished_at"] = NActors::TActivationContext::Now().ToString();
        operation["may_still_complete"] = outcome == EMutationOutcome::Ambiguous;
        Options.Diagnostics->Operation(id, operation, false);
        Options.Diagnostics->History("operation_outcome", std::move(operation), NActors::TActivationContext::Now());
        // An older invocation, or one whose record was resolved, only reports.
        if (!InFlight || InFlight->first != sequence || !Desired.PendingMutation
            || Desired.PendingMutation->ExpectedGeneration != plan.ExpectedGeneration
            || Desired.PendingMutation->Update != plan.Update) {
            return;
        }
        InFlight.reset();
        LastOutcome = outcome;
        LastSendAt = result.Rpc.SentAt;
        if (outcome == EMutationOutcome::Success || outcome == EMutationOutcome::GenerationMoved) {
            LastSuccess = Desired.PendingMutation;
            Persist(ApplyOutcome(Desired, outcome));
            if (!Desired.PendingMutation) {
                MayApplyElsewhere = false;
                ResetResends();
            }
        } else if (outcome == EMutationOutcome::NotAppliedHere) {
            // Not applied before distconf: resend the identical update elsewhere.
            NotAppliedEndpoints.insert(plan.Endpoint);
        } else if (outcome == EMutationOutcome::Ambiguous) {
            MayApplyElsewhere = true;
        } else if (outcome == EMutationOutcome::Rejected) {
            YDB_LOG_ERROR_CTX_COMP(*NActors::TlsActivationContext, SkipperLogComponent, "Mutation rejected",
                {"event", "mutation_rejected"}, {"category", "sdk"}, {"endpoint", plan.Endpoint},
                {"status", result.Rpc.Status}, {"issues", result.Rpc.Issues});
            if (Desired.PendingMutation->Kind == EOperationKind::Failover) {
                RejectedGeneration = plan.ExpectedGeneration;
            } else {
                LifecycleRejectedGeneration = plan.ExpectedGeneration;
            }
            // A rejection resolves only this invocation. Retain the durable
            // overlay across restart because earlier attempts may still commit.
        }
    }

    bool HasInFlightAttempt() const {
        return std::any_of(Outstanding.begin(), Outstanding.end(), [](const auto& entry) {
            return entry.first.second == EYdbMethod::Update;
        });
    }

    TDecision Available(EOperationKind action, const TString& target) const {
        auto refuse = [](TString reason) {
            TDecision result;
            result.Kind = EDecisionKind::Blocked;
            result.Reason = std::move(reason);
            return result;
        };
        if (IsStopping()) {
            return refuse("CONTROLLER_UNAVAILABLE");
        }
        if (Desired.Mode != EMode::Manual) {
            return refuse("MODE_NOT_MANUAL");
        }
        if (Dirty) {
            return refuse("STATE_WRITE_FAILED");
        }
        if (HasInFlightAttempt()) {
            return refuse("ATTEMPT_IN_FLIGHT");
        }
        if (Selection.View && (RejectedGeneration == Selection.View->Generation
            || (action != EOperationKind::Failover && LifecycleRejectedGeneration == Selection.View->Generation)))
        {
            return refuse("REJECTED_AT_GENERATION");
        }
        if (action == EOperationKind::Failover) {
            return Decision;
        }
        return PlanLifecycle(action, target, Selection, Health, Confirmations, Decision,
            Desired.PendingMutation, CurrentEndpoints, NotAppliedEndpoints);
    }

    TPileActionMap BuildActionMap() const {
        TPileActionMap actions;
        if (Selection.View) {
            for (const auto& [name, state] : Selection.View->Piles) {
                (void)state;
                for (const auto kind : {EOperationKind::Failover, EOperationKind::Switchover, EOperationKind::TakeOffline, EOperationKind::Rejoin}) {
                    actions[name][kind] = Available(kind, name);
                }
            }
        }
        return actions;
    }

    void RefreshRevision(const TPileActionMap& actions) {
        NJson::TJsonValue signature(NJson::JSON_MAP);
        signature["mode"] = ToString(Desired.Mode);
        signature["mode_revision"] = ::ToString(ModeRevision);
        signature["decision"] = ToString(Decision.Kind) + ":" + Decision.Reason;
        signature["plan"] = Decision.Plan ? PlanJson(*Decision.Plan) : NJson::TJsonValue();
        signature["dirty"] = Dirty;
        signature["inflight"] = HasInFlightAttempt();
        signature["uncertain"] = MayApplyElsewhere;
        signature["stopping"] = Stopping;
        signature["latch"] = RejectedGeneration ? ::ToString(*RejectedGeneration) : TString();
        signature["lifecycle_latch"] = LifecycleRejectedGeneration ? ::ToString(*LifecycleRejectedGeneration) : TString();
        signature["pending"] = Desired.PendingMutation ? UpdateJson(Desired.PendingMutation->Update) : NJson::TJsonValue();
        if (Selection.View) {
            signature["generation"] = ::ToString(Selection.View->Generation);
            signature["states"] = UpdateJson(Selection.View->Piles);
            signature["conversion"] = Selection.View->ConversionInProgress;
            if (Selection.View->Pending) {
                signature["pile_operation"] = Selection.View->Pending->Kind;
                signature["pile_operation_target"] = Selection.View->Pending->Pile ? NJson::TJsonValue(*Selection.View->Pending->Pile) : NJson::TJsonValue();
            }
            for (const auto& [name, pileActions] : actions) {
                for (const auto& [kind, decision] : pileActions) {
                    if (kind == EOperationKind::Failover) {
                        continue; // the common failover decision is already in the signature
                    }
                    signature["actions"][name][ToString(kind)]["reason"] = decision.Reason;
                    signature["actions"][name][ToString(kind)]["plan"] = decision.Plan ? PlanJson(*decision.Plan) : NJson::TJsonValue();
                }
            }
        }
        for (const auto& [name, verdict] : Health.Piles) {
            signature["health"][name] = ToString(verdict.Verdict);
        }
        for (const auto& [name, participation] : Health.Participation) {
            signature["participation"][name] = participation.Value ? NJson::TJsonValue(*participation.Value) : NJson::TJsonValue();
        }
        const auto bytes = Compact(signature);
        if (bytes != SemanticSignature) {
            SemanticSignature = bytes;
            ++SemanticRevision;
        }
        SnapshotRevision = BootId + ":snapshot:" + ::ToString(SemanticRevision);
    }

    TCommandResult Error(unsigned status, const TString& code, const TString& reason = {}) const {
        TCommandResult result;
        result.Status = status;
        result.Body["error"] = code;
        result.Body["reason"] = reason.empty() ? code : reason;
        return result;
    }

    TCommandResult ChangeMode(const TControllerCommand& command, TMonotonic now) {
        if (command.ExpectedBootId != BootId || command.ExpectedModeRevision != ModeRevision) {
            return Error(409, "MODE_REVISION_CHANGED");
        }
        ++ModeRevision; // matching requests consume the token, even on refusal or no-op
        if (command.Mode == EMode::Auto) {
            if (command.SnapshotRevision != SnapshotRevision) {
                return Error(409, "SNAPSHOT_CHANGED");
            }
            if (Desired.PendingMutation && Desired.PendingMutation->Kind != EOperationKind::Failover) {
                return Error(409, "PENDING_MUTATION", "Auto cannot retry lifecycle work");
            }
        }
        auto next = Desired;
        next.Mode = command.Mode;
        const auto previousMode = Desired.Mode;
        if (!Persist(next)) {
            const auto error = StateWriteError;
            // Observe takes effect immediately even when the old file survives.
            // A failed activation keeps the previous in-memory mode unless the
            // rename installed it. The old file may still say Auto after Off.
            if (command.Mode != EMode::Observe && !Options.StateFile->WasLastWriteRenamed()) {
                Desired.Mode = previousMode;
            }
            Evaluate(now, false);
            if (Dirty || Desired.Mode != command.Mode) {
                return Error(503, "STATE_WRITE_FAILED", StateWriteError.empty() ? error : StateWriteError);
            }
        }
        NJson::TJsonValue event(NJson::JSON_MAP);
        event["from"] = ToString(previousMode);
        event["mode"] = ToString(Desired.Mode);
        Options.Diagnostics->History("mode", std::move(event), NActors::TActivationContext::Now());
        Evaluate(now, true);
        TCommandResult result;
        result.Body["mode"] = ToString(Desired.Mode);
        result.Body["mode_revision"] = ::ToString(ModeRevision);
        result.Body["may_still_complete"] = Desired.PendingMutation.has_value();
        return result;
    }

    TCommandResult CreateReview(const TControllerCommand& command, TMonotonic now) {
        if (command.Action != EOperationKind::Failover && !command.Target) {
            return Error(400, "TARGET_REQUIRED");
        }
        if (command.SnapshotRevision != SnapshotRevision) {
            return Error(409, "SNAPSHOT_CHANGED");
        }
        const auto decision = Available(command.Action, command.Target.value_or(TString()));
        if (decision.Kind != EDecisionKind::Ready || !decision.Plan) {
            const auto code = decision.Reason == "MODE_NOT_MANUAL" || decision.Reason == "ATTEMPT_IN_FLIGHT"
                || decision.Reason == "REJECTED_AT_GENERATION" ? decision.Reason : TString("PLAN_CHANGED");
            return Error(Dirty ? 503 : 409, Dirty ? TString("STATE_WRITE_FAILED")
                : decision.Blocker == EBlockerReason::PendingMutation ? TString("PENDING_MUTATION") : code, decision.Reason);
        }
        const TString id = BootId + ":plan:" + ::ToString(++NextPlan);
        Reviews.emplace(id, TReview{command, *decision.Plan, ModeRevision, now + TDuration::Seconds(120)});
        ReviewOrder.push_back(id);
        while (ReviewOrder.size() > 256) {
            Reviews.erase(ReviewOrder.front());
            ReviewOrder.pop_front();
        }
        TCommandResult result;
        result.Body["plan"] = PlanJson(*decision.Plan);
        result.Body["plan_id"] = id;
        result.Body["action"] = ToString(command.Action);
        result.Body["target_pile"] = command.Target ? NJson::TJsonValue(*command.Target) : NJson::TJsonValue();
        result.Body["cluster_id"] = Options.Config.ClusterId;
        result.Body["mode_revision"] = ::ToString(ModeRevision);
        result.Body["expires_at"] = (NActors::TActivationContext::Now() + TDuration::Seconds(120)).ToString();
        result.Body["reason"] = decision.Reason;
        result.Body["before"] = UpdateJson(Selection.View->Piles);
        auto after = Selection.View->Piles;
        for (const auto& [pile, state] : decision.Plan->Update) {
            after[pile] = state;
        }
        result.Body["after"] = UpdateJson(after);
        std::optional<TString> effectivePrimary;
        if (command.Action == EOperationKind::Switchover) {
            effectivePrimary = command.Target;
        } else {
            for (const auto& [pile, state] : after) {
                if (state == EPileState::Primary) {
                    effectivePrimary = pile;
                }
            }
        }
        NJson::TJsonValue alternates(NJson::JSON_ARRAY);
        for (const auto& [pile, state] : after) {
            if (IsSynchronized(state) && (!effectivePrimary || pile != *effectivePrimary)) {
                alternates.AppendValue(pile);
            }
        }
        result.Body["alternate_synchronized_piles"] = static_cast<ui64>(alternates.GetArray().size());
        result.Body["alternate_synchronized_pile_names"] = std::move(alternates);
        result.Body["cli_command"] = decision.CliCommand;
        result.Body["pending_mutation"] = Desired.PendingMutation ? PlanJson(TPlan{Desired.PendingMutation->ExpectedGeneration,
            Desired.PendingMutation->Update, decision.Plan->Endpoint}) : NJson::TJsonValue();
        result.Body["evidence"] = NJson::TJsonValue(NJson::JSON_MAP);
        for (const auto& [name, verdict] : Health.Piles) {
            result.Body["evidence"][name]["verdict"] = ToString(verdict.Verdict);
            for (const auto& reason : verdict.Reasons) {
                if (!result.Body["evidence"][name]["reasons"].IsArray()) {
                    result.Body["evidence"][name]["reasons"].SetType(NJson::JSON_ARRAY);
                }
                result.Body["evidence"][name]["reasons"].AppendValue(reason);
            }
        }
        return result;
    }

    TCommandResult ExecuteReview(const TControllerCommand& command, TMonotonic now) {
        const auto found = Reviews.find(command.PlanId);
        if (found == Reviews.end()) {
            return Error(409, command.PlanId.StartsWith(BootId + ":plan:") ? "PLAN_CONSUMED" : "PLAN_UNKNOWN");
        }
        const auto review = found->second;
        Reviews.erase(found); // refusal also consumes the review
        const auto decision = Available(review.Command.Action, review.Command.Target.value_or(TString()));
        if (Desired.Mode != EMode::Manual) {
            return Error(409, "MODE_NOT_MANUAL");
        }
        if (HasInFlightAttempt()) {
            return Error(409, "ATTEMPT_IN_FLIGHT");
        }
        if (Dirty) {
            return Error(503, "STATE_WRITE_FAILED", StateWriteError);
        }
        if (decision.Reason == "REJECTED_AT_GENERATION") {
            return Error(409, "REJECTED_AT_GENERATION");
        }
        if (now >= review.ExpiresAt || ModeRevision != review.ModeRevision
            || SnapshotRevision != review.Command.SnapshotRevision || !decision.Plan
            || *decision.Plan != review.Plan || decision.Kind != EDecisionKind::Ready)
        {
            return Error(409, "PLAN_CHANGED", decision.Reason);
        }
        if (!Dispatch(review.Plan, review.Command.Action, EDispatchOrigin::Manual)) {
            if (IsStopping()) {
                return Error(503, "CONTROLLER_UNAVAILABLE");
            }
            if (Decision.Reason == "EVIDENCE_EXPIRED") {
                return Error(409, "EVIDENCE_EXPIRED");
            }
            return Error(503, "STATE_WRITE_FAILED", StateWriteError);
        }
        TCommandResult result;
        result.Status = 202;
        result.Body["operation_id"] = BootId + ":operation:" + ::ToString(LastMutation->Id);
        return result;
    }

    void HandleCommand(TEvSkipper::TEvCommand::TPtr& ev) {
        const auto& command = ev->Get()->Command;
        TCommandResult result;
        const auto receipt = Receipts.find(command.RequestId);
        const bool recorded = command.Kind != ECommandKind::CreatePlan;
        if (IsStopping()) {
            result = Error(503, "CONTROLLER_UNAVAILABLE");
        } else if (recorded && receipt != Receipts.end()) {
            result = receipt->second.first == command ? receipt->second.second : Error(409, "REQUEST_ID_CONFLICT");
        } else {
            const auto now = NActors::TActivationContext::Monotonic();
            // No Auto dispatch here: Off must be processed before any subsequent sends.
            Evaluate(now, false);
            RefreshRevision(BuildActionMap());
            if (command.Kind == ECommandKind::Mode) {
                result = ChangeMode(command, now);
            } else if (command.Kind == ECommandKind::CreatePlan) {
                result = CreateReview(command, now);
            } else {
                result = ExecuteReview(command, now);
            }
            if (recorded) {
                Receipts.emplace(command.RequestId, std::make_pair(command, result));
                ReceiptOrder.push_back(command.RequestId);
                while (ReceiptOrder.size() > 1024) {
                    Receipts.erase(ReceiptOrder.front());
                    ReceiptOrder.pop_front();
                }
            }
            Publish(now);
        }
        Send(ev->Sender, new TEvSkipper::TEvCommandResult(std::move(result)), 0, ev->Cookie);
    }

    void HandleReady(TEvSkipper::TEvReady::TPtr& ev) {
        Send(ev->Sender, new TEvSkipper::TEvReadyAck(), 0, ev->Cookie);
    }

    void Publish(TMonotonic now) {
        auto actions = BuildActionMap();
        RefreshRevision(actions);
        auto snapshot = std::make_shared<TSkipperSnapshot>();
        snapshot->ClusterId = Options.Config.ClusterId;
        snapshot->Mode = Desired.Mode;
        snapshot->ControllerBootId = BootId;
        snapshot->ModeRevision = ModeRevision;
        snapshot->SnapshotRevision = SnapshotRevision;
        snapshot->ControllerReady = !IsStopping();
        snapshot->Stopping = IsStopping();
        snapshot->EvidenceTtl = Options.Config.Timing.EvidenceTtl;
        snapshot->LastObservedView = PreviousView;
        snapshot->MayApplyElsewhere = MayApplyElsewhere;
        snapshot->PendingAttempts = PendingAttempts;
        snapshot->PendingFirstSendUtc = PendingFirstSendUtc;
        snapshot->PendingLastSendUtc = PendingLastSendUtc;
        if (Desired.Mode == EMode::Auto && Desired.PendingMutation && LastOutcome == EMutationOutcome::Ambiguous && LastSendAt) {
            const ui64 factor = ui64{1} << std::min<ui64>(AmbiguousResends, 20);
            snapshot->RetryAt = *LastSendAt + std::min(Options.Config.Timing.MutationClientTimeout * factor, TDuration::Minutes(5));
        }
        snapshot->RejectedGeneration = RejectedGeneration;
        snapshot->LifecycleRejectedGeneration = LifecycleRejectedGeneration;
        snapshot->BootAt = BootAt;
        snapshot->BootUtc = BootUtc;
        snapshot->Diagnostics = Options.Diagnostics;
        snapshot->LastTickAt = now;
        snapshot->Selection = Selection;
        snapshot->GenerationHighWater = Desired.GenerationHighWater;
        for (const auto& [uri, endpoint] : Endpoints) {
            (void)uri;
            if (!endpoint.Retiring && !endpoint.Snapshot.Ignored) {
                snapshot->Endpoints.push_back(endpoint.Snapshot);
            }
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
        for (const auto& [name, pile] : snapshot->Piles) {
            if (pile.Verdict.Verdict != EPileVerdict::Unknown) {
                LastKnownPiles[name] = pile;
            }
        }
        if (Selection.View) {
            std::erase_if(LastKnownPiles, [&](const auto& entry) { return !Selection.View->Piles.contains(entry.first); });
        }
        snapshot->LastKnownPiles = LastKnownPiles;
        snapshot->Participation = Health.Participation;
        snapshot->Actions = std::move(actions);
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
        hFunc(TEvSkipper::TEvInventoryReply, HandleInventory);
        hFunc(TEvSkipper::TEvStop, HandleStop);
        hFunc(TEvSkipper::TEvCommand, HandleCommand);
        hFunc(TEvSkipper::TEvReady, HandleReady);
    )

    struct TReview {
        TControllerCommand Command;
        TPlan Plan;
        ui64 ModeRevision = 0;
        TMonotonic ExpiresAt;
    };

    TSkipperOptions Options;
    TString BootId;
    TMonotonic BootAt;
    TInstant BootUtc;
    ui64 ModeRevision = 1;
    ui64 SemanticRevision = 0;
    ui64 NextPlan = 0;
    TString SnapshotRevision;
    TString SemanticSignature;
    bool MayApplyElsewhere = false;
    ui64 PendingAttempts = 0;
    std::optional<TInstant> PendingFirstSendUtc;
    std::optional<TInstant> PendingLastSendUtc;
    std::optional<ui64> LifecycleRejectedGeneration;
    std::vector<TEndpointState> CurrentEndpoints;
    std::map<TString, TPileSnapshot> LastKnownPiles;
    std::map<TString, TReview> Reviews;
    std::deque<TString> ReviewOrder;
    std::map<TString, std::pair<TControllerCommand, TCommandResult>> Receipts;
    std::deque<TString> ReceiptOrder;
    TPersistentState Desired;
    bool Dirty = false;
    TString StateWriteError;
    bool Stopping = false;
    TMonotonic NextStatePoll;
    TMonotonic NextHealthRound;
    TMonotonic NextInventory;
    size_t InventoryRotation = 0;
    std::map<TString, size_t> DiscoveryRotation;
    bool InventoryInFlight = false;
    std::optional<TString> InventorySource;
    std::vector<TDiscoveredEndpoint> InventoryCandidates;
    size_t InventoryDiscoveryRotation = 0;
    TMonotonic NextEndpointRefresh;
    ui64 NextSequence = 0;
    std::map<TString, TEndpointData> Endpoints;
    std::map<TString, size_t> HealthRotation;
    std::map<TString, size_t> ConnectivityRotation;
    std::set<TString> WarnedAbsent;
    std::map<TSlot, ui64> Outstanding;
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
