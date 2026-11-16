#include "policy.h"
#include "mutation.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace NBridgeSkipper {
namespace {

bool IsDown(EPileVerdict verdict) {
    return verdict == EPileVerdict::Failed || verdict == EPileVerdict::Lost;
}

const TPileVerdict& PileVerdict(const THealthEvaluation& health, const TString& pile) {
    static const TPileVerdict unknown;
    const auto found = health.Piles.find(pile);
    return found == health.Piles.end() ? unknown : found->second;
}

TDecision Blocked(EBlockerReason reason) {
    TDecision decision;
    decision.Kind = EDecisionKind::Blocked;
    decision.Blocker = reason;
    decision.Reason = ToString(reason);
    return decision;
}

TString ShellQuote(const TString& value) {
    TString result = "'";
    for (const char c : value) {
        if (c == '\'') {
            result += "'\\''";
        } else {
            result += c;
        }
    }
    result += '\'';
    return result;
}

TString PileList(const std::set<TString>& piles) {
    TString result;
    for (const auto& pile : piles) {
        if (!result.empty()) {
            result += ", ";
        }
        result += ShellQuote(pile);
    }
    return result;
}

TExactUpdate BuildFailoverUpdate(
    const TClusterView& view,
    const std::set<TString>& down,
    const TString& survivor)
{
    TExactUpdate update;
    bool primaryDown = false;
    for (const auto& pile : down) {
        update[pile] = EPileState::Disconnected;
        const auto found = view.Piles.find(pile);
        if (found != view.Piles.end() && found->second == EPileState::Primary) {
            primaryDown = true;
        }
    }
    if (primaryDown) {
        update[survivor] = EPileState::Primary;
        for (const auto& [pile, state] : view.Piles) {
            if (state == EPileState::Promoted && pile != survivor && !down.contains(pile)) {
                update[pile] = EPileState::Synchronized;
            }
        }
    }
    return update;
}

// D2: a plan at the pending generation may replace the record only when it
// keeps every pending entry and adds nothing but DISCONNECTED targets, so
// both plans commit only in a component holding the common primary's quorum.
bool IsSupersetReplacement(const TExactUpdate& pending, const TExactUpdate& update) {
    for (const auto& [pile, state] : pending) {
        const auto it = update.find(pile);
        if (it == update.end() || it->second != state) {
            return false;
        }
    }
    for (const auto& [pile, state] : update) {
        if (!pending.contains(pile) && state != EPileState::Disconnected) {
            return false;
        }
    }
    return true;
}

std::pair<std::optional<TString>, std::optional<TString>> Roles(const TNamedPileStates& piles) {
    std::pair<std::optional<TString>, std::optional<TString>> roles;
    for (const auto& [pile, state] : piles) {
        if (state == EPileState::Primary) {
            roles.first = pile;
        } else if (state == EPileState::Promoted) {
            roles.second = pile;
        }
    }
    return roles;
}

} // namespace

void TConfirmations::Update(const TClusterView& view, const std::map<TString, TPileVerdict>& verdicts, TMonotonic now) {
    if (Generation != view.Generation) {
        if (Roles(States) != Roles(view.Piles)) {
            Piles.clear();
        } else {
            for (auto it = Piles.begin(); it != Piles.end();) {
                const auto state = view.Piles.find(it->first);
                const auto previous = States.find(it->first);
                if (state == view.Piles.end() || previous == States.end() || state->second != previous->second
                    || it->second.FailureEvidence != EFailureEvidence::Ordinary)
                {
                    it = Piles.erase(it);
                } else {
                    ++it;
                }
            }
        }
        Generation = view.Generation;
        States = view.Piles;
    }
    for (auto it = Piles.begin(); it != Piles.end();) {
        const auto verdict = verdicts.find(it->first);
        if (verdict == verdicts.end() || !IsDown(verdict->second.Verdict)
            || verdict->second.FailureEvidence != it->second.FailureEvidence)
        {
            it = Piles.erase(it);
        } else {
            ++it;
        }
    }
    for (const auto& [pile, verdict] : verdicts) {
        if (!IsDown(verdict.Verdict)) {
            continue;
        }
        auto [it, inserted] = Piles.try_emplace(pile,
            TConfirmation{now, verdict.NewestEvidenceSentAt, verdict.NewestEvidenceSentAt, verdict.FailureEvidence});
        if (!inserted) {
            it->second.NewestEvidenceSentAt = verdict.NewestEvidenceSentAt;
        }
    }
}

bool TConfirmations::IsConfirmed(const TString& pile, TDuration confirmDuration) const {
    const auto found = Piles.find(pile);
    if (found == Piles.end()) {
        return false;
    }
    const auto& confirmation = found->second;
    return confirmation.NewestEvidenceSentAt - confirmation.FirstEvidenceSentAt >= confirmDuration;
}

TDecision Decide(
    const TViewSelection& selection,
    const THealthEvaluation& health,
    const TConfirmations& confirmations,
    const std::optional<TPendingMutation>& pending,
    const std::vector<TEndpointState>& endpoints,
    TDuration confirmDuration,
    const std::set<TString>& excludedEndpoints)
{
    if (selection.Blocker) {
        return Blocked(*selection.Blocker);
    }
    if (!selection.View) {
        return Blocked(EBlockerReason::NoView);
    }
    const auto& view = *selection.View;
    std::optional<TString> primary;
    std::optional<TString> promoted;
    for (const auto& [pile, state] : view.Piles) {
        if (state == EPileState::Suspended && PileVerdict(health, pile).Verdict == EPileVerdict::Lost) {
            return Blocked(EBlockerReason::SuspendedPileLost);
        }
        if (state == EPileState::Primary) {
            primary = pile;
        } else if (state == EPileState::Promoted) {
            promoted = pile;
        }
    }

    // Confirmations are per generation; only the view knows the current one.
    const bool sameGeneration = confirmations.Generation == view.Generation;
    std::set<TString> down;
    std::set<TString> unknown;
    bool confirming = false;
    for (const auto& pile : health.Participating) {
        const auto verdict = PileVerdict(health, pile).Verdict;
        if (IsDown(verdict)) {
            down.insert(pile);
            if (!sameGeneration || !confirmations.IsConfirmed(pile, confirmDuration)) {
                confirming = true;
            }
        } else if (verdict != EPileVerdict::Healthy && verdict != EPileVerdict::Replicating) {
            unknown.insert(pile);
        }
    }
    if (confirming) {
        TDecision decision;
        decision.Kind = EDecisionKind::Confirming;
        decision.Reason = "CONFIRMING: " + PileList(down);
        return decision;
    }
    if (down.empty()) {
        TDecision decision;
        decision.Kind = unknown.empty() ? EDecisionKind::Stable : EDecisionKind::Unknown;
        decision.Reason = unknown.empty() ? TString("NO_DOWN_PILES") : TString("UNKNOWN: ") + PileList(unknown);
        return decision;
    }
    // SelectView admits only views with exactly one primary.
    Y_ENSURE(primary);
    // Without BSC, require a current own fallback observation as well as self
    // quorum and no attributed RED/GREY part. This is not proof of disk health.
    const auto eligible = [&](const TString& pile) {
        const auto& verdict = PileVerdict(health, pile);
        return verdict.Verdict == EPileVerdict::Healthy
            || (!health.BscAvailable && verdict.SelfQuorum && verdict.OwnFallback && !verdict.FallbackRed);
    };

    std::optional<TString> survivor;
    if (!down.contains(*primary)) {
        // A primary that is not Down stays primary whatever its own verdict:
        // disconnecting the confirmed Down piles keeps every role.
        survivor = primary;
    } else {
        if (promoted && !down.contains(*promoted) && eligible(*promoted)) {
            survivor = promoted;
        }
        if (!survivor) {
            for (const auto& [pile, state] : view.Piles) {
                if (state == EPileState::Synchronized && !down.contains(pile) && eligible(pile)) {
                    survivor = pile;
                    break;
                }
            }
        }
        if (!survivor) {
            return Blocked(EBlockerReason::NoCandidate);
        }
    }

    const auto update = BuildFailoverUpdate(view, down, *survivor);
    // BuildFailoverUpdate emits only this shape; the server validates the rest.
    Y_DEBUG_ABORT_UNLESS(IsFailoverShape(view, update));
    // The commit uses the receiving node's view of the quorum, so the endpoint
    // must itself see every pile that stays in the config quorum.
    const auto endpoint = SelectMutationEndpoint(view, endpoints, update, health.Connectivity, excludedEndpoints, health.Participation);
    if (!endpoint) {
        return Blocked(EBlockerReason::NoMutationEndpoint);
    }
    if (pending && pending->ExpectedGeneration == view.Generation && (pending->Kind != EOperationKind::Failover || !IsSupersetReplacement(pending->Update, update))) {
        return Blocked(EBlockerReason::PendingMutation);
    }
    TDecision decision;
    decision.Kind = EDecisionKind::Ready;
    decision.Reason = "CONFIRMED_DOWN: " + PileList(down);
    decision.Plan = TPlan{view.Generation, update, *endpoint};
    decision.CliCommand = RenderCliCommand(*decision.Plan);
    return decision;
}

std::set<TString> LifecycleEndpointExclusions(
    EOperationKind kind,
    const TExactUpdate& update,
    const std::vector<TEndpointState>& endpoints,
    const std::set<TString>& excludedEndpoints)
{
    auto excluded = excludedEndpoints;
    if (kind == EOperationKind::TakeOffline) {
        for (const auto& endpoint : endpoints) {
            const auto target = update.find(endpoint.Pile);
            if (target != update.end() && target->second == EPileState::Suspended) {
                excluded.insert(endpoint.Endpoint);
            }
        }
    }
    return excluded;
}

TDecision PlanLifecycle(
    EOperationKind kind,
    const TString& target,
    const TViewSelection& selection,
    const THealthEvaluation& health,
    const TConfirmations& confirmations,
    const TDecision& failover,
    const std::optional<TPendingMutation>& pending,
    const std::vector<TEndpointState>& endpoints,
    const std::set<TString>& excludedEndpoints)
{
    const auto refuse = [](TString reason) {
        TDecision result;
        result.Kind = EDecisionKind::Blocked;
        result.Reason = std::move(reason);
        return result;
    };
    if (selection.Blocker || !selection.View) {
        return refuse("Fresh coherent state required");
    }
    const auto& view = *selection.View;
    const auto item = view.Piles.find(target);
    if (item == view.Piles.end()) {
        return refuse("Target pile is absent");
    }
    if (view.ConversionInProgress || (view.Pending && (!view.Pending->Pile || *view.Pending->Pile == target))) {
        return refuse("Target has a pending pile operation or conversion");
    }
    if (failover.Kind == EDecisionKind::Ready || failover.Kind == EDecisionKind::Confirming || !confirmations.Piles.empty()) {
        return refuse("Resolve the participating-pile incident first");
    }
    for (const auto& [name, state] : view.Piles) {
        const auto participation = EvaluatedParticipation(name, state, health.Connectivity, health.Participation);
        const auto verdict = PileVerdict(health, name).Verdict;
        if (!participation || (*participation && verdict != EPileVerdict::Healthy && verdict != EPileVerdict::Replicating)
            || (state == EPileState::Suspended && verdict == EPileVerdict::Lost))
        {
            return refuse("Participation and healthy or replicating participants required");
        }
    }
    TExactUpdate update;
    if (kind == EOperationKind::Switchover) {
        if (item->second != EPileState::Synchronized || PileVerdict(health, target).Verdict != EPileVerdict::Healthy) {
            return refuse("Switchover requires a healthy SYNCHRONIZED target");
        }
        update[target] = EPileState::Promoted;
        for (const auto& [name, state] : view.Piles) {
            if (state == EPileState::Promoted) {
                update[name] = EPileState::Synchronized;
            }
        }
    } else if (kind == EOperationKind::TakeOffline) {
        if (item->second != EPileState::Synchronized) {
            return refuse("Take offline requires a non-primary SYNCHRONIZED target");
        }
        update[target] = EPileState::Suspended;
    } else if (kind == EOperationKind::Rejoin) {
        if (item->second != EPileState::Disconnected) {
            return refuse("Rejoin requires DISCONNECTED; repeating it resets synchronization");
        }
        update[target] = EPileState::NotSynchronized;
    } else {
        return refuse("Unsupported lifecycle action");
    }
    if (pending && (pending->ExpectedGeneration != view.Generation || pending->Kind != kind || pending->Update != update)) {
        auto result = refuse("Incompatible pending mutation may still complete");
        result.Blocker = EBlockerReason::PendingMutation;
        return result;
    }
    const auto exclude = LifecycleEndpointExclusions(kind, update, endpoints, excludedEndpoints);
    const auto endpoint = SelectMutationEndpoint(view, endpoints, update, health.Connectivity, exclude, health.Participation);
    if (!endpoint) {
        return refuse("No surviving mutation endpoint");
    }
    TDecision result;
    result.Kind = EDecisionKind::Ready;
    result.Reason = "Reviewed lifecycle operation; zero data loss";
    result.Plan = TPlan{view.Generation, std::move(update), *endpoint};
    result.CliCommand = RenderCliCommand(*result.Plan);
    return result;
}

TString RenderCliCommand(const TPlan& plan) {
    TString command = "ydb -e " + ShellQuote(plan.Endpoint) + " admin cluster bridge update";
    const bool needsFile = std::any_of(plan.Update.begin(), plan.Update.end(), [](const auto& entry) {
        // --set splits at the first colon and rejects an empty pile name.
        return entry.first.empty() || entry.first.Contains(':');
    });
    if (needsFile) {
        NJson::TJsonValue updates(NJson::JSON_ARRAY);
        for (const auto& [pile, state] : plan.Update) {
            NJson::TJsonValue entry(NJson::JSON_MAP);
            entry.InsertValue("pile_name", pile);
            entry.InsertValue("state", ToString(state));
            updates.AppendValue(entry);
        }
        command = "printf '%s' " + ShellQuote(NJson::WriteJson(updates, false)) + " | " + command;
        command += " --file '/dev/stdin'";
    } else {
        for (const auto& [pile, state] : plan.Update) {
            command += " --set " + ShellQuote(pile + ":" + ToString(state));
        }
    }
    command += " --expected-generation " + ShellQuote(::ToString(plan.ExpectedGeneration));
    return command;
}

} // namespace NBridgeSkipper
