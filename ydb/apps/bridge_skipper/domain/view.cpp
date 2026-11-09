#include "view.h"

#include <algorithm>

namespace NBridgeSkipper {
namespace {

bool CompatibleStates(const TNamedPileStates& left, const TNamedPileStates& right) {
    for (const auto& [name, state] : left) {
        const auto other = right.find(name);
        if (other == right.end()) {
            if (state != EPileState::Inactive) {
                return false;
            }
        } else if (other->second != state) {
            return false;
        }
    }
    return true;
}

// An absent pile counts as INACTIVE.
EPileState StateOf(const TNamedPileStates& piles, const TString& name) {
    const auto found = piles.find(name);
    return found == piles.end() ? EPileState::Inactive : found->second;
}

bool OnlyFollowUps(const TNamedPileStates& base, const TNamedPileStates& next) {
    std::optional<TString> primary;
    std::optional<TString> promoted;
    std::set<TString> names;
    for (const auto& [name, state] : base) {
        names.insert(name);
        if (state == EPileState::Primary) {
            primary = name;
        } else if (state == EPileState::Promoted) {
            promoted = name;
        }
    }
    for (const auto& entry : next) {
        names.insert(entry.first);
    }
    const bool promotionCompleted = primary && promoted && StateOf(next, *primary) == EPileState::Synchronized
        && StateOf(next, *promoted) == EPileState::Primary;
    for (const auto& name : names) {
        const auto before = StateOf(base, name);
        const auto after = StateOf(next, name);
        if (before == after
            || (before == EPileState::NotSynchronized && after == EPileState::Synchronized)
            || (before == EPileState::Suspended && after == EPileState::Disconnected)
            || (promotionCompleted && (name == *primary || name == *promoted)))
        {
            continue;
        }
        return false;
    }
    return true;
}

} // namespace

TViewSelection SelectView(
    const std::vector<TClusterView>& views,
    ui64 highWater,
    TMonotonic now,
    TDuration ttl)
{
    TViewSelection result;
    std::vector<const TClusterView*> candidates;
    ui64 generation = 0;
    for (const auto& view : views) {
        if (now - view.SentAt >= ttl) {
            continue;
        }
        if (candidates.empty() || view.Generation > generation) {
            candidates.clear();
            generation = view.Generation;
        }
        if (view.Generation == generation) {
            candidates.push_back(&view);
        }
    }
    if (candidates.empty()) {
        result.Blocker = EBlockerReason::NoView;
        return result;
    }
    const auto* newest = candidates.front();
    for (const auto* view : candidates) {
        if (!CompatibleStates(newest->Piles, view->Piles) || !CompatibleStates(view->Piles, newest->Piles)) {
            result.Blocker = EBlockerReason::ViewConflict;
            return result;
        }
        if (view->SentAt > newest->SentAt || (view->SentAt == newest->SentAt && view->Endpoint < newest->Endpoint)) {
            newest = view;
        }
    }
    if (generation < highWater) {
        result.Blocker = EBlockerReason::StaleHistory;
        return result;
    }
    result.View = *newest;
    for (const auto* view : candidates) {
        result.CurrentEndpoints.insert(view->Endpoint);
        result.ValidUntil = std::min(result.ValidUntil, view->SentAt + ttl);
    }
    return result;
}

ETransitionExplanation ExplainTransition(
    const TClusterView& previous,
    const TClusterView& next,
    const std::optional<TPendingMutation>& pending)
{
    if (next.Generation < previous.Generation) {
        return ETransitionExplanation::Unexplained;
    }
    if (next.Generation == previous.Generation) {
        // SelectView treats these as one view; it may alternate between them.
        if (CompatibleStates(previous.Piles, next.Piles) && CompatibleStates(next.Piles, previous.Piles)) {
            return ETransitionExplanation::Explained;
        }
        return ETransitionExplanation::Unexplained;
    }
    if (OnlyFollowUps(previous.Piles, next.Piles)) {
        return ETransitionExplanation::Explained;
    }
    if (pending && pending->ExpectedGeneration == previous.Generation) {
        auto expected = previous.Piles;
        for (const auto& [name, state] : pending->Update) {
            expected[name] = state;
        }
        if (OnlyFollowUps(expected, next.Piles)) {
            return ETransitionExplanation::Explained;
        }
    }
    return ETransitionExplanation::Unexplained;
}

} // namespace NBridgeSkipper
