#pragma once

#include "health.h"
#include "view.h"

namespace NBridgeSkipper {

// D8: Down is confirmed when the send times of the evidence that supports the
// Down verdict span at least `confirm`. A failed SelfCheck keeps the cached
// reply until the TTL, so elapsed time could confirm on cached replies that span
// only about one round. The entry is erased whenever the verdict stops being
// Down. The span is not elapsed time since the first Down observation: it starts
// at the newest supporting evidence of that observation, so confirmation can
// complete less than `confirm` after DownSince.
struct TConfirmation {
    // Observation time of the first Down evaluation, kept for display only.
    TMonotonic DownSince;
    // The verdict's newest evidence at the first Down evaluation.
    TMonotonic FirstEvidenceSentAt;
    TMonotonic NewestEvidenceSentAt;
};

// Entries belong to Generation. A generation change keeps an entry only when
// the pile's state and the PRIMARY and PROMOTED roles are unchanged between
// the two views (a no-op bump); any other change clears it (B9).
struct TConfirmations {
    std::optional<ui64> Generation;
    TNamedPileStates States;
    std::map<TString, TConfirmation> Piles;

    void Update(const TClusterView& view, const std::map<TString, TPileVerdict>& verdicts, TMonotonic now);

    bool IsConfirmed(const TString& pile, TDuration confirmDuration) const;
};

TDecision Decide(
    const TViewSelection& selection,
    const THealthEvaluation& health,
    const TConfirmations& confirmations,
    const std::optional<TPendingMutation>& pending,
    const std::vector<TEndpointState>& endpoints,
    TDuration confirmDuration,
    const std::set<TString>& excludedEndpoints = {});

TString RenderCliCommand(const TPlan& plan);

} // namespace NBridgeSkipper
