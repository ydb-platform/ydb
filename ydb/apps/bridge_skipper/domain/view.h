#pragma once

#include "types.h"

namespace NBridgeSkipper {

struct TViewSelection {
    std::optional<TClusterView> View;
    std::optional<EBlockerReason> Blocker;
    // Endpoints whose fresh reply is at the selected generation; empty without a View.
    std::set<TString> CurrentEndpoints;
    // Exclusive end of freshness for the selected generation's admitted views.
    TMonotonic ValidUntil = TMonotonic::Max();
};

// Takes decoded views only: decode is the single validator, because duplicate
// names are detectable only before the map is built.
TViewSelection SelectView(
    const std::vector<TClusterView>& views,
    ui64 highWater,
    TMonotonic now,
    TDuration ttl);

enum class ETransitionExplanation { Explained, Unexplained };

// Explained: at the same generation, the piles differ only by a pile that is
// INACTIVE in one view and absent from the other (SelectView treats such views
// as one); or the generation advanced and the piles equal the previous ones, or
// the previous ones overlaid with pending, up to YDB follow-ups (PROMOTED to
// PRIMARY with the old primary to SYNCHRONIZED, NOT_SYNCHRONIZED to
// SYNCHRONIZED, SUSPENDED to DISCONNECTED). The overlay applies only when
// pending was made for the previous generation. The actor may supply either
// the persisted pending update or the last success.
ETransitionExplanation ExplainTransition(
    const TClusterView& previous,
    const TClusterView& next,
    const std::optional<TPendingMutation>& pending);

} // namespace NBridgeSkipper
