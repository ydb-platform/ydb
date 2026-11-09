#include "flat_compaction_router.h"

#include <util/generic/hash_set.h>

#include <algorithm>

namespace NKikimr::NTabletFlatExecutor {

TTierCompactionRouter::TTierCompactionRouter(const TCompactCfg& conf,
        std::optional<NTable::TTieringPolicy> policy, TInstant now,
        TVector<ui32> tierOrder, bool allowCooling)
    : Context(conf.Params ? conf.Params->Context : nullptr)
    , Policy(std::move(policy))
    , Now(now)
    , TierOrder(std::move(tierOrder))
    , AllowCooling(allowCooling)
    , UseConservativeMasks(conf.Params && conf.Params->Tiering
        && conf.Params->Tiering->UseConservativeMasks)
    , HasUnknownRemaining(conf.Params && conf.Params->Tiering
        && conf.Params->Tiering->HasUnknownRemaining)
{
    Y_ENSURE(conf.Params && conf.Params->Mode == NTable::TCompactionParams::EMode::TierAware,
        "Tier-aware router requires a tier-aware compaction");
    Y_ENSURE(Context, "Tier-aware router requires a prepared compaction context");
    Y_ENSURE(!conf.IsFulltextCompact, "Tier-aware fulltext compaction is not supported");
    Y_ENSURE(!TierOrder.empty(), "Compaction tier order must not be empty");

    THashSet<ui32> tiers;
    for (ui32 tier : TierOrder) {
        Y_ENSURE(tiers.insert(tier).second, "Compaction tier order contains a duplicate tier");
    }
    for (size_t index = 0; index < conf.Outputs.size(); ++index) {
        const ui32 tier = conf.Outputs[index].TierId;
        Y_ENSURE(tiers.contains(tier), "Compaction output tier is not in the tier order");
        Y_ENSURE(OutputByTier.emplace(tier, ui32(index)).second,
            "Duplicate compaction output tier");
    }
    for (ui32 tier : TierOrder) {
        // A cap may stop at any intermediate tier, even when no policy rule
        // directly targets it.
        Y_ENSURE(OutputByTier.contains(tier), "Missing compaction output for a tier in the tier order");
    }
    Y_ENSURE(OutputByTier.contains(0), "Tier-aware compaction requires a default output");
    for (ui32 tier : Context->InputTiers) {
        Y_ENSURE(OutputByTier.contains(tier), "Missing compaction source-tier output");
    }
    if (Policy) {
        for (const auto& rule : Policy->Rules) {
            Y_ENSURE(OutputByTier.contains(rule.TierId), "Missing compaction policy-target output");
        }
    }
}

void TTierCompactionRouter::Prepare(const NTable::TRowScheme& scheme)
{
    Y_ENSURE(!Prepared, "Tier-aware compaction router prepared twice");
    if (Policy) {
        Predicate = MakeHolder<NTable::TTierPredicate>(*Policy, Now, scheme);
    }
    Cap = MakeHolder<NTable::TCompactionTierCap>(scheme.Keys, Context->Remaining, TierOrder,
        UseConservativeMasks, HasUnknownRemaining);
    Prepared = true;
}

ui32 TTierCompactionRouter::RouteKey(TCellsRef key, const NTable::TRowState* row, ui32 sourceTier)
{
    Y_ENSURE(Prepared, "Tier-aware compaction router is not prepared");
    const auto source = std::find(TierOrder.begin(), TierOrder.end(), sourceTier);
    Y_ENSURE(source != TierOrder.end(), "Compaction source tier is not in the tier order");

    ui32 desiredTier = sourceTier;
    if (Predicate && row) {
        const auto decision = Predicate->Evaluate(*row);
        if (!decision.IsUndecided()) {
            desiredTier = decision.GetTierId();
        }
    }
    const auto desired = std::find(TierOrder.begin(), TierOrder.end(), desiredTier);
    Y_ENSURE(desired != TierOrder.end(), "Compaction desired tier is not in the tier order");
    if (!AllowCooling && desired > source) {
        desiredTier = sourceTier;
    }

    const ui32 tier = Cap->LimitTier(key, sourceTier, desiredTier);
    auto output = OutputByTier.find(tier);
    Y_ENSURE(output != OutputByTier.end(), "Missing compaction destination output");
    return output->second;
}

TVector<ui32> TTierCompactionRouter::GetUnrepresentableTierIds() const
{
    return Predicate ? Predicate->GetUnrepresentableTierIds() : TVector<ui32>{};
}

}
