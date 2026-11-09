#include "flat_compaction_prepare.h"

#include <util/generic/hash_set.h>

namespace NKikimr::NTable {

std::optional<ui32> FindTierCompactionLevel(const NLocalDb::TCompactionPolicy& policy,
        TConstArrayRef<TCompactionPartInfo> remaining, ui32 tierId, TEpoch epoch,
        ui32 preferredLevel)
{
    const auto canInsert = [&](ui32 level) {
        if (level == 255) {
            if (tierId != 0) {
                return false;
            }
        } else if (level == 0 || level > policy.Generations.size()
                || policy.Generations[level - 1].TierId.value_or(0) != tierId) {
            return false;
        }
        for (const auto& part : remaining) {
            if (part.TierId != tierId || part.Level == level) {
                continue;
            }
            if ((part.Level < level && part.Epoch() < epoch)
                    || (part.Level > level && part.Epoch() > epoch)) {
                return false;
            }
        }
        return true;
    };
    if (canInsert(preferredLevel)) {
        return preferredLevel;
    }
    for (ui32 level = 1; level <= policy.Generations.size(); ++level) {
        if (canInsert(level)) {
            return level;
        }
    }
    if (canInsert(255)) {
        return 255;
    }
    return std::nullopt;
}

void PrepareTieringCompaction(TCompactionParams& params,
        const TScheme::TTableInfo& table, TIntrusiveConstPtr<TRowScheme> rowScheme,
        TSubset& subset, TConstArrayRef<TPartView> parts,
        TConstArrayRef<TIntrusiveConstPtr<TColdPart>> coldParts,
        bool enableDataTiering, TInstant now)
{
    Y_ENSURE(params.Tiering, "Missing tier-managed compaction settings");
    auto& settings = *params.Tiering;
    const bool coolingRequested = params.Mode == TCompactionParams::EMode::TierAware;
    TCompactionState state;
    state.PartLevels = settings.PartLevels;
    const auto inventory = DescribeCompactionParts(parts, coldParts, state, *table.CompactionPolicy);
    settings.ExpectedInventory = inventory;
    settings.HasMemoryInputs = !subset.Frozen.empty();
    const auto closeInputs = [&] {
        params.Context = TCompactionContext::Build(inventory, subset);
        params.Context->CopyInputs(params);
        subset.Flatten = params.Parts;
        subset.ColdParts = params.ColdParts;
        params.Context->ValidateSubset(subset);
    };
    closeInputs();

    auto schema = std::make_shared<TScheme::TTableInfo>(table);
    NKikimrCompaction::TCompactionPolicy policy;
    table.CompactionPolicy->Serialize(policy);
    schema->CompactionPolicy = new NLocalDb::TCompactionPolicy(policy);
    settings.Schema = std::move(schema);
    settings.RowScheme = std::move(rowScheme);
    settings.Now = now;
    settings.TierOrder.clear();
    THashSet<ui32> tiers;
    auto addTier = [&](ui32 tier) {
        if (tiers.insert(tier).second) {
            settings.TierOrder.push_back(tier);
        }
    };
    addTier(0);
    for (const auto& generation : table.CompactionPolicy->Generations) {
        addTier(generation.TierId.value_or(0));
    }
    const auto selectMode = [&] {
        if (params.Context->RequiresTierAware()) {
            params.Mode = TCompactionParams::EMode::TierAware;
        } else if (params.Mode == TCompactionParams::EMode::Regular
                && !params.Context->InputTiers.empty()) {
            params.TierId = params.Context->InputTiers.front();
        }
    };
    selectMode();

    const auto hasPlacement = [&] {
        if (!params.Context->Epochs) {
            return true;
        }
        const auto fits = [&](ui32 tier) {
            return FindTierCompactionLevel(*table.CompactionPolicy,
                params.Context->Remaining, tier, params.Context->Epochs->Max).has_value();
        };
        if (params.Mode == TCompactionParams::EMode::Regular) {
            return fits(params.TierId);
        }
        for (ui32 tier : settings.TierOrder) {
            if (!fits(tier)) {
                return false;
            }
        }
        return true;
    };
    if (!hasPlacement()) {
        // Legacy borrowed FinalParts can overlap the epochs of configured
        // default generations. Reconcile them before writing, rather than
        // discovering an impossible destination after replacing the inputs.
        subset.Flatten.assign(parts.begin(), parts.end());
        subset.ColdParts.assign(coldParts.begin(), coldParts.end());
        closeInputs();
        selectMode();
    }
    Y_ENSURE(hasPlacement(), "Compaction outputs have no admissible generation");

    settings.AllowCooling = coolingRequested && enableDataTiering && table.TieringPolicy.has_value();
    if (settings.AllowCooling) {
        for (const auto& rule : table.TieringPolicy->Rules) {
            Y_ENSURE(tiers.contains(rule.TierId),
                "Compaction policy target has no generation: " << rule.TierId);
        }
    }
    const auto checkFamily = [&](ui32 tier) {
        Y_ENSURE(table.Families.contains(tier), "Compaction tier family does not exist: " << tier);
    };
    if (params.Mode == TCompactionParams::EMode::TierAware) {
        for (ui32 tier : settings.TierOrder) {
            checkFamily(tier);
        }
    } else {
        checkFamily(params.TierId);
    }
}

bool IsTieringCompactionCurrent(const TCompactionParams& params,
        const TScheme::TTableInfo& table, TIntrusiveConstPtr<TRowScheme> rowScheme,
        TConstArrayRef<TPartView> parts,
        TConstArrayRef<TIntrusiveConstPtr<TColdPart>> coldParts,
        bool enableDataTiering)
{
    if (!params.Tiering) {
        return true;
    }
    const auto& settings = *params.Tiering;
    if (!params.Context || !settings.Schema || settings.RowScheme.Get() != rowScheme.Get()
            || (settings.AllowCooling && !enableDataTiering)) {
        return false;
    }
    const auto& before = *settings.Schema;
    if (!(*before.CompactionPolicy == *table.CompactionPolicy)
            || before.TieringPolicy != table.TieringPolicy
            || before.Families != table.Families || before.Rooms != table.Rooms
            || before.ByKeyFilterPrefixes != table.ByKeyFilterPrefixes
            || before.SpecialTableType != table.SpecialTableType
            || before.ColdBorrow != table.ColdBorrow) {
        return false;
    }
    TCompactionState state;
    state.PartLevels = settings.PartLevels;
    const auto current = DescribeCompactionParts(parts, coldParts, state, *table.CompactionPolicy);
    if (current.size() != settings.ExpectedInventory.size()) {
        return false;
    }
    // Only publications admitted by the strategy may replace remaining
    // sources in this inventory. The original context stays pinned: its
    // conservative masks also cover those publications' resulting keys.
    THashMap<TLogoBlobID, const TCompactionPartInfo*> expected;
    for (const auto& part : settings.ExpectedInventory) {
        if (!expected.emplace(part.Label(), &part).second) {
            return false;
        }
    }
    THashSet<TLogoBlobID> inputs;
    for (const auto& part : params.Context->Inputs) {
        const auto it = expected.find(part.Label());
        if (it == expected.end() || !part.SameState(*it->second)) {
            return false;
        }
        inputs.insert(part.Label());
    }
    for (const auto& part : current) {
        const auto it = expected.find(part.Label());
        if (it == expected.end() || !it->second->SameState(part)) {
            return false;
        }
        if (params.Context->Epochs && params.Context->Epochs->Contains(part.Epoch())
                && !inputs.contains(part.Label())) {
            return false;
        }
        expected.erase(it);
    }
    return expected.empty();
}

}
