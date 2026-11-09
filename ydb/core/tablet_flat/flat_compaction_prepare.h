#pragma once

#include "flat_comp.h"

namespace NKikimr::NTable {

    // Find a level that preserves epoch ordering within the destination tier.
    // Level 255 is the default tier's final list; it is never a cold tier.
    std::optional<ui32> FindTierCompactionLevel(
        const NLocalDb::TCompactionPolicy& policy,
        TConstArrayRef<TCompactionPartInfo> remaining, ui32 tierId, TEpoch epoch,
        ui32 preferredLevel = Max<ui32>());

    // Called after freezing memory and choosing the tx-status sources. Expands
    // the actual subset, captures schema/time and selects the execution mode.
    void PrepareTieringCompaction(TCompactionParams& params,
        const TScheme::TTableInfo& table, TIntrusiveConstPtr<TRowScheme> rowScheme,
        TSubset& subset, TConstArrayRef<TPartView> parts,
        TConstArrayRef<TIntrusiveConstPtr<TColdPart>> coldParts,
        bool enableDataTiering, TInstant now);

    // Must be called before replacing any table data or publishing outputs.
    // Known parallel publications may advance Tiering.ExpectedInventory; the
    // pinned inputs and closed epoch interval must still match the context.
    bool IsTieringCompactionCurrent(const TCompactionParams& params,
        const TScheme::TTableInfo& table, TIntrusiveConstPtr<TRowScheme> rowScheme,
        TConstArrayRef<TPartView> parts,
        TConstArrayRef<TIntrusiveConstPtr<TColdPart>> coldParts,
        bool enableDataTiering);

}
