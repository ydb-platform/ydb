#pragma once

#include "flat_page_conf.h"
#include "flat_table_subset.h"

#include <ydb/core/base/localdb.h>

#include <optional>

namespace NKikimr::NTable {

    class TCompactionParams;
    struct TCompactionState;

    // Runtime placement of one table-owned part view. ColdPart means that its
    // metadata has not been loaded; table inventory assigns it to the default tier.
    struct TCompactionPartInfo {
        TCompactionPartInfo(TPartView part, ui32 level, ui32 tierId);
        TCompactionPartInfo(TIntrusiveConstPtr<TColdPart> part, ui32 level, ui32 tierId);

        const TLogoBlobID& Label() const;
        TEpoch Epoch() const;
        bool SameState(const TCompactionPartInfo& other) const;

        TPartView Part;
        TIntrusiveConstPtr<TColdPart> ColdPart;
        ui32 Level;
        ui32 TierId;
    };

    // Preserve unloaded parts' existing default-generation placement, but move
    // references to non-default generations or finals to the default final (255).
    ui32 ResolveColdPartLevel(ui32 level, const NLocalDb::TCompactionPolicy& policy);

    // Resolve placement using the existing part -> level -> generation binding.
    // Final levels are decoded using the policy's stable tier slots. Missing
    // bindings and unloaded parts belong to the default tier.
    TVector<TCompactionPartInfo> DescribeCompactionParts(
        TConstArrayRef<TPartView> parts,
        TConstArrayRef<TIntrusiveConstPtr<TColdPart>> coldParts,
        const TCompactionState& state,
        const NLocalDb::TCompactionPolicy& policy);

    struct TCompactionEpochRange {
        TEpoch Min;
        TEpoch Max;

        bool Contains(TEpoch epoch) const {
            return Min <= epoch && epoch <= Max;
        }
    };

    // An immutable, pinned view of all inputs and of the remaining sources.
    // It does not reserve parts or authorize publication: the strategy must
    // protect/revalidate this view when it starts and finishes the operation.
    class TCompactionContext final : public TThrRefBase {
    public:
        // Extra epochs describe sources outside the part inventory. This
        // overload checks their epochs; their owner retains/protects them.
        static TIntrusiveConstPtr<TCompactionContext> Build(
            TVector<TCompactionPartInfo> parts,
            TConstArrayRef<TLogoBlobID> initialParts,
            TConstArrayRef<TEpoch> extraEpochs = {},
            TConstArrayRef<ui32> extraInputTiers = {});

        // Frozen memtables and tx-status parts contribute to the output epoch,
        // and therefore to both bounds of the interval that must be closed.
        // This overload also pins and checks the identity of those sources.
        static TIntrusiveConstPtr<TCompactionContext> Build(
            TVector<TCompactionPartInfo> parts, const TSubset& initialSubset);

        bool RequiresTierAware() const {
            return InputTiers.size() > 1;
        }

        void CopyInputs(TCompactionParams& params) const;
        void ValidateSubset(const TSubset& subset) const;
        // Checks part placement/views only. The caller also protects the
        // schema, channel configuration and the frozen memory snapshots.
        bool IsCurrent(TConstArrayRef<TCompactionPartInfo> parts) const;

        const std::optional<TCompactionEpochRange> Epochs;
        const TVector<TCompactionPartInfo> Inputs;
        const TVector<TCompactionPartInfo> Remaining;
        const TVector<ui32> InputTiers;

    private:
        static TIntrusiveConstPtr<TCompactionContext> BuildImpl(
            TVector<TCompactionPartInfo> parts, TConstArrayRef<TLogoBlobID> initialParts,
            TConstArrayRef<TEpoch> extraEpochs, TConstArrayRef<ui32> extraInputTiers,
            const TSubset* initialSubset);

        TCompactionContext(std::optional<TCompactionEpochRange> epochs,
            TVector<TCompactionPartInfo> inputs, TVector<TCompactionPartInfo> remaining,
            TVector<ui32> inputTiers, TVector<TEpoch> extraEpochs,
            TVector<ui32> extraInputTiers, const TSubset* initialSubset);

        const TVector<TEpoch> ExtraEpochs;
        const TVector<ui32> ExtraInputTiers;
        const bool TrackExtraSources;
        TVector<TIntrusiveConstPtr<TMemTable>> MemorySources;
        TVector<TIntrusiveConstPtr<TTxStatusPart>> StatusSources;
    };

    // Each call creates an independent cursor, pinning the selected metadata.
    // Missing bounds/bloom and unloaded parts conservatively mean "maybe".
    // Bloom prefixes containing PG columns are ignored because equal PG values
    // may have different binary encodings.
    THolder<NPage::IKeySpace> MakeCompactionKeySpace(
        TIntrusiveConstPtr<TKeyCellDefaults> keys,
        TConstArrayRef<TCompactionPartInfo> parts);

    struct TCompactionGarbageConf {
        bool Final = false;
        THolder<NPage::IKeySpace> UnderlayMask;
    };

    // KeepEraseMarkers disables both unconditional and per-key finalization.
    // Otherwise the underlay conservatively covers every remaining part,
    // including newer epochs and sources in other tiers.
    TCompactionGarbageConf MakeCompactionGarbageConf(
        TIntrusiveConstPtr<TKeyCellDefaults> keys,
        TConstArrayRef<TCompactionPartInfo> remaining, bool keepEraseMarkers);

}
