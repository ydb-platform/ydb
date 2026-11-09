#include "flat_compaction_context.h"

#include "flat_comp.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ymath.h>

#include <algorithm>

namespace NKikimr::NTable {
namespace {

    TVector<TEpoch> CollectExtraEpochs(const TSubset& subset) {
        TVector<TEpoch> epochs;
        epochs.reserve(subset.Frozen.size() + subset.TxStatus.size());
        for (const auto& mem : subset.Frozen) {
            Y_ENSURE(mem.MemTable, "Missing compaction memory source");
            epochs.push_back(mem->Epoch);
        }
        for (const auto& part : subset.TxStatus) {
            Y_ENSURE(part, "Missing compaction transaction-status source");
            epochs.push_back(part->Epoch);
        }
        std::sort(epochs.begin(), epochs.end());
        return epochs;
    }

}

    TCompactionPartInfo::TCompactionPartInfo(TPartView part, ui32 level, ui32 tierId)
        : Part(std::move(part))
        , Level(level)
        , TierId(tierId)
    {
        Y_ENSURE(Part, "Missing compaction part");
        Y_ENSURE(level > 0, "Invalid compaction part level");
    }

    TCompactionPartInfo::TCompactionPartInfo(TIntrusiveConstPtr<TColdPart> part,
            ui32 level, ui32 tierId)
        : ColdPart(std::move(part))
        , Level(level)
        , TierId(tierId)
    {
        Y_ENSURE(ColdPart, "Missing unloaded compaction part");
        Y_ENSURE(level > 0, "Invalid compaction part level");
    }

    const TLogoBlobID& TCompactionPartInfo::Label() const {
        Y_ENSURE(bool(Part) != bool(ColdPart), "Ambiguous compaction part");
        return Part ? Part->Label : ColdPart->Label;
    }

    TEpoch TCompactionPartInfo::Epoch() const {
        Y_ENSURE(bool(Part) != bool(ColdPart), "Ambiguous compaction part");
        return Part ? Part->Epoch : ColdPart->Epoch;
    }

    bool TCompactionPartInfo::SameState(const TCompactionPartInfo& other) const {
        return Level == other.Level && TierId == other.TierId
            && Part.Part.Get() == other.Part.Part.Get()
            && Part.Screen.Get() == other.Part.Screen.Get()
            && Part.Slices.Get() == other.Part.Slices.Get()
            && ColdPart.Get() == other.ColdPart.Get();
    }

    ui32 ResolveColdPartLevel(ui32 level, const NLocalDb::TCompactionPolicy& policy) {
        if (level == 255) {
            return level;
        }
        Y_ENSURE(level > 0 && level <= policy.Generations.size(),
            "Unloaded compaction part refers to an unknown generation");
        return policy.Generations[level - 1].TierId.value_or(0) == 0 ? level : 255;
    }

    TVector<TCompactionPartInfo> DescribeCompactionParts(
            TConstArrayRef<TPartView> parts,
            TConstArrayRef<TIntrusiveConstPtr<TColdPart>> coldParts,
            const TCompactionState& state, const NLocalDb::TCompactionPolicy& policy)
    {
        auto tierForLevel = [&](ui32 level) {
            if (level == 255) {
                return ui32(0);
            }
            Y_ENSURE(level > 0 && level <= policy.Generations.size(),
                "Compaction part refers to an unknown generation");
            return policy.Generations[level - 1].TierId.value_or(0);
        };

        TVector<TCompactionPartInfo> result;
        result.reserve(parts.size() + coldParts.size());
        for (const auto& part : parts) {
            Y_ENSURE(part, "Missing compaction part");
            const ui32 level = state.PartLevels.Value(part->Label, 255);
            result.emplace_back(part, level, tierForLevel(level));
        }
        for (const auto& part : coldParts) {
            Y_ENSURE(part, "Missing unloaded compaction part");
            const ui32 level = ResolveColdPartLevel(state.PartLevels.Value(part->Label, 255), policy);
            result.emplace_back(part, level, 0);
        }
        return result;
    }

    TCompactionContext::TCompactionContext(std::optional<TCompactionEpochRange> epochs,
            TVector<TCompactionPartInfo> inputs, TVector<TCompactionPartInfo> remaining,
            TVector<ui32> inputTiers, TVector<TEpoch> extraEpochs,
            TVector<ui32> extraInputTiers, const TSubset* initialSubset)
        : Epochs(std::move(epochs))
        , Inputs(std::move(inputs))
        , Remaining(std::move(remaining))
        , InputTiers(std::move(inputTiers))
        , ExtraEpochs(std::move(extraEpochs))
        , ExtraInputTiers(std::move(extraInputTiers))
        , TrackExtraSources(initialSubset != nullptr)
    {
        if (initialSubset) {
            THashSet<const TMemTable*> memory;
            for (const auto& mem : initialSubset->Frozen) {
                Y_ENSURE(memory.insert(mem.MemTable.Get()).second,
                    "Duplicate compaction memory source");
                MemorySources.push_back(mem.MemTable);
            }
            THashSet<TLogoBlobID> status;
            for (const auto& part : initialSubset->TxStatus) {
                Y_ENSURE(status.insert(part->Label).second,
                    "Duplicate compaction transaction-status source");
            }
            StatusSources = initialSubset->TxStatus;
        }
    }

    TIntrusiveConstPtr<TCompactionContext> TCompactionContext::Build(
            TVector<TCompactionPartInfo> parts, TConstArrayRef<TLogoBlobID> initialParts,
            TConstArrayRef<TEpoch> extraEpochs, TConstArrayRef<ui32> extraInputTiers)
    {
        return BuildImpl(std::move(parts), initialParts, extraEpochs, extraInputTiers, nullptr);
    }

    TIntrusiveConstPtr<TCompactionContext> TCompactionContext::BuildImpl(
            TVector<TCompactionPartInfo> parts, TConstArrayRef<TLogoBlobID> initialParts,
            TConstArrayRef<TEpoch> extraEpochs, TConstArrayRef<ui32> extraInputTiers,
            const TSubset* initialSubset)
    {
        THashMap<TLogoBlobID, size_t> byLabel;
        byLabel.reserve(parts.size());
        for (size_t index = 0; index < parts.size(); ++index) {
            const auto& part = parts[index];
            Y_ENSURE(part.Epoch() != TEpoch::Max(), "Cannot close an unknown part epoch");
            Y_ENSURE(byLabel.emplace(part.Label(), index).second,
                "Duplicate part in compaction inventory");
        }

        std::optional<TCompactionEpochRange> epochs;
        auto addEpoch = [&](TEpoch epoch) {
            Y_ENSURE(epoch != TEpoch::Max(), "Cannot close an unknown source epoch");
            if (!epochs) {
                epochs = TCompactionEpochRange{epoch, epoch};
            } else {
                epochs->Min = Min(epochs->Min, epoch);
                epochs->Max = Max(epochs->Max, epoch);
            }
        };
        for (const auto& label : initialParts) {
            auto it = byLabel.find(label);
            Y_ENSURE(it != byLabel.end(), "Unknown initial compaction part");
            addEpoch(parts[it->second].Epoch());
        }
        TVector<TEpoch> extra(extraEpochs.begin(), extraEpochs.end());
        for (TEpoch epoch : extra) {
            addEpoch(epoch);
        }
        std::sort(extra.begin(), extra.end());

        TVector<TCompactionPartInfo> inputs;
        TVector<TCompactionPartInfo> remaining;
        TVector<ui32> inputTiers;
        THashSet<ui32> seenTiers;
        TVector<ui32> extraTiers(extraInputTiers.begin(), extraInputTiers.end());
        std::sort(extraTiers.begin(), extraTiers.end());
        extraTiers.erase(std::unique(extraTiers.begin(), extraTiers.end()), extraTiers.end());
        Y_ENSURE(extraTiers.empty() || !extra.empty(),
            "Additional input tiers require source epochs");
        for (ui32 tier : extraTiers) {
            seenTiers.insert(tier);
            inputTiers.push_back(tier);
        }
        for (auto& part : parts) {
            if (epochs && epochs->Contains(part.Epoch())) {
                if (seenTiers.insert(part.TierId).second) {
                    inputTiers.push_back(part.TierId);
                }
                inputs.push_back(std::move(part));
            } else {
                remaining.push_back(std::move(part));
            }
        }
        return new TCompactionContext(std::move(epochs), std::move(inputs),
            std::move(remaining), std::move(inputTiers), std::move(extra), std::move(extraTiers),
            initialSubset);
    }

    TIntrusiveConstPtr<TCompactionContext> TCompactionContext::Build(
            TVector<TCompactionPartInfo> parts, const TSubset& initialSubset)
    {
        TVector<TLogoBlobID> labels;
        labels.reserve(initialSubset.Flatten.size() + initialSubset.ColdParts.size());
        THashMap<TLogoBlobID, TEpoch> initialEpochs;
        for (const auto& part : initialSubset.Flatten) {
            Y_ENSURE(part, "Missing initial compaction part");
            labels.push_back(part->Label);
            Y_ENSURE(initialEpochs.emplace(part->Label, part->Epoch).second,
                "Duplicate initial compaction part");
        }
        for (const auto& part : initialSubset.ColdParts) {
            Y_ENSURE(part, "Missing initial unloaded compaction part");
            labels.push_back(part->Label);
            Y_ENSURE(initialEpochs.emplace(part->Label, part->Epoch).second,
                "Duplicate initial compaction part");
        }
        for (const auto& part : parts) {
            auto it = initialEpochs.find(part.Label());
            Y_ENSURE(it == initialEpochs.end() || it->second == part.Epoch(),
                "Initial compaction part epoch has changed");
        }
        const TVector<ui32> extraTiers = initialSubset.Frozen.empty()
            ? TVector<ui32>{} : TVector<ui32>{0};
        return BuildImpl(std::move(parts), labels, CollectExtraEpochs(initialSubset), extraTiers,
            &initialSubset);
    }

    void TCompactionContext::CopyInputs(TCompactionParams& params) const {
        params.Parts.clear();
        params.ColdParts.clear();
        for (const auto& part : Inputs) {
            if (part.Part) {
                params.Parts.push_back(part.Part);
            } else {
                params.ColdParts.push_back(part.ColdPart);
            }
        }
    }

    void TCompactionContext::ValidateSubset(const TSubset& subset) const {
        Y_ENSURE(subset.Flatten.size() + subset.ColdParts.size() == Inputs.size(),
            "Compaction subset does not cover the closed epoch interval");
        THashMap<TLogoBlobID, const TCompactionPartInfo*> expected;
        for (const auto& part : Inputs) {
            expected.emplace(part.Label(), &part);
        }
        for (const auto& part : subset.Flatten) {
            Y_ENSURE(part, "Missing subset part");
            auto it = expected.find(part->Label);
            Y_ENSURE(it != expected.end(), "Unexpected subset part");
            const auto& before = it->second->Part;
            Y_ENSURE(before.Part.Get() == part.Part.Get()
                    && before.Screen.Get() == part.Screen.Get()
                    && before.Slices.Get() == part.Slices.Get(),
                "Compaction subset part has changed");
            expected.erase(it);
        }
        for (const auto& part : subset.ColdParts) {
            Y_ENSURE(part, "Missing unloaded subset part");
            auto it = expected.find(part->Label);
            Y_ENSURE(it != expected.end() && it->second->ColdPart.Get() == part.Get(),
                "Unloaded compaction subset part has changed");
            expected.erase(it);
        }
        Y_ENSURE(expected.empty(), "Missing subset part");
        Y_ENSURE(CollectExtraEpochs(subset) == ExtraEpochs,
            "Compaction subset memory or transaction-status epochs have changed");
        const TVector<ui32> extraTiers = subset.Frozen.empty()
            ? TVector<ui32>{} : TVector<ui32>{0};
        Y_ENSURE(extraTiers == ExtraInputTiers,
            "Compaction subset memory placement has changed");
        if (TrackExtraSources) {
            THashSet<const TMemTable*> memory;
            for (const auto& mem : MemorySources) {
                memory.insert(mem.Get());
            }
            Y_ENSURE(subset.Frozen.size() == MemorySources.size(),
                "Compaction memory sources have changed");
            for (const auto& mem : subset.Frozen) {
                Y_ENSURE(memory.erase(mem.MemTable.Get()), "Compaction memory source has changed");
            }
            THashSet<const TTxStatusPart*> status;
            for (const auto& part : StatusSources) {
                status.insert(part.Get());
            }
            Y_ENSURE(subset.TxStatus.size() == StatusSources.size(),
                "Compaction transaction-status sources have changed");
            for (const auto& part : subset.TxStatus) {
                Y_ENSURE(status.erase(part.Get()), "Compaction transaction-status source has changed");
            }
        }
        Y_ENSURE(subset.Epoch() == (Epochs ? Epochs->Max : TEpoch::Min()),
            "Compaction output epoch differs from the closed interval");
    }

    bool TCompactionContext::IsCurrent(TConstArrayRef<TCompactionPartInfo> parts) const {
        if (parts.size() != Inputs.size() + Remaining.size()) {
            return false;
        }
        THashMap<TLogoBlobID, const TCompactionPartInfo*> expected;
        for (const auto* group : {&Inputs, &Remaining}) {
            for (const auto& part : *group) {
                expected.emplace(part.Label(), &part);
            }
        }
        for (const auto& part : parts) {
            if (bool(part.Part) == bool(part.ColdPart)) {
                return false;
            }
            auto it = expected.find(part.Label());
            if (it == expected.end() || !it->second->SameState(part)) {
                return false;
            }
            expected.erase(it);
        }
        return expected.empty();
    }

}
