#include "flat_compaction_context.h"

#include "flat_bloom_hash.h"
#include "flat_part_slice.h"

#include <util/generic/hash_set.h>

#include <algorithm>

namespace NKikimr::NTable {
namespace {

    class TCompactionKeySpace final : public NPage::IKeySpace {
    public:
        TCompactionKeySpace(TIntrusiveConstPtr<TKeyCellDefaults> keys,
                TConstArrayRef<TCompactionPartInfo> parts,
                std::optional<ui32> tierId, std::optional<ui32> level)
            : Keys(std::move(keys))
        {
            Y_ENSURE(Keys, "Compaction key space requires a key schema");
            Parts.reserve(parts.size());
            for (const auto& part : parts) {
                if ((!tierId || part.TierId == *tierId) && (!level || part.Level == *level)) {
                    Y_ENSURE(bool(part.Part) != bool(part.ColdPart),
                        "Compaction key space requires exactly one part representation");
                    Parts.emplace_back(part);
                }
            }
        }

        void Reset() override {
            for (auto& part : Parts) {
                part.SliceIndex = 0;
            }
        }

        bool HasKey(TCellsRef key) override {
            // A prefix query cannot prove the absence of a complete key using
            // full-key bloom filters or the search-key bounds convention.
            Y_ENSURE(key.size() == Keys->Size(),
                "Compaction key space requires a complete key");
            if (key.empty()) {
                // Bounds helpers interpret an empty search key as +inf.
                return !Parts.empty();
            }
            std::optional<NBloom::TPrefix> prefix;
            for (auto& cursor : Parts) {
                if (cursor.Info.ColdPart) {
                    return true;
                }

                const auto& part = cursor.Info.Part;
                if (part.Slices) {
                    const auto& slices = *part.Slices;
                    while (cursor.SliceIndex < slices.size()
                            && TBounds::CompareLastKeySearchKey(
                                slices[cursor.SliceIndex], key, *Keys) < 0) {
                        ++cursor.SliceIndex;
                    }
                    if (cursor.SliceIndex == slices.size()
                            || TBounds::CompareSearchKeyFirstKey(
                                key, slices[cursor.SliceIndex], *Keys) < 0) {
                        continue;
                    }
                }

                bool rejected = false;
                for (auto it = part->ByKeyPrefixes.rbegin(); it != part->ByKeyPrefixes.rend(); ++it) {
                    const auto& [length, bloom] = *it;
                    if (!bloom || length == 0 || length > key.size()) {
                        continue;
                    }
                    if (!prefix) {
                        prefix.emplace(key);
                    }
                    if (!bloom->MightHave(prefix->Get(length))) {
                        rejected = true;
                        break;
                    }
                }
                if (!rejected) {
                    return true;
                }
            }
            return false;
        }

    private:
        struct TPartCursor {
            explicit TPartCursor(const TCompactionPartInfo& info)
                : Info(info)
            { }

            TCompactionPartInfo Info;
            size_t SliceIndex = 0;
        };

        const TIntrusiveConstPtr<TKeyCellDefaults> Keys;
        TVector<TPartCursor> Parts;
    };

}

THolder<NPage::IKeySpace> MakeCompactionKeySpace(
        TIntrusiveConstPtr<TKeyCellDefaults> keys,
        TConstArrayRef<TCompactionPartInfo> parts,
        std::optional<ui32> tierId, std::optional<ui32> level)
{
    return MakeHolder<TCompactionKeySpace>(std::move(keys), parts, tierId, level);
}

TCompactionGarbageConf MakeCompactionGarbageConf(
        TIntrusiveConstPtr<TKeyCellDefaults> keys,
        TConstArrayRef<TCompactionPartInfo> remaining, bool keepEraseMarkers)
{
    TCompactionGarbageConf result;
    if (keepEraseMarkers) {
        // Even a negative underlay answer would enable per-key finalization.
        return result;
    }
    if (remaining.empty()) {
        result.Final = true;
    } else {
        result.UnderlayMask = MakeCompactionKeySpace(std::move(keys), remaining);
    }
    return result;
}

TCompactionTierCap::TCompactionTierCap(TIntrusiveConstPtr<TKeyCellDefaults> keys,
        TConstArrayRef<TCompactionPartInfo> remaining, TVector<ui32> tierOrder,
        bool useConservativeMasks, bool hasUnknownRemaining)
    : TierOrder(std::move(tierOrder))
    , HasUnknownRemaining(hasUnknownRemaining)
{
    Y_ENSURE(!TierOrder.empty(), "Compaction tier order must not be empty");
    THashSet<ui32> tiers;
    for (ui32 tierId : TierOrder) {
        Y_ENSURE(tiers.insert(tierId).second, "Compaction tier order contains a duplicate tier");
    }
    for (const auto& part : remaining) {
        Y_ENSURE(tiers.contains(part.TierId),
            "Compaction tier order does not contain a remaining part's tier");
    }

    Masks.reserve(TierOrder.size());
    for (ui32 tierId : TierOrder) {
        // A concurrent TAC may move a remaining key to another tier. Its
        // replacement cannot add keys to this pinned union of disk sources.
        Masks.push_back(MakeCompactionKeySpace(keys, remaining,
            useConservativeMasks ? std::nullopt : std::optional<ui32>(tierId)));
    }
}

void TCompactionTierCap::Reset()
{
    for (auto& mask : Masks) {
        mask->Reset();
    }
}

ui32 TCompactionTierCap::LimitTier(TCellsRef key, ui32 sourceTier, ui32 desiredTier)
{
    const auto source = std::find(TierOrder.begin(), TierOrder.end(), sourceTier);
    const auto desired = std::find(TierOrder.begin(), TierOrder.end(), desiredTier);
    Y_ENSURE(source != TierOrder.end(), "Compaction source tier is not in the tier order");
    Y_ENSURE(desired != TierOrder.end(), "Compaction desired tier is not in the tier order");

    const size_t sourceIndex = source - TierOrder.begin();
    const size_t desiredIndex = desired - TierOrder.begin();
    if (sourceIndex < desiredIndex) {
        for (size_t index = sourceIndex + 1; index < desiredIndex; ++index) {
            if (HasUnknownRemaining || Masks[index]->HasKey(key)) {
                return TierOrder[index];
            }
        }
    } else {
        for (size_t index = sourceIndex; index > desiredIndex + 1;) {
            --index;
            if (HasUnknownRemaining || Masks[index]->HasKey(key)) {
                return TierOrder[index];
            }
        }
    }
    return desiredTier;
}

}
