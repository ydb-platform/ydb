#include "flat_compaction_context.h"

#include "flat_bloom_hash.h"
#include "flat_part_slice.h"

namespace NKikimr::NTable {
namespace {

    class TCompactionKeySpace final : public NPage::IKeySpace {
    public:
        TCompactionKeySpace(TIntrusiveConstPtr<TKeyCellDefaults> keys,
                TConstArrayRef<TCompactionPartInfo> parts)
            : Keys(std::move(keys))
        {
            Y_ENSURE(Keys, "Compaction key space requires a key schema");
            // Equal PG values may have different binary encodings. Only raw
            // bloom prefixes before the first PG column can prove absence.
            for (const auto& type : Keys->Types) {
                if (type.GetTypeId() == NScheme::NTypeIds::Pg) {
                    break;
                }
                ++BloomPrefixLimit;
            }
            Parts.reserve(parts.size());
            for (const auto& part : parts) {
                Y_ENSURE(bool(part.Part) != bool(part.ColdPart),
                    "Compaction key space requires exactly one part representation");
                Parts.emplace_back(part);
            }
        }

        void Reset() override {
            for (auto& part : Parts) {
                part.SliceIndex = 0;
            }
        }

        bool HasKey(TCellsRef key) override {
            // TKeyCellDefaults is nonempty, so this also rejects empty keys.
            // Prefix queries cannot prove the absence of a complete key using
            // full-key bloom filters or the search-key bounds convention.
            Y_ENSURE(key.size() == Keys->Size(),
                "Compaction key space requires a complete key");
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

                if (part->ByKeyPrefixes.empty() || BloomPrefixLimit == 0) {
                    return true;
                }
                if (!prefix) {
                    prefix.emplace(key);
                }
                if (part->MightHaveKeyPrefix(*prefix, BloomPrefixLimit)) {
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
            // Only advances: IKeySpace requires HasKey queries in increasing
            // key order. Reset() rewinds the cursors owned by this mask only.
            size_t SliceIndex = 0;
        };

        const TIntrusiveConstPtr<TKeyCellDefaults> Keys;
        ui32 BloomPrefixLimit = 0;
        TVector<TPartCursor> Parts;
    };

}

THolder<NPage::IKeySpace> MakeCompactionKeySpace(
        TIntrusiveConstPtr<TKeyCellDefaults> keys,
        TConstArrayRef<TCompactionPartInfo> parts)
{
    return MakeHolder<TCompactionKeySpace>(std::move(keys), parts);
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

}
