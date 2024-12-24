#pragma once
#include "common.h"
#include "meta.h"

#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NOlap {
class TPortionInfoConstructor;
struct TIndexInfo;

class TPortionMetaConstructor {
private:
    std::optional<NArrow::TFirstLastSpecialKeys> FirstAndLastPK;
    std::optional<TString> TierName;
    std::optional<TSnapshot> RecordSnapshotMin;
    std::optional<TSnapshot> RecordSnapshotMax;
    std::optional<NPortion::EProduced> Produced;
    std::optional<ui64> CompactionLevel;

    std::optional<ui32> RecordsCount;
    std::optional<ui64> ColumnRawBytes;
    std::optional<ui32> ColumnBlobBytes;
    std::optional<ui32> IndexRawBytes;
    std::optional<ui32> IndexBlobBytes;

    std::optional<ui32> DeletionsCount;

    std::vector<TUnifiedBlobId> BlobIds;

    friend class TPortionInfoConstructor;
    friend class TPortionAccessorConstructor;
    void FillMetaInfo(const NArrow::TFirstLastSpecialKeys& primaryKeys, const ui32 deletionsCount,
        const std::optional<NArrow::TMinMaxSpecialKeys>& snapshotKeys, const TIndexInfo& indexInfo);

public:
    TPortionMetaConstructor() = default;
    TPortionMetaConstructor(const TPortionMeta& meta, const bool withBlobs);

    const TBlobRange RestoreBlobRange(const TBlobRangeLink16& linkRange) const {
        return linkRange.RestoreRange(GetBlobId(linkRange.GetBlobIdxVerified()));
    }

    ui32 GetBlobIdsCount() const {
        return BlobIds.size();
    }

    const TUnifiedBlobId& GetBlobId(const TBlobRangeLink16::TLinkId linkId) const {
        AFL_VERIFY(linkId < BlobIds.size());
        return BlobIds[linkId];
    }

    TBlobRangeLink16::TLinkId RegisterBlobId(const TUnifiedBlobId& blobId) {
        AFL_VERIFY(blobId.IsValid());
        TBlobRangeLink16::TLinkId idx = 0;
        for (auto&& i : BlobIds) {
            if (i == blobId) {
                return idx;
            }
            ++idx;
        }
        BlobIds.emplace_back(blobId);
        return idx;
    }

    std::optional<TBlobRangeLink16::TLinkId> GetBlobIdxOptional(const TUnifiedBlobId& blobId) const {
        AFL_VERIFY(blobId.IsValid());
        TBlobRangeLink16::TLinkId idx = 0;
        for (auto&& i : BlobIds) {
            if (i == blobId) {
                return idx;
            }
            ++idx;
        }
        return std::nullopt;
    }

    TBlobRangeLink16::TLinkId GetBlobIdxVerified(const TUnifiedBlobId& blobId) const {
        auto result = GetBlobIdxOptional(blobId);
        AFL_VERIFY(result);
        return *result;
    }

    void SetCompactionLevel(const ui64 level) {
        CompactionLevel = level;
    }

    void SetTierName(const TString& tierName);
    void ResetTierName(const TString& tierName) {
        TierName.reset();
        SetTierName(tierName);
    }

    void UpdateRecordsMeta(const NPortion::EProduced prod) {
        Produced = prod;
    }

    TPortionMeta Build();

    [[nodiscard]] bool LoadMetadata(
        const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo, const IBlobGroupSelector& groupSelector);
};

}   // namespace NKikimr::NOlap
