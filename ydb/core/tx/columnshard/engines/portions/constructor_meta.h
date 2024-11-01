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
    class TAddressBlobId {
    private:
        TChunkAddress Address;
        YDB_READONLY(TBlobRangeLink16::TLinkId, BlobIdx, 0);

    public:
        const TChunkAddress& GetAddress() const {
            return Address;
        }

        TAddressBlobId(const TChunkAddress& address, const TBlobRangeLink16::TLinkId blobIdx)
            : Address(address)
            , BlobIdx(blobIdx) {
        }
    };
    std::vector<TAddressBlobId> BlobIdxs;
    bool NeedBlobIdxsSort = false;

    friend class TPortionInfoConstructor;
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

    void RegisterBlobIdx(const TChunkAddress& address, const TBlobRangeLink16::TLinkId blobIdx) {
        if (BlobIdxs.size() && address < BlobIdxs.back().GetAddress()) {
            NeedBlobIdxsSort = true;
        }
        BlobIdxs.emplace_back(address, blobIdx);
    }

    const TBlobRange RestoreBlobRangeSlow(const TBlobRangeLink16& linkRange, const TChunkAddress& address) const {
        for (auto&& i : BlobIdxs) {
            if (i.GetAddress() == address) {
                return linkRange.RestoreRange(GetBlobId(i.GetBlobIdx()));
            }
        }
        AFL_VERIFY(false);
        return TBlobRange();
    }

    void ReorderBlobs() {
        if (NeedBlobIdxsSort) {
            auto pred = [](const TAddressBlobId& l, const TAddressBlobId& r) {
                return l.GetAddress() < r.GetAddress();
            };
            std::sort(BlobIdxs.begin(), BlobIdxs.end(), pred);
        }
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
