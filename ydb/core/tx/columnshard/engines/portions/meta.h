#pragma once
#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/protos/portion_info.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/replace_key.h>

#include <util/stream/output.h>

namespace NKikimr::NOlap {

struct TIndexInfo;

struct TPortionMeta {
private:
    NArrow::TFirstLastSpecialKeys ReplaceKeyEdges;   // first and last PK rows
    YDB_READONLY_DEF(TString, TierName);
    YDB_READONLY(ui32, DeletionsCount, 0);
    YDB_READONLY(ui32, CompactionLevel, 0);
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY(ui64, ColumnRawBytes, 0);
    YDB_READONLY(ui32, ColumnBlobBytes, 0);
    YDB_READONLY(ui32, IndexRawBytes, 0);
    YDB_READONLY(ui32, IndexBlobBytes, 0);
    YDB_READONLY_DEF(std::vector<TUnifiedBlobId>, BlobIds);

    friend class TPortionMetaConstructor;
    friend class TPortionInfo;
    TPortionMeta(NArrow::TFirstLastSpecialKeys& pk, const TSnapshot& min, const TSnapshot& max)
        : ReplaceKeyEdges(pk)
        , RecordSnapshotMin(min)
        , RecordSnapshotMax(max)
        , IndexKeyStart(pk.GetFirst())
        , IndexKeyEnd(pk.GetLast()) {
        AFL_VERIFY(IndexKeyStart <= IndexKeyEnd)("start", IndexKeyStart.DebugString())("end", IndexKeyEnd.DebugString());
    }
    TSnapshot RecordSnapshotMin;
    TSnapshot RecordSnapshotMax;

    void FullValidation() const {
        for (auto&& i : BlobIds) {
            AFL_VERIFY(i.BlobSize());
        }
        AFL_VERIFY(BlobIds.size());
        AFL_VERIFY(RecordsCount);
        AFL_VERIFY(ColumnRawBytes);
        AFL_VERIFY(ColumnBlobBytes);
    }

public:
    const NArrow::TFirstLastSpecialKeys& GetFirstLastPK() const {
        return ReplaceKeyEdges;
    }

    const TUnifiedBlobId& GetBlobId(const TBlobRangeLink16::TLinkId linkId) const {
        AFL_VERIFY(linkId < GetBlobIds().size());
        return BlobIds[linkId];
    }

    ui32 GetBlobIdsCount() const {
        return BlobIds.size();
    }

    void ResetCompactionLevel(const ui32 level) {
        CompactionLevel = level;
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

    using EProduced = NPortion::EProduced;

    NArrow::TReplaceKey IndexKeyStart;
    NArrow::TReplaceKey IndexKeyEnd;

    EProduced Produced = EProduced::UNSPECIFIED;

    std::optional<TString> GetTierNameOptional() const;

    ui64 GetMetadataMemorySize() const {
        return sizeof(TPortionMeta) + ReplaceKeyEdges.GetMemorySize() + BlobIds.size() * sizeof(TUnifiedBlobId);
    }

    NKikimrTxColumnShard::TIndexPortionMeta SerializeToProto() const;

    EProduced GetProduced() const {
        return Produced;
    }

    TString DebugString() const;
};

class TPortionAddress {
private:
    YDB_READONLY(ui64, PathId, 0);
    YDB_READONLY(ui64, PortionId, 0);

public:
    TPortionAddress(const ui64 pathId, const ui64 portionId)
        : PathId(pathId)
        , PortionId(portionId) {
    }

    TString DebugString() const;

    bool operator<(const TPortionAddress& item) const {
        return std::tie(PathId, PortionId) < std::tie(item.PathId, item.PortionId);
    }

    bool operator==(const TPortionAddress& item) const {
        return std::tie(PathId, PortionId) == std::tie(item.PathId, item.PortionId);
    }
};

}   // namespace NKikimr::NOlap

template <>
struct THash<NKikimr::NOlap::TPortionAddress> {
    inline ui64 operator()(const NKikimr::NOlap::TPortionAddress& x) const noexcept {
        return CombineHashes(x.GetPortionId(), x.GetPathId());
    }
};
