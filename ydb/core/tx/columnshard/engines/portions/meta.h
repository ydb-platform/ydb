#pragma once
#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/protos/portion_info.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/replace_key.h>

#include <util/stream/output.h>

namespace NKikimr::NOlap {

struct TIndexInfo;

class TPortionMetaBase {
protected:
    std::vector<TUnifiedBlobId> BlobIds;

    void FullValidation() const {
        for (auto&& i : BlobIds) {
            AFL_VERIFY(i.BlobSize());
        }
        AFL_VERIFY(BlobIds.size());
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

public:
    TPortionMetaBase() = default;

    const TUnifiedBlobId& GetBlobId(const TBlobRangeLink16::TLinkId linkId) const {
        AFL_VERIFY(linkId < GetBlobIds().size());
        return BlobIds[linkId];
    }
    const std::vector<TUnifiedBlobId>& GetBlobIds() const {
        return BlobIds;
    }

    ui32 GetBlobIdsCount() const {
        return BlobIds.size();
    }

    TPortionMetaBase(std::vector<TUnifiedBlobId>&& blobIds)
        : BlobIds(std::move(blobIds)) {
    }

    TPortionMetaBase(const std::vector<TUnifiedBlobId>& blobIds)
        : BlobIds(blobIds) {
    }

    std::vector<TUnifiedBlobId> ExtractBlobIds() {
        return std::move(BlobIds);
    }

    TBlobRangeLink16::TLinkId GetBlobIdxVerifiedPrivate(const TUnifiedBlobId& blobId) const {
        auto result = GetBlobIdxOptional(blobId);
        AFL_VERIFY(result);
        return *result;
    }

    const std::vector<TUnifiedBlobId>& GetBlobIdsPrivate() const {
        return BlobIds;
    }

    const TBlobRange RestoreBlobRange(const TBlobRangeLink16& linkRange) const {
        return linkRange.RestoreRange(GetBlobId(linkRange.GetBlobIdxVerified()));
    }

    ui64 GetMetadataMemorySize() const {
        return GetBlobIds().capacity() * sizeof(TUnifiedBlobId);
    }

    ui64 GetMetadataDataSize() const {
        return GetBlobIds().size() * sizeof(TUnifiedBlobId);
    }
};

class TPortionMeta {
private:
    std::shared_ptr<arrow::Schema> PKSchema;
    NArrow::TSimpleRowContent FirstPKRow;
    NArrow::TSimpleRowContent LastPKRow;
    YDB_READONLY_DEF(TString, TierName);
    YDB_READONLY(ui32, DeletionsCount, 0);
    YDB_READONLY(ui32, CompactionLevel, 0);
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY(ui64, ColumnRawBytes, 0);
    YDB_READONLY(ui32, ColumnBlobBytes, 0);
    YDB_READONLY(ui32, IndexRawBytes, 0);
    YDB_READONLY(ui32, IndexBlobBytes, 0);

    friend class TPortionMetaConstructor;
    friend class TPortionInfo;
    friend class TCompactedPortionInfo;
    TPortionMeta(NArrow::TFirstLastSpecialKeys& pk, const TSnapshot& min, const TSnapshot& max)
        : PKSchema(pk.GetSchema())
        , FirstPKRow(pk.GetFirst().GetContent())
        , LastPKRow(pk.GetLast().GetContent())
        , RecordSnapshotMin(min)
        , RecordSnapshotMax(max) {
        AFL_VERIFY(IndexKeyStart() <= IndexKeyEnd())("start", IndexKeyStart().DebugString())("end", IndexKeyEnd().DebugString());
    }
    TSnapshot RecordSnapshotMin;
    TSnapshot RecordSnapshotMax;

public:
    void FullValidation() const {
        AFL_VERIFY(RecordsCount);
        AFL_VERIFY(ColumnRawBytes);
        AFL_VERIFY(ColumnBlobBytes);
    }

    NArrow::TSimpleRow IndexKeyStart() const {
        return FirstPKRow.Build(PKSchema);
    }

    NArrow::TSimpleRow IndexKeyEnd() const {
        return LastPKRow.Build(PKSchema);
    }

    void ResetCompactionLevel(const ui32 level) {
        CompactionLevel = level;
    }

    std::optional<TString> GetTierNameOptional() const;

    ui64 GetMetadataMemorySize() const {
        return GetMemorySize();
    }

    ui64 GetMemorySize() const {
        return sizeof(TPortionMeta) + FirstPKRow.GetMemorySize() + LastPKRow.GetMemorySize();
    }

    ui64 GetDataSize() const {
        return sizeof(TPortionMeta) + FirstPKRow.GetDataSize() + LastPKRow.GetDataSize();
    }

    NKikimrTxColumnShard::TIndexPortionMeta SerializeToProto(
        const std::vector<TUnifiedBlobId>& blobIds, const NPortion::EProduced produced) const;

    TString DebugString() const;
};

class TPortionAddress {
private:
    YDB_READONLY_DEF(TInternalPathId, PathId);
    YDB_READONLY(ui64, PortionId, 0);

public:
    TPortionAddress(const TInternalPathId pathId, const ui64 portionId)
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
        return CombineHashes(x.GetPortionId(), x.GetPathId().GetRawValue());
    }
};
