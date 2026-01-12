#pragma once
#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/portions/index_chunk.h>
#include <ydb/core/tx/columnshard/engines/protos/portion_info.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/replace_key.h>

#include <util/stream/output.h>

namespace NKikimr::NOlap {

struct TIndexInfo;

class TPortionMetaBase {
protected:
    std::vector<TUnifiedBlobId> BlobIds;

    void FullValidation() const;

    std::optional<TBlobRangeLink16::TLinkId> GetBlobIdxOptional(const TUnifiedBlobId& blobId) const;

    TBlobRangeLink16::TLinkId GetBlobIdxVerified(const TUnifiedBlobId& blobId) const;

    static std::vector<ui32> DoCalcSliceBorderOffsets(const std::vector<TColumnRecord>& records, const std::vector<TIndexChunk>& indexes);

public:
    TPortionMetaBase() = default;

    const TUnifiedBlobId& GetBlobId(const TBlobRangeLink16::TLinkId linkId) const;
    const std::vector<TUnifiedBlobId>& GetBlobIds() const;

    ui32 GetBlobIdsCount() const;

    TPortionMetaBase(std::vector<TUnifiedBlobId>&& blobIds);

    TPortionMetaBase(const std::vector<TUnifiedBlobId>& blobIds);

    std::vector<TUnifiedBlobId> ExtractBlobIds();

    TBlobRangeLink16::TLinkId GetBlobIdxVerifiedPrivate(const TUnifiedBlobId& blobId) const;

    const std::vector<TUnifiedBlobId>& GetBlobIdsPrivate() const;

    const TBlobRange RestoreBlobRange(const TBlobRangeLink16& linkRange) const;

    ui64 GetMetadataMemorySize() const;

    ui64 GetMetadataDataSize() const;
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
    YDB_READONLY(ui32, NumSlices, 1);

    friend class TPortionMetaConstructor;
    friend class TPortionInfo;
    friend class TCompactedPortionInfo;
    TPortionMeta(NArrow::TFirstLastSpecialKeys& pk, const TSnapshot& min, const TSnapshot& max);
    TSnapshot RecordSnapshotMin;
    TSnapshot RecordSnapshotMax;

public:
    void FullValidation() const;

    NArrow::TSimpleRow IndexKeyStart() const;

    NArrow::TSimpleRow IndexKeyEnd() const;

    void ResetCompactionLevel(const ui32 level);

    std::optional<TString> GetTierNameOptional() const;

    ui64 GetMetadataMemorySize() const;

    ui64 GetMemorySize() const;

    ui64 GetDataSize() const;

    NKikimrTxColumnShard::TIndexPortionMeta
    SerializeToProto(const std::vector<TUnifiedBlobId>& blobIds,
                     const NPortion::EProduced produced) const;

    TString DebugString() const;
};

class TPortionAddress {
private:
    YDB_READONLY_DEF(TInternalPathId, PathId);
    YDB_READONLY(ui64, PortionId, 0);

public:
    TPortionAddress(const TInternalPathId pathId, const ui64 portionId);

    TString DebugString() const;

    bool operator<(const TPortionAddress& item) const;

    bool operator==(const TPortionAddress& item) const;

    const TString Debug() const;
};

}   // namespace NKikimr::NOlap

template <>
struct THash<NKikimr::NOlap::TPortionAddress> {
    ui64 operator()(const NKikimr::NOlap::TPortionAddress& x) const noexcept;
};
