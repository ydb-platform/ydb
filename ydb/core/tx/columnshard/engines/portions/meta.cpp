#include "meta.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

NKikimrTxColumnShard::TIndexPortionMeta TPortionMeta::SerializeToProto(const std::vector<TUnifiedBlobId>& blobIds, const NPortion::EProduced produced) const {
    AFL_VERIFY(blobIds.size());
    FullValidation();
    NKikimrTxColumnShard::TIndexPortionMeta portionMeta;
    portionMeta.SetTierName(TierName);
    portionMeta.SetCompactionLevel(CompactionLevel);
    portionMeta.SetDeletionsCount(DeletionsCount);
    portionMeta.SetRecordsCount(RecordsCount);
    portionMeta.SetColumnRawBytes(ColumnRawBytes);
    portionMeta.SetColumnBlobBytes(ColumnBlobBytes);
    portionMeta.SetIndexRawBytes(IndexRawBytes);
    portionMeta.SetIndexBlobBytes(IndexBlobBytes);
    switch (produced) {
        case NPortion::EProduced::UNSPECIFIED:
            Y_ABORT_UNLESS(false);
        case NPortion::EProduced::INSERTED:
            portionMeta.SetIsInserted(true);
            break;
        case NPortion::EProduced::COMPACTED:
            portionMeta.SetIsCompacted(true);
            break;
        case NPortion::EProduced::SPLIT_COMPACTED:
            portionMeta.SetIsSplitCompacted(true);
            break;
        case NPortion::EProduced::EVICTED:
            portionMeta.SetIsEvicted(true);
            break;
        case NPortion::EProduced::INACTIVE:
            Y_ABORT("Unexpected inactive case");
            //portionMeta->SetInactive(true);
            break;
    }

    portionMeta.MutablePrimaryKeyBordersV1()->SetFirst(FirstPKRow.GetData());
    portionMeta.MutablePrimaryKeyBordersV1()->SetLast(LastPKRow.GetData());
    if (!HasAppData() || AppDataVerified().ColumnShardConfig.GetPortionMetaV0Usage()) {
        portionMeta.SetPrimaryKeyBorders(
            NArrow::TFirstLastSpecialKeys(FirstPKRow.GetData(), LastPKRow.GetData(), PKSchema).SerializePayloadToString());
    }

    RecordSnapshotMin.SerializeToProto(*portionMeta.MutableRecordSnapshotMin());
    RecordSnapshotMax.SerializeToProto(*portionMeta.MutableRecordSnapshotMax());
    for (auto&& i : blobIds) {
        *portionMeta.AddBlobIds() = i.GetLogoBlobId().AsBinaryString();
    }
    return portionMeta;
}

TString TPortionMeta::DebugString() const {
    TStringBuilder sb;
    sb << "(";
    if (TierName) {
        sb << "tier_name=" << TierName << ";";
    }
    sb << ")";
    return sb;
}

std::optional<TString> TPortionMeta::GetTierNameOptional() const {
    if (TierName && TierName != NBlobOperations::TGlobal::DefaultStorageId) {
        return TierName;
    } else {
        return std::nullopt;
    }
}

TString TPortionAddress::DebugString() const {
    return TStringBuilder() << "(path_id=" << PathId << ";portion_id=" << PortionId << ")";
}

void TPortionMetaBase::FullValidation() const {
    for (auto&& i : BlobIds) {
        AFL_VERIFY(i.BlobSize());
    }
    AFL_VERIFY(BlobIds.size());
}

std::optional<TBlobRangeLink16::TLinkId> TPortionMetaBase::GetBlobIdxOptional(const TUnifiedBlobId& blobId) const {
    AFL_VERIFY(blobId.IsValid());
    TBlobRangeLink16::TLinkId idx = 0;
    for (auto &&i : BlobIds) {
        if (i == blobId) {
            return idx;
        }
        ++idx;
    }
    return std::nullopt;
}
TBlobRangeLink16::TLinkId TPortionMetaBase::GetBlobIdxVerified(const TUnifiedBlobId& blobId) const {
    auto result = GetBlobIdxOptional(blobId);
    AFL_VERIFY(result);
    return *result;
}

const TUnifiedBlobId& TPortionMetaBase::GetBlobId(const TBlobRangeLink16::TLinkId linkId) const {
    AFL_VERIFY(linkId < GetBlobIds().size());
    return BlobIds[linkId];
}

const std::vector<TUnifiedBlobId>& TPortionMetaBase::GetBlobIds() const {
    return BlobIds;
}

ui32 TPortionMetaBase::GetBlobIdsCount() const {
    return BlobIds.size();
}

TPortionMetaBase::TPortionMetaBase(std::vector<TUnifiedBlobId>&& blobIds)
    : BlobIds(std::move(blobIds)) {
}

TPortionMetaBase::TPortionMetaBase(const std::vector<TUnifiedBlobId>& blobIds)
    : BlobIds(blobIds) {
}

std::vector<TUnifiedBlobId> TPortionMetaBase::ExtractBlobIds() {
    return std::move(BlobIds);
}

TBlobRangeLink16::TLinkId TPortionMetaBase::GetBlobIdxVerifiedPrivate(const TUnifiedBlobId& blobId) const {
    auto result = GetBlobIdxOptional(blobId);
    AFL_VERIFY(result);
    return *result;
}

const std::vector<TUnifiedBlobId>& TPortionMetaBase::GetBlobIdsPrivate() const {
    return BlobIds;
}
const TBlobRange TPortionMetaBase::RestoreBlobRange(const TBlobRangeLink16& linkRange) const {
    return linkRange.RestoreRange(GetBlobId(linkRange.GetBlobIdxVerified()));
}

ui64 TPortionMetaBase::GetMetadataMemorySize() const {
    return GetBlobIds().capacity() * sizeof(TUnifiedBlobId);
}

ui64 TPortionMetaBase::GetMetadataDataSize() const {
    return GetBlobIds().size() * sizeof(TUnifiedBlobId);
}

TPortionMeta::TPortionMeta(NArrow::TFirstLastSpecialKeys& pk, const TSnapshot& min, const TSnapshot& max)
    : PKSchema(pk.GetSchema()), FirstPKRow(pk.GetFirst().GetContent()),
      LastPKRow(pk.GetLast().GetContent()), RecordSnapshotMin(min),
      RecordSnapshotMax(max) {
    AFL_VERIFY_DEBUG(IndexKeyStart() <= IndexKeyEnd())
        ("start", IndexKeyStart().DebugString())
        ("end", IndexKeyEnd().DebugString());
}

void TPortionMeta::FullValidation() const {
    AFL_VERIFY(RecordsCount);
    AFL_VERIFY(ColumnRawBytes);
    AFL_VERIFY(ColumnBlobBytes);
}

NArrow::TSimpleRow TPortionMeta::IndexKeyStart() const {
    return FirstPKRow.Build(PKSchema);
}

NArrow::TSimpleRow TPortionMeta::IndexKeyEnd() const {
    return LastPKRow.Build(PKSchema);
}

void TPortionMeta::ResetCompactionLevel(const ui32 level) {
    CompactionLevel = level;
}

ui64 TPortionMeta::GetMetadataMemorySize() const {
    return GetMemorySize();
}

ui64 TPortionMeta::GetMemorySize() const {
    return sizeof(TPortionMeta) + FirstPKRow.GetMemorySize() +
            LastPKRow.GetMemorySize();
}

ui64 TPortionMeta::GetDataSize() const {
    return sizeof(TPortionMeta) + FirstPKRow.GetDataSize() +
            LastPKRow.GetDataSize();
}

TPortionAddress::TPortionAddress(const TInternalPathId pathId,
                                 const ui64 portionId)
    : PathId(pathId)
    , PortionId(portionId) {
}

bool TPortionAddress::operator<(const TPortionAddress& item) const {
    return std::tie(PathId, PortionId) < std::tie(item.PathId, item.PortionId);
}

bool TPortionAddress::operator==(const TPortionAddress& item) const {
    return std::tie(PathId, PortionId) == std::tie(item.PathId, item.PortionId);
}

const TString TPortionAddress::Debug() const {
    return TStringBuilder{} << "PathId: " << PathId.DebugString()
                          << ", PortionId: " << PortionId;
}

} // namespace NKikimr::NOlap

ui64 THash<NKikimr::NOlap::TPortionAddress>::operator()(const NKikimr::NOlap::TPortionAddress& x) const noexcept {
    return CombineHashes(x.GetPortionId(), x.GetPathId().GetRawValue());
}
