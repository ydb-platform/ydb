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

}   // namespace NKikimr::NOlap
