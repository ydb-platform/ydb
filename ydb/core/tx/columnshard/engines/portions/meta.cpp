#include "meta.h"

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

NKikimrTxColumnShard::TIndexPortionMeta TPortionMeta::SerializeToProto() const {
    NKikimrTxColumnShard::TIndexPortionMeta portionMeta;
    portionMeta.SetTierName(TierName);
    portionMeta.SetDeletionsCount(DeletionsCount);
    switch (Produced) {
        case TPortionMeta::EProduced::UNSPECIFIED:
            Y_ABORT_UNLESS(false);
        case TPortionMeta::EProduced::INSERTED:
            portionMeta.SetIsInserted(true);
            break;
        case TPortionMeta::EProduced::COMPACTED:
            portionMeta.SetIsCompacted(true);
            break;
        case TPortionMeta::EProduced::SPLIT_COMPACTED:
            portionMeta.SetIsSplitCompacted(true);
            break;
        case TPortionMeta::EProduced::EVICTED:
            portionMeta.SetIsEvicted(true);
            break;
        case TPortionMeta::EProduced::INACTIVE:
            Y_ABORT("Unexpected inactive case");
            //portionMeta->SetInactive(true);
            break;
    }

    portionMeta.SetPrimaryKeyBorders(ReplaceKeyEdges.SerializeToStringDataOnlyNoCompression());

    RecordSnapshotMin.SerializeToProto(*portionMeta.MutableRecordSnapshotMin());
    RecordSnapshotMax.SerializeToProto(*portionMeta.MutableRecordSnapshotMax());
    return portionMeta;
}

TString TPortionMeta::DebugString() const {
    TStringBuilder sb;
    sb << "(produced=" << Produced << ";";
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

}
