#include "meta.h"

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

void TPortionMeta::FillBatchInfo(const NArrow::TFirstLastSpecialKeys& primaryKeys, const NArrow::TMinMaxSpecialKeys& snapshotKeys, const TIndexInfo& indexInfo) {
    {
        ReplaceKeyEdges = primaryKeys.BuildAccordingToSchemaVerified(indexInfo.GetReplaceKey());
        IndexKeyStart = ReplaceKeyEdges->GetFirst();
        IndexKeyEnd = ReplaceKeyEdges->GetLast();
        AFL_VERIFY(IndexKeyStart);
        AFL_VERIFY(IndexKeyEnd);
        AFL_VERIFY(*IndexKeyStart <= *IndexKeyEnd)("start", IndexKeyStart->DebugString())("end", IndexKeyEnd->DebugString());
    }

    {
        auto cPlanStep = snapshotKeys.GetBatch()->GetColumnByName(TIndexInfo::SPEC_COL_PLAN_STEP);
        auto cTxId = snapshotKeys.GetBatch()->GetColumnByName(TIndexInfo::SPEC_COL_TX_ID);
        Y_ABORT_UNLESS(cPlanStep && cTxId);
        Y_ABORT_UNLESS(cPlanStep->type_id() == arrow::UInt64Type::type_id);
        Y_ABORT_UNLESS(cTxId->type_id() == arrow::UInt64Type::type_id);
        const arrow::UInt64Array& cPlanStepArray = static_cast<const arrow::UInt64Array&>(*cPlanStep);
        const arrow::UInt64Array& cTxIdArray = static_cast<const arrow::UInt64Array&>(*cTxId);
        RecordSnapshotMin = TSnapshot(cPlanStepArray.GetView(0), cTxIdArray.GetView(0));
        RecordSnapshotMax = TSnapshot(cPlanStepArray.GetView(snapshotKeys.GetBatch()->num_rows() - 1), cTxIdArray.GetView(snapshotKeys.GetBatch()->num_rows() - 1));
    }
}

bool TPortionMeta::DeserializeFromProto(const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo) {
    if (Produced != TPortionMeta::EProduced::UNSPECIFIED) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "DeserializeFromProto")("error", "parsing duplication");
        return true;
    }
    FirstPkColumn = indexInfo.GetPKFirstColumnId();
    TierName = portionMeta.GetTierName();
    if (portionMeta.GetIsInserted()) {
        Produced = TPortionMeta::EProduced::INSERTED;
    } else if (portionMeta.GetIsCompacted()) {
        Produced = TPortionMeta::EProduced::COMPACTED;
    } else if (portionMeta.GetIsSplitCompacted()) {
        Produced = TPortionMeta::EProduced::SPLIT_COMPACTED;
    } else if (portionMeta.GetIsEvicted()) {
        Produced = TPortionMeta::EProduced::EVICTED;
    } else {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "DeserializeFromProto")("error", "incorrect portion meta")("meta", portionMeta.DebugString());
        return false;
    }
    Y_ABORT_UNLESS(Produced != TPortionMeta::EProduced::UNSPECIFIED);

    if (portionMeta.HasPrimaryKeyBorders()) {
        ReplaceKeyEdges = std::make_shared<NArrow::TFirstLastSpecialKeys>(portionMeta.GetPrimaryKeyBorders(), indexInfo.GetReplaceKey());
        IndexKeyStart = ReplaceKeyEdges->GetFirst();
        IndexKeyEnd = ReplaceKeyEdges->GetLast();
        AFL_VERIFY(IndexKeyStart);
        AFL_VERIFY(IndexKeyEnd);
        AFL_VERIFY (*IndexKeyStart <= *IndexKeyEnd)("start", IndexKeyStart->DebugString())("end", IndexKeyEnd->DebugString());
    }

    if (portionMeta.HasRecordSnapshotMin()) {
        RecordSnapshotMin = TSnapshot(portionMeta.GetRecordSnapshotMin().GetPlanStep(), portionMeta.GetRecordSnapshotMin().GetTxId());
    }
    if (portionMeta.HasRecordSnapshotMax()) {
        RecordSnapshotMax = TSnapshot(portionMeta.GetRecordSnapshotMax().GetPlanStep(), portionMeta.GetRecordSnapshotMax().GetTxId());
    }
    return true;
}

NKikimrTxColumnShard::TIndexPortionMeta TPortionMeta::SerializeToProto() const {
    NKikimrTxColumnShard::TIndexPortionMeta portionMeta;
    portionMeta.SetTierName(TierName);

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

    if (ReplaceKeyEdges) {
        portionMeta.SetPrimaryKeyBorders(ReplaceKeyEdges->SerializeToStringDataOnlyNoCompression());
    }

    if (RecordSnapshotMin) {
        portionMeta.MutableRecordSnapshotMin()->SetPlanStep(RecordSnapshotMin->GetPlanStep());
        portionMeta.MutableRecordSnapshotMin()->SetTxId(RecordSnapshotMin->GetTxId());
    }
    if (RecordSnapshotMax) {
        portionMeta.MutableRecordSnapshotMax()->SetPlanStep(RecordSnapshotMax->GetPlanStep());
        portionMeta.MutableRecordSnapshotMax()->SetTxId(RecordSnapshotMax->GetTxId());
    }
    return portionMeta;
}

std::optional<NKikimrTxColumnShard::TIndexPortionMeta> TPortionMeta::SerializeToProto(const ui32 columnId, const ui32 chunk) const {
    if (!IsChunkWithPortionInfo(columnId, chunk)) {
        return {};
    }

    return SerializeToProto();
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

TString TPortionAddress::DebugString() const {
    return TStringBuilder() << "(path_id=" << PathId << ";portion_id=" << PortionId << ")";
}

}
