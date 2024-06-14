#include "constructor_meta.h"
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

void TPortionMetaConstructor::FillMetaInfo(const NArrow::TFirstLastSpecialKeys& primaryKeys, const NArrow::TMinMaxSpecialKeys& snapshotKeys, const TIndexInfo& indexInfo) {
    AFL_VERIFY(!FirstAndLastPK);
    FirstAndLastPK = *primaryKeys.BuildAccordingToSchemaVerified(indexInfo.GetReplaceKey());
    AFL_VERIFY(!RecordSnapshotMin);
    AFL_VERIFY(!RecordSnapshotMax);
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

TPortionMetaConstructor::TPortionMetaConstructor(const TPortionMeta& meta) {
    FirstAndLastPK = meta.ReplaceKeyEdges;
    RecordSnapshotMin = meta.RecordSnapshotMin;
    RecordSnapshotMax = meta.RecordSnapshotMax;
    TierName = meta.GetTierNameOptional();
    if (!meta.StatisticsStorage.IsEmpty()) {
        StatisticsStorage = meta.StatisticsStorage;
    }
    if (meta.Produced != NPortion::EProduced::UNSPECIFIED) {
        Produced = meta.Produced;
    }
}

NKikimr::NOlap::TPortionMeta TPortionMetaConstructor::Build() {
    AFL_VERIFY(FirstAndLastPK);
    AFL_VERIFY(RecordSnapshotMin);
    AFL_VERIFY(RecordSnapshotMax);
    TPortionMeta result(*FirstAndLastPK, *RecordSnapshotMin, *RecordSnapshotMax);
    if (TierName) {
        result.TierName = *TierName;
    }
    AFL_VERIFY(Produced);
    result.Produced = *Produced;
    if (StatisticsStorage) {
        result.StatisticsStorage = *StatisticsStorage;
    }
    return result;
}

bool TPortionMetaConstructor::LoadMetadata(const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo) {
    if (!!Produced) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "DeserializeFromProto")("error", "parsing duplication");
        return true;
    }
    if (portionMeta.HasStatisticsStorage()) {
        auto parsed = NStatistics::TPortionStorage::BuildFromProto(portionMeta.GetStatisticsStorage());
        if (!parsed) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "DeserializeFromProto")("error", parsed.GetErrorMessage());
            return false;
        }
        StatisticsStorage = parsed.DetachResult();
        if (StatisticsStorage->IsEmpty()) {
            StatisticsStorage.reset();
        }
    }
    if (portionMeta.GetTierName()) {
        TierName = portionMeta.GetTierName();
    }
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
    AFL_VERIFY(Produced != TPortionMeta::EProduced::UNSPECIFIED);
    AFL_VERIFY(portionMeta.HasPrimaryKeyBorders());
    FirstAndLastPK = NArrow::TFirstLastSpecialKeys(portionMeta.GetPrimaryKeyBorders(), indexInfo.GetReplaceKey());

    AFL_VERIFY(portionMeta.HasRecordSnapshotMin());
    RecordSnapshotMin = TSnapshot(portionMeta.GetRecordSnapshotMin().GetPlanStep(), portionMeta.GetRecordSnapshotMin().GetTxId());
    AFL_VERIFY(portionMeta.HasRecordSnapshotMax());
    RecordSnapshotMax = TSnapshot(portionMeta.GetRecordSnapshotMax().GetPlanStep(), portionMeta.GetRecordSnapshotMax().GetTxId());
    return true;
}

void TPortionMetaConstructor::SetTierName(const TString& tierName) {
    AFL_VERIFY(!TierName);
    if (!tierName || tierName == NBlobOperations::TGlobal::DefaultStorageId) {
        TierName.reset();
    } else {
        TierName = tierName;
    }
}

}