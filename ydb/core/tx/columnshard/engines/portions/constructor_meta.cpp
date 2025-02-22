#include "constructor_meta.h"
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

void TPortionMetaConstructor::FillMetaInfo(const NArrow::TFirstLastSpecialKeys& primaryKeys, const ui32 deletionsCount, const std::optional<NArrow::TMinMaxSpecialKeys>& snapshotKeys, const TIndexInfo& indexInfo) {
    AFL_VERIFY(!FirstAndLastPK);
    FirstAndLastPK = *primaryKeys.BuildAccordingToSchemaVerified(indexInfo.GetReplaceKey());
    AFL_VERIFY(!RecordSnapshotMin);
    AFL_VERIFY(!RecordSnapshotMax);
    DeletionsCount = deletionsCount;
    if (snapshotKeys) {
        auto cPlanStep = snapshotKeys->GetBatch()->GetColumnByName(TIndexInfo::SPEC_COL_PLAN_STEP);
        auto cTxId = snapshotKeys->GetBatch()->GetColumnByName(TIndexInfo::SPEC_COL_TX_ID);
        Y_ABORT_UNLESS(cPlanStep && cTxId);
        Y_ABORT_UNLESS(cPlanStep->type_id() == arrow::UInt64Type::type_id);
        Y_ABORT_UNLESS(cTxId->type_id() == arrow::UInt64Type::type_id);
        const arrow::UInt64Array& cPlanStepArray = static_cast<const arrow::UInt64Array&>(*cPlanStep);
        const arrow::UInt64Array& cTxIdArray = static_cast<const arrow::UInt64Array&>(*cTxId);
        RecordSnapshotMin = TSnapshot(cPlanStepArray.GetView(0), cTxIdArray.GetView(0));
        RecordSnapshotMax = TSnapshot(cPlanStepArray.GetView(snapshotKeys->GetBatch()->num_rows() - 1), cTxIdArray.GetView(snapshotKeys->GetBatch()->num_rows() - 1));
    } else {
        RecordSnapshotMin = TSnapshot::Zero();
        RecordSnapshotMax = TSnapshot::Zero();
    }
}

TPortionMetaConstructor::TPortionMetaConstructor(const TPortionMeta& meta, const bool withBlobs) {
    FirstAndLastPK = meta.ReplaceKeyEdges;
    RecordSnapshotMin = meta.RecordSnapshotMin;
    RecordSnapshotMax = meta.RecordSnapshotMax;
    CompactionLevel = meta.GetCompactionLevel();
    DeletionsCount = meta.GetDeletionsCount();
    TierName = meta.GetTierNameOptional();
    if (withBlobs) {
        BlobIds = meta.BlobIds;
    }
    if (meta.Produced != NPortion::EProduced::UNSPECIFIED) {
        Produced = meta.Produced;
    }
}

TPortionMeta TPortionMetaConstructor::Build() {
    AFL_VERIFY(FirstAndLastPK);
    AFL_VERIFY(RecordSnapshotMin);
    AFL_VERIFY(RecordSnapshotMax);
    TPortionMeta result(*FirstAndLastPK, *RecordSnapshotMin, *RecordSnapshotMax);
    if (TierName) {
        result.TierName = *TierName;
    }
    TBase::FullValidation();
    result.BlobIds = BlobIds;
    result.BlobIds.shrink_to_fit();
    result.CompactionLevel = *TValidator::CheckNotNull(CompactionLevel);
    result.DeletionsCount = *TValidator::CheckNotNull(DeletionsCount);
    result.Produced = *TValidator::CheckNotNull(Produced);

    result.RecordsCount = *TValidator::CheckNotNull(RecordsCount);
    result.ColumnRawBytes = *TValidator::CheckNotNull(ColumnRawBytes);
    result.ColumnBlobBytes = *TValidator::CheckNotNull(ColumnBlobBytes);
    result.IndexRawBytes = *TValidator::CheckNotNull(IndexRawBytes);
    result.IndexBlobBytes = *TValidator::CheckNotNull(IndexBlobBytes);

    return result;
}

bool TPortionMetaConstructor::LoadMetadata(const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo, const IBlobGroupSelector& groupSelector) {
    AFL_VERIFY(!Produced)("produced", Produced);
    if (portionMeta.GetTierName()) {
        TierName = portionMeta.GetTierName();
    }
    if (portionMeta.HasDeletionsCount()) {
        DeletionsCount = portionMeta.GetDeletionsCount();
    } else {
        DeletionsCount = 0;
    }
    for (auto&& i : portionMeta.GetBlobIds()) {
        TLogoBlobID logo = TLogoBlobID::FromBinary(i);
        BlobIds.emplace_back(TUnifiedBlobId(groupSelector.GetGroup(logo), logo));
    }
    CompactionLevel = portionMeta.GetCompactionLevel();
    RecordsCount = TValidator::CheckNotNull(portionMeta.GetRecordsCount());
    ColumnRawBytes = TValidator::CheckNotNull(portionMeta.GetColumnRawBytes());
    ColumnBlobBytes = TValidator::CheckNotNull(portionMeta.GetColumnBlobBytes());
    IndexRawBytes = portionMeta.GetIndexRawBytes();
    IndexBlobBytes = portionMeta.GetIndexBlobBytes();
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