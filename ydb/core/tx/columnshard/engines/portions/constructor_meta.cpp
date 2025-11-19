#include "constructor_meta.h"

#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

#include <ydb/library/formats/arrow/size_calcer.h>

namespace NKikimr::NOlap {

void TPortionMetaConstructor::FillMetaInfo(const NArrow::TFirstLastSpecialKeys& primaryKeys, const ui32 deletionsCount,
    const std::optional<NArrow::TMinMaxSpecialKeys>& snapshotKeys, const TIndexInfo& indexInfo) {
    AFL_VERIFY(!FirstAndLastPK);
    AFL_VERIFY(primaryKeys.GetSchema()->num_fields() == indexInfo.GetReplaceKey()->num_fields());
    for (i32 i = 0; i < primaryKeys.GetSchema()->num_fields(); ++i) {
        AFL_VERIFY(primaryKeys.GetSchema()->field(i)->type()->id() == indexInfo.GetReplaceKey()->field(i)->type()->id());
    }
    FirstAndLastPK = primaryKeys;
    AFL_VERIFY(!RecordSnapshotMin);
    AFL_VERIFY(!RecordSnapshotMax);
    DeletionsCount = deletionsCount;
    if (snapshotKeys) {
        RecordSnapshotMin = TSnapshot(snapshotKeys->GetFirst().GetValueVerified<ui64>(TIndexInfo::SPEC_COL_PLAN_STEP),
            snapshotKeys->GetFirst().GetValueVerified<ui64>(TIndexInfo::SPEC_COL_TX_ID));
        RecordSnapshotMax = TSnapshot(snapshotKeys->GetLast().GetValueVerified<ui64>(TIndexInfo::SPEC_COL_PLAN_STEP),
            snapshotKeys->GetLast().GetValueVerified<ui64>(TIndexInfo::SPEC_COL_TX_ID));
    } else {
        RecordSnapshotMin = TSnapshot::Zero();
        RecordSnapshotMax = TSnapshot::Zero();
    }
}

TPortionMetaConstructor::TPortionMetaConstructor(const TPortionMeta& meta) {
    FirstAndLastPK = NArrow::TFirstLastSpecialKeys(meta.IndexKeyStart(), meta.IndexKeyEnd(), meta.IndexKeyStart().GetSchema());
    RecordSnapshotMin = meta.RecordSnapshotMin;
    RecordSnapshotMax = meta.RecordSnapshotMax;
    CompactionLevel = meta.GetCompactionLevel();
    DeletionsCount = meta.GetDeletionsCount();
    TierName = meta.GetTierNameOptional();
}

TPortionMeta TPortionMetaConstructor::Build() {
    AFL_VERIFY(FirstAndLastPK);
    TMemoryProfileGuard mGuard1("meta_construct/pk");
    static TAtomicCounter sumValues = 0;
    static TAtomicCounter sumValuesMeta = 0;
    static TAtomicCounter countValues = 0;
//    FirstAndLastPK->Reallocate();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("memory_size", FirstAndLastPK->GetMemorySize())("data_size", FirstAndLastPK->GetDataSize())(
        "sum", sumValues.Add(FirstAndLastPK->GetMemorySize()))("count", countValues.Inc());
    TMemoryProfileGuard mGuard("meta_construct/others");
    AFL_VERIFY(RecordSnapshotMin);
    AFL_VERIFY(RecordSnapshotMax);
    TPortionMeta result(*FirstAndLastPK, *RecordSnapshotMin, *RecordSnapshotMax);
    if (TierName) {
        result.TierName = *TierName;
    }
    result.CompactionLevel = *TValidator::CheckNotNull(CompactionLevel);
    result.DeletionsCount = *TValidator::CheckNotNull(DeletionsCount);

    result.RecordsCount = *TValidator::CheckNotNull(RecordsCount);
    result.ColumnRawBytes = *TValidator::CheckNotNull(ColumnRawBytes);
    result.ColumnBlobBytes = *TValidator::CheckNotNull(ColumnBlobBytes);
    result.IndexRawBytes = *TValidator::CheckNotNull(IndexRawBytes);
    result.IndexBlobBytes = *TValidator::CheckNotNull(IndexBlobBytes);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("memory_size", result.GetMemorySize())("data_size", result.GetDataSize())(
        "sum", sumValuesMeta.Add(result.GetMemorySize()))("count", countValues.Inc())("size_of_meta", sizeof(TPortionMeta));

    return result;
}

bool TPortionMetaConstructor::LoadMetadata(
    const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo, const IBlobGroupSelector& /*groupSelector*/) {
    if (portionMeta.GetTierName()) {
        TierName = portionMeta.GetTierName();
    }
    if (portionMeta.HasDeletionsCount()) {
        DeletionsCount = portionMeta.GetDeletionsCount();
    } else {
        DeletionsCount = 0;
    }
    CompactionLevel = portionMeta.GetCompactionLevel();
    RecordsCount = TValidator::CheckNotNull(portionMeta.GetRecordsCount());
    ColumnRawBytes = TValidator::CheckNotNull(portionMeta.GetColumnRawBytes());
    ColumnBlobBytes = TValidator::CheckNotNull(portionMeta.GetColumnBlobBytes());
    IndexRawBytes = portionMeta.GetIndexRawBytes();
    IndexBlobBytes = portionMeta.GetIndexBlobBytes();
    if (portionMeta.HasPrimaryKeyBordersV1()) {
        FirstAndLastPK = NArrow::TFirstLastSpecialKeys(
            portionMeta.GetPrimaryKeyBordersV1().GetFirst(), portionMeta.GetPrimaryKeyBordersV1().GetLast(), indexInfo.GetReplaceKey());
    } else {
        AFL_VERIFY(portionMeta.HasPrimaryKeyBorders());
        FirstAndLastPK = NArrow::TFirstLastSpecialKeys(portionMeta.GetPrimaryKeyBorders(), indexInfo.GetReplaceKey());
    }

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

}   // namespace NKikimr::NOlap
