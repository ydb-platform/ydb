#include "constructor_meta.h"

#include <ydb/core/formats/arrow/key_comparator.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

#include <ydb/library/formats/arrow/size_calcer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap {

void TPortionMetaConstructor::FillMetaInfo(const NArrow::TFirstLastSpecialKeys& primaryKeys, const ui32 deletionsCount,
    const std::optional<NArrow::TMinMaxSpecialKeys>& snapshotKeys, const TIndexInfo& indexInfo) {
    AFL_VERIFY(!FirstAndLastPK);
    AFL_VERIFY(primaryKeys.GetSchema()->num_fields() == indexInfo.GetReplaceKey()->num_fields());
    for (i32 i = 0; i < primaryKeys.GetSchema()->num_fields(); ++i) {
        AFL_VERIFY(primaryKeys.GetSchema()->field(i)->type()->id() == indexInfo.GetReplaceKey()->field(i)->type()->id());
    }
    ResetPrimaryKeys(primaryKeys);
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
    StoredPrimaryKeys = meta.StoredPrimaryKeys;
    FirstAndLastPK = NArrow::TFirstLastSpecialKeys(meta.IndexKeyStart(), meta.IndexKeyEnd(), meta.IndexKeyStart().GetSchema());
    RecordSnapshotMin = meta.RecordSnapshotMin;
    RecordSnapshotMax = meta.RecordSnapshotMax;
    CompactionLevel = meta.GetCompactionLevel();
    DeletionsCount = meta.GetDeletionsCount();
    TierName = meta.GetTierNameOptional();
    BsIndexBlobBytes = meta.GetBsIndexBlobBytes();
    RebuildDecimalPrimaryKey = meta.GetRebuildDecimalPrimaryKey();
    DecimalKeyOrder = meta.GetDecimalKeyOrder();
}

TPortionMeta TPortionMetaConstructor::Build() {
    AFL_VERIFY(FirstAndLastPK);
    TMemoryProfileGuard mGuard1("meta_construct/pk", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    static TAtomicCounter sumValues = 0;
    static TAtomicCounter sumValuesMeta = 0;
    static TAtomicCounter countValues = 0;
    //    FirstAndLastPK->Reallocate();
    YDB_LOG_DEBUG("",
        {"memorySize", FirstAndLastPK->GetMemorySize()},
        {"dataSize", FirstAndLastPK->GetDataSize()},
        {"sum", sumValues.Add(FirstAndLastPK->GetMemorySize())},
        {"count", countValues.Inc()});
    TMemoryProfileGuard mGuard("meta_construct/others", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    AFL_VERIFY(RecordSnapshotMin);
    AFL_VERIFY(RecordSnapshotMax);
    TPortionMeta result(*FirstAndLastPK, *RecordSnapshotMin, *RecordSnapshotMax);
    result.StoredPrimaryKeys = StoredPrimaryKeys;
    result.RebuildDecimalPrimaryKey = RebuildDecimalPrimaryKey;
    result.DecimalKeyOrder = DecimalKeyOrder;
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
    result.BsIndexBlobBytes = BsIndexBlobBytes;
    result.NumSlices = *TValidator::CheckNotNull(NumSlices);
    YDB_LOG_DEBUG("",
        {"memorySize", result.GetMemorySize()},
        {"dataSize", result.GetDataSize()},
        {"sum", sumValuesMeta.Add(result.GetMemorySize())},
        {"count", countValues.Inc()},
        {"sizeOfMeta", sizeof(TPortionMeta)});

    return result;
}

bool TPortionMetaConstructor::LoadMetadata(
    const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo, const IBlobGroupSelector& /*groupSelector*/) {
    RebuildDecimalPrimaryKey = portionMeta.GetRebuildDecimalPrimaryKey();
    DecimalKeyOrder = portionMeta.GetDecimalKeyOrder();
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
    if (portionMeta.HasBsIndexBlobBytes()) {
        BsIndexBlobBytes = portionMeta.GetBsIndexBlobBytes();
    }
    NumSlices = portionMeta.HasNumSlices() ? portionMeta.GetNumSlices() : 1;
    if (portionMeta.HasPrimaryKeyBordersV1()) {
        FirstAndLastPK = NArrow::TFirstLastSpecialKeys(
            portionMeta.GetPrimaryKeyBordersV1().GetFirst(), portionMeta.GetPrimaryKeyBordersV1().GetLast(), indexInfo.GetReplaceKey());
    } else {
        AFL_VERIFY(portionMeta.HasPrimaryKeyBorders());
        FirstAndLastPK = NArrow::TFirstLastSpecialKeys(portionMeta.GetPrimaryKeyBorders(), indexInfo.GetReplaceKey());
    }

    // Old first/last rows are not bounds under the other ordering. Keep the
    // correctly ordered prefix and widen the first Decimal component while the
    // manual actualization is pending. Reads/compaction reject this old order.
    const auto& pkColumns = indexInfo.GetPrimaryKeyColumns();
    for (ui32 i = 0; i < pkColumns.size(); ++i) {
        if (pkColumns[i].second.GetTypeId() != NScheme::NTypeIds::Decimal) {
            continue;
        }
        const bool numeric = NArrow::TKeyColumnComparator::IsDecimal(*indexInfo.GetReplaceKey()->field(i));
        if (numeric != DecimalKeyOrder) {
            StoredPrimaryKeys = std::make_shared<NArrow::TFirstLastSpecialKeys>(*FirstAndLastPK);
            const auto makeBorder = [&](const NArrow::TSimpleRow& row, const bool upper) {
                auto batch = row.ToBatch();
                auto arrays = batch->columns();
                TString bytes(16, upper ? '\xff' : '\0');
                if (numeric) {
                    // Values strictly outside the valid YQL Decimal range.
                    const auto value = upper ? NYql::NDecimal::Nan() + 1 : -NYql::NDecimal::Inf() - 1;
                    memcpy(bytes.Detach(), &value, sizeof(value));
                }
                arrow::FixedSizeBinaryBuilder builder(batch->schema()->field(i)->type());
                NArrow::TStatusValidator::Validate(builder.Append(bytes.data()));
                NArrow::TStatusValidator::Validate(builder.Finish(&arrays[i]));
                return NArrow::TSimpleRow(arrow::RecordBatch::Make(batch->schema(), 1, arrays), 0);
            };
            FirstAndLastPK = NArrow::TFirstLastSpecialKeys(
                makeBorder(FirstAndLastPK->GetFirst(), false), makeBorder(FirstAndLastPK->GetLast(), true), indexInfo.GetReplaceKey());
        }
        break;
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
