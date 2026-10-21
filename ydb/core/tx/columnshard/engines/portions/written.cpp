#include "constructor_portion.h"
#include "data_accessor.h"
#include "written.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>

namespace NKikimr::NOlap {

void TWrittenPortionInfo::DoSaveMetaToDatabase(const std::vector<TUnifiedBlobId>& blobIds, NIceDb::TNiceDb& db) const {
    auto metaProto = GetMeta().SerializeToProto(blobIds, NPortion::EProduced::INSERTED);
    using IndexPortions = NColumnShard::Schema::IndexPortions;
    const auto removeSnapshot = GetRemoveSnapshotOptional();
    const auto commitSnapshot = GetCommitSnapshotOptional();
    AFL_VERIFY(InsertWriteId);
    db.Table<IndexPortions>()
        .Key(GetPathId().GetRawValue(), GetPortionId())
        .Update(NIceDb::TUpdate<IndexPortions::SchemaVersion>(GetSchemaVersionVerified()),
            NIceDb::TUpdate<IndexPortions::ShardingVersion>(GetShardingVersionDef(0)),
            NIceDb::TUpdate<IndexPortions::CommitPlanStep>(commitSnapshot ? commitSnapshot->GetPlanStep() : 0),
            NIceDb::TUpdate<IndexPortions::CommitTxId>(commitSnapshot ? commitSnapshot->GetTxId() : 0),
            NIceDb::TUpdate<IndexPortions::InsertWriteId>((ui64)*InsertWriteId),
            NIceDb::TUpdate<IndexPortions::XPlanStep>(removeSnapshot ? removeSnapshot->GetPlanStep() : 0),
            NIceDb::TUpdate<IndexPortions::XTxId>(removeSnapshot ? removeSnapshot->GetTxId() : 0),
            NIceDb::TUpdate<IndexPortions::MinSnapshotPlanStep>(1), NIceDb::TUpdate<IndexPortions::MinSnapshotTxId>(1),
            NIceDb::TUpdate<IndexPortions::Metadata>(metaProto.SerializeAsString()));
}

std::unique_ptr<TPortionInfoConstructor> TWrittenPortionInfo::BuildConstructor(const bool withMetadata) const {
    return std::make_unique<TWrittenPortionInfoConstructor>(*this, withMetadata);
}

void TWrittenPortionInfo::FillDefaultColumn(NAssembling::TColumnAssemblingInfo& column, const std::optional<TSnapshot>& defaultSnapshot) const {
    TSnapshot defaultSnapshotLocal = TSnapshot::Max();
    if (CommitSnapshot.Has()) {
        defaultSnapshotLocal = CommitSnapshot.Get();
    } else if (defaultSnapshot) {
        defaultSnapshotLocal = *defaultSnapshot;
    }

    if (column.GetColumnId() == (ui32)IIndexInfo::ESpecialColumn::PLAN_STEP) {
        column.AddBlobInfo(0, GetRecordsCount(), TPortionDataAccessor::TAssembleBlobInfo(GetRecordsCount(),
                                                     std::make_shared<arrow::UInt64Scalar>(defaultSnapshotLocal.GetPlanStep())));
    }
    if (column.GetColumnId() == (ui32)IIndexInfo::ESpecialColumn::TX_ID) {
        column.AddBlobInfo(0, GetRecordsCount(),
            TPortionDataAccessor::TAssembleBlobInfo(GetRecordsCount(), std::make_shared<arrow::UInt64Scalar>(defaultSnapshotLocal.GetTxId())));
    }
    if (column.GetColumnId() == (ui32)IIndexInfo::ESpecialColumn::WRITE_ID) {
        column.AddBlobInfo(0, GetRecordsCount(),
            TPortionDataAccessor::TAssembleBlobInfo(GetRecordsCount(), std::make_shared<arrow::UInt64Scalar>((ui64)GetInsertWriteId())));
    }
    if (column.GetColumnId() == (ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG) {
        AFL_VERIFY(GetRecordsCount() == GetMeta().GetDeletionsCount() || GetMeta().GetDeletionsCount() == 0)("deletes", GetMeta().GetDeletionsCount())(
                                                                         "count", GetRecordsCount());
        column.AddBlobInfo(0, GetRecordsCount(), TPortionDataAccessor::TAssembleBlobInfo(GetRecordsCount(),
                                                     std::make_shared<arrow::BooleanScalar>((bool)GetMeta().GetDeletionsCount())));
    }
}

bool TWrittenPortionInfo::DoIsVisible(const TSnapshot& snapshot, const bool checkCommitSnapshot) const {
    if (!checkCommitSnapshot) {
        return true;
    }
    if (CommitSnapshot.Has()) {
        return CommitSnapshot.Get() <= snapshot;
    } else {
        return false;
    }
}

bool TWrittenPortionInfo::IsAborted() const {
    return HasRemoveSnapshot() && HasCommitSnapshot() && GetRemoveSnapshotVerified() <= GetCommitSnapshotVerified();
}

bool TWrittenPortionInfo::MayGetForScanAt(const TSnapshot& snapshot) const {
    // aborted portion, in fact, never existed, so there is no point to take it for scan
    return !IsAborted() && !IsRemovedFor(snapshot);
}

void TWrittenPortionInfo::CommitToDatabase(IDbWrapper& wrapper) {
    AFL_VERIFY(HasCommitSnapshot());
    wrapper.CommitPortion(*this, CommitSnapshot.Get());
}

}   // namespace NKikimr::NOlap
