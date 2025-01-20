#include "write.h"

#include "batch_builder/builder.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NColumnShard {

TWriteOperation::TWriteOperation(const TOperationWriteId writeId, const ui64 lockId, const ui64 cookie, const EOperationStatus& status,
    const TInstant createdAt, const std::optional<ui32> granuleShardingVersionId, const NEvWrite::EModificationType mType)
    : Status(status)
    , CreatedAt(createdAt)
    , WriteId(writeId)
    , LockId(lockId)
    , Cookie(cookie)
    , GranuleShardingVersionId(granuleShardingVersionId)
    , ModificationType(mType)
{
}

void TWriteOperation::Start(TColumnShard& owner, const ui64 tableId, const NEvWrite::IDataContainer::TPtr& data, const NActors::TActorId& source,
    const std::shared_ptr<NOlap::ISnapshotSchema>& schema, const TActorContext& ctx, const NOlap::TSnapshot& applyToSnapshot) {
    Y_ABORT_UNLESS(Status == EOperationStatus::Draft);

    NEvWrite::TWriteMeta writeMeta((ui64)WriteId, tableId, source, GranuleShardingVersionId);
    writeMeta.SetLockId(LockId);
    writeMeta.SetModificationType(ModificationType);
    std::shared_ptr<NConveyor::ITask> task =
        std::make_shared<NOlap::TBuildBatchesTask>(owner.TabletID(), ctx.SelfID, owner.BufferizationWriteActorId,
            NEvWrite::TWriteData(writeMeta, data, owner.TablesManager.GetPrimaryIndex()->GetReplaceKey(),
                owner.StoragesManager->GetInsertOperator()->StartWritingAction(NOlap::NBlobOperations::EConsumer::WRITING_OPERATOR)),
                schema, applyToSnapshot, owner.Counters.GetCSCounters().WritingCounters);
    NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);

    Status = EOperationStatus::Started;
}

void TWriteOperation::CommitOnExecute(
    TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) const {
    Y_ABORT_UNLESS(Status == EOperationStatus::Prepared);

    TBlobGroupSelector dsGroupSelector(owner.Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

    for (auto gWriteId : InsertWriteIds) {
        auto pathExists = [&](ui64 pathId) {
            return owner.TablesManager.HasTable(pathId);
        };

        const auto counters = owner.InsertTable->Commit(dbTable, snapshot.GetPlanStep(), snapshot.GetTxId(), { gWriteId }, pathExists);
        owner.Counters.GetTabletCounters()->OnWriteCommitted(counters);
    }
}

void TWriteOperation::CommitOnComplete(TColumnShard& owner, const NOlap::TSnapshot& /*snapshot*/) const {
    Y_ABORT_UNLESS(Status == EOperationStatus::Prepared);
    owner.UpdateInsertTableCounters();
}

void TWriteOperation::OnWriteFinish(
    NTabletFlatExecutor::TTransactionContext& txc, const std::vector<TInsertWriteId>& insertWriteIds, const bool ephemeralFlag) {
    Y_ABORT_UNLESS(Status == EOperationStatus::Started);
    Status = EOperationStatus::Prepared;
    InsertWriteIds = insertWriteIds;

    if (ephemeralFlag) {
        return;
    }

    NIceDb::TNiceDb db(txc.DB);
    NKikimrTxColumnShard::TInternalOperationData proto;
    ToProto(proto);

    TString metadata;
    Y_ABORT_UNLESS(proto.SerializeToString(&metadata));

    db.Table<Schema::Operations>().Key((ui64)WriteId).Update(NIceDb::TUpdate<Schema::Operations::Status>((ui32)Status),
        NIceDb::TUpdate<Schema::Operations::CreatedAt>(CreatedAt.Seconds()), NIceDb::TUpdate<Schema::Operations::Metadata>(metadata),
        NIceDb::TUpdate<Schema::Operations::LockId>(LockId), NIceDb::TUpdate<Schema::Operations::Cookie>(Cookie),
        NIceDb::TUpdate<Schema::Operations::GranuleShardingVersionId>(GranuleShardingVersionId.value_or(0)));
}

void TWriteOperation::ToProto(NKikimrTxColumnShard::TInternalOperationData& proto) const {
    for (auto&& writeId : InsertWriteIds) {
        proto.AddInternalWriteIds((ui64)writeId);
    }
    proto.SetModificationType((ui32)ModificationType);
}

void TWriteOperation::FromProto(const NKikimrTxColumnShard::TInternalOperationData& proto) {
    for (auto&& writeId : proto.GetInternalWriteIds()) {
        InsertWriteIds.push_back(TInsertWriteId(writeId));
    }
    if (proto.HasModificationType()) {
        ModificationType = (NEvWrite::EModificationType)proto.GetModificationType();
    } else {
        ModificationType = NEvWrite::EModificationType::Replace;
    }
}

void TWriteOperation::AbortOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const {
    Y_ABORT_UNLESS(Status == EOperationStatus::Prepared);

    TBlobGroupSelector dsGroupSelector(owner.Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

    THashSet<TInsertWriteId> writeIds;
    writeIds.insert(InsertWriteIds.begin(), InsertWriteIds.end());
    owner.InsertTable->Abort(dbTable, writeIds);
}

void TWriteOperation::AbortOnComplete(TColumnShard& /*owner*/) const {
    Y_ABORT_UNLESS(Status == EOperationStatus::Prepared);
}

}  // namespace NKikimr::NColumnShard
