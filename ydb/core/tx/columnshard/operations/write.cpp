#include "write.h"

#include "batch_builder/builder.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NColumnShard {

TWriteOperation::TWriteOperation(const TUnifiedPathId& pathId, const TOperationWriteId writeId, const ui64 lockId, const ui64 cookie,
    const EOperationStatus& status, const TInstant createdAt, const std::optional<ui32> granuleShardingVersionId,
    const NEvWrite::EModificationType mType, const bool isBulk)
    : PathId(pathId)
    , Status(status)
    , CreatedAt(createdAt)
    , WriteId(writeId)
    , LockId(lockId)
    , Cookie(cookie)
    , GranuleShardingVersionId(granuleShardingVersionId)
    , ModificationType(mType)
    , BulkFlag(isBulk)
{
}

void TWriteOperation::Start(
    TColumnShard& owner, const NEvWrite::IDataContainer::TPtr& data, const NActors::TActorId& source, const NOlap::TWritingContext& context) {
    Y_ABORT_UNLESS(Status == EOperationStatus::Draft);

    auto writeMeta = std::make_shared<NEvWrite::TWriteMeta>(
        (ui64)WriteId, PathId, source, GranuleShardingVersionId, GetIdentifier(),
        context.GetWritingCounters()->GetWriteFlowCounters());
    writeMeta->SetLockId(LockId);
    writeMeta->SetModificationType(ModificationType);
    writeMeta->SetBulk(IsBulk());
    auto writingAction = owner.StoragesManager->GetInsertOperator()->StartWritingAction(NOlap::NBlobOperations::EConsumer::WRITING_OPERATOR);
    writingAction->SetBulk(IsBulk());
    NEvWrite::TWriteData writeData(writeMeta, data, owner.TablesManager.GetPrimaryIndex()->GetReplaceKey(), std::move(writingAction));
    std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildBatchesTask>(std::move(writeData), context);
    NConveyorComposite::TInsertServiceOperator::SendTaskToExecute(task);

    Status = EOperationStatus::Started;
}

void TWriteOperation::CommitOnExecute(
    TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) const {
    Y_ABORT_UNLESS(Status == EOperationStatus::Prepared || InsertWriteIds.empty());

    TBlobGroupSelector dsGroupSelector(owner.Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

    for (auto&& i : InsertWriteIds) {
        owner.MutableIndexAs<NOlap::TColumnEngineForLogs>().MutableGranuleVerified(PathId.InternalPathId).CommitPortionOnExecute(txc, i, snapshot);
    }
}

void TWriteOperation::CommitOnComplete(TColumnShard& owner, const NOlap::TSnapshot& /*snapshot*/) const {
    Y_ABORT_UNLESS(Status == EOperationStatus::Prepared || InsertWriteIds.empty());
    for (auto&& i : InsertWriteIds) {
        owner.MutableIndexAs<NOlap::TColumnEngineForLogs>().MutableGranuleVerified(PathId.InternalPathId).CommitPortionOnComplete(
            i, owner.MutableIndexAs<NOlap::TColumnEngineForLogs>());
    }
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

    db.Table<Schema::Operations>()
        .Key((ui64)WriteId)
        .Update(NIceDb::TUpdate<Schema::Operations::Status>((ui32)Status), NIceDb::TUpdate<Schema::Operations::CreatedAt>(CreatedAt.Seconds()),
            NIceDb::TUpdate<Schema::Operations::Metadata>(metadata), NIceDb::TUpdate<Schema::Operations::LockId>(LockId),
            NIceDb::TUpdate<Schema::Operations::Cookie>(Cookie),
            NIceDb::TUpdate<Schema::Operations::GranuleShardingVersionId>(GranuleShardingVersionId.value_or(0)));
}

void TWriteOperation::ToProto(NKikimrTxColumnShard::TInternalOperationData& proto) const {
    for (auto&& writeId : InsertWriteIds) {
        proto.AddInternalWriteIds((ui64)writeId);
    }
    proto.SetModificationType((ui32)ModificationType);
    proto.SetWritePortions(true);
    proto.SetIsBulk(IsBulk());
    PathId.InternalPathId.ToProto(proto);
}

void TWriteOperation::FromProto(const NKikimrTxColumnShard::TInternalOperationData& proto) {
    for (auto&& writeId : proto.GetInternalWriteIds()) {
        InsertWriteIds.push_back(TInsertWriteId(writeId));
    }
    PathId.InternalPathId = TInternalPathId::FromProto(proto);
    AFL_VERIFY(PathId.InternalPathId);
    if (proto.HasModificationType()) {
        ModificationType = (NEvWrite::EModificationType)proto.GetModificationType();
    } else {
        ModificationType = NEvWrite::EModificationType::Replace;
    }
    BulkFlag = proto.GetIsBulk();
}

void TWriteOperation::AbortOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const {
    Y_ABORT_UNLESS(Status != EOperationStatus::Draft);
    StopWriting();
    TBlobGroupSelector dsGroupSelector(owner.Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

    for (auto&& i : InsertWriteIds) {
        owner.MutableIndexAs<NOlap::TColumnEngineForLogs>().MutableGranuleVerified(PathId.InternalPathId).AbortPortionOnExecute(
            txc, i, owner.GetCurrentSnapshotForInternalModification());
    }
}

void TWriteOperation::AbortOnComplete(TColumnShard& owner) const {
    Y_ABORT_UNLESS(Status != EOperationStatus::Draft);
    for (auto&& i : InsertWriteIds) {
        owner.MutableIndexAs<NOlap::TColumnEngineForLogs>().MutableGranuleVerified(PathId.InternalPathId).AbortPortionOnComplete(
            i, owner.MutableIndexAs<NOlap::TColumnEngineForLogs>());
    }
}

}   // namespace NKikimr::NColumnShard
