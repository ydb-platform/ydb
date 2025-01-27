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

namespace NKikimr::NColumnShard {

TWriteOperation::TWriteOperation(const ui64 pathId, const TOperationWriteId writeId, const ui64 lockId, const ui64 cookie,
    const EOperationStatus& status, const TInstant createdAt, const std::optional<ui32> granuleShardingVersionId,
    const NEvWrite::EModificationType mType, const bool writePortions)
    : PathId(pathId)
    , Status(status)
    , CreatedAt(createdAt)
    , WriteId(writeId)
    , LockId(lockId)
    , Cookie(cookie)
    , GranuleShardingVersionId(granuleShardingVersionId)
    , ModificationType(mType)
    , WritePortions(writePortions) {
}

void TWriteOperation::Start(
    TColumnShard& owner, const NEvWrite::IDataContainer::TPtr& data, const NActors::TActorId& source, const NOlap::TWritingContext& context) {
    Y_ABORT_UNLESS(Status == EOperationStatus::Draft);

    auto writeMeta = std::make_shared<NEvWrite::TWriteMeta>((ui64)WriteId, GetPathId(), source, GranuleShardingVersionId, GetIdentifier(),
        context.GetWritingCounters()->GetWriteFlowCounters());
    writeMeta->SetLockId(LockId);
    writeMeta->SetModificationType(ModificationType);
    NEvWrite::TWriteData writeData(writeMeta, data, owner.TablesManager.GetPrimaryIndex()->GetReplaceKey(),
        owner.StoragesManager->GetInsertOperator()->StartWritingAction(NOlap::NBlobOperations::EConsumer::WRITING_OPERATOR), WritePortions);
    std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildBatchesTask>(std::move(writeData), context);
    NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);

    Status = EOperationStatus::Started;
}

void TWriteOperation::CommitOnExecute(
    TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) const {
    Y_ABORT_UNLESS(Status == EOperationStatus::Prepared || InsertWriteIds.empty());

    TBlobGroupSelector dsGroupSelector(owner.Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

    if (!WritePortions) {
        THashSet<TInsertWriteId> insertWriteIds(InsertWriteIds.begin(), InsertWriteIds.end());
        auto pathExists = [&](ui64 pathId) {
            return owner.TablesManager.HasTable(pathId);
        };
        if (insertWriteIds.size()) {
            const auto counters = owner.InsertTable->Commit(dbTable, snapshot.GetPlanStep(), snapshot.GetTxId(), insertWriteIds, pathExists);
            owner.Counters.GetTabletCounters()->OnWriteCommitted(counters);
        }
    } else {
        for (auto&& i : InsertWriteIds) {
            owner.MutableIndexAs<NOlap::TColumnEngineForLogs>().MutableGranuleVerified(PathId).CommitPortionOnExecute(txc, i, snapshot);
        }
    }
}

void TWriteOperation::CommitOnComplete(TColumnShard& owner, const NOlap::TSnapshot& /*snapshot*/) const {
    Y_ABORT_UNLESS(Status == EOperationStatus::Prepared || InsertWriteIds.empty());
    if (!WritePortions) {
        owner.UpdateInsertTableCounters();
    } else {
        for (auto&& i : InsertWriteIds) {
            owner.MutableIndexAs<NOlap::TColumnEngineForLogs>().MutableGranuleVerified(PathId).CommitPortionOnComplete(
                i, owner.MutableIndexAs<NOlap::TColumnEngineForLogs>());
        }
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
    proto.SetWritePortions(WritePortions);
    proto.SetPathId(PathId);
}

void TWriteOperation::FromProto(const NKikimrTxColumnShard::TInternalOperationData& proto) {
    for (auto&& writeId : proto.GetInternalWriteIds()) {
        InsertWriteIds.push_back(TInsertWriteId(writeId));
    }
    WritePortions = proto.GetWritePortions();
    PathId = proto.GetPathId();
    AFL_VERIFY(!WritePortions || PathId);
    if (proto.HasModificationType()) {
        ModificationType = (NEvWrite::EModificationType)proto.GetModificationType();
    } else {
        ModificationType = NEvWrite::EModificationType::Replace;
    }
}

void TWriteOperation::AbortOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const {
    Y_ABORT_UNLESS(Status != EOperationStatus::Draft);
    StopWriting();
    TBlobGroupSelector dsGroupSelector(owner.Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

    if (!WritePortions) {
        THashSet<TInsertWriteId> writeIds;
        writeIds.insert(InsertWriteIds.begin(), InsertWriteIds.end());
        owner.InsertTable->Abort(dbTable, writeIds);
    } else {
        for (auto&& i : InsertWriteIds) {
            owner.MutableIndexAs<NOlap::TColumnEngineForLogs>().MutableGranuleVerified(PathId).AbortPortionOnExecute(txc, i, owner.GetCurrentSnapshotForInternalModification());
        }
    }
}

void TWriteOperation::AbortOnComplete(TColumnShard& owner) const {
    Y_ABORT_UNLESS(Status != EOperationStatus::Draft);
    if (WritePortions) {
        for (auto&& i : InsertWriteIds) {
            owner.MutableIndexAs<NOlap::TColumnEngineForLogs>().MutableGranuleVerified(PathId).AbortPortionOnComplete(
                i, owner.MutableIndexAs<NOlap::TColumnEngineForLogs>());
        }
    }
}

}   // namespace NKikimr::NColumnShard
