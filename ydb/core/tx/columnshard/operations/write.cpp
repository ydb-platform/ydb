#include "batch_builder/builder.h"
#include "write.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NColumnShard {

    TWriteOperation::TWriteOperation(const TWriteId writeId, const ui64 lockId, const ui64 cookie, const EOperationStatus& status, const TInstant createdAt,
        const std::optional<ui32> granuleShardingVersionId, const NEvWrite::EModificationType mType)
        : Status(status)
        , CreatedAt(createdAt)
        , WriteId(writeId)
        , LockId(lockId)
        , Cookie(cookie)
        , GranuleShardingVersionId(granuleShardingVersionId)
        , ModificationType(mType)
    {
    }

    void TWriteOperation::Start(TColumnShard& owner, const ui64 tableId, const NEvWrite::IDataContainer::TPtr& data,
        const NActors::TActorId& source, const std::shared_ptr<NOlap::ISnapshotSchema>& schema, const TActorContext& ctx) {
        Y_ABORT_UNLESS(Status == EOperationStatus::Draft);

        NEvWrite::TWriteMeta writeMeta((ui64)WriteId, tableId, source, GranuleShardingVersionId);
        writeMeta.SetModificationType(ModificationType);
        std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildBatchesTask>(owner.TabletID(), ctx.SelfID, owner.BufferizationWriteActorId,
            NEvWrite::TWriteData(writeMeta, data, owner.TablesManager.GetPrimaryIndex()->GetReplaceKey(),
                owner.StoragesManager->GetInsertOperator()->StartWritingAction(NOlap::NBlobOperations::EConsumer::WRITING_OPERATOR)),
            schema, owner.GetLastTxSnapshot());
        NConveyor::TCompServiceOperator::SendTaskToExecute(task);

        Status = EOperationStatus::Started;
    }

    void TWriteOperation::Commit(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) const {
        Y_ABORT_UNLESS(Status == EOperationStatus::Prepared);

        TBlobGroupSelector dsGroupSelector(owner.Info());
        NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

        for (auto gWriteId : GlobalWriteIds) {
            auto pathExists = [&](ui64 pathId) {
                return owner.TablesManager.HasTable(pathId);
            };

            const auto counters = owner.InsertTable->Commit(dbTable, snapshot.GetPlanStep(), snapshot.GetTxId(), { gWriteId },
                                                      pathExists);
            owner.Counters.GetTabletCounters().OnWriteCommitted(counters);
        }
        owner.UpdateInsertTableCounters();
    }

    void TWriteOperation::OnWriteFinish(NTabletFlatExecutor::TTransactionContext& txc, const TVector<TWriteId>& globalWriteIds) {
        Y_ABORT_UNLESS(Status == EOperationStatus::Started);
        Status = EOperationStatus::Prepared;
        GlobalWriteIds = globalWriteIds;

        NIceDb::TNiceDb db(txc.DB);
        NKikimrTxColumnShard::TInternalOperationData proto;
        ToProto(proto);

        TString metadata;
        Y_ABORT_UNLESS(proto.SerializeToString(&metadata));

        db.Table<Schema::Operations>().Key((ui64)WriteId).Update(
            NIceDb::TUpdate<Schema::Operations::Status>((ui32)Status),
            NIceDb::TUpdate<Schema::Operations::CreatedAt>(CreatedAt.Seconds()),
            NIceDb::TUpdate<Schema::Operations::Metadata>(metadata),
            NIceDb::TUpdate<Schema::Operations::LockId>(LockId),
            NIceDb::TUpdate<Schema::Operations::Cookie>(Cookie),
            NIceDb::TUpdate<Schema::Operations::GranuleShardingVersionId>(GranuleShardingVersionId.value_or(0)));
    }

    void TWriteOperation::ToProto(NKikimrTxColumnShard::TInternalOperationData& proto) const {
        for (auto&& writeId : GlobalWriteIds) {
            proto.AddInternalWriteIds((ui64)writeId);
        }
        proto.SetModificationType((ui32)ModificationType);
    }

    void TWriteOperation::FromProto(const NKikimrTxColumnShard::TInternalOperationData& proto) {
        for (auto&& writeId : proto.GetInternalWriteIds()) {
            GlobalWriteIds.push_back(TWriteId(writeId));
        }
        if (proto.HasModificationType()) {
            ModificationType = (NEvWrite::EModificationType)proto.GetModificationType();
        } else {
            ModificationType = NEvWrite::EModificationType::Replace;
        }
    }

    void TWriteOperation::Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const {
        Y_ABORT_UNLESS(Status == EOperationStatus::Prepared);

        TBlobGroupSelector dsGroupSelector(owner.Info());
        NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

        THashSet<TWriteId> writeIds;
        writeIds.insert(GlobalWriteIds.begin(), GlobalWriteIds.end());
        owner.InsertTable->Abort(dbTable, writeIds);
    }

    bool TOperationsManager::Load(NTabletFlatExecutor::TTransactionContext& txc) {
        NIceDb::TNiceDb db(txc.DB);
        {
            auto rowset = db.Table<Schema::Operations>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                const TWriteId writeId = (TWriteId)rowset.GetValue<Schema::Operations::WriteId>();
                const ui64 createdAtSec = rowset.GetValue<Schema::Operations::CreatedAt>();
                const ui64 lockId = rowset.GetValue<Schema::Operations::LockId>();
                const ui64 cookie = rowset.GetValueOrDefault<Schema::Operations::Cookie>(0);
                const TString metadata = rowset.GetValue<Schema::Operations::Metadata>();
                const EOperationStatus status = (EOperationStatus)rowset.GetValue<Schema::Operations::Status>();
                std::optional<ui32> granuleShardingVersionId;
                if (rowset.HaveValue<Schema::Operations::GranuleShardingVersionId>() && rowset.GetValue<Schema::Operations::GranuleShardingVersionId>()) {
                    granuleShardingVersionId = rowset.GetValue<Schema::Operations::GranuleShardingVersionId>();
                }

                NKikimrTxColumnShard::TInternalOperationData metaProto;
                Y_ABORT_UNLESS(metaProto.ParseFromString(metadata));

                auto operation = std::make_shared<TWriteOperation>(writeId, lockId, cookie, status, TInstant::Seconds(createdAtSec), granuleShardingVersionId, NEvWrite::EModificationType::Upsert);
                operation->FromProto(metaProto);
                AFL_VERIFY(operation->GetStatus() != EOperationStatus::Draft);

                auto [_, isOk] = Operations.emplace(operation->GetWriteId(), operation);
                if (!isOk) {
                    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "duplicated_operation")("operation", *operation);
                    return false;
                }
                Locks[lockId].push_back(operation->GetWriteId());
                LastWriteId = std::max(LastWriteId, operation->GetWriteId());
                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowset = db.Table<Schema::OperationTxIds>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                const ui64 lockId = rowset.GetValue<Schema::OperationTxIds::LockId>();
                const ui64 txId = rowset.GetValue<Schema::OperationTxIds::TxId>();
                AFL_VERIFY(Locks.contains(lockId))("lock_id", lockId);
                Tx2Lock[txId] = lockId;
                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        return true;
    }

    bool TOperationsManager::CommitTransaction(TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tx_id", txId));
        auto lockId = GetLockForTx(txId);
        if (!lockId) {
            ACFL_ERROR("details", "unknown_transaction");
            return true;
        }
        auto tIt = Locks.find(*lockId);
        AFL_VERIFY(tIt != Locks.end())("tx_id", txId)("lock_id", *lockId);

        TVector<TWriteOperation::TPtr> commited;
        for (auto&& opId : tIt->second) {
            auto opPtr = Operations.FindPtr(opId);
            (*opPtr)->Commit(owner, txc, snapshot);
            commited.emplace_back(*opPtr);
        }
        OnTransactionFinish(commited, txId, txc);
        return true;
    }

    bool TOperationsManager::AbortTransaction(TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tx_id", txId));

        auto lockId = GetLockForTx(txId);
        if (!lockId) {
            ACFL_ERROR("details", "unknown_transaction");
            return true;
        }
        auto tIt = Locks.find(*lockId);
        AFL_VERIFY(tIt != Locks.end())("tx_id", txId)("lock_id", *lockId);

        TVector<TWriteOperation::TPtr> aborted;
        for (auto&& opId : tIt->second) {
            auto opPtr = Operations.FindPtr(opId);
            (*opPtr)->Abort(owner, txc);
            aborted.emplace_back(*opPtr);
        }

        OnTransactionFinish(aborted, txId, txc);
        return true;
    }

    TWriteOperation::TPtr TOperationsManager::GetOperation(const TWriteId writeId) const {
        auto it = Operations.find(writeId);
        if (it == Operations.end()) {
            return nullptr;
        }
        return it->second;
    }

    void TOperationsManager::OnTransactionFinish(const TVector<TWriteOperation::TPtr>& operations, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
        auto lockId = GetLockForTx(txId);
        AFL_VERIFY(!!lockId)("tx_id", txId);
        Locks.erase(*lockId);
        Tx2Lock.erase(txId);
        for (auto&& op : operations) {
            RemoveOperation(op, txc);
        }
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::OperationTxIds>().Key(txId, *lockId).Delete();
    }

    void TOperationsManager::RemoveOperation(const TWriteOperation::TPtr& op, NTabletFlatExecutor::TTransactionContext& txc) {
        Operations.erase(op->GetWriteId());
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Operations>().Key((ui64)op->GetWriteId()).Delete();
    }

    TWriteId TOperationsManager::BuildNextWriteId() {
        return ++LastWriteId;
    }

    std::optional<ui64> TOperationsManager::GetLockForTx(const ui64 txId) const {
        auto lockIt = Tx2Lock.find(txId);
        if (lockIt != Tx2Lock.end()) {
            return lockIt->second;
        }
        return std::nullopt;
    }

    void TOperationsManager::LinkTransaction(const ui64 lockId, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
        Tx2Lock[txId] = lockId;
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::OperationTxIds>().Key(txId, lockId).Update();
    }

    TWriteOperation::TPtr TOperationsManager::RegisterOperation(const ui64 lockId, const ui64 cookie, const std::optional<ui32> granuleShardingVersionId, const NEvWrite::EModificationType mType) {
        auto writeId = BuildNextWriteId();
        auto operation = std::make_shared<TWriteOperation>(writeId, lockId, cookie, EOperationStatus::Draft, AppData()->TimeProvider->Now(), granuleShardingVersionId, mType);
        Y_ABORT_UNLESS(Operations.emplace(operation->GetWriteId(), operation).second);
        Locks[operation->GetLockId()].push_back(operation->GetWriteId());
        return operation;
    }

    EOperationBehaviour TOperationsManager::GetBehaviour(const NEvents::TDataEvents::TEvWrite& evWrite) {
        if (evWrite.Record.HasTxId() && evWrite.Record.HasLocks() && evWrite.Record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Commit) {
            return EOperationBehaviour::CommitWriteLock;
        }

        if (evWrite.Record.HasLockTxId() && evWrite.Record.HasLockNodeId()) {
            if (evWrite.Record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE) {
                return EOperationBehaviour::WriteWithLock;
            }

            return EOperationBehaviour::Undefined;
        }

        if (evWrite.Record.HasTxId() && evWrite.Record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_PREPARE) {
            return EOperationBehaviour::InTxWrite;
        }
        return EOperationBehaviour::Undefined;
    }
}   // namespace NKikimr::NColumnShard
