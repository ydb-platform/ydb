#include "write.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>

#include <ydb/core/tablet_flat/tablet_flat_executor.h>


namespace NKikimr::NColumnShard {

    TWriteOperation::TWriteOperation(const TWriteId writeId, const ui64 txId, const EOperationStatus& status, const TInstant createdAt, const ui64 globalWriteId)
        : Status(status)
        , CreatedAt(createdAt)
        , WriteId(writeId)
        , TxId(txId)
        , GlobalWriteId(globalWriteId)
    {
    }

    void TWriteOperation::Start(TColumnShard& owner, const ui64 tableId, const NEvWrite::IDataContainer::TPtr& data, const NActors::TActorId& source, const TActorContext& ctx) {
        Y_VERIFY(Status == EOperationStatus::Draft);

        NEvWrite::TWriteMeta writeMeta((ui64)WriteId, tableId, source);
        const auto& snapshotSchema = owner.TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema();
        auto writeController = std::make_shared<NOlap::TIndexedWriteController>(ctx.SelfID, NEvWrite::TWriteData(writeMeta, data), snapshotSchema);
        ctx.Register(CreateWriteActor(owner.TabletID(), writeController, owner.BlobManager->StartBlobBatch(), TInstant::Max(), owner.Settings.MaxSmallBlobSize));
        Status = EOperationStatus::Started;
    }

    void TWriteOperation::Commit(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) const {
        Y_VERIFY(Status == EOperationStatus::Prepared);

        TBlobGroupSelector dsGroupSelector(owner.Info());
        NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

        owner.BatchCache.Commit(WriteId);

        auto pathExists = [&](ui64 pathId) {
            return owner.TablesManager.HasTable(pathId);
        };

        auto counters = owner.InsertTable->Commit(dbTable, snapshot.GetPlanStep(), snapshot.GetTxId(), 0, { WriteId },
                                                    pathExists);

        owner.IncCounter(COUNTER_BLOBS_COMMITTED, counters.Rows);
        owner.IncCounter(COUNTER_BYTES_COMMITTED, counters.Bytes);
        owner.IncCounter(COUNTER_RAW_BYTES_COMMITTED, counters.RawBytes);
        owner.UpdateInsertTableCounters();
    }

    void TWriteOperation::OnWriteFinish(NTabletFlatExecutor::TTransactionContext& txc, const ui64 globalWriteId) {
        Y_VERIFY(Status == EOperationStatus::Started);
        Status = EOperationStatus::Prepared;
        GlobalWriteId = globalWriteId;
        NIceDb::TNiceDb db(txc.DB);
        Schema::Operations_Write(db, *this);
    }

    void TWriteOperation::Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const {
        Y_VERIFY(Status == EOperationStatus::Prepared);
        owner.BatchCache.EraseInserted(WriteId);

        TBlobGroupSelector dsGroupSelector(owner.Info());
        NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

        owner.InsertTable->Abort(dbTable, 0, {WriteId});

        TBlobManagerDb blobManagerDb(txc.DB);
        auto allAborted = owner.InsertTable->GetAborted();
        for (auto& [abortedWriteId, abortedData] : allAborted) {
            owner.InsertTable->EraseAborted(dbTable, abortedData);
            owner.BlobManager->DeleteBlob(abortedData.BlobId, blobManagerDb);
        }
    }

    bool TOperationsManager::Init(NTabletFlatExecutor::TTransactionContext& txc) {
        NIceDb::TNiceDb db(txc.DB);
        auto rowset = db.Table<Schema::Operations>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const TWriteId writeId = (TWriteId) rowset.GetValue<Schema::Operations::WriteId>();
            const ui64 createdAtSec = rowset.GetValue<Schema::Operations::CreatedAt>();
            const ui64 txId = rowset.GetValue<Schema::Operations::TxId>();
            const ui64 globalWriteId = rowset.GetValue<Schema::Operations::GlobalWriteId>();
            const EOperationStatus status = (EOperationStatus) rowset.GetValue<Schema::Operations::Status>();

            auto operation = std::make_shared<TWriteOperation>(writeId, txId, status, TInstant::Seconds(createdAtSec), globalWriteId);

            Y_VERIFY(operation->GetStatus() != EOperationStatus::Draft);

            auto [_, isOk] = Operations.emplace(operation->GetWriteId(), operation);
            if (!isOk) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "duplicated_operation")("operation", *operation);
                return false;
            }
            Transactions[txId].push_back(operation->GetWriteId());
            LastWriteId = std::max(LastWriteId, operation->GetWriteId());
            if (!rowset.Next()) {
                return false;
            }
        }
        return true;
    }

    bool TOperationsManager::CommitTransaction(TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_SCAN)("tx_id", txId)("event", "transaction_commit_fails"));
        auto tIt = Transactions.find(txId);
        if (tIt == Transactions.end()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("details", "skip_unknown_transaction");
            return true;
        }

        TVector<TWriteOperation::TPtr> commited;
        for (auto&& opId : tIt->second) {
            auto opPtr = Operations.FindPtr(opId);
            (*opPtr)->Commit(owner, txc, snapshot);
            commited.emplace_back(*opPtr);
        }

        Transactions.erase(txId);
        for (auto&& op: commited) {
            RemoveOperation(op, txc);
        }
        return true;
    }

    bool TOperationsManager::AbortTransaction(TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_SCAN)("tx_id", txId)("event", "transaction_abort_fails"));
        auto tIt = Transactions.find(txId);
        if (tIt == Transactions.end()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("details", "unknown_transaction");
            return true;
        }

        TVector<TWriteOperation::TPtr> aborted;
        for (auto&& opId : tIt->second) {
            auto opPtr = Operations.FindPtr(opId);
            (*opPtr)->Abort(owner, txc);
            aborted.emplace_back(*opPtr);
        }

        Transactions.erase(txId);
        for (auto&& op: aborted) {
            RemoveOperation(op, txc);
        }
        return true;
    }

    TWriteOperation::TPtr TOperationsManager::GetOperation(const TWriteId writeId) const {
        auto it = Operations.find(writeId);
        if (it == Operations.end()) {
            return nullptr;
        }
        return it->second;
    }

    void TOperationsManager::RemoveOperation(const TWriteOperation::TPtr& op, NTabletFlatExecutor::TTransactionContext& txc) {
        NIceDb::TNiceDb db(txc.DB);
        Operations.erase(op->GetWriteId());
        Schema::Operations_Erase(db, op->GetWriteId());
    }

    TWriteId TOperationsManager::BuildNextWriteId() {
        return ++LastWriteId;
    }

    TWriteOperation::TPtr TOperationsManager::RegisterOperation(const ui64 txId) {
        auto writeId = BuildNextWriteId();
        auto operation = std::make_shared<TWriteOperation>(writeId, txId, EOperationStatus::Draft, AppData()->TimeProvider->Now());
        Y_VERIFY(Operations.emplace(operation->GetWriteId(), operation).second);

        Transactions[operation->GetTxId()].push_back(operation->GetWriteId());
        return operation;
    }
}
