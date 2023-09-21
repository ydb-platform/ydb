#include "write.h"
#include "slice_builder.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/conveyor/usage/service.h>

#include <ydb/core/tablet_flat/tablet_flat_executor.h>


namespace NKikimr::NColumnShard {

    TWriteOperation::TWriteOperation(const TWriteId writeId, const ui64 txId, const EOperationStatus& status, const TInstant createdAt)
        : Status(status)
        , CreatedAt(createdAt)
        , WriteId(writeId)
        , TxId(txId)
    {
    }

    void TWriteOperation::Start(TColumnShard& owner, const ui64 tableId, const NEvWrite::IDataContainer::TPtr& data, const NActors::TActorId& source, const TActorContext& ctx) {
        Y_VERIFY(Status == EOperationStatus::Draft);

        NEvWrite::TWriteMeta writeMeta((ui64)WriteId, tableId, source);
        std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildSlicesTask>(owner.TabletID(), ctx.SelfID,
            owner.StoragesManager->GetInsertOperator()->StartWritingAction(), NEvWrite::TWriteData(writeMeta, data));
        NConveyor::TCompServiceOperator::SendTaskToExecute(task);

        Status = EOperationStatus::Started;
    }

    void TWriteOperation::Commit(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) const {
        Y_VERIFY(Status == EOperationStatus::Prepared);

        TBlobGroupSelector dsGroupSelector(owner.Info());
        NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

        for (auto gWriteId : GlobalWriteIds) {
            auto pathExists = [&](ui64 pathId) {
                return owner.TablesManager.HasTable(pathId);
            };

            auto counters = owner.InsertTable->Commit(dbTable, snapshot.GetPlanStep(), snapshot.GetTxId(), { gWriteId },
                                                        pathExists);

            owner.IncCounter(COUNTER_BLOBS_COMMITTED, counters.Rows);
            owner.IncCounter(COUNTER_BYTES_COMMITTED, counters.Bytes);
            owner.IncCounter(COUNTER_RAW_BYTES_COMMITTED, counters.RawBytes);
        }
        owner.UpdateInsertTableCounters();
    }

    void TWriteOperation::OnWriteFinish(NTabletFlatExecutor::TTransactionContext& txc, const TVector<TWriteId>& globalWriteIds) {
        Y_VERIFY(Status == EOperationStatus::Started);
        Status = EOperationStatus::Prepared;
        GlobalWriteIds = globalWriteIds;
        NIceDb::TNiceDb db(txc.DB);
        Schema::Operations_Write(db, *this);
    }

    void TWriteOperation::ToProto(NKikimrTxColumnShard::TInternalOperationData& proto) const  {
        for (auto&& writeId : GlobalWriteIds) {
            proto.AddInternalWriteIds((ui64)writeId);
        }
    }

    void TWriteOperation::FromProto(const NKikimrTxColumnShard::TInternalOperationData& proto) {
        for (auto&& writeId : proto.GetInternalWriteIds()) {
            GlobalWriteIds.push_back(TWriteId(writeId));
        }
    }

    void TWriteOperation::Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const {
        Y_VERIFY(Status == EOperationStatus::Prepared);

        TBlobGroupSelector dsGroupSelector(owner.Info());
        NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);

        THashSet<TWriteId> writeIds;
        writeIds.insert(GlobalWriteIds.begin(), GlobalWriteIds.end());
        owner.InsertTable->Abort(dbTable, writeIds);
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
            const TString metadata = rowset.GetValue<Schema::Operations::Metadata>();
            NKikimrTxColumnShard::TInternalOperationData metaProto;
            Y_VERIFY(metaProto.ParseFromString(metadata));
            const EOperationStatus status = (EOperationStatus) rowset.GetValue<Schema::Operations::Status>();

            auto operation = std::make_shared<TWriteOperation>(writeId, txId, status, TInstant::Seconds(createdAtSec));
            operation->FromProto(metaProto);

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
