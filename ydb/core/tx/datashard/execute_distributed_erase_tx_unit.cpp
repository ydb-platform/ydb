#include "change_collector.h"
#include "datashard_active_transaction.h"
#include "datashard_distributed_erase.h"
#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "datashard_user_db.h"
#include "execution_unit_ctors.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"

#include <util/generic/bitmap.h>

namespace NKikimr {
namespace NDataShard {

class TExecuteDistributedEraseTxUnit : public TExecutionUnit {
    using IChangeCollector = NMiniKQL::IChangeCollector;

public:
    TExecuteDistributedEraseTxUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::ExecuteDistributedEraseTx, true, self, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        if (DataShard.IsStopping()) {
            // Avoid doing any new work when datashard is stopping
            return false;
        }

        return !op->HasRuntimeConflicts();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(op->IsDistributedEraseTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        TDataShardLocksDb locksDb(DataShard, txc);
        TSetupSysLocks guardLocks(op, DataShard, &locksDb);

        const auto& eraseTx = tx->GetDistributedEraseTx();
        const auto& request = eraseTx->GetRequest();
        auto [readVersion, writeVersion] = DataShard.GetReadWriteVersions(op.Get());

        if (eraseTx->HasDependents()) {
            NMiniKQL::TEngineHostCounters engineHostCounters;
            TDataShardUserDb userDb(DataShard, txc.DB, op->GetGlobalTxId(), readVersion, writeVersion, engineHostCounters, TAppData::TimeProvider->Now());
            TDataShardChangeGroupProvider groupProvider(DataShard, txc.DB, /* distributed tx group */ 0);
            THolder<IDataShardChangeCollector> changeCollector{CreateChangeCollector(DataShard, userDb, groupProvider, txc.DB, request.GetTableId())};

            auto presentRows = TDynBitMap().Set(0, request.KeyColumnsSize());
            if (!Execute(txc, request, presentRows, eraseTx->GetConfirmedRows(), writeVersion, op->GetGlobalTxId(),
                    &userDb, &groupProvider, changeCollector.Get()))
            {
                return EExecutionStatus::Restart;
            }

            if (!userDb.GetVolatileReadDependencies().empty()) {
                for (ui64 txId : userDb.GetVolatileReadDependencies()) {
                    op->AddVolatileDependency(txId);
                    bool ok = DataShard.GetVolatileTxManager().AttachBlockedOperation(txId, op->GetTxId());
                    Y_VERIFY_S(ok, "Unexpected failure to attach " << *op << " to volatile tx " << txId);
                }

                if (txc.DB.HasChanges()) {
                    txc.DB.RollbackChanges();
                }
                return EExecutionStatus::Continue;
            }

            if (Pipeline.AddLockDependencies(op, guardLocks)) {
                if (txc.DB.HasChanges()) {
                    txc.DB.RollbackChanges();
                }
                return EExecutionStatus::Continue;
            }

            if (changeCollector) {
                op->ChangeRecords() = std::move(changeCollector->GetCollected());
            }
        } else if (eraseTx->HasDependencies()) {
            THashMap<ui64, TDynBitMap> presentRows;
            for (const auto& dependency : eraseTx->GetDependencies()) {
                Y_ABORT_UNLESS(!presentRows.contains(dependency.GetShardId()));
                presentRows.emplace(dependency.GetShardId(), DeserializeBitMap<TDynBitMap>(dependency.GetPresentRows()));
            }

            for (const auto& [_, readSets] : op->InReadSets()) {
                for (const auto& rs : readSets) {
                    NKikimrTxDataShard::TDistributedEraseRS body;
                    Y_ABORT_UNLESS(body.ParseFromArray(rs.Body.data(), rs.Body.size()));
                    Y_ABORT_UNLESS(presentRows.contains(rs.Origin));

                    auto confirmedRows = DeserializeBitMap<TDynBitMap>(body.GetConfirmedRows());
                    if (!Execute(txc, request, presentRows.at(rs.Origin), confirmedRows, writeVersion, op->GetGlobalTxId())) {
                        return EExecutionStatus::Restart;
                    }
                }
            }

            if (Pipeline.AddLockDependencies(op, guardLocks)) {
                if (txc.DB.HasChanges()) {
                    txc.DB.RollbackChanges();
                }
                return EExecutionStatus::Continue;
            }
        } else {
            Y_FAIL_S("Invalid distributed erase tx: " << eraseTx->GetBody().ShortDebugString());
        }

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        DataShard.SysLocksTable().ApplyLocks();
        DataShard.SubscribeNewLocks(ctx);
        Pipeline.AddCommittingOp(op);

        return EExecutionStatus::ExecutedNoMoreRestarts;
    }

    bool Execute(TTransactionContext& txc, const NKikimrTxDataShard::TEvEraseRowsRequest& request,
            const TDynBitMap& presentRows, const TDynBitMap& confirmedRows, const TRowVersion& writeVersion,
            ui64 globalTxId,
            TDataShardUserDb* userDb = nullptr,
            TDataShardChangeGroupProvider* groupProvider = nullptr,
            IDataShardChangeCollector* changeCollector = nullptr)
    {
        const ui64 tableId = request.GetTableId();
        const TTableId fullTableId(DataShard.GetPathOwnerId(), tableId);

        Y_ABORT_UNLESS(DataShard.GetUserTables().contains(tableId));
        const TUserTable& tableInfo = *DataShard.GetUserTables().at(tableId);

        const bool breakWriteConflicts = DataShard.SysLocksTable().HasWriteLocks(fullTableId);
        bool checkVolatileDependencies = bool(DataShard.GetVolatileTxManager().GetTxMap());

        absl::flat_hash_set<ui64> volatileDependencies;
        bool volatileOrdered = false;

        size_t row = 0;
        bool pageFault = false;
        bool commitAdded = false;
        Y_FOR_EACH_BIT(i, presentRows) {
            if (!confirmedRows.Test(i)) {
                ++row;
                continue;
            }

            const auto& serializedKey = request.GetKeyColumns(row++);
            TSerializedCellVec keyCells;
            Y_ABORT_UNLESS(TSerializedCellVec::TryParse(serializedKey, keyCells));
            Y_ABORT_UNLESS(keyCells.GetCells().size() == tableInfo.KeyColumnTypes.size());

            TVector<TRawTypeValue> key;
            for (size_t ki : xrange(tableInfo.KeyColumnTypes.size())) {
                const auto& kt = tableInfo.KeyColumnTypes[ki];
                const TCell& cell = keyCells.GetCells()[ki];
                key.emplace_back(TRawTypeValue(cell.AsRef(), kt));
            }

            if (breakWriteConflicts || checkVolatileDependencies) {
                if (!DataShard.BreakWriteConflicts(txc.DB, fullTableId, keyCells.GetCells(), volatileDependencies)) {
                    if (breakWriteConflicts) {
                        pageFault = true;
                    } else if (checkVolatileDependencies) {
                        checkVolatileDependencies = false;
                        volatileDependencies.clear();
                        volatileOrdered = true;
                    }
                }
            }

            if (changeCollector) {
                if (!volatileDependencies.empty() || volatileOrdered) {
                    if (!changeCollector->OnUpdateTx(fullTableId, tableInfo.LocalTid, NTable::ERowOp::Erase, key, {}, globalTxId)) {
                        pageFault = true;
                    }
                } else {
                    if (!changeCollector->OnUpdate(fullTableId, tableInfo.LocalTid, NTable::ERowOp::Erase, key, {}, writeVersion)) {
                        pageFault = true;
                    }
                }
            }

            if (pageFault) {
                continue;
            }

            DataShard.SysLocksTable().BreakLocks(fullTableId, keyCells.GetCells());

            if (!volatileDependencies.empty() || volatileOrdered) {
                txc.DB.UpdateTx(tableInfo.LocalTid, NTable::ERowOp::Erase, key, {}, globalTxId);
                DataShard.GetConflictsCache().GetTableCache(tableInfo.LocalTid).AddUncommittedWrite(keyCells.GetCells(), globalTxId, txc.DB);
                if (!commitAdded && userDb) {
                    // Make sure we see our own changes on further iterations
                    userDb->AddCommitTxId(fullTableId, globalTxId, writeVersion);
                    commitAdded = true;
                }
            } else {
                txc.DB.Update(tableInfo.LocalTid, NTable::ERowOp::Erase, key, {}, writeVersion);
                DataShard.GetConflictsCache().GetTableCache(tableInfo.LocalTid).RemoveUncommittedWrites(keyCells.GetCells(), txc.DB);
            }
        }

        if (pageFault && changeCollector) {
            changeCollector->OnRestart();
        }

        if (!volatileDependencies.empty() || volatileOrdered) {
            DataShard.GetVolatileTxManager().PersistAddVolatileTx(
                globalTxId,
                writeVersion,
                /* commitTxIds */ { globalTxId },
                volatileDependencies,
                /* participants */ { },
                groupProvider ? groupProvider->GetCurrentChangeGroup() : std::nullopt,
                volatileOrdered,
                /* arbiter */ false,
                txc);
            // Note: transaction is already committed, no additional waiting needed
        }

        return !pageFault;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
    }
};

THolder<TExecutionUnit> CreateExecuteDistributedEraseTxUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TExecuteDistributedEraseTxUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
