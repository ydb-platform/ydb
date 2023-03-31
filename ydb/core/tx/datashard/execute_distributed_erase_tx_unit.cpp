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
        : TExecutionUnit(EExecutionUnitKind::ExecuteDistributedEraseTx, false, self, pipeline)
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
        Y_VERIFY(op->IsDistributedEraseTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        TDataShardLocksDb locksDb(DataShard, txc);
        TSetupSysLocks guardLocks(op, DataShard, &locksDb);

        const auto& eraseTx = tx->GetDistributedEraseTx();
        const auto& request = eraseTx->GetRequest();
        auto [readVersion, writeVersion] = DataShard.GetReadWriteVersions(op.Get());

        if (eraseTx->HasDependents()) {
            TDataShardUserDb userDb(DataShard, txc.DB, readVersion);
            THolder<IDataShardChangeCollector> changeCollector{CreateChangeCollector(DataShard, userDb, txc.DB, request.GetTableId(), false)};

            auto presentRows = TDynBitMap().Set(0, request.KeyColumnsSize());
            if (!Execute(txc, request, presentRows, eraseTx->GetConfirmedRows(), writeVersion, changeCollector.Get())) {
                return EExecutionStatus::Restart;
            }

            if (Pipeline.AddLockDependencies(op, guardLocks)) {
                txc.Reschedule();
                return EExecutionStatus::Restart;
            }

            if (changeCollector) {
                op->ChangeRecords() = std::move(changeCollector->GetCollected());
            }
        } else if (eraseTx->HasDependencies()) {
            THashMap<ui64, TDynBitMap> presentRows;
            for (const auto& dependency : eraseTx->GetDependencies()) {
                Y_VERIFY(!presentRows.contains(dependency.GetShardId()));
                presentRows.emplace(dependency.GetShardId(), DeserializeBitMap<TDynBitMap>(dependency.GetPresentRows()));
            }

            for (const auto& [_, readSets] : op->InReadSets()) {
                for (const auto& rs : readSets) {
                    NKikimrTxDataShard::TDistributedEraseRS body;
                    Y_VERIFY(body.ParseFromArray(rs.Body.data(), rs.Body.size()));

                    Y_VERIFY(presentRows.contains(rs.Origin));
                    const bool ok = Execute(txc, request, presentRows.at(rs.Origin), DeserializeBitMap<TDynBitMap>(body.GetConfirmedRows()), writeVersion);
                    Y_VERIFY(ok);
                }
            }

            if (Pipeline.AddLockDependencies(op, guardLocks)) {
                txc.Reschedule();
                return EExecutionStatus::Restart;
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
            IDataShardChangeCollector* changeCollector = nullptr)
    {
        const ui64 tableId = request.GetTableId();
        const TTableId fullTableId(DataShard.GetPathOwnerId(), tableId);

        Y_VERIFY(DataShard.GetUserTables().contains(tableId));
        const TUserTable& tableInfo = *DataShard.GetUserTables().at(tableId);

        const bool breakWriteConflicts = DataShard.SysLocksTable().HasWriteLocks(fullTableId);

        size_t row = 0;
        bool pageFault = false;
        Y_FOR_EACH_BIT(i, presentRows) {
            if (!confirmedRows.Test(i)) {
                ++row;
                continue;
            }

            const auto& serializedKey = request.GetKeyColumns(row++);
            TSerializedCellVec keyCells;
            Y_VERIFY(TSerializedCellVec::TryParse(serializedKey, keyCells));
            Y_VERIFY(keyCells.GetCells().size() == tableInfo.KeyColumnTypes.size());

            TVector<TRawTypeValue> key;
            for (size_t ki : xrange(tableInfo.KeyColumnTypes.size())) {
                const auto& kt = tableInfo.KeyColumnTypes[ki];
                const TCell& cell = keyCells.GetCells()[ki];
                key.emplace_back(TRawTypeValue(cell.AsRef(), kt));
            }

            if (changeCollector) {
                if (!changeCollector->OnUpdate(fullTableId, tableInfo.LocalTid, NTable::ERowOp::Erase, key, {}, writeVersion)) {
                    pageFault = true;
                }
            }

            if (breakWriteConflicts) {
                if (!DataShard.BreakWriteConflicts(txc.DB, fullTableId, keyCells.GetCells())) {
                    pageFault = true;
                }
            }

            if (pageFault) {
                continue;
            }

            DataShard.SysLocksTable().BreakLocks(fullTableId, keyCells.GetCells());
            txc.DB.Update(tableInfo.LocalTid, NTable::ERowOp::Erase, key, {}, writeVersion);
        }

        if (pageFault && changeCollector) {
            changeCollector->OnRestart();
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
