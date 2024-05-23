#include "ev_write.h"
#include <ydb/core/tx/columnshard/transactions/locks_db.h>
#include <ydb/core/tx/datashard/datashard_kqp.h>
#include <ydb/core/tx/long_tx_service/public/events.h>

namespace NKikimr::NColumnShard {

bool TEvWriteTransactionOperator::DoParse(TColumnShard& /*owner*/, const TString& data) {
    NKikimrTxColumnShard::TCommitWriteTxBody commitTxBody;
    if (!commitTxBody.ParseFromString(data)) {
        return false;
    }
    LockId = commitTxBody.GetLockId();
    if (commitTxBody.HasKqpLocks() && !commitTxBody.GetKqpLocks().GetLocks().empty()) {
        KqpLocks = commitTxBody.GetKqpLocks();
    }
    return !!LockId;
}

TEvWriteTransactionOperator::TProposeResult TEvWriteTransactionOperator::DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    if (!owner.OperationsManager->LinkTransaction(LockId, GetTxId(), txc)) {
        return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, "unknown lockId");
    }
    return TProposeResult();
}

void TEvWriteTransactionOperator::SubscribeNewLocks(TColumnShard& owner, const TActorContext& ctx) {
    while (auto pendingSubscribeLock = owner.SysLocksTable().NextPendingSubscribeLock()) {
        ctx.Send(NLongTxService::MakeLongTxServiceID(owner.SelfId().NodeId()),
            new NLongTxService::TEvLongTxService::TEvSubscribeLock(
                pendingSubscribeLock.LockId,
                pendingSubscribeLock.LockNodeId));
    }
}

bool TEvWriteTransactionOperator::ExecuteOnProgress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) {
    auto operations = owner.OperationsManager->GetOperations(LockId);
    AFL_VERIFY(operations.size() > 0);

    if (KqpLocks) {
        TColumnShardLocksDb locksDb(owner, txc);
        NDataShard::TLocksGuard guardLocks(owner, &locksDb);

        if (guardLocks.LockTxId) {
            switch (owner.SysLocksTable().EnsureCurrentLock()) {
                case NDataShard::EEnsureCurrentLock::Success:
                    break;
                case NDataShard::EEnsureCurrentLock::Broken:
                    break;
                case NDataShard::EEnsureCurrentLock::TooMany:
                    return false;
                case NDataShard::EEnsureCurrentLock::Abort:
                    return false;
            }
        }

        const auto tabletId = owner.TabletID();
        const auto* kqpLocks = &KqpLocks.value();

        bool validated = false;
        std::tie(validated, BrokenLocks) = NDataShard::KqpValidateLocks(tabletId, owner.SysLocksTable(), kqpLocks, true, InReadSets);
        if (!validated) {
            NDataShard::KqpEraseLocks(tabletId, kqpLocks, owner.SysLocksTable());
            owner.SysLocksTable().ApplyLocks();
            // SubscribeNewLocks(ctx);
            if (locksDb.HasChanges()) {
                //  op->SetWaitCompletionFlag(true);
                return false;
            }
            return true;
        }

        NDataShard::KqpCommitLocks(tabletId, kqpLocks, owner.SysLocksTable(), [](const NKikimrDataEvents::TLock&){});
    }

    return owner.OperationsManager->CommitTransaction(owner, GetTxId(), txc, version);
}

}
