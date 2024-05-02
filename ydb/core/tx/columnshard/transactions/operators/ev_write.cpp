#include "ev_write.h"
#include <ydb/core/tx/columnshard/transactions/locks_db.h>
#include <ydb/core/tx/datashard/datashard_kqp.h>
#include <ydb/core/tx/long_tx_service/public/events.h>

namespace NKikimr::NColumnShard {

void KqpCommitLocks(ui64 origin, const NKikimrDataEvents::TKqpLocks* kqpLocks, NDataShard::TSysLocks& sysLocks) {
    if (kqpLocks == nullptr) {
        return;
    }

    if (NDataShard::NeedCommitLocks(kqpLocks->GetOp())) {
        // We assume locks have been validated earlier
        for (const auto& lockProto : kqpLocks->GetLocks()) {
            if (lockProto.GetDataShard() != origin) {
                continue;
            }
            auto lockKey = NDataShard::MakeLockKey(lockProto);
            sysLocks.CommitLock(lockKey);
        }
    } else {
        NDataShard::KqpEraseLocks(origin, kqpLocks, sysLocks);
    }
}

TEvWriteTransactionOperator::TProposeResult TEvWriteTransactionOperator::ExecuteOnPropose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const {
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
                    // Lock is valid, we may continue with reads and side-effects
                    break;

                case NDataShard::EEnsureCurrentLock::Broken:
                    // Lock is valid, but broken, we could abort early in some
                    // cases, but it doesn't affect correctness.
                    break;

                case NDataShard::EEnsureCurrentLock::TooMany:
                    // Lock cannot be created, it's not necessarily a problem
                    // for read-only transactions, for non-readonly we need to
                    // abort;
                    return false; // TProposeResult(NKikimrTxColumnShard::EResultStatus::OVERLOADED, "too many locks");
                case NDataShard::EEnsureCurrentLock::Abort:
                    return false; //TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, "cannot acquire locks");
            }
        }

        const auto tabletId = owner.TabletID();
        const auto* kqpLocks = &KqpLocks.value();

        auto [validated, brokenLocks] = NDataShard::KqpValidateLocks(tabletId, owner.SysLocksTable(), kqpLocks, true, InReadSets);
        if (!validated) {
            for (auto& brokenLock : brokenLocks) {
                Y_UNUSED(brokenLock);
             //   writeOp->GetWriteResult()->Record.MutableTxLocks()->Add()->Swap(&brokenLock);
            }

            KqpEraseLocks(tabletId, kqpLocks, owner.SysLocksTable());
            owner.SysLocksTable().ApplyLocks();
        // DataShard.SubscribeNewLocks(ctx);
            if (locksDb.HasChanges()) {
        //     op->SetWaitCompletionFlag(true);
                return false; // EExecutionStatus::ExecutedNoMoreRestarts;
            }

            // NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN
            return true; //TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, "Operation is aborting because locks are not valid");
        }

        KqpCommitLocks(tabletId, kqpLocks, owner.SysLocksTable()); // Do it on commit
    }

    return owner.OperationsManager->CommitTransaction(owner, GetTxId(), txc, version);
}

}
