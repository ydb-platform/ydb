#pragma once

#include "abstract.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

class TEvWriteCommitSyncTransactionOperator: public TBaseEvWriteTransactionOperator {
private:
    using TBase = TBaseEvWriteTransactionOperator;
    mutable std::optional<TMonotonic> DeadlockControlInstant;
    virtual void OnTimeout(TColumnShard& owner) = 0;

    virtual bool DoPingTimeout(TColumnShard& owner, const TMonotonic now) override final {
        if (!DeadlockControlInstant) {
            DeadlockControlInstant = now;
        } else if (now - *DeadlockControlInstant > TDuration::Seconds(2)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "tx_timeout")("lock", LockId)("tx_id", GetTxId())(
                "d", now - *DeadlockControlInstant);
            DeadlockControlInstant = now;
            OnTimeout(owner);
            return true;
        }
        return false;
    }
    virtual void DoSerializeToProto(NKikimrTxColumnShard::TCommitWriteTxBody& result) const = 0;

public:
    using TBase::TBase;

    static TEvTxProcessing::TEvReadSetAck* MakeBrokenFlagAck(ui64 step, ui64 txId, ui64 tabletSource, ui64 tabletDest) {
        return new TEvTxProcessing::TEvReadSetAck(step, txId, tabletSource, tabletDest, tabletSource, 0);
    }

    static bool SendBrokenFlagAck(TColumnShard& owner, TEvTxProcessing::TEvReadSetAck* event) {
        return SendPersistent(owner, event, event->Record.GetTabletDest(), event->Record.GetTxId());
    }

    static bool SendPersistent(TColumnShard& owner, IEventBase* event, ui64 tabletDest, ui64 cookie) {
        return owner.Send(
            MakePipePerNodeCacheID(EPipePerNodeCache::Persistent),
            new TEvPipeCache::TEvForward(event, tabletDest, true),
            IEventHandle::FlagTrackDelivery, cookie
        );
    }

    static bool SendBrokenFlagAck(TColumnShard& owner, ui64 step, ui64 txId, ui64 tabletDest) {
        const ui64 tabletSource = owner.TabletID();
        return SendBrokenFlagAck(owner, MakeBrokenFlagAck(step, txId, tabletSource, tabletDest));
    }

    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateReceiveResultAckTx(TColumnShard& owner, const ui64 recvTabletId) const = 0;
    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateReceiveBrokenFlagTx(
        TColumnShard& owner, const ui64 sendTabletId, const bool broken) const = 0;
    NKikimrTxColumnShard::TCommitWriteTxBody SerializeToProto() {
        NKikimrTxColumnShard::TCommitWriteTxBody result;
        AFL_VERIFY(LockId);
        result.SetLockId(LockId);
        DoSerializeToProto(result);
        return result;
    }
};

}   // namespace NKikimr::NColumnShard
