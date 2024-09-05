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

public:
    using TBase::TBase;

    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateReceiveResultAckTx(TColumnShard& owner, const ui64 recvTabletId) const = 0;
    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateReceiveBrokenFlagTx(
        TColumnShard& owner, const ui64 sendTabletId, const bool broken) const = 0;
    virtual NKikimrTxColumnShard::TCommitWriteTxBody SerializeToProto() const = 0;
};

}   // namespace NKikimr::NColumnShard
