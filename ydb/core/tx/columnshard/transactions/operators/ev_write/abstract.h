#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

class TBaseEvWriteTransactionOperator: public TTxController::ITransactionOperator {
private:
    using TBase = TTxController::ITransactionOperator;
    using TProposeResult = TTxController::TProposeResult;

protected:
    ui64 LockId = 0;

private:
    virtual bool DoParseImpl(TColumnShard& owner, const NKikimrTxColumnShard::TCommitWriteTxBody& commitTxBody) = 0;
    virtual TProposeResult DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override final {
        owner.GetOperationsManager().LinkTransactionOnExecute(LockId, GetTxId(), txc);
        return TProposeResult();
    }
    virtual void DoStartProposeOnComplete(TColumnShard& owner, const TActorContext& /*ctx*/) override final {
        owner.GetOperationsManager().LinkTransactionOnComplete(LockId, GetTxId());
    }
    virtual void DoFinishProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) override final {
    }
    virtual void DoFinishProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override final {
    }
    virtual bool DoCheckAllowUpdate(const TFullTxInfo& currentTxInfo) const override final {
        return (currentTxInfo.Source == GetTxInfo().Source && currentTxInfo.Cookie == GetTxInfo().Cookie);
    }
    virtual bool DoParse(TColumnShard& owner, const TString& data) override final {
        NKikimrTxColumnShard::TCommitWriteTxBody commitTxBody;
        if (!commitTxBody.ParseFromString(data)) {
            return false;
        }
        LockId = commitTxBody.GetLockId();
        return DoParseImpl(owner, commitTxBody);
    }

    virtual bool DoIsAsync() const override final {
        return false;
    }

    virtual void DoSendReply(TColumnShard& owner, const TActorContext& ctx) override {
        const auto& txInfo = GetTxInfo();
        std::unique_ptr<NActors::IEventBase> evResult;
        TLogContextGuard gLogging(
            NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("send_reply_tx_id", GetTxId())("send_reply_lock_id", LockId));
        if (IsFail()) {
            evResult = NEvents::TDataEvents::TEvWriteResult::BuildError(owner.TabletID(), txInfo.GetTxId(),
                NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, GetProposeStartInfoVerified().GetStatusMessage());
        } else {
            evResult = NEvents::TDataEvents::TEvWriteResult::BuildPrepared(
                owner.TabletID(), txInfo.GetTxId(), owner.GetProgressTxController().BuildCoordinatorInfo(txInfo));
        }
        ctx.Send(txInfo.Source, evResult.release(), 0, txInfo.Cookie);
    }
    std::optional<NOlap::TSnapshot> Version;

public:
    using TBase::TBase;
    TBaseEvWriteTransactionOperator(const TFullTxInfo& txInfo, const ui64 lockId)
        : TBase(txInfo)
        , LockId(lockId) {
    }

    virtual bool IsTxBroken() const {
        return false;
    }

    ui64 GetLockId() const {
        return LockId;
    }

    virtual bool ProgressOnExecute(
        TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override {
        Version = version;
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("progress_tx_id", GetTxId())("lock_id", LockId)("broken", IsTxBroken());
        if (IsTxBroken()) {
            owner.GetOperationsManager().AbortTransactionOnExecute(owner, GetTxId(), txc);
        } else {
            owner.GetOperationsManager().CommitTransactionOnExecute(owner, GetTxId(), txc, version);
        }
        return true;
    }

    virtual bool ProgressOnComplete(TColumnShard& owner, const TActorContext& ctx) override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("progress_tx_id", GetTxId())("lock_id", LockId)("broken", IsTxBroken());
        AFL_VERIFY(Version);
        if (IsTxBroken()) {
            owner.GetOperationsManager().AbortTransactionOnComplete(owner, GetTxId());
            auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(
                owner.TabletID(), GetTxId(), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN, "lock invalidated");
            ctx.Send(TxInfo.Source, result.release(), 0, TxInfo.Cookie);
        } else {
            owner.GetOperationsManager().CommitTransactionOnComplete(owner, GetTxId(), *Version);
            auto result = NEvents::TDataEvents::TEvWriteResult::BuildCompleted(owner.TabletID(), GetTxId());
            ctx.Send(TxInfo.Source, result.release(), 0, TxInfo.Cookie);
        }
        return true;
    }

    virtual bool ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override {
        owner.GetOperationsManager().AbortTransactionOnExecute(owner, GetTxId(), txc);
        return true;
    }
    virtual bool CompleteOnAbort(TColumnShard& owner, const TActorContext& /*ctx*/) override {
        owner.GetOperationsManager().AbortTransactionOnComplete(owner, GetTxId());
        return true;
    }
};

}   // namespace NKikimr::NColumnShard
