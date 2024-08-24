#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

class TEvWriteTransactionOperator: public TTxController::ITransactionOperator, public TMonitoringObjectsCounter<TEvWriteTransactionOperator> {
private:
    using TBase = TTxController::ITransactionOperator;
    using TProposeResult = TTxController::TProposeResult;
    static inline auto Registrator = TFactory::TRegistrator<TEvWriteTransactionOperator>(NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE);

private:
    ui64 LockId = 0;
    std::set<ui64> ReceivingShards;
    std::set<ui64> SendingShards;
    std::set<ui64> WaitShards;
    bool Broken = false;
    bool Finished = false;
    bool Started = false;
    std::optional<bool> ResultBroken;

private:
    virtual TProposeResult DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override {
        owner.OperationsManager->LinkTransaction(LockId, GetTxId(), txc);
        return TProposeResult();
    }
    virtual void DoStartProposeOnComplete(TColumnShard& owner, const TActorContext& /*ctx*/) override {
        Start(owner);
    }
    virtual void DoFinishProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) override {
        AFL_VERIFY(Finished || !IsAsync());
    }
    virtual void DoFinishProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
    }
    virtual TString DoGetOpType() const override {
        return "EvWrite";
    }
    virtual bool DoIsAsync() const override {
        return ReceivingShards.size();
    }
    virtual bool DoCheckAllowUpdate(const TFullTxInfo& currentTxInfo) const override {
        return (currentTxInfo.Source == GetTxInfo().Source && currentTxInfo.Cookie == GetTxInfo().Cookie);
    }
    virtual TString DoDebugString() const override {
        return "EV_WRITE";
    }
    virtual void DoSendReply(TColumnShard& owner, const TActorContext& ctx) override {
        const auto& txInfo = GetTxInfo();
        std::unique_ptr<NActors::IEventBase> evResult;
        if (IsFail()) {
            evResult = NEvents::TDataEvents::TEvWriteResult::BuildError(owner.TabletID(), txInfo.GetTxId(),
                NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, GetProposeStartInfoVerified().GetStatusMessage());
        } else if (ResultBroken && *ResultBroken) {
            evResult = NEvents::TDataEvents::TEvWriteResult::BuildError(
                owner.TabletID(), txInfo.GetTxId(), NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, "transactions lock invalidated");
        } else {
            evResult = NEvents::TDataEvents::TEvWriteResult::BuildPrepared(
                owner.TabletID(), txInfo.GetTxId(), owner.GetProgressTxController().BuildCoordinatorInfo(txInfo));
        }
        ctx.Send(txInfo.Source, evResult.release(), 0, txInfo.Cookie);
    }

    virtual bool DoParse(TColumnShard& owner, const TString& data) override;
    virtual void DoOnTabletInit(TColumnShard& owner) override {
        if (IsAsync()) {
            Start(owner);
        }
    }

public:
    using TBase::TBase;

    virtual bool ProgressOnExecute(
        TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override {
        return owner.OperationsManager->CommitTransaction(owner, GetTxId(), txc, version);
    }

    virtual bool ProgressOnComplete(TColumnShard& owner, const TActorContext& ctx) override {
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildCompleted(owner.TabletID(), GetTxId());
        ctx.Send(TxInfo.Source, result.release(), 0, TxInfo.Cookie);
        return true;
    }

    virtual bool ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override {
        return owner.OperationsManager->AbortTransaction(owner, GetTxId(), txc);
    }
    virtual bool CompleteOnAbort(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
        return true;
    }

    void Receive(TColumnShard& owner, const ui64 tabletId, const bool broken);

    bool IsFinished() const {
        return Finished;
    }

    void Send(TColumnShard& owner, const std::optional<ui64>& destTabletId = {});
    void Finish(TColumnShard& owner);
    void Start(TColumnShard& owner);
    void Ask(TColumnShard& owner);
};

}   // namespace NKikimr::NColumnShard
