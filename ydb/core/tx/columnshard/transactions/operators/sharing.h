#pragma once

#include "propose_tx.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/export/session/task.h>

namespace NKikimr::NColumnShard {

class TSharingTransactionOperator: public IProposeTxOperator, public TMonitoringObjectsCounter<TSharingTransactionOperator> {
private:
    using TBase = IProposeTxOperator;

    std::shared_ptr<NOlap::NDataSharing::TSessionsManager> SharingSessionsManager;
    std::shared_ptr<NOlap::NDataSharing::TDestinationSession> SharingTask;
    bool SessionExistsFlag = false;
    using TProposeResult = TTxController::TProposeResult;
    mutable std::unique_ptr<NTabletFlatExecutor::ITransaction> TxPropose;
    mutable std::unique_ptr<NTabletFlatExecutor::ITransaction> TxConfirm;
    mutable std::unique_ptr<NTabletFlatExecutor::ITransaction> TxAbort;
    static inline auto Registrator = TFactory::TRegistrator<TSharingTransactionOperator>(NKikimrTxColumnShard::TX_KIND_SHARING);
    THashSet<TActorId> NotifySubscribers;
    virtual TTxController::TProposeResult DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override;
    virtual void DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override;
    virtual void DoFinishProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) override {
    }
    virtual void DoFinishProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
    }
    virtual bool DoIsAsync() const override {
        AFL_VERIFY(SharingTask);
        return !SharingTask->IsFinished();
    }
    virtual bool DoParse(TColumnShard& owner, const TString& data) override;
    virtual TString DoDebugString() const override {
        return "SHARING";
    }

public:
    using TBase::TBase;
    virtual void RegisterSubscriber(const TActorId& actorId) override {
        NotifySubscribers.insert(actorId);
    }

    virtual bool ProgressOnExecute(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override;

    virtual bool ProgressOnComplete(TColumnShard& owner, const TActorContext& ctx) override;

    virtual bool ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override;
    virtual bool CompleteOnAbort(TColumnShard& owner, const TActorContext& ctx) override;
};

}
