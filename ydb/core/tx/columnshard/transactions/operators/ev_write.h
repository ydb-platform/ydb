#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

    class TEvWriteTransactionOperator: public TTxController::ITransactionOperator, public TMonitoringObjectsCounter<TEvWriteTransactionOperator> {
        using TBase = TTxController::ITransactionOperator;
        using TProposeResult = TTxController::TProposeResult;
        static inline auto Registrator = TFactory::TRegistrator<TEvWriteTransactionOperator>(NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE);
    private:
        virtual TProposeResult DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override {
            owner.OperationsManager->LinkTransaction(LockId, GetTxId(), txc);
            return TProposeResult();
        }
        virtual void DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {

        }
        virtual void DoFinishProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) override {
        }
        virtual void DoFinishProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) override {
        }
        virtual bool DoIsAsync() const override {
            return false;
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
                evResult = NEvents::TDataEvents::TEvWriteResult::BuildError(owner.TabletID(), txInfo.GetTxId(), NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, GetProposeStartInfoVerified().GetStatusMessage());
            } else {
                evResult = NEvents::TDataEvents::TEvWriteResult::BuildPrepared(owner.TabletID(), txInfo.GetTxId(), owner.GetProgressTxController().BuildCoordinatorInfo(txInfo));
            }
            ctx.Send(txInfo.Source, evResult.release(), 0, txInfo.Cookie);
        }

        virtual bool DoParse(TColumnShard& /*owner*/, const TString& data) override {
            NKikimrTxColumnShard::TCommitWriteTxBody commitTxBody;
            if (!commitTxBody.ParseFromString(data)) {
                return false;
            }
            LockId = commitTxBody.GetLockId();
            return !!LockId;
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

    private:
        ui64 LockId = 0;
    };

}
