#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

    class TEvWriteTransactionOperator : public TTxController::ITransactionOperatior {
        using TBase = TTxController::ITransactionOperatior;
        using TProposeResult = TTxController::TProposeResult;
        static inline auto Registrator = TFactory::TRegistrator<TEvWriteTransactionOperator>(NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE);
    public:
        using TBase::TBase;

        virtual bool Parse(const TString& data) override {
            NKikimrTxColumnShard::TCommitWriteTxBody commitTxBody;
            if (!commitTxBody.ParseFromString(data)) {
                return false;
            }
            LockId = commitTxBody.GetLockId();
            return !!LockId;
        }

        TProposeResult Propose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, bool /*proposed*/) const override {
            owner.OperationsManager->LinkTransaction(LockId, GetTxId(), txc);
            return TProposeResult();
        }

        virtual bool Progress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) override {
            return owner.OperationsManager->CommitTransaction(owner, GetTxId(), txc, version);
        }

        virtual bool Complete(TColumnShard& owner, const TActorContext& ctx) override {
            auto result = NEvents::TDataEvents::TEvWriteResult::BuildCompleted(owner.TabletID(), GetTxId());
            ctx.Send(TxInfo.Source, result.release(), 0, TxInfo.Cookie);
            return true;
        }

        virtual bool Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) override {
            return owner.OperationsManager->AbortTransaction(owner, GetTxId(), txc);
        }
    private:
        ui64 LockId = 0;
    };

}
