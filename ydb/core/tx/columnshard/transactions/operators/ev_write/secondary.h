#pragma once

#include "sync.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/tablet/ext_tx_base.h>

namespace NKikimr::NColumnShard {

class TEvWriteCommitSecondaryTransactionOperator: public TEvWriteCommitSyncTransactionOperator,
                                                  public TMonitoringObjectsCounter<TEvWriteCommitSecondaryTransactionOperator> {
private:
    using TBase = TEvWriteCommitSyncTransactionOperator;
    using TProposeResult = TTxController::TProposeResult;
    static inline auto Registrator =
        TFactory::TRegistrator<TEvWriteCommitSecondaryTransactionOperator>(NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE_SECONDARY);

    virtual void DoSerializeToProto(NKikimrTxColumnShard::TCommitWriteTxBody& result) const override {
        auto& data = *result.MutableSecondaryTabletData();
        if (TxBroken) {
            data.SetTxBroken(*TxBroken);
        }
        data.SetSelfBroken(SelfBroken);
        data.SetNeedReceiveBroken(NeedReceiveBroken);
        data.SetReceiveAck(ReceiveAck);
        data.SetArbiterTabletId(ArbiterTabletId);
    }

private:
    ui64 ArbiterTabletId;
    bool NeedReceiveBroken = false;
    bool ReceiveAck = false;
    bool SelfBroken = false;
    std::optional<bool> TxBroken;

    virtual bool DoParseImpl(TColumnShard& /*owner*/, const NKikimrTxColumnShard::TCommitWriteTxBody& commitTxBody) override {
        if (!commitTxBody.HasSecondaryTabletData()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("event", "cannot read proto")("proto", commitTxBody.DebugString());
            return false;
        }
        auto& protoData = commitTxBody.GetSecondaryTabletData();
        SelfBroken = protoData.GetSelfBroken();
        ArbiterTabletId = protoData.GetArbiterTabletId();
        NeedReceiveBroken = protoData.GetNeedReceiveBroken();
        ReceiveAck = protoData.GetReceiveAck();
        if (protoData.HasTxBroken()) {
            TxBroken = protoData.GetTxBroken();
        }
        return true;
    }

private:
    virtual TString DoGetOpType() const override {
        return "EvWriteSecondary";
    }
    virtual TString DoDebugString() const override {
        return "EV_WRITE_SECONDARY";
    }
    class TTxWriteReceivedAck: public TExtendedTransactionBase {
    private:
        using TBase = TExtendedTransactionBase;
        const ui64 TxId;
        bool NeedContinueFlag = false;

        virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /* ctx */) override {
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitSecondaryTransactionOperator>(TxId, true);
            if (!op) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "duplication_tablet_ack_flag")("txId", TxId);
            } else {
                op->ReceiveAck = true;
                if (!op->NeedReceiveBroken) {
                    op->TxBroken = false;
                }
                Self->GetProgressTxController().WriteTxOperatorInfo(txc, TxId, op->SerializeToProto().SerializeAsString());
                if (!op->NeedReceiveBroken) {
                    NeedContinueFlag = true;
                }
            }
            return true;
        }
        virtual void DoComplete(const NActors::TActorContext& ctx) override {
            if (NeedContinueFlag) {
                Self->EnqueueProgressTx(ctx, TxId);
            }
        }

    public:
        TTxWriteReceivedAck(TColumnShard& owner, const ui64 txId)
            : TBase(&owner)
            , TxId(txId) {
        }
    };

    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateReceiveResultAckTx(
        TColumnShard& owner, const ui64 recvTabletId) const override {
        AFL_VERIFY(recvTabletId == ArbiterTabletId)("recv", recvTabletId)("arbiter", ArbiterTabletId);
        return std::make_unique<TTxWriteReceivedAck>(owner, GetTxId());
    }

    class TTxWriteReceivedBrokenFlag: public TExtendedTransactionBase {
    private:
        using TBase = TExtendedTransactionBase;
        const ui64 TxId;
        const bool BrokenFlag;

        virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /* ctx */) override {
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitSecondaryTransactionOperator>(TxId, true);
            if (!op) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "duplication_tablet_broken_flag")("txId", TxId);
            } else {
                op->TxBroken = BrokenFlag;
                Self->GetProgressTxController().WriteTxOperatorInfo(txc, TxId, op->SerializeToProto().SerializeAsString());
                if (BrokenFlag) {
                    Self->GetProgressTxController().ExecuteOnCancel(TxId, txc);
                }
            }
            return true;
        }
        virtual void DoComplete(const NActors::TActorContext& ctx) override {
            if (NYDBTest::TControllers::GetColumnShardController()->GetInterruptionOnLockedTransactions()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_continue");
                return;
            }
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitSecondaryTransactionOperator>(TxId, true);
            if (!op) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "duplication_tablet_broken_flag")("txId", TxId);
            } else {
                op->SendBrokenFlagAck(*Self);
                if (BrokenFlag) {
                    Self->GetProgressTxController().CompleteOnCancel(TxId, ctx);
                }
                Self->EnqueueProgressTx(ctx, TxId);
            }
        }

    public:
        TTxWriteReceivedBrokenFlag(TColumnShard* owner, const ui64 txId, const bool broken)
            : TBase(owner)
            , TxId(txId)
            , BrokenFlag(broken) {
        }
    };

    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateReceiveBrokenFlagTx(
        TColumnShard& owner, const ui64 sendTabletId, const bool broken) const override {
        AFL_VERIFY(ArbiterTabletId == sendTabletId);
        return std::make_unique<TTxWriteReceivedBrokenFlag>(&owner, GetTxId(), broken);
    }

    void SendBrokenFlagAck(TColumnShard& owner) {
        owner.Send(MakePipePerNodeCacheID(EPipePerNodeCache::Persistent),
            new TEvPipeCache::TEvForward(
                new TEvTxProcessing::TEvReadSetAck(GetStep(), GetTxId(), owner.TabletID(), ArbiterTabletId, owner.TabletID(), 0), ArbiterTabletId, true),
            IEventHandle::FlagTrackDelivery, GetTxId());
    }

    void SendResult(TColumnShard& owner) {
        if (NYDBTest::TControllers::GetColumnShardController()->GetInterruptionOnLockedTransactions()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_continue");
            return;
        }
        NKikimrTx::TReadSetData readSetData;
        readSetData.SetDecision(SelfBroken ? NKikimrTx::TReadSetData::DECISION_ABORT : NKikimrTx::TReadSetData::DECISION_COMMIT);
        owner.Send(MakePipePerNodeCacheID(EPipePerNodeCache::Persistent),
            new TEvPipeCache::TEvForward(new TEvTxProcessing::TEvReadSet(
                                             0, GetTxId(), owner.TabletID(), ArbiterTabletId, owner.TabletID(), readSetData.SerializeAsString()),
                ArbiterTabletId, true),
            IEventHandle::FlagTrackDelivery, GetTxId());
    }

    virtual void DoOnTabletInit(TColumnShard& /*owner*/) override {
    }

    class TTxStartPreparation: public TExtendedTransactionBase {
    private:
        using TBase = TExtendedTransactionBase;
        const ui64 TxId;

        virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /*ctx*/) override {
            auto& lock = Self->GetOperationsManager().GetLockVerified(Self->GetOperationsManager().GetLockForTxVerified(TxId));
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitSecondaryTransactionOperator>(TxId);
            op->SelfBroken = lock.IsBroken();
            Self->GetProgressTxController().WriteTxOperatorInfo(txc, TxId, op->SerializeToProto().SerializeAsString());
            return true;
        }
        virtual void DoComplete(const NActors::TActorContext& /*ctx*/) override {
            if (auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitSecondaryTransactionOperator>(TxId, true)) {
                op->SendResult(*Self);
            }
        }

    public:
        TTxStartPreparation(TColumnShard* owner, const ui64 txId)
            : TBase(owner)
            , TxId(txId) {
        }
    };

    virtual bool DoIsInProgress() const override {
        return !TxBroken && (NeedReceiveBroken || !ReceiveAck);
    }
    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> DoBuildTxPrepareForProgress(TColumnShard* owner) const override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "prepare_for_progress_started")("lock_id", LockId);
        return std::make_unique<TTxStartPreparation>(owner, GetTxId());
    }

    virtual void OnTimeout(TColumnShard& owner) override {
        SendResult(owner);
    }

public:
    using TBase::TBase;
    virtual bool IsTxBroken() const override {
        AFL_VERIFY(TxBroken);
        return *TxBroken;
    }

    TEvWriteCommitSecondaryTransactionOperator(
        const TFullTxInfo& txInfo, const ui64 lockId, const ui64 arbiterTabletId, const bool needReceiveBroken)
        : TBase(txInfo, lockId)
        , ArbiterTabletId(arbiterTabletId)
        , NeedReceiveBroken(needReceiveBroken) {
    }
};

}   // namespace NKikimr::NColumnShard
