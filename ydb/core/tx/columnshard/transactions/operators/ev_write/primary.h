#pragma once

#include "sync.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/tablet/ext_tx_base.h>

#include <util/string/join.h>

namespace NKikimr::NColumnShard {

class TEvWriteCommitPrimaryTransactionOperator: public TEvWriteCommitSyncTransactionOperator,
                                                public TMonitoringObjectsCounter<TEvWriteCommitPrimaryTransactionOperator> {
private:
    using TBase = TEvWriteCommitSyncTransactionOperator;
    using TProposeResult = TTxController::TProposeResult;
    static inline auto Registrator =
        TFactory::TRegistrator<TEvWriteCommitPrimaryTransactionOperator>(NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE_PRIMARY);

private:
    std::set<ui64> ReceivingShards;
    std::set<ui64> SendingShards;
    std::set<ui64> WaitShardsBrokenFlags;
    std::set<ui64> WaitShardsResultAck;
    std::optional<bool> TxBroken;

    virtual void DoSerializeToProto(NKikimrTxColumnShard::TCommitWriteTxBody& result) const override {
        auto& data = *result.MutablePrimaryTabletData();
        if (TxBroken) {
            data.SetTxBroken(*TxBroken);
        }
        for (auto&& i : ReceivingShards) {
            data.AddReceivingShards(i);
        }
        for (auto&& i : SendingShards) {
            data.AddSendingShards(i);
        }
        for (auto&& i : WaitShardsBrokenFlags) {
            data.AddWaitShardsBrokenFlags(i);
        }
        for (auto&& i : WaitShardsResultAck) {
            data.AddWaitShardsResultAck(i);
        }
    }

    virtual bool DoParseImpl(TColumnShard& /*owner*/, const NKikimrTxColumnShard::TCommitWriteTxBody& commitTxBody) override {
        if (!commitTxBody.HasPrimaryTabletData()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("event", "cannot read proto")("proto", commitTxBody.DebugString());
            return false;
        }
        auto& protoData = commitTxBody.GetPrimaryTabletData();
        for (auto&& i : protoData.GetReceivingShards()) {
            ReceivingShards.emplace(i);
        }
        for (auto&& i : protoData.GetSendingShards()) {
            SendingShards.emplace(i);
        }
        for (auto&& i : protoData.GetWaitShardsBrokenFlags()) {
            WaitShardsBrokenFlags.emplace(i);
        }
        for (auto&& i : protoData.GetWaitShardsResultAck()) {
            WaitShardsResultAck.emplace(i);
        }
        if (protoData.HasTxBroken()) {
            TxBroken = protoData.GetTxBroken();
        }
        return true;
    }

private:
    virtual TString DoGetOpType() const override {
        return "EvWritePrimary";
    }
    virtual TString DoDebugString() const override {
        return "EV_WRITE_PRIMARY";
    }
    class TTxWriteReceivedBrokenFlag: public TExtendedTransactionBase {
    private:
        using TBase = TExtendedTransactionBase;
        const ui64 TxId;
        const ui64 Step;
        const ui64 TabletId;
        const bool BrokenFlag;
        std::unique_ptr<TEvTxProcessing::TEvReadSetAck> BrokenFlagAck;

        virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /*ctx*/) override {
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitPrimaryTransactionOperator>(TxId, true);
            if (!op) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("event", "repeated shard broken_flag info, operator not found")("shard_id", TabletId);
                // send the ack anyway, so that the secondary waits less time to progress
                TEvWriteCommitSyncTransactionOperator::SendBrokenFlagAck(*Self, Step, TxId, TabletId);
                return true;
            }
            
            BrokenFlagAck = TEvWriteCommitSyncTransactionOperator::MakeBrokenFlagAck(op->GetStep(), op->GetTxId(), Self->TabletID(), TabletId);
            if (!op->WaitShardsBrokenFlags.erase(TabletId)) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("event", "repeated shard broken_flag info")("shard_id", TabletId);
                // we cannot send the ack here, because the previous transaction (that successfully erased TabletId) may be not completed yet
            } else {
                op->TxBroken = op->TxBroken.value_or(false) || BrokenFlag;
                Self->GetProgressTxController().WriteTxOperatorInfo(txc, TxId, op->SerializeToProto().SerializeAsString());
                // we cannot send the ack right away, we must make sure that we have stored the TxBroken value
                // but we can proceed right away if we have collected all the broken flags from the secondary
                op->InitializeRequests(*Self);
            }
            return true;
        }

        virtual void DoComplete(const NActors::TActorContext& /*ctx*/) override {
            if (BrokenFlagAck != nullptr) {
                TEvWriteCommitSyncTransactionOperator::SendBrokenFlagAck(*Self, std::move(BrokenFlagAck));
            }
        }

    public:
        TTxWriteReceivedBrokenFlag(TColumnShard& owner, const ui64 txId, const ui64 step, const ui64 tabletId, const bool broken)
            : TBase(&owner, ::ToString(txId))
            , TxId(txId)
            , Step(step)
            , TabletId(tabletId)
            , BrokenFlag(broken) {
        }
    };

    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateReceiveBrokenFlagTx(
        TColumnShard& owner, const ui64 sendTabletId, const bool broken) const override {
        return std::make_unique<TTxWriteReceivedBrokenFlag>(owner, GetTxId(), GetStep(), sendTabletId, broken);
    }

    class TTxWriteReceivedResultAck: public TExtendedTransactionBase {
    private:
        using TBase = TExtendedTransactionBase;
        const ui64 TxId;
        const ui64 TabletId;

        virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /*ctx*/) override {
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitPrimaryTransactionOperator>(TxId, true);
            if (!op) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "ack_tablet_duplication")("receive", TabletId)("reason", "operation absent");
            } else if (!op->WaitShardsResultAck.erase(TabletId)) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("event", "ack_tablet_duplication")("wait", JoinSeq(",", op->WaitShardsResultAck))("receive", TabletId);
            } else {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "ack_tablet")("wait", JoinSeq(",", op->WaitShardsResultAck))("receive", TabletId);
                Self->GetProgressTxController().WriteTxOperatorInfo(txc, TxId, op->SerializeToProto().SerializeAsString());
                op->CheckFinished(*Self);
            }
            return true;
        }
        virtual void DoComplete(const NActors::TActorContext& /*ctx*/) override {
        }

    public:
        TTxWriteReceivedResultAck(TColumnShard& owner, const ui64 txId, const ui64 tabletId)
            : TBase(&owner, "write_received_result_ack")
            , TxId(txId)
            , TabletId(tabletId) {
        }
    };

    virtual bool IsTxBroken() const override {
        AFL_VERIFY(TxBroken);
        return *TxBroken;
    }

    void InitializeRequests(TColumnShard& owner) {
        if (WaitShardsBrokenFlags.empty()) {
            if (WaitShardsResultAck.size()) {
                SendResult(owner);
            } else {
                CheckFinished(owner);
            }
        }
    }

    void CheckFinished(TColumnShard& owner) {
        if (!IsInProgress()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "finished");
            owner.EnqueueProgressTx(NActors::TActivationContext::AsActorContext(), GetTxId());
        }
    }

    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateReceiveResultAckTx(
        TColumnShard& owner, const ui64 recvTabletId) const override
    {
        return std::make_unique<TTxWriteReceivedResultAck>(owner, GetTxId(), recvTabletId);
    }

    void SendResult(TColumnShard& owner) {
        AFL_VERIFY(!!TxBroken);
        NKikimrTx::TReadSetData readSetData;
        readSetData.SetDecision(*TxBroken ? NKikimrTx::TReadSetData::DECISION_ABORT : NKikimrTx::TReadSetData::DECISION_COMMIT);
        for (auto&& tabletDest : ReceivingShards) {
            if (WaitShardsResultAck.contains(tabletDest)) {
                auto event = std::make_unique<TEvTxProcessing::TEvReadSet>(TxInfo.PlanStep, GetTxId(), owner.TabletID(), tabletDest, owner.TabletID(), readSetData.SerializeAsString());
                TEvWriteCommitSyncTransactionOperator::SendPersistent(owner, std::move(event), tabletDest, GetTxId());
            }
        }
    }

    virtual void DoOnTabletInit(TColumnShard& /*owner*/) override {
    }

    class TTxStartPreparation: public TExtendedTransactionBase {
    private:
        using TBase = TExtendedTransactionBase;
        const ui64 TxId;

        virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /*ctx*/) override {
            auto& lock = Self->GetOperationsManager().GetLockVerified(Self->GetOperationsManager().GetLockForTxVerified(TxId));
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitPrimaryTransactionOperator>(TxId);
            if (op->WaitShardsBrokenFlags.contains(Self->TabletID())) {
                // TxStartPreparation may be executed AFTER all the ReadSets from secondary.
                // So, TxBroken may already be set, we must not ignore that.
                op->TxBroken = op->TxBroken.value_or(false) || lock.IsBroken();
                AFL_VERIFY(op->WaitShardsBrokenFlags.erase(Self->TabletID()));
                AFL_VERIFY(op->WaitShardsResultAck.erase(Self->TabletID()));
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "remove_tablet_id")("wait_broken_flags", JoinSeq(",", op->WaitShardsBrokenFlags))("wait_result_ack", JoinSeq(",", op->WaitShardsResultAck))("receive", Self->TabletID());
                Self->GetProgressTxController().WriteTxOperatorInfo(txc, TxId, op->SerializeToProto().SerializeAsString());
                op->InitializeRequests(*Self);
            }
            return true;
        }
        virtual void DoComplete(const NActors::TActorContext& /*ctx*/) override {
        }

    public:
        TTxStartPreparation(TColumnShard* owner, const ui64 txId)
            : TBase(owner, "start_preparation")
            , TxId(txId) {
        }
    };

    virtual void OnTimeout(TColumnShard& owner) override {
        InitializeRequests(owner);
    }

    virtual bool DoIsInProgress() const override {
        return WaitShardsBrokenFlags.size() || WaitShardsResultAck.size();
    }
    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> DoBuildTxPrepareForProgress(TColumnShard* owner) const override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "prepare_for_progress_started")("lock_id", LockId);
        return std::make_unique<TTxStartPreparation>(owner, GetTxId());
    }

public:
    using TBase::TBase;
    TEvWriteCommitPrimaryTransactionOperator(
        const TFullTxInfo& txInfo, const ui64 lockId, const std::set<ui64>& receivingShards, const std::set<ui64>& sendingShards)
        : TBase(txInfo, lockId)
        , ReceivingShards(receivingShards)
        , SendingShards(sendingShards) {
        WaitShardsBrokenFlags = SendingShards;
        WaitShardsResultAck = ReceivingShards;
    }
};

}   // namespace NKikimr::NColumnShard
