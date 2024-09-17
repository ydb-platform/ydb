#pragma once

#include "sync.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_sharing/common/transactions/tx_extension.h>

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
    mutable TAtomicCounter ControlCounter = 0;

    virtual NKikimrTxColumnShard::TCommitWriteTxBody SerializeToProto() const override {
        NKikimrTxColumnShard::TCommitWriteTxBody result;
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
        return result;
    }

    virtual bool DoParseImpl(TColumnShard& /*owner*/, const NKikimrTxColumnShard::TCommitWriteTxBody& commitTxBody) override {
        if (!commitTxBody.HasPrimaryTabletData()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot read proto")("proto", commitTxBody.DebugString());
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
        AFL_VERIFY(ReceivingShards.empty() == SendingShards.empty());
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
    class TTxWriteReceivedBrokenFlag: public NOlap::NDataSharing::TExtendedTransactionBase<TColumnShard> {
    private:
        using TBase = NOlap::NDataSharing::TExtendedTransactionBase<TColumnShard>;
        const ui64 TxId;
        const ui64 TabletId;
        const bool BrokenFlag;

        virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /*ctx*/) override {
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitPrimaryTransactionOperator>(TxId);
            auto copy = *op;
            if (copy.WaitShardsBrokenFlags.erase(TabletId)) {
                copy.TxBroken = copy.TxBroken.value_or(false) || BrokenFlag;
                Self->GetProgressTxController().WriteTxOperatorInfo(txc, TxId, copy.SerializeToProto().SerializeAsString());
            } else {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "repeated shard broken_flag info")("shard_id", TabletId);
            }
            return true;
        }
        virtual void DoComplete(const NActors::TActorContext& /*ctx*/) override {
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitPrimaryTransactionOperator>(TxId);
            if (op->WaitShardsBrokenFlags.erase(TabletId)) {
                op->TxBroken = op->TxBroken.value_or(false) || BrokenFlag;
                op->SendBrokenFlagAck(*Self, TabletId);
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "remove_tablet_id")("wait", JoinSeq(",", op->WaitShardsBrokenFlags))(
                    "receive", TabletId);
                op->InitializeRequests(*Self);
            } else {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "repeated shard broken_flag info")("shard_id", TabletId);
            }
        }

    public:
        TTxWriteReceivedBrokenFlag(TColumnShard& owner, const ui64 txId, const ui64 tabletId, const bool broken)
            : TBase(&owner, ::ToString(txId))
            , TxId(txId)
            , TabletId(tabletId)
            , BrokenFlag(broken) {
        }
    };

    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateReceiveBrokenFlagTx(
        TColumnShard& owner, const ui64 sendTabletId, const bool broken) const override {
        return std::make_unique<TTxWriteReceivedBrokenFlag>(owner, GetTxId(), sendTabletId, broken);
    }

    class TTxWriteReceivedResultAck: public NOlap::NDataSharing::TExtendedTransactionBase<TColumnShard> {
    private:
        using TBase = NOlap::NDataSharing::TExtendedTransactionBase<TColumnShard>;
        const ui64 TxId;
        const ui64 TabletId;

        virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /*ctx*/) override {
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitPrimaryTransactionOperator>(TxId);
            auto copy = *op;
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "ack_tablet")("wait", JoinSeq(",", op->WaitShardsResultAck))("receive", TabletId);
            AFL_VERIFY(copy.WaitShardsResultAck.erase(TabletId));
            Self->GetProgressTxController().WriteTxOperatorInfo(txc, TxId, copy.SerializeToProto().SerializeAsString());
            return true;
        }
        virtual void DoComplete(const NActors::TActorContext& /*ctx*/) override {
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitPrimaryTransactionOperator>(TxId);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "ack_tablet")("wait", JoinSeq(",", op->WaitShardsResultAck))(
                "receive", TabletId);
            if (!op->WaitShardsResultAck.erase(TabletId)) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "ack_tablet_duplication")("wait", JoinSeq(",", op->WaitShardsResultAck))(
                    "receive", TabletId);
            }
            op->CheckFinished(*Self);
        }

    public:
        TTxWriteReceivedResultAck(TColumnShard& owner, const ui64 txId, const ui64 tabletId)
            : TBase(&owner)
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
            WaitShardsResultAck.erase(owner.TabletID());
            if (WaitShardsResultAck.size()) {
                SendResult(owner);
            } else {
                CheckFinished(owner);
            }
        }
    }

    void CheckFinished(TColumnShard& owner) {
        if (WaitShardsResultAck.empty()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "finished");
            owner.EnqueueProgressTx(NActors::TActivationContext::AsActorContext(), GetTxId());
        }
    }

    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateReceiveResultAckTx(
        TColumnShard& owner, const ui64 recvTabletId) const override
    {
        return std::make_unique<TTxWriteReceivedResultAck>(owner, GetTxId(), recvTabletId);
    }

    void SendBrokenFlagAck(TColumnShard& owner, const std::optional<ui64> tabletId = {}) {
        for (auto&& i : SendingShards) {
            if (!WaitShardsBrokenFlags.contains(i)) {
                if (tabletId && *tabletId != i) {
                    continue;
                }
                owner.Send(MakePipePerNodeCacheID(EPipePerNodeCache::Persistent),
                    new TEvPipeCache::TEvForward(
                        new TEvTxProcessing::TEvReadSetAck(0, GetTxId(), owner.TabletID(), i, owner.TabletID(), 0), i, true),
                    IEventHandle::FlagTrackDelivery, GetTxId());
            }
        }
    }

    void SendResult(TColumnShard& owner) {
        AFL_VERIFY(!!TxBroken);
        NKikimrTx::TReadSetData readSetData;
        readSetData.SetDecision(*TxBroken ? NKikimrTx::TReadSetData::DECISION_ABORT : NKikimrTx::TReadSetData::DECISION_COMMIT);
        for (auto&& i : ReceivingShards) {
            if (WaitShardsResultAck.contains(i)) {
                owner.Send(MakePipePerNodeCacheID(EPipePerNodeCache::Persistent),
                    new TEvPipeCache::TEvForward(new TEvTxProcessing::TEvReadSet(TxInfo.PlanStep, GetTxId(), owner.TabletID(), i,
                                                     owner.TabletID(), readSetData.SerializeAsString()),
                        i, true),
                    IEventHandle::FlagTrackDelivery, GetTxId());
            }
        }
    }

    virtual void DoOnTabletInit(TColumnShard& owner) override {
        InitializeRequests(owner);
        CheckFinished(owner);
    }

    class TTxStartPreparation: public NOlap::NDataSharing::TExtendedTransactionBase<TColumnShard> {
    private:
        using TBase = NOlap::NDataSharing::TExtendedTransactionBase<TColumnShard>;
        const ui64 TxId;

        virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /*ctx*/) override {
            auto& lock = Self->GetOperationsManager().GetLockVerified(Self->GetOperationsManager().GetLockForTxVerified(TxId));
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitPrimaryTransactionOperator>(TxId);
            if (op->WaitShardsBrokenFlags.contains(Self->TabletID())) {
                auto copy = *op;
                copy.TxBroken = lock.IsBroken();
                AFL_VERIFY(copy.WaitShardsBrokenFlags.erase(Self->TabletID()));
                if (copy.WaitShardsBrokenFlags.empty()) {
                    AFL_VERIFY(copy.WaitShardsResultAck.erase(Self->TabletID()));
                }
                
                Self->GetProgressTxController().WriteTxOperatorInfo(txc, TxId, copy.SerializeToProto().SerializeAsString());
            }
            return true;
        }
        virtual void DoComplete(const NActors::TActorContext& /*ctx*/) override {
            auto& lock = Self->GetOperationsManager().GetLockVerified(Self->GetOperationsManager().GetLockForTxVerified(TxId));
            auto op = Self->GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitPrimaryTransactionOperator>(TxId);
            if (op->WaitShardsBrokenFlags.contains(Self->TabletID())) {
                op->TxBroken = lock.IsBroken();
                AFL_VERIFY(op->WaitShardsBrokenFlags.erase(Self->TabletID()));
                if (op->WaitShardsBrokenFlags.empty()) {
                    AFL_VERIFY(op->WaitShardsResultAck.erase(Self->TabletID()));
                }
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "remove_tablet_id")("wait", JoinSeq(",", op->WaitShardsBrokenFlags))(
                    "receive", Self->TabletID());
                op->CheckFinished(*Self);
            }
        }

    public:
        TTxStartPreparation(TColumnShard* owner, const ui64 txId)
            : TBase(owner)
            , TxId(txId) {
        }
    };

    virtual void OnTimeout(TColumnShard& owner) override {
        InitializeRequests(owner);
    }

    virtual std::unique_ptr<NTabletFlatExecutor::ITransaction> DoBuildTxPrepareForProgress(TColumnShard* owner) const override {
        if (WaitShardsResultAck.empty()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_prepare_for_progress")("lock_id", LockId);
            return nullptr;
        }
        AFL_VERIFY(ControlCounter.Inc() <= 1);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "prepare_for_progress_started")("lock_id", LockId);
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
