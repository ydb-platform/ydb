#include "ev_write.h"
#include <ydb/core/protos/tx.pb.h>
#include <ydb/core/tx/columnshard/transactions/transactions/tx_finish_async.h>

namespace NKikimr::NColumnShard {

bool TEvWriteTransactionOperator::DoParse(TColumnShard& /*owner*/, const TString& data) {
    NKikimrTxColumnShard::TCommitWriteTxBody commitTxBody;
    if (!commitTxBody.ParseFromString(data)) {
        return false;
    }
    LockId = commitTxBody.GetLockId();
    for (auto&& i : commitTxBody.GetReceivingShards()) {
        ReceivingShards.emplace(i);
    }
    for (auto&& i : commitTxBody.GetSendingShards()) {
        SendingShards.emplace(i);
    }
    AFL_VERIFY(ReceivingShards.empty() == SendingShards.empty());
    AFL_VERIFY(!!LockId);
    Broken = commitTxBody.GetBroken();

    return true;
}

void TEvWriteTransactionOperator::Send(TColumnShard& owner, const std::optional<ui64>& destTabletId) {
    if (!Started) {
        return;
    }
    AFL_VERIFY(SendingShards.contains(owner.TabletID()));
    if (destTabletId) {
        AFL_VERIFY(ReceivingShards.contains(*destTabletId));
    }
    NKikimrTx::TReadSetData readSetData;
    readSetData.SetDecision(Broken ? NKikimrTx::TReadSetData::DECISION_ABORT : NKikimrTx::TReadSetData::DECISION_COMMIT);
    const TString readSetDataString = readSetData.SerializeAsString();
    if (destTabletId) {
        NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(
                new TEvTxProcessing::TEvReadSet(0, GetTxId(), owner.TabletID(), *destTabletId, owner.TabletID(), readSetDataString),
                *destTabletId,
                true),
            IEventHandle::FlagTrackDelivery, GetTxId());
    } else {
        for (auto&& recShardId : ReceivingShards) {
            NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(
                    new TEvTxProcessing::TEvReadSet(0, GetTxId(), owner.TabletID(), recShardId, owner.TabletID(), readSetDataString), recShardId,
                    true),
                IEventHandle::FlagTrackDelivery, GetTxId());
        }
    }
}

void TEvWriteTransactionOperator::Ask(TColumnShard& owner) {
    AFL_VERIFY(Started && !Finished)("start", Started)("finish", Finished);
    AFL_VERIFY(ReceivingShards.contains(owner.TabletID()));
    for (auto&& sendShardId : WaitShards) {
        NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(new TEvTxProcessing::TEvReadSetAsk(GetTxId(), owner.TabletID()), sendShardId, true),
            IEventHandle::FlagTrackDelivery, GetTxId());
    }
}

void TEvWriteTransactionOperator::Finish(TColumnShard& owner) {
    AFL_VERIFY(Started && !Finished)("start", Started)("finish", Finished);
    Finished = true;
    owner.Execute(new TTxFinishAsyncTransaction(owner, GetTxId()));
}

void TEvWriteTransactionOperator::Start(TColumnShard& owner) {
    AFL_VERIFY(!Started && !Finished);
    Started = true;
    ResultBroken = Broken;
    if (ReceivingShards.contains(owner.TabletID())) {
        WaitShards = SendingShards;
        Ask(owner);
    }
    if (SendingShards.contains(owner.TabletID())) {
        Send(owner);
    }
    if (WaitShards.empty()) {
        Finish(owner);
    }
}

void TEvWriteTransactionOperator::Receive(TColumnShard& owner, const ui64 tabletId, const bool broken) {
    if (Finished) {
        return;
    }
    AFL_VERIFY(Started && !Finished)("start", Started)("finish", Finished);
    AFL_VERIFY(ResultBroken);
    ResultBroken = *ResultBroken || broken;
    AFL_VERIFY(SendingShards.contains(tabletId));
    if (WaitShards.erase(tabletId)  && WaitShards.empty()) {
        Finish(owner);
    }
}

}   // namespace NKikimr::NColumnShard
