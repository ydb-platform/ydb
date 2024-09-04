#include "shard_writer.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>


namespace NKikimr::NEvWrite {

    TWritersController::TWritersController(const ui32 writesCount, const NActors::TActorIdentity& longTxActorId, const NLongTxService::TLongTxId& longTxId)
        : WritesCount(writesCount)
        , LongTxActorId(longTxActorId)
        , LongTxId(longTxId)
    {
        Y_ABORT_UNLESS(writesCount);
        WriteIds.resize(WritesCount.Val());
    }

    void TWritersController::OnSuccess(const ui64 shardId, const ui64 writeId, const ui32 writePartId) {
        WriteIds[WritesIndex.Inc() - 1] = TWriteIdForShard(shardId, writeId, writePartId);
        Counters->OnCSReply(TMonotonic::Now() - StartInstant);
        if (!WritesCount.Dec()) {
            Counters->OnFullReply(TMonotonic::Now() - StartInstant);
            auto req = MakeHolder<NLongTxService::TEvLongTxService::TEvAttachColumnShardWrites>(LongTxId);
            for (auto&& i : WriteIds) {
                req->AddWrite(i.GetShardId(), i.GetWriteId());
            }
            LongTxActorId.Send(NLongTxService::MakeLongTxServiceID(LongTxActorId.NodeId()), req.Release());
        }
    }

    void TWritersController::OnFail(const Ydb::StatusIds::StatusCode code, const TString& message) {
        Counters->OnCSFailed(code);
        NYql::TIssues issues;
        issues.AddIssue(message);
        LongTxActorId.Send(LongTxActorId, new TEvPrivate::TEvShardsWriteResult(code, issues));
    }

    TShardWriter::TShardWriter(const ui64 shardId, const ui64 tableId, const TString& dedupId, const IShardInfo::TPtr& data,
        const NWilson::TProfileSpan& parentSpan, TWritersController::TPtr externalController, const ui32 writePartIdx, const EModificationType mType)
        : ShardId(shardId)
        , WritePartIdx(writePartIdx)
        , TableId(tableId)
        , DedupId(dedupId)
        , DataForShard(data)
        , ExternalController(externalController)
        , LeaderPipeCache(MakePipePerNodeCacheID(false))
        , ActorSpan(parentSpan.BuildChildrenSpan("ShardWriter"))
        , ModificationType(mType)
    {
    }

    void TShardWriter::Bootstrap() {
        auto ev = MakeHolder<TEvWrite>(SelfId(), ExternalController->GetLongTxId(), TableId, DedupId, "", WritePartIdx, ModificationType);
        DataForShard->Serialize(*ev);
        SendToTablet(std::move(ev));
        Become(&TShardWriter::StateMain);
    }

    void TShardWriter::Handle(TEvWriteResult::TPtr& ev) {
        const auto* msg = ev->Get();
        Y_ABORT_UNLESS(msg->Record.GetOrigin() == ShardId);

        const auto ydbStatus = msg->GetYdbStatus();
        if (ydbStatus == Ydb::StatusIds::OVERLOADED) {
            if (RetryWriteRequest()) {
                return;
            }
        }

        auto gPassAway = PassAwayGuard();
        if (ydbStatus != Ydb::StatusIds::SUCCESS) {
            ExternalController->OnFail(ydbStatus,
                TStringBuilder() << "Cannot write data into shard " << ShardId << " in longTx " <<
                ExternalController->GetLongTxId().ToString());
            return;
        }

        ExternalController->OnSuccess(ShardId, msg->Record.GetWriteId(), WritePartIdx);
    }

    void TShardWriter::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        NWilson::TProfileSpan pSpan(0, ActorSpan.GetTraceId(), "DeliveryProblem");
        const auto* msg = ev->Get();
        Y_ABORT_UNLESS(msg->TabletId == ShardId);

        if (RetryWriteRequest()) {
            return;
        }

        auto gPassAway = PassAwayGuard();

        const TString errMsg = TStringBuilder() << "Shard " << ShardId << " is not available after " << NumRetries << " retries";
        if (msg->NotDelivered) {
            ExternalController->OnFail(Ydb::StatusIds::UNAVAILABLE, errMsg);
        } else {
            ExternalController->OnFail(Ydb::StatusIds::UNDETERMINED, errMsg);
        }
    }
    
    void TShardWriter::HandleTimeout(const TActorContext& /*ctx*/) {
        RetryWriteRequest(false);
    }

    bool TShardWriter::RetryWriteRequest(bool delayed) {
        if (NumRetries >= MaxRetriesPerShard) {
            return false;
        }
        if (delayed) {
            Schedule(OverloadTimeout(), new TEvents::TEvWakeup());
        } else {
            ++NumRetries;
            auto ev = MakeHolder<TEvWrite>(SelfId(), ExternalController->GetLongTxId(), TableId, DedupId, "", WritePartIdx, ModificationType);
            DataForShard->Serialize(*ev);
            SendToTablet(std::move(ev));
        }
        return true;
    }

}
