#include "shard_writer.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>


namespace NKikimr::NEvWrite {

    TWritersController::TWritersController(const ui32 writesCount, const NActors::TActorIdentity& longTxActorId, const NLongTxService::TLongTxId& longTxId, const bool immediateWrite)
        : WritesCount(writesCount)
        , LongTxActorId(longTxActorId)
        , ImmediateWrite(immediateWrite)
        , LongTxId(longTxId)
    {
        Y_ABORT_UNLESS(writesCount);
        WriteIds.resize(WritesCount.Val());
    }

    void TWritersController::OnSuccess(const ui64 shardId, const ui64 writeId, const ui32 writePartId) {
        WriteIds[WritesIndex.Inc() - 1] = TWriteIdForShard(shardId, writeId, writePartId);
        Counters->OnCSReply(TMonotonic::Now() - StartInstant);
        if (!WritesCount.Dec()) {
            SendReply();
        }
    }

    void TWritersController::OnFail(const Ydb::StatusIds::StatusCode code, const TString& message) {
        Counters->OnCSFailed(code);
        FailsCount.Inc();
        if (!Code) {
            TGuard<TMutex> g(Mutex);
            if (!Code) {
                Issues.AddIssue(message);
                Code = code;
            }
        }
        if (!WritesCount.Dec()) {
            SendReply();
        }
    }

    TShardWriter::TShardWriter(const ui64 shardId, const ui64 tableId, const ui64 schemaVersion, const TString& dedupId, const IShardInfo::TPtr& data,
        const NWilson::TProfileSpan& parentSpan, TWritersController::TPtr externalController, const ui32 writePartIdx, const EModificationType mType, const bool immediateWrite)
        : ShardId(shardId)
        , WritePartIdx(writePartIdx)
        , TableId(tableId)
        , SchemaVersion(schemaVersion)
        , DedupId(dedupId)
        , DataForShard(data)
        , ExternalController(externalController)
        , LeaderPipeCache(MakePipePerNodeCacheID(false))
        , ActorSpan(parentSpan.BuildChildrenSpan("ShardWriter"))
        , ModificationType(mType)
        , ImmediateWrite(immediateWrite)
    {
    }

    void TShardWriter::SendWriteRequest() {
        if (ImmediateWrite) {
            auto ev = MakeHolder<NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            DataForShard->Serialize(*ev, TableId, SchemaVersion);
            SendToTablet(std::move(ev));
        } else {
            auto ev = MakeHolder<TEvColumnShard::TEvWrite>(SelfId(), ExternalController->GetLongTxId(), TableId, DedupId, "", WritePartIdx, ModificationType);
            DataForShard->Serialize(*ev);
            SendToTablet(std::move(ev));
        }
    }

    void TShardWriter::Bootstrap() {
        SendWriteRequest();
        Become(&TShardWriter::StateMain);
    }

    void TShardWriter::Handle(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        const auto* msg = ev->Get();
        Y_ABORT_UNLESS(msg->Record.GetOrigin() == ShardId);

        const auto ydbStatus = msg->GetStatus();
        if (ydbStatus == NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED) {
            if (RetryWriteRequest(true)) {
                return;
            }
        }

        auto gPassAway = PassAwayGuard();
        if (ydbStatus != NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
            ExternalController->OnFail(Ydb::StatusIds::GENERIC_ERROR,
                TStringBuilder() << "Cannot write data into shard " << ShardId << " in longTx " <<
                ExternalController->GetLongTxId().ToString());
            return;
        }

        ExternalController->OnSuccess(ShardId, 0, WritePartIdx);
    }

    void TShardWriter::Handle(TEvColumnShard::TEvWriteResult::TPtr& ev) {
        const auto* msg = ev->Get();
        Y_ABORT_UNLESS(msg->Record.GetOrigin() == ShardId);

        const auto ydbStatus = msg->GetYdbStatus();
        if (ydbStatus == Ydb::StatusIds::OVERLOADED) {
            if (RetryWriteRequest(true)) {
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

        if (RetryWriteRequest(true)) {
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

    bool TShardWriter::RetryWriteRequest(const bool delayed) {
        if (NumRetries >= MaxRetriesPerShard) {
            return false;
        }
        if (delayed) {
            Schedule(OverloadTimeout(), new TEvents::TEvWakeup());
        } else {
            ++NumRetries;
            SendWriteRequest();
        }
        return true;
    }

}
