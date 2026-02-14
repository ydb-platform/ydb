#include "shard_writer.h"
#include "common/error_codes.h"

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
            SendReply();
        }
    }

    NO_SANITIZE_THREAD
    void TWritersController::OnFail(const Ydb::StatusIds::StatusCode code, const TString& message) {
        Counters->OnCSFailed(code);
        FailsCount.Inc();
        if (AtomicCas(&HasCodeFail, 1, 0)) {
            AFL_VERIFY(!Code);
            Issues.AddIssue(message);
            Code = code;
        }
        if (!WritesCount.Dec()) {
            SendReply();
        }
    }

    TShardWriter::TShardWriter(const ui64 shardId, const ui64 tableId, const ui64 schemaVersion, const TString& dedupId, const IShardInfo::TPtr& data,
        const NWilson::TProfileSpan& parentSpan, TWritersController::TPtr externalController, const ui32 writePartIdx,
        const std::optional<TDuration> timeout, const TString& userSID)
        : ShardId(shardId)
        , WritePartIdx(writePartIdx)
        , TableId(tableId)
        , SchemaVersion(schemaVersion)
        , DedupId(dedupId)
        , DataForShard(data)
        , ExternalController(externalController)
        , LeaderPipeCache(MakePipePerNodeCacheID(false))
        , ActorSpan(parentSpan.BuildChildrenSpan("ShardWriter"))
        , Timeout(timeout)
        , RetryBySubscription(AppData()->FeatureFlags.GetEnableCsOverloadsSubscriptionRetries())
        , UserSID(userSID) {
    }

    void TShardWriter::SendWriteRequest() {
        auto ev = MakeHolder<NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        ev->SetUserSID(UserSID);
        DataForShard->Serialize(*ev, TableId, SchemaVersion);
        if (Timeout) {
            ev->Record.SetTimeoutSeconds(Timeout->Seconds());
        }
        if (RetryBySubscription) {
            ev->Record.SetOverloadSubscribe(++LastOverloadSeqNo);
        }
        SendToTablet(std::move(ev));
    }

    void TShardWriter::Bootstrap() {
        SendWriteRequest();
        if (Timeout) {
            Schedule(*Timeout, new TEvents::TEvWakeup(1));
        }
        Become(&TShardWriter::StateMain);
    }

    void TShardWriter::Handle(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        const auto* msg = ev->Get();
        Y_ABORT_UNLESS(msg->Record.GetOrigin() == ShardId);

        const auto ydbStatus = msg->GetStatus();
        if (ydbStatus == NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED) {
            if (RetryBySubscription) {
                if (msg->Record.HasOverloadSubscribed() && msg->Record.GetOverloadSubscribed() == LastOverloadSeqNo && !IsMaxRetriesReached()) {
                    return;
                }
            } else if (RetryWriteRequest(true)) {
                return;
            }
        }

        LastOverloadSeqNo = 0;

        auto gPassAway = PassAwayGuard();
        if (ydbStatus != NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
            auto statusInfo = NEvWrite::NErrorCodes::TOperator::GetStatusInfo(ydbStatus).DetachResult();
            const auto issues = msg->Record.GetIssues();
            const TString issueString = issues.empty() ? "unspecified error" : issues[0].message();
            ExternalController->OnFail(statusInfo.GetYdbStatusCode(),
                TStringBuilder() << "Cannot write data into shard(" << statusInfo.GetIssueGeneralText() << ": " << issueString << ") " << ShardId
                                 << " in longTx " << ExternalController->GetLongTxId().ToString());
            return;
        }

        ExternalController->OnSuccess(ShardId, 0, WritePartIdx);
    }

    void TShardWriter::Handle(TEvColumnShard::TEvOverloadReady::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        AFL_VERIFY(RetryBySubscription);
        AFL_VERIFY(record.GetSeqNo() <= LastOverloadSeqNo)("event_seq_no", record.GetSeqNo())("last_overload_seq_no", LastOverloadSeqNo);
        AFL_VERIFY(record.GetTabletID() == ShardId)("ev_tablet_id", record.GetTabletID())("shard_id", ShardId);
        if (record.GetSeqNo() != LastOverloadSeqNo) {
            return;
        }

        if (!RetryWriteRequest(false)) {
            auto gPassAway = PassAwayGuard();
            const TString errMsg = TStringBuilder() << "Shard " << ShardId << " is still overloaded after " << NumRetries << " retries";
            ExternalController->OnFail(Ydb::StatusIds::OVERLOADED, errMsg);
            ExternalController->GetCounters()->OnRetryBySubscribeOnOverloadLimitExceeded();
            LastOverloadSeqNo = 0;
        } else {
            ExternalController->GetCounters()->OnRetryBySubscribeOnOverload();
        }
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

        LastOverloadSeqNo = 0;
    }

    void TShardWriter::Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag) {
            auto gPassAway = PassAwayGuard();
            ExternalController->OnFail(Ydb::StatusIds::TIMEOUT, TStringBuilder()
                                                                    << "Cannot write data (TIMEOUT) into shard " << ShardId << " in longTx "
                                                                    << ExternalController->GetLongTxId().ToString());
            ExternalController->GetCounters()->OnGlobalTimeout();
        } else {
            ExternalController->GetCounters()->OnRetryTimeout();
            RetryWriteRequest(false);
        }
    }

    bool TShardWriter::RetryWriteRequest(const bool delayed) {
        if (IsMaxRetriesReached()) {
            return false;
        }
        if (delayed) {
            Schedule(OverloadTimeout(), new TEvents::TEvWakeup(0));
        } else {
            ++NumRetries;
            SendWriteRequest();
        }
        return true;
    }

    bool TShardWriter::IsMaxRetriesReached() const {
        return NumRetries >= GetMaxRetriesPerShard();
    }
    
    ui32 TShardWriter::GetMaxRetriesPerShard() const {
        return AppData() ? AppData()->ColumnShardConfig.GetProxyMaxRetriesPerShard() : MaxRetriesPerShard;
    }

    void TShardWriter::PassAway() {
        if (RetryBySubscription && LastOverloadSeqNo) {
            SendToTablet(MakeHolder<TEvColumnShard::TEvOverloadUnsubscribe>(LastOverloadSeqNo));
            LastOverloadSeqNo = 0;
        }

        Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));

        TBase::PassAway();
    }
}
