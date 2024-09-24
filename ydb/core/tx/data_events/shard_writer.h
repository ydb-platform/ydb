#pragma once

#include "common/modification_type.h"
#include "events.h"
#include "shards_splitter.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/wilson/wilson_profile_span.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>


namespace NKikimr::NEvWrite {

class TWriteIdForShard {
private:
    YDB_READONLY(ui64, ShardId, 0);
    YDB_READONLY(ui64, WriteId, 0);
    YDB_READONLY(ui64, WritePartId, 0);
public:
    TWriteIdForShard() = default;
    TWriteIdForShard(const ui64 shardId, const ui64 writeId, const ui32 writePartId)
        : ShardId(shardId)
        , WriteId(writeId)
        , WritePartId(writePartId) {
    }
};

class TCSUploadCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;
    NMonitoring::THistogramPtr CSReplyDuration;
    NMonitoring::THistogramPtr SucceedFullReplyDuration;
    NMonitoring::THistogramPtr FailedFullReplyDuration;
    NMonitoring::THistogramPtr BytesDistribution;
    NMonitoring::THistogramPtr RowsDistribution;
    NMonitoring::TDynamicCounters::TCounterPtr RowsCount;
    NMonitoring::TDynamicCounters::TCounterPtr BytesCount;
    NMonitoring::TDynamicCounters::TCounterPtr FailsCount;
public:
    TCSUploadCounters()
        : TBase("CSUpload")
        , RequestsCount(TBase::GetDeriviative("Requests"))
        , CSReplyDuration(TBase::GetHistogram("Replies/Shard/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 10)))
        , SucceedFullReplyDuration(TBase::GetHistogram("Replies/Success/Full/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 10)))
        , FailedFullReplyDuration(TBase::GetHistogram("Replies/Failed/Full/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 10)))
        , BytesDistribution(TBase::GetHistogram("Requests/Bytes", NMonitoring::ExponentialHistogram(15, 2, 1024)))
        , RowsDistribution(TBase::GetHistogram("Requests/Rows", NMonitoring::ExponentialHistogram(15, 2, 16)))
        , RowsCount(TBase::GetDeriviative("Rows"))
        , BytesCount(TBase::GetDeriviative("Bytes"))
        , FailsCount(TBase::GetDeriviative("Fails")) {

    }

    void OnRequest(const ui64 rows, const ui64 bytes) const {
        BytesDistribution->Collect(bytes);
        RowsDistribution->Collect(rows);
        BytesCount->Add(bytes);
        RowsCount->Add(rows);
    }

    void OnCSFailed(const Ydb::StatusIds::StatusCode /*code*/) {
        FailsCount->Add(1);
    }

    void OnCSReply(const TDuration d) const {
        CSReplyDuration->Collect(d.MilliSeconds());
    }

    void OnSucceedFullReply(const TDuration d) const {
        SucceedFullReplyDuration->Collect(d.MilliSeconds());
    }

    void OnFailedFullReply(const TDuration d) const {
        FailedFullReplyDuration->Collect(d.MilliSeconds());
    }
};
// External transaction controller class
class TWritersController {
private:
    TAtomicCounter WritesCount = 0;
    TAtomicCounter WritesIndex = 0;
    TAtomicCounter FailsCount = 0;
    TMutex Mutex;
    NYql::TIssues Issues;
    std::optional<Ydb::StatusIds::StatusCode> Code;
    NActors::TActorIdentity LongTxActorId;
    std::vector<TWriteIdForShard> WriteIds;
    const TMonotonic StartInstant = TMonotonic::Now();
    const bool ImmediateWrite = false;
    YDB_READONLY_DEF(NLongTxService::TLongTxId, LongTxId);
    YDB_READONLY(std::shared_ptr<TCSUploadCounters>, Counters, std::make_shared<TCSUploadCounters>());
    void SendReply() {
        if (FailsCount.Val()) {
            Counters->OnFailedFullReply(TMonotonic::Now() - StartInstant);
            AFL_VERIFY(Code);
            LongTxActorId.Send(LongTxActorId, new TEvPrivate::TEvShardsWriteResult(*Code, Issues));
        } else if (ImmediateWrite) {
            Counters->OnSucceedFullReply(TMonotonic::Now() - StartInstant);
            LongTxActorId.Send(LongTxActorId, new TEvPrivate::TEvShardsWriteResult(Ydb::StatusIds::SUCCESS));
        } else {
            Counters->OnSucceedFullReply(TMonotonic::Now() - StartInstant);
            auto req = MakeHolder<NLongTxService::TEvLongTxService::TEvAttachColumnShardWrites>(LongTxId);
            for (auto&& i : WriteIds) {
                req->AddWrite(i.GetShardId(), i.GetWriteId());
            }
            LongTxActorId.Send(NLongTxService::MakeLongTxServiceID(LongTxActorId.NodeId()), req.Release());
        }
    }
public:
    using TPtr = std::shared_ptr<TWritersController>;

    struct TEvPrivate {
        enum EEv {
            EvShardsWriteResult = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expected EvEnd < EventSpaceEnd");

        struct TEvShardsWriteResult: TEventLocal<TEvShardsWriteResult, EvShardsWriteResult> {
            TEvShardsWriteResult() = default;
            Ydb::StatusIds::StatusCode Status;
            const NYql::TIssues Issues;

            explicit TEvShardsWriteResult(Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const NYql::TIssues& issues = {})
                : Status(status)
                , Issues(issues) {
            }
        };

    };

    TWritersController(const ui32 writesCount, const NActors::TActorIdentity& longTxActorId, const NLongTxService::TLongTxId& longTxId, const bool immediateWrite);
    void OnSuccess(const ui64 shardId, const ui64 writeId, const ui32 writePartId);
    void OnFail(const Ydb::StatusIds::StatusCode code, const TString& message);
};

class TShardWriter: public NActors::TActorBootstrapped<TShardWriter> {
private:
    using TBase = NActors::TActorBootstrapped<TShardWriter>;
    using IShardInfo = IShardsSplitter::IShardInfo;
    static const constexpr ui32 MaxRetriesPerShard = 10;
    static const constexpr ui32 OverloadedDelayMs = 200;

    const ui64 ShardId;
    const ui64 WritePartIdx;
    const ui64 TableId;
    const ui64 SchemaVersion;
    const TString DedupId;
    const IShardInfo::TPtr DataForShard;
    ui32 NumRetries = 0;
    TWritersController::TPtr ExternalController;
    const TActorId LeaderPipeCache;
    NWilson::TProfileSpan ActorSpan;
    EModificationType ModificationType;
    const bool ImmediateWrite = false;

    void SendWriteRequest();
    static TDuration OverloadTimeout() {
        return TDuration::MilliSeconds(OverloadedDelayMs);
    }
    void SendToTablet(THolder<IEventBase> event) {
        Send(LeaderPipeCache, new TEvPipeCache::TEvForward(event.Release(), ShardId, true),
            IEventHandle::FlagTrackDelivery, 0, ActorSpan.GetTraceId());
    }
    virtual void PassAway() override {
        Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }
public:
    TShardWriter(const ui64 shardId, const ui64 tableId, const ui64 schemaVersion, const TString& dedupId, const IShardInfo::TPtr& data,
        const NWilson::TProfileSpan& parentSpan, TWritersController::TPtr externalController, const ui32 writePartIdx,
        const EModificationType mType, const bool immediateWrite);

    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvColumnShard::TEvWriteResult, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(NEvents::TDataEvents::TEvWriteResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Bootstrap();

    void Handle(TEvColumnShard::TEvWriteResult::TPtr& ev);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);
    void Handle(NEvents::TDataEvents::TEvWriteResult::TPtr& ev);
    void HandleTimeout(const TActorContext& ctx);
private:
    bool RetryWriteRequest(const bool delayed = true);
};
}
