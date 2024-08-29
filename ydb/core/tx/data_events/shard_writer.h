#pragma once

#include "shards_splitter.h"
#include "common/modification_type.h"

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
    NMonitoring::THistogramPtr FullReplyDuration;
    NMonitoring::THistogramPtr BytesDistribution;
    NMonitoring::THistogramPtr RowsDistribution;
    NMonitoring::TDynamicCounters::TCounterPtr RowsCount;
    NMonitoring::TDynamicCounters::TCounterPtr BytesCount;
    NMonitoring::TDynamicCounters::TCounterPtr FailsCount;
public:
    TCSUploadCounters()
        : TBase("CSUpload")
        , RequestsCount(TBase::GetDeriviative("Requests"))
        , CSReplyDuration(TBase::GetHistogram("Replies/Shard/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 1)))
        , FullReplyDuration(TBase::GetHistogram("Replies/Full/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 1)))
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

    void OnFullReply(const TDuration d) const {
        FullReplyDuration->Collect(d.MilliSeconds());
    }
};
// External transaction controller class
class TWritersController {
private:
    TAtomicCounter WritesCount = 0;
    TAtomicCounter WritesIndex = 0;
    NActors::TActorIdentity LongTxActorId;
    std::vector<TWriteIdForShard> WriteIds;
    const TMonotonic StartInstant = TMonotonic::Now();
    YDB_READONLY_DEF(NLongTxService::TLongTxId, LongTxId);
    YDB_READONLY(std::shared_ptr<TCSUploadCounters>, Counters, std::make_shared<TCSUploadCounters>());
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

    TWritersController(const ui32 writesCount, const NActors::TActorIdentity& longTxActorId, const NLongTxService::TLongTxId& longTxId);
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
    const TString DedupId;
    const IShardInfo::TPtr DataForShard;
    ui32 NumRetries = 0;
    TWritersController::TPtr ExternalController;
    const TActorId LeaderPipeCache;
    NWilson::TProfileSpan ActorSpan;
    EModificationType ModificationType;

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
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ_SHARD_WRITER;
    }

    TShardWriter(const ui64 shardId, const ui64 tableId, const TString& dedupId, const IShardInfo::TPtr& data,
        const NWilson::TProfileSpan& parentSpan, TWritersController::TPtr externalController, const ui32 writePartIdx, const EModificationType mType);

    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWriteResult, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Bootstrap();

    void Handle(TEvWriteResult::TPtr& ev);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);
    void HandleTimeout(const TActorContext& ctx);
private:
    bool RetryWriteRequest(bool delayed = true);
};
}
