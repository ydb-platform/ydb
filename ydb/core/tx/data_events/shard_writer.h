#pragma once

#include "shards_splitter.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/public/api/grpc/draft/ydb_long_tx_v1.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/wilson/wilson_profile_span.h>


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
        , WritePartId(writePartId)
    {
    }
};    

// External transaction controller class
class TWritersController {
private:
    TAtomicCounter WritesCount = 0;
    TAtomicCounter WritesIndex = 0;
    NActors::TActorIdentity LongTxActorId;
    std::vector<TWriteIdForShard> WriteIds;
    YDB_READONLY_DEF(NLongTxService::TLongTxId, LongTxId);
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
        const NWilson::TProfileSpan& parentSpan, TWritersController::TPtr externalController, const ui32 writePartIdx);

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
