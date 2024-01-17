#pragma once
#include "defs.h"
#include "events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/map.h>

namespace NKikimr::NSQS {

class TPurgeActor : public TActorBootstrapped<TPurgeActor> {
    struct TShard {
        TInstant TargetBoundary = TInstant::Zero(); // Target state
        TInstant BoundaryPurged = TInstant::Zero(); // Current state in database
        bool Purging = false;
        bool KeysTruncated = false;

        struct TMessageBoundary {
            ui64 Offset = 0;
            TInstant SentTimestamp = TInstant::Zero();
        };
        TMessageBoundary CurrentLastMessage;
        TMessageBoundary PreviousSuccessfullyProcessedLastMessage;
    };

public:
    TPurgeActor(
        const TQueuePath& queuePath,
        ui32 tablesFormat,
        TIntrusivePtr<TQueueCounters> counters,
        const TActorId& queueLeader,
        bool isFifo
    );

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_PURGE_ACTOR;
    }

private:
    void MakeGetRetentionOffsetRequest(const ui64 shardId, TShard* shard);
    void MakeStage1Request(const ui64 shardId, TShard* shard, const std::pair<ui64, ui64>& offsets);
    void MakeStage2Request(ui64 cleanupVersion, const NClient::TValue& messages, const TMaybe<NClient::TValue>& inflyMessages, const ui64 shardId, TShard* shard);

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvPurgeQueue, HandlePurgeQueue);
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
            hFunc(TEvPoisonPill, HandlePoisonPill);
        }
    }

    void HandlePurgeQueue(TSqsEvents::TEvPurgeQueue::TPtr& ev);
    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);
    void HandlePoisonPill(TEvPoisonPill::TPtr&);

private:
    const TQueuePath QueuePath_;
    const ui32 TablesFormat_;
    /// A state of shard processing
    TMap<ui64, TShard> Shards_;
    const TString RequestId_;
    TIntrusivePtr<TQueueCounters> Counters_;
    TIntrusivePtr<NMonitoring::TCounterForPtr> PurgedMessagesCounter_;
    const TActorId QueueLeader_;
    const bool IsFifo_;
};

} // namespace NKikimr::NSQS
