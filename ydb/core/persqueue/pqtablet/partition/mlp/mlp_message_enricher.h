#pragma once

#include "mlp.h"
#include "mlp_common.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

// A read that never returns must not leave the consumer waiting on this actor.
inline constexpr TDuration MessageEnricherDeadline = TDuration::Seconds(5);
inline constexpr ui64 MessageEnricherDeadlineWakeupTag = 100;

class TMessageEnricherActor : public TBaseActor<TMessageEnricherActor>
                            , public TConstantLogPrefix {

public:
    TMessageEnricherActor(ui64 tabletId,
                          const ui32 partitionId,
                          const TString& consumerName,
                          std::deque<TReadResult>&& replies,
                          const NActors::TActorId& parentActorId);

    void Bootstrap();
    void PassAway() override;

    TStructuredMessage BuildLogPrefix() const override {
        return YDB_LOG_CREATE_MESSAGE(
            {"partition", PartitionId},
            {"consumer", ConsumerName});
    }

    const ui64 TabletId;

private:
    const ui32 PartitionId;
    const TString ConsumerName;
    const NActors::TActorId ParentActorId;
    struct TOffsetEntry {
        ui64 Offset;
        size_t ReplyIndex;
        size_t MessageIndex;
        bool Processed = false;
    };

    struct TPendingResponse {
        std::unique_ptr<TEvPQ::TEvMLPReadResponse> Response = std::make_unique<TEvPQ::TEvMLPReadResponse>();
        ui32 TotalMessages = 0;
        ui32 EnrichedCount = 0;
        bool Sent = false;

        bool IsComplete() const { return EnrichedCount == TotalMessages; }
    };

    void Handle(TEvPersQueue::TEvResponse::TPtr&);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    void Handle(TEvents::TEvUndelivered::TPtr&);
    void Handle(TEvents::TEvWakeup::TPtr&);

    STFUNC(StateWork);

    void Complete(Ydb::StatusIds::StatusCode status, const TString& message);
    void TrySendReplyIfComplete(size_t replyIndex);
    void SendPartialReply(size_t replyIndex);
    void TrySendReplyImpl(size_t replyIndex, bool waitForCompletion);
    void MarkEntryMissing(TOffsetEntry& entry);
    void ProcessQueue();
    void SendToPQTablet(std::unique_ptr<IEventBase> ev);

    std::deque<TReadResult> Replies;
    std::vector<TPendingResponse> PendingResponses;
    std::vector<TOffsetEntry> SortedEntries;
    size_t NextEntryIdx = 0;
    size_t RepliesSent = 0;
    bool Completed = false;

    bool FirstRequest = true;
};

} // namespace NKikimr::NPQ::NMLP
