#pragma once

#include "mlp.h"
#include "mlp_common.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

class TMessageEnricherActor : public TBaseActor<TMessageEnricherActor>
                            , public TConstantLogPrefix {

public:
    TMessageEnricherActor(ui64 tabletId,
                          const ui32 partitionId,
                          const TString& consumerName,
                          std::deque<TReadResult>&& replies);

    void Bootstrap();
    void PassAway() override;

private:
    struct TOffsetEntry {
        ui64 Offset;
        size_t ReplyIndex;
        size_t MessageIndex;
        bool Processed = false;
    };

    struct TPendingResponse {
        NActors::TActorId Sender;
        ui64 Cookie;
        std::unique_ptr<TEvPQ::TEvMLPReadResponse> Response;
        size_t TotalMessages = 0;
        size_t EnrichedCount = 0;
        bool Sent = false;

        bool IsComplete() const { return EnrichedCount == TotalMessages; }
    };

    void Handle(TEvPersQueue::TEvResponse::TPtr&);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);

    STFUNC(StateWork);

    void BuildSortedIndex();
    void EnsureResponse(size_t replyIndex);
    void TrySendReply(size_t replyIndex);
    void ProcessQueue();
    void SendToPQTablet(std::unique_ptr<IEventBase> ev);

private:
    const ui64 TabletId;
    const ui32 PartitionId;
    const TString ConsumerName;
    std::vector<TReadResult> Replies;
    std::vector<TPendingResponse> PendingResponses;
    std::vector<TOffsetEntry> SortedEntries;
    size_t NextEntryIdx = 0;
    size_t RepliesSent = 0;

    bool FirstRequest = true;
};

} // namespace NKikimr::NPQ::NMLP
