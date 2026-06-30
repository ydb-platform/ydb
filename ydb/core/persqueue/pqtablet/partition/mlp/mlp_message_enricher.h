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
        std::unique_ptr<TEvPQ::TEvMLPReadResponse> Response = std::make_unique<TEvPQ::TEvMLPReadResponse>();
        ui32 TotalMessages = 0;
        ui32 EnrichedCount = 0;
        bool Sent = false;

        bool IsComplete() const { return EnrichedCount == TotalMessages; }
    };

    void Handle(TEvPersQueue::TEvResponse::TPtr&);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);

    STFUNC(StateWork);

    void TrySendReplyIfComplete(size_t replyIndex);
    void SendPartialReply(size_t replyIndex);
    void TrySendReplyImpl(size_t replyIndex, bool waitForCompletion, bool allowEmpty);
    void ProcessQueue();
    void SendToPQTablet(std::unique_ptr<IEventBase> ev);

private:
    const ui64 TabletId;
    const ui32 PartitionId;
    const TString ConsumerName;
    std::deque<TReadResult> Replies;
    std::vector<TPendingResponse> PendingResponses;
    std::vector<TOffsetEntry> SortedEntries;
    size_t NextEntryIdx = 0;
    size_t RepliesSent = 0;

    bool FirstRequest = true;
};

} // namespace NKikimr::NPQ::NMLP
