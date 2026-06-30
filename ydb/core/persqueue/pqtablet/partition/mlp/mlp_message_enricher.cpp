#include "mlp_message_enricher.h"

#include <ydb/core/protos/pqdata_mlp.pb.h>

#include <algorithm>

namespace NKikimr::NPQ::NMLP {

TMessageEnricherActor::TMessageEnricherActor(ui64 tabletId, ui32 partitionId, const TString& consumerName, std::deque<TReadResult>&& replies)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_ENRICHER)
    , TabletId(tabletId)
    , PartitionId(partitionId)
    , ConsumerName(consumerName)
    , Replies(std::move(replies))
{
    size_t messagesCount = 0;
    PendingResponses.resize(Replies.size());
    for (size_t i = 0; i < Replies.size(); ++i) {
        auto& pr = PendingResponses[i];
        pr.TotalMessages = Replies[i].Messages.size();
        pr.EnrichedCount = 0;
        pr.Sent = false;

        messagesCount += pr.TotalMessages;
    }

    SortedEntries.reserve(messagesCount);
    for (size_t replyIndex = 0; replyIndex < Replies.size(); ++replyIndex) {
        auto& msgs = Replies[replyIndex].Messages;
        for (size_t messageIndex = 0; messageIndex < msgs.size(); ++messageIndex) {
            SortedEntries.push_back({msgs[messageIndex].Offset, replyIndex, messageIndex, false});
        }
    }
    SortBy(SortedEntries, [](const TOffsetEntry& item) { return item.Offset; });
}

void TMessageEnricherActor::Bootstrap() {
    Become(&TThis::StateWork);

    for (size_t i = 0; i < PendingResponses.size(); ++i) {
        TrySendReplyImpl(i, true, true); // send empty results now
    }

    ProcessQueue();
}

void TMessageEnricherActor::PassAway() {
    LOG_D("PassAway");
    for (size_t i = 0; i < PendingResponses.size(); ++i) {
        if (!PendingResponses[i].Sent) {
            const TReadResult& reply = Replies[i];
            Send(reply.Sender, new TEvPQ::TEvMLPErrorResponse(PartitionId, Ydb::StatusIds::SCHEME_ERROR, "Shutdown"), 0, reply.Cookie);
            PendingResponses[i].Sent = true;
            ++RepliesSent;
        }
    }
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
    TBase::PassAway();
}

void TMessageEnricherActor::TrySendReplyImpl(size_t replyIndex, bool waitForCompletion, bool allowEmpty) {
    auto& pr = PendingResponses[replyIndex];
    if (pr.Sent) {
        return;
    }
    if (!pr.IsComplete() && waitForCompletion) {
        return;
    }
    const TReadResult& reply = Replies[replyIndex];
    if (pr.EnrichedCount > 0 || allowEmpty) {
        Send(reply.Sender, pr.Response.release(), 0, reply.Cookie);
    } else {
        Send(reply.Sender, std::make_unique<TEvPQ::TEvMLPErrorResponse>(PartitionId, Ydb::StatusIds::INTERNAL_ERROR, "Messages were not found").release(), 0, reply.Cookie);
    }
    pr.Sent = true;
    ++RepliesSent;
}

void TMessageEnricherActor::TrySendReplyIfComplete(size_t replyIndex) {
    return TrySendReplyImpl(replyIndex, true, false);
}

void TMessageEnricherActor::SendPartialReply(size_t replyIndex) {
    return TrySendReplyImpl(replyIndex, false, false);
}

void TMessageEnricherActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvResponse");

    if (!IsSucess(ev)) {
        LOG_W("Fetch messages failed: " << ev->Get()->Record.DebugString());
        return PassAway();
    }

    auto& response = ev->Get()->Record;
    if (!response.GetPartitionResponse().HasCmdReadResult()) {
        ProcessQueue();
        return;
    }

    auto& results = response.GetPartitionResponse().GetCmdReadResult().GetResult();
    size_t& entryIndex = NextEntryIdx;
    int resultIndex = 0;
    int resultsSize = results.size();
    ui64 maxReturnedOffset = 0;

    if (resultsSize > 0) {
        maxReturnedOffset = results[resultsSize - 1].GetOffset();
    }

    // Two-pointer: both SortedEntries and results are sorted by offset
    while (entryIndex < SortedEntries.size() && resultIndex < resultsSize) {
        auto& entry = SortedEntries[entryIndex];
        if (entry.Processed) {
            ++entryIndex;
            continue;
        }

        auto resultOffset = results[resultIndex].GetOffset();

        if (entry.Offset < resultOffset) {
            entry.Processed = true;
            --PendingResponses[entry.ReplyIndex].TotalMessages;
            TrySendReplyIfComplete(entry.ReplyIndex);
            ++entryIndex;
        } else if (entry.Offset > resultOffset) {
            // unneeded Offset — advance result pointer
            ++resultIndex;
        } else if (PendingResponses[entry.ReplyIndex].Sent) {
            Y_VERIFY_DEBUG(false, "Response already processed");
            entry.Processed = true;
            ++entryIndex;
        } else {
            auto& result = results[resultIndex];
            auto& msg = Replies[entry.ReplyIndex].Messages[entry.MessageIndex];

            TEvPQ::TEvMLPReadResponse* readResponse = PendingResponses[entry.ReplyIndex].Response.get();
            auto* message = readResponse->Record.AddMessage();
            message->MutableId()->SetPartitionId(PartitionId);
            message->MutableId()->SetOffset(entry.Offset);
            message->SetData(result.GetData());
            message->MutableMessageMeta()->SetMessageGroupId(result.GetSourceId());
            message->MutableMessageMeta()->SetSentTimestampMilliseconds(result.GetWriteTimestampMS());
            message->MutableMessageMeta()->SetApproximateReceiveCount(msg.ApproximateReceiveCount);
            message->MutableMessageMeta()->SetApproximateFirstReceiveTimestampMilliseconds(msg.ApproximateFirstReceiveTimestamp.MilliSeconds());

            entry.Processed = true;
            ++PendingResponses[entry.ReplyIndex].EnrichedCount;
            TrySendReplyIfComplete(entry.ReplyIndex);
            ++entryIndex;

            // Don't advance resultIndex yet — there may be more entries with the same offset (duplicates across replies)
            if (entryIndex < SortedEntries.size() && SortedEntries[entryIndex].Offset == resultOffset) [[unlikely]] {
                continue;
            }
            ++resultIndex;
        }
    }

    // Mark remaining entries whose offsets are <= maxReturnedOffset as missing
    if (resultsSize > 0) {
        while (entryIndex < SortedEntries.size() && SortedEntries[entryIndex].Offset <= maxReturnedOffset) {
            auto& entry = SortedEntries[entryIndex];
            if (!entry.Processed) {
                entry.Processed = true;
                --PendingResponses[entry.ReplyIndex].TotalMessages;
                TrySendReplyIfComplete(entry.ReplyIndex);
            }
            ++entryIndex;
        }
    }

    if (RepliesSent == PendingResponses.size()) {
        return PassAway();
    }

    ProcessQueue();
}

void TMessageEnricherActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
    LOG_D("Handle TEvPipeCache::TEvDeliveryProblem");
    PassAway();
}

STFUNC(TMessageEnricherActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E("Unexpected " << EventStr("StateWork", ev));
    }
}

void TMessageEnricherActor::ProcessQueue() {
    if (NextEntryIdx >= SortedEntries.size()) {
        // All entries processed — send any remaining unsent replies
        for (size_t i = 0; i < PendingResponses.size(); ++i) {
            SendPartialReply(i);
        }
        return PassAway();
    }

    auto minOffset = SortedEntries[NextEntryIdx].Offset;
    auto count = 1;
    LOG_D("Fetching from offset " << minOffset << " count " << count << " from " << TabletId);
    SendToPQTablet(MakeEvPQRead(ConsumerName, PartitionId, minOffset, count));
}

void TMessageEnricherActor::SendToPQTablet(std::unique_ptr<IEventBase> ev) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev.release(), TabletId, FirstRequest, 1);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
    FirstRequest = false;
}

NActors::IActor* CreateMessageEnricher(ui64 tabletId,
                                       const ui32 partitionId,
                                       const TString& consumerName,
                                       std::deque<TReadResult>&& replies) {
    return new TMessageEnricherActor(tabletId, partitionId, consumerName, std::move(replies));
}

} // namespace NKikimr::NPQ::NMLP
