#include "mlp_message_enricher.h"

#include <ydb/core/protos/pqdata_mlp.pb.h>

#include <algorithm>

namespace NKikimr::NPQ::NMLP {

TMessageEnricherActor::TMessageEnricherActor(ui64 tabletId, ui32 partitionId, const TString& consumerName, std::deque<TReadResult>&& replies)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_ENRICHER)
    , TabletId(tabletId)
    , PartitionId(partitionId)
    , ConsumerName(consumerName)
    , Replies(std::make_move_iterator(replies.begin()), std::make_move_iterator(replies.end()))
{
    PendingResponses.resize(Replies.size());
    for (size_t i = 0; i < Replies.size(); ++i) {
        auto& pr = PendingResponses[i];
        pr.Sender = Replies[i].Sender;
        pr.Cookie = Replies[i].Cookie;
        pr.TotalMessages = Replies[i].Messages.size();
        pr.EnrichedCount = 0;
        pr.Sent = false;
    }
    BuildSortedIndex();
}

void TMessageEnricherActor::BuildSortedIndex() {
    for (size_t ri = 0; ri < Replies.size(); ++ri) {
        auto& msgs = Replies[ri].Messages;
        for (size_t mi = 0; mi < msgs.size(); ++mi) {
            SortedEntries.push_back({msgs[mi].Offset, ri, mi, false});
        }
    }
    std::stable_sort(SortedEntries.begin(), SortedEntries.end(), [](const TOffsetEntry& a, const TOffsetEntry& b) { return a.Offset < b.Offset; });
}

void TMessageEnricherActor::Bootstrap() {
    Become(&TThis::StateWork);

    for (size_t i = 0; i < PendingResponses.size(); ++i) {
        if (PendingResponses[i].TotalMessages == 0) {
            Send(PendingResponses[i].Sender, new TEvPQ::TEvMLPReadResponse(), 0, PendingResponses[i].Cookie);
            PendingResponses[i].Sent = true;
            ++RepliesSent;
        }
    }

    ProcessQueue();
}

void TMessageEnricherActor::PassAway() {
    LOG_D("PassAway");
    for (auto& pr : PendingResponses) {
        if (!pr.Sent) {
            Send(pr.Sender, new TEvPQ::TEvMLPErrorResponse(PartitionId, Ydb::StatusIds::SCHEME_ERROR, "Shutdown"), 0, pr.Cookie);
            pr.Sent = true;
        }
    }

    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
    TBase::PassAway();
}

void TMessageEnricherActor::EnsureResponse(size_t replyIndex) {
    auto& pr = PendingResponses[replyIndex];
    if (!pr.Response) {
        pr.Response = std::make_unique<TEvPQ::TEvMLPReadResponse>();
    }
}

void TMessageEnricherActor::TrySendReply(size_t replyIndex) {
    auto& pr = PendingResponses[replyIndex];
    if (pr.Sent) {
        return;
    }
    if (!pr.IsComplete()) {
        return;
    }
    if (pr.EnrichedCount > 0) {
        Send(pr.Sender, pr.Response.release(), 0, pr.Cookie);
    } else {
        Send(pr.Sender, std::make_unique<TEvPQ::TEvMLPErrorResponse>(PartitionId, Ydb::StatusIds::INTERNAL_ERROR, "Messages was not found").release(), 0, pr.Cookie);
    }
    pr.Sent = true;
    ++RepliesSent;
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
    size_t ei = NextEntryIdx;
    int ri = 0;
    int resultsSize = results.size();
    ui64 maxReturnedOffset = 0;

    if (resultsSize > 0) {
        maxReturnedOffset = results[resultsSize - 1].GetOffset();
    }

    // Two-pointer: both SortedEntries and results are sorted by offset
    while (ei < SortedEntries.size() && ri < resultsSize) {
        auto& entry = SortedEntries[ei];
        if (entry.Processed) {
            ++ei;
            continue;
        }

        auto resultOffset = results[ri].GetOffset();

        if (entry.Offset < resultOffset) {
            entry.Processed = true;
            --PendingResponses[entry.ReplyIndex].TotalMessages;
            ++ei;
        } else if (entry.Offset == resultOffset) {
            auto& result = results[ri];
            auto& msg = Replies[entry.ReplyIndex].Messages[entry.MessageIndex];

            EnsureResponse(entry.ReplyIndex);
            auto* message = PendingResponses[entry.ReplyIndex].Response->Record.AddMessage();
            message->MutableId()->SetPartitionId(PartitionId);
            message->MutableId()->SetOffset(entry.Offset);
            message->SetData(result.GetData());
            message->MutableMessageMeta()->SetMessageGroupId(result.GetSourceId());
            message->MutableMessageMeta()->SetSentTimestampMilliseconds(result.GetWriteTimestampMS());
            message->MutableMessageMeta()->SetApproximateReceiveCount(msg.ApproximateReceiveCount);
            message->MutableMessageMeta()->SetApproximateFirstReceiveTimestampMilliseconds(msg.ApproximateFirstReceiveTimestamp.MilliSeconds());

            entry.Processed = true;
            ++PendingResponses[entry.ReplyIndex].EnrichedCount;
            ++ei;

            // Don't advance ri yet — there may be more entries with the same offset (duplicates across replies)
            if (ei < SortedEntries.size() && SortedEntries[ei].Offset == resultOffset) {
                continue;
            }
            ++ri;
        } else {
            // entry.Offset > resultOffset — advance result pointer
            ++ri;
        }
    }

    // Mark remaining entries whose offsets are <= maxReturnedOffset as missing
    if (resultsSize > 0) {
        while (ei < SortedEntries.size() && SortedEntries[ei].Offset <= maxReturnedOffset) {
            auto& entry = SortedEntries[ei];
            if (!entry.Processed) {
                entry.Processed = true;
                --PendingResponses[entry.ReplyIndex].TotalMessages;
            }
            ++ei;
        }
    }

    // Advance NextEntryIdx past all processed entries
    while (NextEntryIdx < SortedEntries.size() && SortedEntries[NextEntryIdx].Processed) {
        ++NextEntryIdx;
    }

    // Try to send completed replies
    for (size_t i = 0; i < PendingResponses.size(); ++i) {
        TrySendReply(i);
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
            TrySendReply(i);
        }
        if (RepliesSent == PendingResponses.size()) {
            return PassAway();
        }
        return;
    }

    auto minOffset = SortedEntries[NextEntryIdx].Offset;
    auto maxOffset = SortedEntries.back().Offset;
    auto count = maxOffset - minOffset + 1;
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
