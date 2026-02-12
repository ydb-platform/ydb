#include "mlp_message_enricher.h"

namespace NKikimr::NPQ::NMLP {

TMessageEnricherActor::TMessageEnricherActor(ui64 tabletId, ui32 partitionId, const TString& consumerName, std::deque<TReadResult>&& replies)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_ENRICHER)
    , TabletId(tabletId)
    , PartitionId(partitionId)
    , ConsumerName(consumerName)
    , Queue(std::move(replies))
    , PendingResponse(std::make_unique<TEvPQ::TEvMLPReadResponse>())
{
}

void TMessageEnricherActor::Bootstrap() {
    Become(&TThis::StateWork);
    ProcessQueue();
}

void TMessageEnricherActor::PassAway() {
    LOG_D("PassAway");
    for (auto& reply : Queue) {
        Send(reply.Sender, new TEvPQ::TEvMLPErrorResponse(PartitionId, Ydb::StatusIds::SCHEME_ERROR, "Shutdown"), 0, reply.Cookie);
    }

    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));

    TBase::PassAway();
}

void TMessageEnricherActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvResponse");

    if (!IsSucess(ev)) {
        LOG_W("Fetch messages failed: " << ev->Get()->Record.DebugString());
        return PassAway();
    }

    auto& response = ev->Get()->Record;
    if (response.GetPartitionResponse().HasCmdReadResult()) {
        for (auto& result : response.GetPartitionResponse().GetCmdReadResult().GetResult()) {
            auto offset = result.GetOffset();

            while(!Queue.empty()) {
                auto& reply = Queue.front();
                if (offset < reply.Offsets.front()) {
                    break;
                }
                while (!reply.Offsets.empty() && offset > reply.Offsets.front()) {
                    reply.Offsets.pop_front();
                }
                if (!reply.Offsets.empty() && offset == reply.Offsets.front()) {
                    auto* message = PendingResponse->Record.AddMessage();
                    message->MutableId()->SetPartitionId(PartitionId);
                    message->MutableId()->SetOffset(offset);
                    message->SetData(result.GetData());
                    message->MutableMessageMeta()->SetMessageGroupId(result.GetSourceId());
                    message->MutableMessageMeta()->SetSentTimestampMilliseconds(result.GetWriteTimestampMS());

                    reply.Offsets.pop_front();
                }
                if (reply.Offsets.empty()) {
                    if (PendingResponse->Record.MessageSize() > 0) {
                        Send(reply.Sender, PendingResponse.release(), 0, reply.Cookie);
                    } else {
                        auto r = std::make_unique<TEvPQ::TEvMLPErrorResponse>(PartitionId, Ydb::StatusIds::INTERNAL_ERROR, "Messages was not found");
                        Send(reply.Sender, std::move(r), 0, reply.Cookie);
                    }
                    PendingResponse = std::make_unique<TEvPQ::TEvMLPReadResponse>();
                    Queue.pop_front();
                    continue;
                }
            }
        }
    }

    if (Queue.empty()) {
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
    while(!Queue.empty()) {
        auto& reply = Queue.front();
        if (reply.Offsets.empty()) {
            Send(reply.Sender, new TEvPQ::TEvMLPReadResponse(), 0, reply.Cookie);

            Queue.pop_front();
            continue;
        }

        auto firstOffset = reply.Offsets.front();
        auto lastOffset = Queue.back().Offsets.back();
        auto count = lastOffset - firstOffset + 1;
        LOG_D("Fetching from offset " << firstOffset << " count " << count << " from " << TabletId);
        SendToPQTablet(MakeEvPQRead(ConsumerName, PartitionId, firstOffset, count));

        return;
    }

    if (Queue.empty()) {
        return PassAway();
    }
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
