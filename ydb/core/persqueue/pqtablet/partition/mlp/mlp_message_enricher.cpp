#include "mlp_message_enricher.h"

namespace NKikimr::NPQ::NMLP {

TMessageEnricherActor::TMessageEnricherActor(const TActorId& tabletActorId, ui32 partitionId, const TString& consumerName, std::deque<TReadResult>&& replies)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_ENRICHER)
    , TabletActorId(tabletActorId)
    , PartitionId(partitionId)
    , ConsumerName(consumerName)
    , Queue(std::move(replies))
    , Backoff(5, TDuration::MilliSeconds(50))
    , PendingResponse(std::make_unique<TEvPQ::TEvMLPReadResponse>())
{
}

void TMessageEnricherActor::Bootstrap() {
    Become(&TThis::StateWork);
    ProcessQueue();
    Schedule(Timeout, new TEvents::TEvWakeup());
}

void TMessageEnricherActor::PassAway() {
    LOG_D("PassAway");
    for (auto& reply : Queue) {
        Send(reply.Sender, new TEvPQ::TEvMLPErrorResponse(Ydb::StatusIds::SCHEME_ERROR, "Shutdown"), 0, reply.Cookie);
    }

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
                    Send(reply.Sender, PendingResponse.release(), 0, reply.Cookie);
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

void TMessageEnricherActor::Handle(TEvPQ::TEvError::TPtr&) {
    LOG_D("Handle TEvPQ::TEvError");
    ProcessQueue();
}

void TMessageEnricherActor::Handle(TEvents::TEvWakeup::TPtr&) {
    LOG_D("TEvents::TEvWakeup");

    for (auto& reply : Queue) {
        Send(reply.Sender, new TEvPQ::TEvMLPErrorResponse(Ydb::StatusIds::TIMEOUT, "Enrich timeout"), 0, reply.Cookie);
    }
    Queue.clear();

    PassAway();
}

STFUNC(TMessageEnricherActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvResponse, Handle);
        hFunc(TEvPQ::TEvError, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
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
        LOG_D("Fetching from offset " << firstOffset << " count " << count << " from " << TabletActorId);

        auto request = std::make_unique<TEvPersQueue::TEvRequest>();
        auto* partitionRequest = request->Record.MutablePartitionRequest();
        partitionRequest->SetPartition(PartitionId);
        auto* read = partitionRequest->MutableCmdRead();
        read->SetClientId(ConsumerName);
        read->SetOffset(firstOffset);
        read->SetTimeoutMs(0);

        Send(TabletActorId, std::move(request), 0, ++Cookie);

        return;
    }

    if (Queue.empty()) {
        return PassAway();
    }
}

NActors::IActor* CreateMessageEnricher(const NActors::TActorId& tabletActorId,
                                       const ui32 partitionId,
                                       const TString& consumerName,
                                       std::deque<TReadResult>&& replies) {
    return new TMessageEnricherActor(tabletActorId, partitionId, consumerName, std::move(replies));
}

} // namespace NKikimr::NPQ::NMLP
