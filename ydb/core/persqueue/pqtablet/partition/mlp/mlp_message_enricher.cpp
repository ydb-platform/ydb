#include "mlp_message_enricher.h"

#include <ydb/core/persqueue/events/global.h>

namespace NKikimr::NPQ::NMLP {

TMessageEnricherActor::TMessageEnricherActor(ui32 partitionId, const TActorId& partitionActor, const TString& consumerName, std::deque<TReadResult>&& replies)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_ENRICHER)
    , PartitionId(partitionId)
    , PartitionActorId(partitionActor)
    , ConsumerName(consumerName)
    , Queue(std::move(replies))
    , Backoff(5, TDuration::MilliSeconds(50))
    , PendingResponse(std::make_unique<TEvPersQueue::TEvMLPReadResponse>())
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
        Send(reply.Sender, new TEvPersQueue::TEvMLPErrorResponse(NPersQueue::NErrorCode::EErrorCode::ERROR, "Shutdown"), 0, reply.Cookie);
    }

    TBase::PassAway();
}

TString TMessageEnricherActor::BuildLogPrefix() const {
    return TStringBuilder() << "[" << SelfId() << "] ";
}

void TMessageEnricherActor::Handle(TEvPQ::TEvProxyResponse::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvProxyResponse");
    if (Cookie != ev->Cookie) {
        LOG_D("Cookie mismatch: " << Cookie << " != " << ev->Cookie);
        //return PassAway();
    }

    if (!IsSucess(ev)) {
        LOG_W("Fetch messages failed: " << ev->Get()->Response->DebugString());
        return PassAway();
    }

    auto& response = ev->Get()->Response;
    if (response->GetPartitionResponse().HasCmdReadResult()) {
        for (auto& result : response->GetPartitionResponse().GetCmdReadResult().GetResult()) {
            auto offset = result.GetOffset();

            while(!Queue.empty()) {
                auto& reply = Queue.front();
                if (offset < reply.Offsets.front()) {
                    break;
                }
                while (!reply.Offsets.empty() && offset > reply.Offsets.front()) {
                    reply.Offsets.pop_front();
                }
                // TODO multi part messages
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
                    PendingResponse = std::make_unique<TEvPersQueue::TEvMLPReadResponse>();
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
}

void TMessageEnricherActor::Handle(TEvents::TEvWakeup::TPtr&) {
    LOG_D("TEvents::TEvWakeup");
    PassAway();
}

STFUNC(TMessageEnricherActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvProxyResponse, Handle);
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
            Send(reply.Sender, new TEvPersQueue::TEvMLPReadResponse(), 0, reply.Cookie);

            Queue.pop_front();
            continue;
        }

        auto offset = reply.Offsets.front();
        LOG_D("Fetching from offset " << offset << " from " << PartitionActorId);
        Send(PartitionActorId, MakeEvRead(SelfId(), ConsumerName, offset, 1, ++Cookie));

        return;
    }

    if (Queue.empty()) {
        return PassAway();
    }
}

} // namespace NKikimr::NPQ::NMLP
