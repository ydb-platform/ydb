#include "retry_queue.h"

#include <util/generic/utility.h>
#include <ydb/library/actors/core/log.h>

namespace NYql::NDq {

const ui64 PingPeriodSeconds = 2;

void TRetryEventsQueue::Init(
    const TTxId& txId,
    const NActors::TActorId& senderId,
    const NActors::TActorId& selfId,
    ui64 eventQueueId,
    bool keepAlive,
    bool useConnect) {
    TxId = txId;
    SenderId = senderId;
    SelfId = selfId;
    Y_ASSERT(SelfId.NodeId() == SenderId.NodeId());
    EventQueueId = eventQueueId;
    KeepAlive = keepAlive;
    UseConnect = useConnect;
}

void TRetryEventsQueue::OnNewRecipientId(const NActors::TActorId& recipientId, bool unsubscribe, bool connected) {
    if (unsubscribe) {
        Unsubscribe();
    }
    RecipientId = recipientId;
    LocalRecipient = RecipientId.NodeId() == SelfId.NodeId();
    NextSeqNo = 1;
    Events.clear();
    MyConfirmedSeqNo = 0;
    ReceivedEventsSeqNos.clear();
    Connected = connected;
    RetryState = Nothing();
    if (Connected) {
        ScheduleHeartbeat();
    }
}

void TRetryEventsQueue::HandleNodeDisconnected(ui32 nodeId) {
    if (nodeId == RecipientId.NodeId()) {
        Connected = false;
        ScheduleRetry();
    }
}

void TRetryEventsQueue::HandleNodeConnected(ui32 nodeId) {
    if (nodeId == RecipientId.NodeId()) {
        if (!Connected) {
            Connected = true;
            RetryState = Nothing();

            // (Re)send all events
            for (const IRetryableEvent::TPtr& ev : Events) {
                SendRetryable(ev);
            }
        }
        if (KeepAlive) {
            ScheduleHeartbeat();
        }
    }
}

TRetryEventsQueue::ESessionState TRetryEventsQueue::HandleUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    if (ev->Sender != RecipientId) {
        return ESessionState::WrongSession;
    }
    if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::Disconnected) {
        Connected = false;
        ScheduleRetry();
        return ESessionState::Disconnected;
    }

    if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::ReasonActorUnknown) {
        return ESessionState::SessionClosed;
    }
    return ESessionState::Disconnected;
}

void TRetryEventsQueue::Retry() {
    RetryScheduled = false;
    if (!Connected) {
        Connect();
    }
}

bool TRetryEventsQueue::Heartbeat() {
    HeartbeatScheduled = false;

    if (!Connected) {
        return false;
    }
    ScheduleHeartbeat();
    auto now = TInstant::Now();
    return (now - LastReceivedDataTime >= TDuration::Seconds(PingPeriodSeconds)
        || (now - LastSentDataTime >= TDuration::Seconds(PingPeriodSeconds)));
}

void TRetryEventsQueue::Connect() {
    auto connectEvent = MakeHolder<NActors::TEvInterconnect::TEvConnectNode>();
    auto proxyId = NActors::TActivationContext::InterconnectProxy(RecipientId.NodeId());
    NActors::TActivationContext::Send(
        new NActors::IEventHandle(proxyId, SenderId, connectEvent.Release(), 0, 0));
}

void TRetryEventsQueue::Unsubscribe() {
    if (Connected) {
        Connected = false;
        auto unsubscribeEvent = MakeHolder<NActors::TEvents::TEvUnsubscribe>();
        NActors::TActivationContext::Send(
            new NActors::IEventHandle(NActors::TActivationContext::InterconnectProxy(RecipientId.NodeId()), SenderId, unsubscribeEvent.Release(), 0, 0));
    }
}

void TRetryEventsQueue::RemoveConfirmedEvents(ui64 confirmedSeqNo) {
    while (!Events.empty() && Events.front()->GetSeqNo() <= confirmedSeqNo) {
        Events.pop_front();
    }
    if (Events.size() > TEvRetryQueuePrivate::UNCONFIRMED_EVENTS_COUNT_LIMIT) {
        throw yexception()
            << "Too many unconfirmed events: " << Events.size()
            << ". Confirmed SeqNo: " << confirmedSeqNo
            << ". Unconfirmed SeqNos: " << Events.front()->GetSeqNo() << "-" << Events.back()->GetSeqNo()
            << ". TxId: \"" << TxId << "\". EventQueueId: " << EventQueueId;
    }
}

void TRetryEventsQueue::SendRetryable(const IRetryableEvent::TPtr& ev) {
    LastSentDataTime = TInstant::Now();
    NActors::TActivationContext::Send(ev->Clone(MyConfirmedSeqNo).Release());
}

void TRetryEventsQueue::ScheduleRetry() {
    if (!UseConnect || RetryScheduled) {
        return;
    } 
    RetryScheduled = true;
    if (!RetryState) {
        RetryState.ConstructInPlace();
    }
    auto ev = MakeHolder<TEvRetryQueuePrivate::TEvRetry>(EventQueueId);
    NActors::TActivationContext::Schedule(RetryState->GetNextDelay(), new NActors::IEventHandle(SelfId, SelfId, ev.Release()));
}

void TRetryEventsQueue::ScheduleHeartbeat() {
    if (!KeepAlive || HeartbeatScheduled) {
        return;
    }

    HeartbeatScheduled = true;
    auto ev = MakeHolder<TEvRetryQueuePrivate::TEvEvHeartbeat>(EventQueueId);
    NActors::TActivationContext::Schedule(TDuration::Seconds(PingPeriodSeconds), new NActors::IEventHandle(SelfId, SelfId, ev.Release()));
}

TDuration TRetryEventsQueue::TRetryState::GetNextDelay() {
    constexpr TDuration MaxDelay = TDuration::Seconds(10);
    constexpr TDuration MinDelay = TDuration::MilliSeconds(100); // from second retry
    TDuration ret = Delay; // The first delay is zero
    Delay = ClampVal(Delay * 2, MinDelay, MaxDelay);
    return ret ? RandomizeDelay(ret) : ret;
}

TDuration TRetryEventsQueue::TRetryState::RandomizeDelay(TDuration baseDelay) {
    const TDuration::TValue half = baseDelay.GetValue() / 2;
    return TDuration::FromValue(half + RandomNumber<TDuration::TValue>(half));
}

void TRetryEventsQueue::PrintInternalState(TStringStream& stream) const {
    stream << "id " << EventQueueId;
    if (LocalRecipient) {
        stream << ", LocalRecipient\n";
        return;
    }
    stream << ", NextSeqNo "
        << NextSeqNo << ", MyConfSeqNo " << MyConfirmedSeqNo << ", SeqNos " << ReceivedEventsSeqNos.size() << ", events size " 
        << Events.size() << ", connected " << Connected  << ", heartbeat shed " << HeartbeatScheduled 
        << ", last received " << LastReceivedDataTime << ", last sent " << LastSentDataTime << "\n";
}

} // namespace NYql::NDq
