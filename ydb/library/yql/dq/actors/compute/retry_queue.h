#pragma once
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/interconnect.h>

#include <util/generic/yexception.h>
#include <util/system/types.h>
#include <util/datetime/base.h>

namespace NYql::NDq {

struct TEvRetryQueuePrivate {
    // Event ids.
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvRetry = EvBegin,
        EvPing,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events.

    struct TEvRetry : NActors::TEventLocal<TEvRetry, EvRetry> {
        explicit TEvRetry(ui64 eventQueueId)
            : EventQueueId(eventQueueId)
        { }
        const ui64 EventQueueId;
    };

    struct TEvPing : NActors::TEventLocal<TEvPing, EvPing> {
        explicit TEvPing(ui64 eventQueueId)
            : EventQueueId(eventQueueId)
        { }
        const ui64 EventQueueId;
    };

    static constexpr size_t UNCONFIRMED_EVENTS_COUNT_LIMIT = 10000;
};

template <class T>
concept TProtobufEvent =
    std::is_base_of_v<google::protobuf::Message, typename T::ProtoRecordType>;

template <class T>
concept TIsMessageTransportMetaRef =
    std::is_same_v<T, const NYql::NDqProto::TMessageTransportMeta&>;

template <class T>
concept THasTransportMeta = requires(T& ev) {
    { ev.Record.GetTransportMeta() } -> TIsMessageTransportMetaRef;
};

template <class T>
concept TProtobufEventWithTransportMeta = TProtobufEvent<T> && THasTransportMeta<T>;

class TRetryEventsQueue {
public:
    struct ICallbacks {
        virtual void SessionClosed(ui64 eventQueueId) = 0;       // Need to create new session.
        virtual ~ICallbacks() = default;
    };

public:
    class IRetryableEvent : public TSimpleRefCount<IRetryableEvent> {
    public:
        using TPtr = TIntrusivePtr<IRetryableEvent>;
        virtual ~IRetryableEvent() = default;
        virtual THolder<NActors::IEventHandle> Clone(ui64 confirmedSeqNo) const = 0;
        virtual ui64 GetSeqNo() const = 0;
    };

    TRetryEventsQueue() {};

    void Init(const TTxId& txId, const NActors::TActorId& senderId, const NActors::TActorId& selfId, ui64 eventQueueId = 0, bool keepAlive = false, ICallbacks* cbs = nullptr);

    template <TProtobufEventWithTransportMeta T>
    void Send(T* ev, ui64 cookie = 0) {
        Send(THolder<T>(ev), cookie);
    }

    template <TProtobufEventWithTransportMeta T>
    void Send(THolder<T> ev, ui64 cookie = 0) {
        if (LocalRecipient) {
            NActors::TActivationContext::Send(new NActors::IEventHandle(RecipientId, SenderId, ev.Release(), cookie));
            return;
        }

        IRetryableEvent::TPtr retryableEvent = Store(RecipientId, SenderId, std::move(ev), cookie);
        if (Connected) {
            SendRetryable(retryableEvent);
        } else {
            ScheduleRetry();
        }
    }

    template <TProtobufEventWithTransportMeta T>
    bool OnEventReceived(const TAutoPtr<NActors::TEventHandle<T>>& ev) {
        return OnEventReceived(ev->Get());
    }

    template <TProtobufEventWithTransportMeta T>
    bool OnEventReceived(const T* ev) { // Returns true if event was not processed (== it was received first time).
        LastReceivedDataTime = TInstant::Now();
        if (LocalRecipient) {
            return true;
        }

        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Record.GetTransportMeta();
        RemoveConfirmedEvents(meta.GetConfirmedSeqNo());

        const ui64 seqNo = meta.GetSeqNo();
        if (seqNo == MyConfirmedSeqNo + 1) {
            ++MyConfirmedSeqNo;
            while (!ReceivedEventsSeqNos.empty() && *ReceivedEventsSeqNos.begin() == MyConfirmedSeqNo + 1) {
                ++MyConfirmedSeqNo;
                ReceivedEventsSeqNos.erase(ReceivedEventsSeqNos.begin());
            }
            return true;
        } else if (seqNo > MyConfirmedSeqNo) {
            if (ReceivedEventsSeqNos.size() > TEvRetryQueuePrivate::UNCONFIRMED_EVENTS_COUNT_LIMIT) {
                throw yexception()
                    << "Too wide window of reordered events: " << ReceivedEventsSeqNos.size()
                    << ". MyConfirmedSeqNo: " << MyConfirmedSeqNo
                    << ". Received SeqNo: " << seqNo
                    << ". TxId: \"" << TxId << "\". EventQueueId: " << EventQueueId;
            }
            return ReceivedEventsSeqNos.insert(seqNo).second;
        }
        return false;
    }

    bool RemoveConfirmedEvents() {
        RemoveConfirmedEvents(MyConfirmedSeqNo);
        return !Events.empty();
    }

    void OnNewRecipientId(const NActors::TActorId& recipientId, bool unsubscribe = true);
    void HandleNodeConnected(ui32 nodeId);
    void HandleNodeDisconnected(ui32 nodeId);
    bool HandleUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev);
    void Retry();
    void Ping();
    void Unsubscribe();

private:
    template <TProtobufEventWithTransportMeta T>
    IRetryableEvent::TPtr Store(const NActors::TActorId& recipient, const NActors::TActorId& sender, THolder<T> ev, ui64 cookie) {
        ev->Record.MutableTransportMeta()->SetSeqNo(NextSeqNo++);
        Events.push_back(MakeIntrusive<TRetryableEvent<T>>(recipient, sender, std::move(ev), cookie));
        return Events.back();
    }

    void RemoveConfirmedEvents(ui64 confirmedSeqNo);
    void SendRetryable(const IRetryableEvent::TPtr& ev);
    void ScheduleRetry();
    void SchedulePing();
    void Connect();

private:
    template <TProtobufEventWithTransportMeta T>
    class TRetryableEvent : public IRetryableEvent {
    public:
        TRetryableEvent(const NActors::TActorId& recipient, const NActors::TActorId& sender, THolder<T> ev, ui64 cookie)
            : Event(std::move(ev))
            , Recipient(recipient)
            , Sender(sender)
            , Cookie(cookie)
        {
        }

        ui64 GetSeqNo() const override {
            return Event->Record.GetTransportMeta().GetSeqNo();
        }

        THolder<NActors::IEventHandle> Clone(ui64 confirmedSeqNo) const override {
            THolder<T> ev = MakeHolder<T>();
            ev->Record = Event->Record;
            ev->Record.MutableTransportMeta()->SetConfirmedSeqNo(confirmedSeqNo);
            return MakeHolder<NActors::IEventHandle>(Recipient, Sender, ev.Release(), NActors::IEventHandle::FlagTrackDelivery, Cookie);
        }

    private:
        const THolder<T> Event;
        const NActors::TActorId Recipient;
        const NActors::TActorId Sender;
        const ui64 Cookie;
    };

    class TRetryState {
    public:
        TDuration GetNextDelay();

    private:
        static TDuration RandomizeDelay(TDuration baseDelay);

    private:
        TDuration Delay; // The first time retry will be done instantly.
    };

private:
    NActors::TActorId SenderId;
    NActors::TActorId SelfId;
    ui64 EventQueueId = 0;
    NActors::TActorId RecipientId;
    bool LocalRecipient = false;
    ui64 NextSeqNo = 1;
    std::deque<IRetryableEvent::TPtr> Events;
    ui64 MyConfirmedSeqNo = 0; // Received events seq no border.
    std::set<ui64> ReceivedEventsSeqNos;
    bool Connected = false;
    bool RetryScheduled = false;
    bool PingScheduled = false;
    TMaybe<TRetryState> RetryState;
    TTxId TxId;
    ICallbacks* Cbs = nullptr;
    bool KeepAlive = false;
    TInstant LastReceivedDataTime = TInstant::Now();
};

} // namespace NYql::NDq
