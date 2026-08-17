#pragma once

#include "interconnect_direct_session.h"

namespace NActors {

    class TIncomingEventQueue {
        IEventHandle Stub{0, 0, {}, {}, nullptr, 0};
        std::atomic<IEventHandle*> Head{&Stub};
        std::atomic<IEventHandle*> Tail{&Stub};

        struct TEventPayload {
            ui64 Conn;
            TIntrusivePtr<IReceiveCallback> Callback;
        };
        static_assert(sizeof(TEventPayload) <= sizeof(TActorId));

        struct TEventPayload2 {
            ui64 ReceivedTimestamp; // GetCycleCountFast()
            ui64 SeqNo;
        };
        static_assert(sizeof(TEventPayload2) <= sizeof(TScopeId));

    public:
        struct TRecord {
            std::unique_ptr<IEventHandle> Ev;
            ui64 Conn;
            TIntrusivePtr<IReceiveCallback> Callback;
            ui64 ReceivedTimestamp;
            ui64 SeqNo;
        };

    public:
        TIncomingEventQueue() {
            Stub.NextLinkPtr.store(0, std::memory_order_relaxed);
        }

        ~TIncomingEventQueue() {
            Y_DEBUG_ABORT_UNLESS(IsEmpty()); // ensure this event queue has been properly drained by owner
        }

        bool Push(TRecord&& record) {
            IEventHandle *last = record.Ev.release();

            // store some metadata in event's unmatching fields
            new(const_cast<TActorId*>(&last->InterconnectSession)) TEventPayload{
                .Conn = record.Conn,
                .Callback = std::move(record.Callback),
            };
            new(const_cast<TScopeId*>(&last->OriginScopeId)) TEventPayload2{
                .ReceivedTimestamp = record.ReceivedTimestamp,
                .SeqNo = record.SeqNo,
            };

            last->NextLinkPtr.store(0, std::memory_order_relaxed);
            IEventHandle *prev = Head.exchange(last, std::memory_order_acq_rel);
            prev->NextLinkPtr.store(reinterpret_cast<uintptr_t>(last), std::memory_order_release);
            return prev == &Stub; // if it was the first event
        }

        bool IsEmpty() const {
            IEventHandle *tail = Tail.load(std::memory_order_relaxed);
            IEventHandle *next = reinterpret_cast<IEventHandle*>(tail->NextLinkPtr.load(std::memory_order_acquire));
            IEventHandle *head = Head.load(std::memory_order_acquire);
            return tail == &Stub && !next && tail == head;
        }

        std::optional<TRecord> Pop() {
            auto decompose = [&](IEventHandle *ev) {
                auto& payload = reinterpret_cast<TEventPayload&>(const_cast<TActorId&>(ev->InterconnectSession));
                ui64 conn = payload.Conn;
                TIntrusivePtr<IReceiveCallback> callback = std::move(payload.Callback);
                payload.~TEventPayload();

                auto& payload2 = reinterpret_cast<TEventPayload2&>(const_cast<TScopeId&>(ev->OriginScopeId));
                ui64 receivedTimestamp = payload2.ReceivedTimestamp;
                ui64 seqNo = payload2.SeqNo;
                payload2.~TEventPayload2();

                return TRecord{std::unique_ptr<IEventHandle>(ev), conn, std::move(callback), receivedTimestamp, seqNo};
            };

            for (;;) {
                IEventHandle *tail = Tail.load(std::memory_order_relaxed);
                IEventHandle *next = reinterpret_cast<IEventHandle*>(tail->NextLinkPtr.load(std::memory_order_acquire));
                IEventHandle *head;

                if (tail == &Stub) {
                    if (!next) {
                        if (head = Head.load(std::memory_order_acquire); tail != head) {
                            continue;
                        } else {
                            return std::nullopt;
                        }
                    }
                    Tail.store(next, std::memory_order_relaxed);
                    tail = next;
                    next = reinterpret_cast<IEventHandle*>(tail->NextLinkPtr.load(std::memory_order_acquire));
                }

                if (next) {
                    Tail.store(next, std::memory_order_relaxed);
                    return decompose(tail);
                }

                head = Head.load(std::memory_order_acquire);
                if (tail != head) {
                    continue;
                }

                Stub.NextLinkPtr.store(0, std::memory_order_relaxed);
                IEventHandle *prev = Head.exchange(&Stub, std::memory_order_acq_rel);
                prev->NextLinkPtr.store(reinterpret_cast<uintptr_t>(&Stub), std::memory_order_release);

                next = reinterpret_cast<IEventHandle*>(tail->NextLinkPtr.load(std::memory_order_acquire));
                if (next) {
                    Tail.store(next, std::memory_order_relaxed);
                    return decompose(tail);
                }
            }
        }
    };


} // NActors
