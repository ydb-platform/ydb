#include "interconnect_channel.h"

#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/executor_thread.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/probes.h>
#include <library/cpp/actors/protos/services_common.pb.h>
#include <library/cpp/actors/prof/tag.h>
#include <library/cpp/digest/crc32c/crc32c.h>

LWTRACE_USING(ACTORLIB_PROVIDER);

namespace NActors {
    bool TEventOutputChannel::FeedDescriptor(TTcpPacketOutTask& task, TEventHolder& event, ui64 *weightConsumed) {
        const size_t descrSize = Params.UseExtendedTraceFmt ? sizeof(TEventDescr2) : sizeof(TEventDescr1);
        const size_t amount = sizeof(TChannelPart) + descrSize;
        if (task.GetVirtualFreeAmount() < amount) {
            return false;
        }

        auto traceId = event.Span.GetTraceId();
        event.Span.EndOk();

        LWTRACK(SerializeToPacketEnd, event.Orbit, PeerNodeId, ChannelId, OutputQueueSize, task.GetDataSize());
        task.Orbit.Take(event.Orbit);

        Y_VERIFY(SerializationInfo);
        event.Descr.Flags = (event.Descr.Flags & ~IEventHandle::FlagForwardOnNondelivery) |
            (SerializationInfo->IsExtendedFormat ? IEventHandle::FlagExtendedFormat : 0);

        TChannelPart *part = static_cast<TChannelPart*>(task.GetFreeArea());
        part->Channel = ChannelId | TChannelPart::LastPartFlag;
        part->Size = descrSize;

        void *descr = part + 1;
        if (Params.UseExtendedTraceFmt) {
            auto *p = static_cast<TEventDescr2*>(descr);
            *p = {
                event.Descr.Type,
                event.Descr.Flags,
                event.Descr.Recipient,
                event.Descr.Sender,
                event.Descr.Cookie,
                {},
                event.Descr.Checksum
            };
            traceId.Serialize(&p->TraceId);
        } else {
            auto *p = static_cast<TEventDescr1*>(descr);
            *p = {
                event.Descr.Type,
                event.Descr.Flags,
                event.Descr.Recipient,
                event.Descr.Sender,
                event.Descr.Cookie,
                {},
                event.Descr.Checksum
            };
        }

        task.AppendBuf(part, amount);
        *weightConsumed += amount;
        OutputQueueSize -= part->Size;
        Metrics->UpdateOutputChannelEvents(ChannelId);

        return true;
    }

    void TEventOutputChannel::DropConfirmed(ui64 confirm) {
        LOG_DEBUG_IC_SESSION("ICOCH98", "Dropping confirmed messages");
        for (auto it = NotYetConfirmed.begin(); it != NotYetConfirmed.end() && it->Serial <= confirm; ) {
            Pool.Release(NotYetConfirmed, it++);
        }
    }

    bool TEventOutputChannel::FeedBuf(TTcpPacketOutTask& task, ui64 serial, ui64 *weightConsumed) {
        for (;;) {
            Y_VERIFY(!Queue.empty());
            TEventHolder& event = Queue.front();

            switch (State) {
                case EState::INITIAL:
                    event.InitChecksum();
                    LWTRACK(SerializeToPacketBegin, event.Orbit, PeerNodeId, ChannelId, OutputQueueSize);
                    if (event.Buffer) {
                        State = EState::BUFFER;
                        Iter = event.Buffer->GetBeginIter();
                        SerializationInfo = &event.Buffer->GetSerializationInfo();
                    } else if (event.Event) {
                        State = EState::CHUNKER;
                        IEventBase *base = event.Event.Get();
                        Chunker.SetSerializingEvent(base);
                        SerializationInfoContainer = base->CreateSerializationInfo();
                        SerializationInfo = &SerializationInfoContainer;
                    } else { // event without buffer and IEventBase instance
                        State = EState::DESCRIPTOR;
                        SerializationInfoContainer = {};
                        SerializationInfo = &SerializationInfoContainer;
                    }
                    break;

                case EState::CHUNKER:
                case EState::BUFFER: {
                    size_t maxBytes = task.GetVirtualFreeAmount();
                    if (maxBytes <= sizeof(TChannelPart)) {
                        return false;
                    }

                    TChannelPart *part = static_cast<TChannelPart*>(task.GetFreeArea());
                    part->Channel = ChannelId;
                    part->Size = 0;
                    task.AppendBuf(part, sizeof(TChannelPart));
                    maxBytes -= sizeof(TChannelPart);
                    Y_VERIFY(maxBytes);

                    auto addChunk = [&](const void *data, size_t len) {
                        event.UpdateChecksum(Params, data, len);
                        task.AppendBuf(data, len);
                        part->Size += len;
                        Y_VERIFY_DEBUG(maxBytes >= len);
                        maxBytes -= len;

                        event.EventActuallySerialized += len;
                        if (event.EventActuallySerialized > MaxSerializedEventSize) {
                            throw TExSerializedEventTooLarge(event.Descr.Type);
                        }
                    };

                    bool complete = false;
                    if (State == EState::CHUNKER) {
                        Y_VERIFY_DEBUG(task.GetFreeArea() == part + 1);
                        while (!complete && maxBytes) {
                            const auto [first, last] = Chunker.FeedBuf(task.GetFreeArea(), maxBytes);
                            for (auto p = first; p != last; ++p) {
                                addChunk(p->first, p->second);
                            }
                            complete = Chunker.IsComplete();
                        }
                        Y_VERIFY(!complete || Chunker.IsSuccessfull());
                        Y_VERIFY_DEBUG(complete || !maxBytes);
                    } else { // BUFFER
                        while (const size_t numb = Min(maxBytes, Iter.ContiguousSize())) {
                            const char *obuf = Iter.ContiguousData();
                            addChunk(obuf, numb);
                            Iter += numb;
                        }
                        complete = !Iter.Valid();
                    }
                    if (complete && event.Descr.Type != 268633601) {
                        Y_VERIFY(event.EventActuallySerialized == event.EventSerializedSize,
                            "EventActuallySerialized# %" PRIu32 " EventSerializedSize# %" PRIu32 " Type# 0x%08" PRIx32,
                            event.EventActuallySerialized, event.EventSerializedSize, event.Descr.Type);
                    }

                    if (!part->Size) {
                        task.Undo(sizeof(TChannelPart));
                    } else {
                        *weightConsumed += sizeof(TChannelPart) + part->Size;
                        OutputQueueSize -= part->Size;
                    }
                    if (complete) {
                        State = EState::DESCRIPTOR;
                    }
                    break;
                }

                case EState::DESCRIPTOR:
                    if (!FeedDescriptor(task, event, weightConsumed)) {
                        return false;
                    }
                    event.Serial = serial;
                    NotYetConfirmed.splice(NotYetConfirmed.end(), Queue, Queue.begin()); // move event to not-yet-confirmed queue
                    SerializationInfoContainer = {};
                    SerializationInfo = nullptr;
                    State = EState::INITIAL;
                    return true; // we have processed whole event, signal to the caller
            }
        }
    }

    void TEventOutputChannel::NotifyUndelivered() {
        LOG_DEBUG_IC_SESSION("ICOCH89", "Notyfying about Undelivered messages! NotYetConfirmed size: %zu, Queue size: %zu", NotYetConfirmed.size(), Queue.size());
        if (State == EState::CHUNKER) {
            Y_VERIFY(!Chunker.IsComplete()); // chunk must have an event being serialized
            Y_VERIFY(!Queue.empty()); // this event must be the first event in queue
            TEventHolder& event = Queue.front();
            Y_VERIFY(Chunker.GetCurrentEvent() == event.Event.Get()); // ensure the event is valid
            Chunker.Abort(); // stop serializing current event
            Y_VERIFY(Chunker.IsComplete());
        }
        for (auto& item : NotYetConfirmed) {
            if (item.Descr.Flags & IEventHandle::FlagGenerateUnsureUndelivered) { // notify only when unsure flag is set
                item.ForwardOnNondelivery(true);
            }
        }
        Pool.Release(NotYetConfirmed);
        for (auto& item : Queue) {
            item.ForwardOnNondelivery(false);
        }
        Pool.Release(Queue);
    }

}
