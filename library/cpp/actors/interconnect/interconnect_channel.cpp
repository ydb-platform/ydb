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
        const size_t amount = sizeof(TChannelPart) + sizeof(TEventDescr2);
        if (task.GetVirtualFreeAmount() < amount) {
            return false;
        }

        auto traceId = event.Span.GetTraceId();
        event.Span.EndOk();

        LWTRACK(SerializeToPacketEnd, event.Orbit, PeerNodeId, ChannelId, OutputQueueSize, task.GetDataSize());
        task.Orbit.Take(event.Orbit);

        Y_VERIFY(SerializationInfo);
        const ui32 flags = (event.Descr.Flags & ~IEventHandle::FlagForwardOnNondelivery) |
            (SerializationInfo->IsExtendedFormat ? IEventHandle::FlagExtendedFormat : 0);

        // prepare descriptor record
        TEventDescr2 descr{
            event.Descr.Type,
            flags,
            event.Descr.Recipient,
            event.Descr.Sender,
            event.Descr.Cookie,
            {},
            event.Descr.Checksum
        };
        traceId.Serialize(&descr.TraceId);

        // and channel header before the descriptor
        TChannelPart part{
            .Channel = static_cast<ui16>(ChannelId | TChannelPart::LastPartFlag),
            .Size = sizeof(descr),
        };

        // append them to the packet
        task.Write(&part, sizeof(part));
        task.Write(&descr, sizeof(descr));

        *weightConsumed += amount;
        OutputQueueSize -= part.Size;
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
                        if (event.EventSerializedSize) {
                            Chunker.SetSerializingEvent(base);
                        }
                        SerializationInfoContainer = base->CreateSerializationInfo();
                        SerializationInfo = &SerializationInfoContainer;
                    } else { // event without buffer and IEventBase instance
                        State = EState::DESCRIPTOR;
                        SerializationInfoContainer = {};
                        SerializationInfo = &SerializationInfoContainer;
                    }
                    if (!event.EventSerializedSize) {
                        State = EState::DESCRIPTOR;
                    }
                    break;

                case EState::CHUNKER:
                case EState::BUFFER: {
                    if (task.GetVirtualFreeAmount() <= sizeof(TChannelPart)) {
                        return false;
                    }

                    TChannelPart part{
                        .Channel = ChannelId,
                        .Size = 0,
                    };

                    auto partBookmark = task.Bookmark(sizeof(part));

                    auto addChunk = [&](const void *data, size_t len) {
                        event.UpdateChecksum(data, len);
                        task.Append(data, len);
                        part.Size += len;

                        event.EventActuallySerialized += len;
                        if (event.EventActuallySerialized > MaxSerializedEventSize) {
                            throw TExSerializedEventTooLarge(event.Descr.Type);
                        }
                    };

                    bool complete = false;
                    if (State == EState::CHUNKER) {
                        while (!complete && !task.IsFull()) {
                            TMutableContiguousSpan out = task.AcquireSpanForWriting();
                            const auto [first, last] = Chunker.FeedBuf(out.data(), out.size());
                            for (auto p = first; p != last; ++p) {
                                addChunk(p->first, p->second);
                            }
                            complete = Chunker.IsComplete();
                        }
                        Y_VERIFY(!complete || Chunker.IsSuccessfull());
                        Y_VERIFY_DEBUG(complete || task.IsFull());
                    } else { // BUFFER
                        while (const size_t numb = Min(task.GetVirtualFreeAmount(), Iter.ContiguousSize())) {
                            const char *obuf = Iter.ContiguousData();
                            addChunk(obuf, numb);
                            Iter += numb;
                        }
                        complete = !Iter.Valid();
                    }
                    if (complete) {
                        Y_VERIFY(event.EventActuallySerialized == event.EventSerializedSize,
                            "EventActuallySerialized# %" PRIu32 " EventSerializedSize# %" PRIu32 " Type# 0x%08" PRIx32,
                            event.EventActuallySerialized, event.EventSerializedSize, event.Descr.Type);
                    }

                    Y_VERIFY_DEBUG(part.Size);
                    task.WriteBookmark(std::exchange(partBookmark, {}), &part, sizeof(part));
                    *weightConsumed += sizeof(TChannelPart) + part.Size;
                    OutputQueueSize -= part.Size;
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
