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
        if (task.GetInternalFreeAmount() < amount) {
            return false;
        }

        auto traceId = event.Span.GetTraceId();
        event.Span.EndOk();

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
            event.Descr.Checksum,
#if IC_FORCE_HARDENED_PACKET_CHECKS
            event.EventSerializedSize
#endif
        };
        traceId.Serialize(&descr.TraceId);

        // and channel header before the descriptor
        TChannelPart part{
            .ChannelFlags = static_cast<ui16>(ChannelId | TChannelPart::LastPartFlag),
            .Size = sizeof(descr)
        };

        // append them to the packet
        task.Write<false>(&part, sizeof(part));
        task.Write<false>(&descr, sizeof(descr));

        *weightConsumed += amount;
        OutputQueueSize -= sizeof(TEventDescr2);
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
                    if (event.Buffer) {
                        State = EState::BODY;
                        Iter = event.Buffer->GetBeginIter();
                        SerializationInfo = &event.Buffer->GetSerializationInfo();
                    } else if (event.Event) {
                        State = EState::BODY;
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
                    EventInExternalDataChannel = Params.UseExternalDataChannel && !SerializationInfo->Sections.empty();
                    if (!event.EventSerializedSize) {
                        State = EState::DESCRIPTOR;
                    } else if (EventInExternalDataChannel) {
                        State = EState::SECTIONS;
                        SectionIndex = 0;
                    }
                    break;

                case EState::BODY:
                    if (FeedPayload(task, event, weightConsumed)) {
                        State = EState::DESCRIPTOR;
                    } else {
                        return false;
                    }
                    break;

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

                case EState::SECTIONS: {
                    if (SectionIndex == 0) {
                        size_t totalSectionSize = 0;
                        for (const auto& section : SerializationInfo->Sections) {
                            totalSectionSize += section.Size;
                        }
                        Y_VERIFY(totalSectionSize == event.EventSerializedSize);
                    }

                    while (SectionIndex != SerializationInfo->Sections.size()) {
                        char sectionInfo[1 + NInterconnect::NDetail::MaxNumberBytes * 4];
                        char *p = sectionInfo;

                        const auto& section = SerializationInfo->Sections[SectionIndex];
                        *p++ = static_cast<ui8>(EXdcCommand::DECLARE_SECTION);
                        p += NInterconnect::NDetail::SerializeNumber(section.Headroom, p);
                        p += NInterconnect::NDetail::SerializeNumber(section.Size, p);
                        p += NInterconnect::NDetail::SerializeNumber(section.Tailroom, p);
                        p += NInterconnect::NDetail::SerializeNumber(section.Alignment, p);
                        Y_VERIFY(p <= std::end(sectionInfo));

                        const size_t declareLen = p - sectionInfo;
                        if (sizeof(TChannelPart) + XdcData.size() + declareLen <= task.GetInternalFreeAmount() &&
                                XdcData.size() + declareLen <= Max<ui16>()) {
                            XdcData.insert(XdcData.end(), sectionInfo, p);
                            ++SectionIndex;
                        } else {
                            break;
                        }
                    }

                    if (XdcData.empty()) {
                        return false;
                    }

                    TChannelPart part{
                        .ChannelFlags = static_cast<ui16>(ChannelId | TChannelPart::XdcFlag),
                        .Size = static_cast<ui16>(XdcData.size())
                    };
                    task.Write<false>(&part, sizeof(part));
                    task.Write<false>(XdcData.data(), XdcData.size());
                    XdcData.clear();

                    if (SectionIndex == SerializationInfo->Sections.size()) {
                        State = EState::BODY;
                    }

                    break;
                }
            }
        }
    }

    template<bool External>
    bool TEventOutputChannel::SerializeEvent(TTcpPacketOutTask& task, TEventHolder& event, size_t *bytesSerialized) {
        auto addChunk = [&](const void *data, size_t len, bool allowCopy) {
            event.UpdateChecksum(data, len);
            if (allowCopy && (reinterpret_cast<uintptr_t>(data) & 63) + len <= 64) {
                task.Write<External>(data, len);
            } else {
                task.Append<External>(data, len);
            }
            *bytesSerialized += len;

            event.EventActuallySerialized += len;
            if (event.EventActuallySerialized > MaxSerializedEventSize) {
                throw TExSerializedEventTooLarge(event.Descr.Type);
            }
        };

        bool complete = false;
        if (event.Event) {
            while (!complete) {
                TMutableContiguousSpan out = task.AcquireSpanForWriting<External>();
                if (!out.size()) {
                    break;
                }
                ui32 bytesFed = 0;
                for (const auto& [buffer, size] : Chunker.FeedBuf(out.data(), out.size())) {
                    addChunk(buffer, size, false);
                    bytesFed += size;
                    Y_VERIFY(bytesFed <= out.size());
                }
                complete = Chunker.IsComplete();
                if (complete) {
                    Y_VERIFY(Chunker.IsSuccessfull());
                }
            }
        } else if (event.Buffer) {
            while (const size_t numb = Min<size_t>(External ? task.GetExternalFreeAmount() : task.GetInternalFreeAmount(),
                    Iter.ContiguousSize())) {
                const char *obuf = Iter.ContiguousData();
                addChunk(obuf, numb, true);
                Iter += numb;
            }
            complete = !Iter.Valid();
        } else {
            Y_FAIL();
        }
        Y_VERIFY(!complete || event.EventActuallySerialized == event.EventSerializedSize,
            "EventActuallySerialized# %" PRIu32 " EventSerializedSize# %" PRIu32 " Type# 0x%08" PRIx32,
            event.EventActuallySerialized, event.EventSerializedSize, event.Descr.Type);

        return complete;
    }

    bool TEventOutputChannel::FeedPayload(TTcpPacketOutTask& task, TEventHolder& event, ui64 *weightConsumed) {
        return EventInExternalDataChannel
            ? FeedExternalPayload(task, event, weightConsumed)
            : FeedInlinePayload(task, event, weightConsumed);
    }

    bool TEventOutputChannel::FeedInlinePayload(TTcpPacketOutTask& task, TEventHolder& event, ui64 *weightConsumed) {
        if (task.GetInternalFreeAmount() <= sizeof(TChannelPart)) {
            return false;
        }

        auto partBookmark = task.Bookmark(sizeof(TChannelPart));

        size_t bytesSerialized = 0;
        const bool complete = SerializeEvent<false>(task, event, &bytesSerialized);

        Y_VERIFY_DEBUG(bytesSerialized);
        Y_VERIFY(bytesSerialized <= Max<ui16>());

        TChannelPart part{
            .ChannelFlags = ChannelId,
            .Size = static_cast<ui16>(bytesSerialized)
        };

        task.WriteBookmark(std::move(partBookmark), &part, sizeof(part));
        *weightConsumed += sizeof(TChannelPart) + part.Size;
        OutputQueueSize -= part.Size;

        return complete;
    }

    bool TEventOutputChannel::FeedExternalPayload(TTcpPacketOutTask& task, TEventHolder& event, ui64 *weightConsumed) {
        const size_t partSize = sizeof(TChannelPart) + sizeof(ui8) + sizeof(ui16) + (Params.Encryption ? 0 : sizeof(ui32));
        if (task.GetInternalFreeAmount() < partSize || task.GetExternalFreeAmount() == 0) {
            return false;
        }

        // clear external checksum for this chunk
        task.ExternalChecksum = 0;

        auto partBookmark = task.Bookmark(partSize);

        size_t bytesSerialized = 0;
        const bool complete = SerializeEvent<true>(task, event, &bytesSerialized);

        Y_VERIFY(0 < bytesSerialized && bytesSerialized <= Max<ui16>());

        char buffer[partSize];
        TChannelPart *part = reinterpret_cast<TChannelPart*>(buffer);
        *part = {
            .ChannelFlags = static_cast<ui16>(ChannelId | TChannelPart::XdcFlag),
            .Size = static_cast<ui16>(partSize - sizeof(TChannelPart))
        };
        char *ptr = reinterpret_cast<char*>(part + 1);
        *ptr++ = static_cast<ui8>(EXdcCommand::PUSH_DATA);
        *reinterpret_cast<ui16*>(ptr) = bytesSerialized;
        ptr += sizeof(ui16);
        if (task.ChecksummingXxhash()) {
            XXH3_state_t state;
            XXH3_64bits_reset(&state);
            task.XdcStream.ScanLastBytes(bytesSerialized, [&state](TContiguousSpan span) {
                XXH3_64bits_update(&state, span.data(), span.size());
            });
            *reinterpret_cast<ui32*>(ptr) = XXH3_64bits_digest(&state);
        } else if (task.ChecksummingCrc32c()) {
            *reinterpret_cast<ui32*>(ptr) = task.ExternalChecksum;
        }

        task.WriteBookmark(std::move(partBookmark), buffer, partSize);

        *weightConsumed += partSize + bytesSerialized;
        OutputQueueSize -= bytesSerialized;

        return complete;
    }

    void TEventOutputChannel::NotifyUndelivered() {
        LOG_DEBUG_IC_SESSION("ICOCH89", "Notyfying about Undelivered messages! NotYetConfirmed size: %zu, Queue size: %zu", NotYetConfirmed.size(), Queue.size());
        if (State == EState::BODY && Queue.front().Event) {
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
