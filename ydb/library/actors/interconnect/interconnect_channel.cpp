#include "interconnect_channel.h"

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/probes.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/prof/tag.h>
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

        Y_ABORT_UNLESS(SerializationInfo);
        const ui32 flags = (event.Descr.Flags & ~(IEventHandle::FlagForwardOnNondelivery | IEventHandle::FlagSubscribeOnSession)) |
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
            Y_ABORT_UNLESS(!Queue.empty());
            TEventHolder& event = Queue.front();

            switch (State) {
                case EState::INITIAL:
                    event.InitChecksum();
                    if (event.Buffer) {
                        State = EState::BODY;
                        Iter = event.Buffer->GetBeginIter();
                        SerializationInfo = &event.Buffer->GetSerializationInfo();
                        SectionIndex = 0;
                        PartLenRemain = 0;
                    } else if (event.Event) {
                        State = EState::BODY;
                        IEventBase *base = event.Event.Get();
                        if (event.EventSerializedSize) {
                            Chunker.SetSerializingEvent(base);
                        }
                        SerializationInfoContainer = base->CreateSerializationInfo();
                        SerializationInfo = &SerializationInfoContainer;
                        SectionIndex = 0;
                        PartLenRemain = 0;
                    } else { // event without buffer and IEventBase instance
                        State = EState::DESCRIPTOR;
                        SerializationInfoContainer = {};
                        SerializationInfo = &SerializationInfoContainer;
                    }
                    if (!event.EventSerializedSize) {
                        State = EState::DESCRIPTOR;
                    } else if (Params.UseExternalDataChannel && !SerializationInfo->Sections.empty()) {
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
                        Y_ABORT_UNLESS(totalSectionSize == event.EventSerializedSize);
                    }

                    while (SectionIndex != SerializationInfo->Sections.size()) {
                        char sectionInfo[1 + NInterconnect::NDetail::MaxNumberBytes * 4];
                        char *p = sectionInfo;

                        const auto& section = SerializationInfo->Sections[SectionIndex];
                        char& type = *p++;
                        type = static_cast<ui8>(EXdcCommand::DECLARE_SECTION);
                        p += NInterconnect::NDetail::SerializeNumber(section.Headroom, p);
                        p += NInterconnect::NDetail::SerializeNumber(section.Size, p);
                        p += NInterconnect::NDetail::SerializeNumber(section.Tailroom, p);
                        p += NInterconnect::NDetail::SerializeNumber(section.Alignment, p);
                        if (section.IsInline && Params.UseXdcShuffle) {
                            type = static_cast<ui8>(EXdcCommand::DECLARE_SECTION_INLINE);
                        }
                        Y_ABORT_UNLESS(p <= std::end(sectionInfo));

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
                        SectionIndex = 0;
                        PartLenRemain = 0;
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
            Y_DEBUG_ABORT_UNLESS(len <= PartLenRemain);
            PartLenRemain -= len;

            event.EventActuallySerialized += len;
            if (event.EventActuallySerialized > MaxSerializedEventSize) {
                throw TExSerializedEventTooLarge(event.Descr.Type);
            }
        };

        bool complete = false;
        if (event.Event) {
            while (!complete) {
                TMutableContiguousSpan out = task.AcquireSpanForWriting<External>().SubSpan(0, PartLenRemain);
                if (!out.size()) {
                    break;
                }
                for (const auto& [buffer, size] : Chunker.FeedBuf(out.data(), out.size())) {
                    addChunk(buffer, size, false);
                }
                complete = Chunker.IsComplete();
                if (complete) {
                    Y_ABORT_UNLESS(Chunker.IsSuccessfull());
                }
            }
        } else if (event.Buffer) {
            while (const size_t numb = Min<size_t>(External ? task.GetExternalFreeAmount() : task.GetInternalFreeAmount(),
                    Iter.ContiguousSize(), PartLenRemain)) {
                const char *obuf = Iter.ContiguousData();
                addChunk(obuf, numb, true);
                Iter += numb;
            }
            complete = !Iter.Valid();
        } else {
            Y_ABORT();
        }
        Y_ABORT_UNLESS(!complete || event.EventActuallySerialized == event.EventSerializedSize,
            "EventActuallySerialized# %" PRIu32 " EventSerializedSize# %" PRIu32 " Type# 0x%08" PRIx32,
            event.EventActuallySerialized, event.EventSerializedSize, event.Descr.Type);

        return complete;
    }

    bool TEventOutputChannel::FeedPayload(TTcpPacketOutTask& task, TEventHolder& event, ui64 *weightConsumed) {
        for (;;) {
            // calculate inline or external part size (it may cover a few sections, not just single one)
            while (!PartLenRemain) {
                const auto& sections = SerializationInfo->Sections;
                if (!Params.UseExternalDataChannel || sections.empty()) {
                    // all data goes inline
                    IsPartInline = true;
                    PartLenRemain = Max<size_t>();
                } else if (!Params.UseXdcShuffle) {
                    // when UseXdcShuffle feature is not supported by the remote side, we transfer whole event over XDC
                    IsPartInline = false;
                    PartLenRemain = Max<size_t>();
                } else {
                    Y_ABORT_UNLESS(SectionIndex < sections.size());
                    IsPartInline = sections[SectionIndex].IsInline;
                    while (SectionIndex < sections.size() && IsPartInline == sections[SectionIndex].IsInline) {
                        PartLenRemain += sections[SectionIndex].Size;
                        ++SectionIndex;
                    }
                }
            }

            // serialize bytes
            const auto complete = IsPartInline
                ? FeedInlinePayload(task, event, weightConsumed)
                : FeedExternalPayload(task, event, weightConsumed);
            if (!complete) { // no space to serialize
                return false;
            } else if (*complete) { // event serialized
                return true;
            }
        }
    }

    std::optional<bool> TEventOutputChannel::FeedInlinePayload(TTcpPacketOutTask& task, TEventHolder& event, ui64 *weightConsumed) {
        if (task.GetInternalFreeAmount() <= sizeof(TChannelPart)) {
            return std::nullopt;
        }

        auto partBookmark = task.Bookmark(sizeof(TChannelPart));

        size_t bytesSerialized = 0;
        const bool complete = SerializeEvent<false>(task, event, &bytesSerialized);

        Y_DEBUG_ABORT_UNLESS(bytesSerialized);
        Y_ABORT_UNLESS(bytesSerialized <= Max<ui16>());

        TChannelPart part{
            .ChannelFlags = ChannelId,
            .Size = static_cast<ui16>(bytesSerialized)
        };

        task.WriteBookmark(std::move(partBookmark), &part, sizeof(part));
        *weightConsumed += sizeof(TChannelPart) + part.Size;
        OutputQueueSize -= part.Size;

        return complete;
    }

    std::optional<bool> TEventOutputChannel::FeedExternalPayload(TTcpPacketOutTask& task, TEventHolder& event, ui64 *weightConsumed) {
        const size_t partSize = sizeof(TChannelPart) + sizeof(ui8) + sizeof(ui16) + (Params.Encryption ? 0 : sizeof(ui32));
        if (task.GetInternalFreeAmount() < partSize || task.GetExternalFreeAmount() == 0) {
            return std::nullopt;
        }

        // clear external checksum for this chunk
        task.ExternalChecksum = 0;

        auto partBookmark = task.Bookmark(partSize);

        size_t bytesSerialized = 0;
        const bool complete = SerializeEvent<true>(task, event, &bytesSerialized);

        Y_ABORT_UNLESS(0 < bytesSerialized && bytesSerialized <= Max<ui16>());

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
            Y_ABORT_UNLESS(!Chunker.IsComplete()); // chunk must have an event being serialized
            Y_ABORT_UNLESS(!Queue.empty()); // this event must be the first event in queue
            TEventHolder& event = Queue.front();
            Y_ABORT_UNLESS(Chunker.GetCurrentEvent() == event.Event.Get()); // ensure the event is valid
            Chunker.Abort(); // stop serializing current event
            Y_ABORT_UNLESS(Chunker.IsComplete());
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
