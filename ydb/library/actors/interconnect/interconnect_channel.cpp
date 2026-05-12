#include "interconnect_channel.h"
#include "interconnect_zc_processor.h"
#include "rdma/mem_pool.h"

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/probes.h>
#include <ydb/library/actors/protos/interconnect.pb.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/prof/tag.h>
#include <ydb/library/actors/util/rc_buf.h>
#include <library/cpp/digest/crc32c/crc32c.h>

LWTRACE_USING(ACTORLIB_PROVIDER);

static void AddFakeCredRecord(NActorsInterconnect::TRdmaCreds& creds) noexcept {
    NActorsInterconnect::TRdmaCred* cred = creds.AddCreds();
    // fixed64, fixed32 - any value
    cred->SetAddress(0);
    cred->SetRkey(12345);
    // uint64 - protobuf uses VLC - max possible value 
    cred->SetSize(Max<ui64>());
}

// Calculate min size required to save one cred
static ui32 CalcRdmaCredsMinSizeSerialized() noexcept {
    NActorsInterconnect::TRdmaCreds tmp;
    AddFakeCredRecord(tmp);
    return tmp.ByteSizeLong();
}

namespace NActors {
    const ui32 TEventOutputChannel::RdmaCredsMinSizeSerialized = CalcRdmaCredsMinSizeSerialized();

    namespace {
        bool IsRdmaSectionLayoutConsistentWithData(const TEventHolder& event, ssize_t rdmaDeviceIndex) {
#ifdef NDEBUG
            Y_UNUSED(event);
            Y_UNUSED(rdmaDeviceIndex);
            return true;
#else
            Y_ABORT_UNLESS(event.Buffer);
            Y_ABORT_UNLESS(rdmaDeviceIndex >= 0);

            auto iter = event.Buffer->GetBeginIter();
            const auto& sections = event.Buffer->GetSerializationInfo().Sections;
            for (size_t sectionIndex = 0; sectionIndex < sections.size(); ++sectionIndex) {
                const auto& section = sections[sectionIndex];
                size_t bytesLeft = section.Size;
                size_t chunkIndex = 0;
                while (bytesLeft) {
                    Y_ABORT_UNLESS(iter.Valid());
                    TRcBuf buf = iter.GetChunk();
                    const char* data = iter.ContiguousData();
                    Y_ABORT_UNLESS(data >= buf.GetData() && data <= buf.GetData() + buf.GetSize());
                    const size_t offset = data - buf.GetData();
                    const size_t leftInChunk = buf.GetSize() - offset;
                    const size_t chunkSize = Min(leftInChunk, bytesLeft);

                    if (section.IsRdmaCapable) {
                        auto memReg = NInterconnect::NRdma::TryExtractFromRcBuf(buf);
                        if (memReg.Empty()) {
                            if (NActors::TlsActivationContext) {
                                LOG_WARN_S(*NActors::TlsActivationContext, NActorsServices::INTERCONNECT_SESSION,
                                    TStringBuilder() << "IsRdmaSectionLayoutConsistentWithData: RDMA preflight failed,"
                                        << " eventType# " << event.Descr.Type
                                        << " eventSerializedSize# " << event.EventSerializedSize
                                        << " bufferSize# " << event.Buffer->GetSize()
                                        << " sectionIndex# " << sectionIndex
                                        << " sectionSize# " << section.Size
                                        << " bytesLeft# " << bytesLeft
                                        << " chunkIndex# " << chunkIndex
                                        << " chunkOffset# " << offset
                                        << " chunkSize# " << chunkSize
                                        << " chunkBufSize# " << buf.GetSize()
                                        << " rdmaDeviceIndex# " << rdmaDeviceIndex
                                        << " sections# " << SerializeEventSections(event.Buffer->GetSerializationInfo()));
                            }
                            return false;
                        }
                        Y_UNUSED(memReg.GetRKey(rdmaDeviceIndex));
                    }

                    iter += chunkSize;
                    bytesLeft -= chunkSize;
                    ++chunkIndex;
                }
            }

            return true;
#endif
        }
    }

    bool TEventOutputChannel::FeedDescriptor(TTcpPacketOutTask& task, TEventHolder& event) {
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
        NSan::Unpoison(&descr.TraceId, sizeof(descr.TraceId));

        // and channel header before the descriptor
        TChannelPart part{
            .ChannelFlags = static_cast<ui16>(ChannelId | TChannelPart::LastPartFlag),
            .Size = sizeof(descr)
        };

        // append them to the packet
        task.Write<false>(&part, sizeof(part));
        task.Write<false>(&descr, sizeof(descr));
        OutputQueueSize -= sizeof(TEventDescr2);
        Metrics->UpdateOutputChannelEvents(ChannelId);

        return true;
    }

    void TEventOutputChannel::DropConfirmed(ui64 confirm, TEventHolderPool& pool) {
        LOG_DEBUG_IC_SESSION("ICOCH98", "Dropping confirmed messages");
        for (auto it = NotYetConfirmed.begin(); it != NotYetConfirmed.end() && it->Serial <= confirm; ) {
            pool.Release(NotYetConfirmed, it++);
        }
    }

    bool TEventOutputChannel::FeedBuf(TTcpPacketOutTask& task, ui64 serial, ssize_t rdmaDeviceIndex) {
        for (;;) {
            Y_ABORT_UNLESS(!Queue.empty());
            TEventHolder& event = Queue.front();

            switch (State) {
                case EState::INITIAL:
                    event.InitChecksum();
                    if (event.EnqueueTime) {
                        TDuration duration = NActors::TlsActivationContext->Now() - event.EnqueueTime;
                        Metrics->UpdateIcQueueTimeHistogram(duration.MicroSeconds());
                    }
                    event.Span && event.Span.Event("FeedBuf:INITIAL");
                    UseRdmaForCurrentEvent = false;
                    RdmaCredsBuffer.Clear();
                    RdmaCredPartPos = 0;
                    RdmaCredsPerByteAvg = 1.0 / RdmaCredsMinSizeSerialized;
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
                        SerializationInfoContainer = base->CreateSerializationInfo(Params.UseExternalDataChannel);
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
                        XXH3_64bits_reset(&RdmaCumulativeChecksumState);

                        bool hasRdmaSections = false;
                        // Check each section can be send via rdma
                        for (const auto& section : SerializationInfo->Sections) {
                            hasRdmaSections |= section.IsRdmaCapable;
                        }
                        if (hasRdmaSections && Params.UseXdcShuffle && Params.UseRdma && RdmaMemPool && rdmaDeviceIndex >= 0) {
                            if (SerializeEventRdma(event)) {
                                SerializationInfo = &event.Buffer->GetSerializationInfo();
                                UseRdmaForCurrentEvent = IsRdmaSectionLayoutConsistentWithData(event, rdmaDeviceIndex);
                                Chunker.DiscardEvent();
                            }
                        }
                    }
                    break;

                case EState::BODY:
                    if (FeedPayload(task, event, rdmaDeviceIndex)) {
                        State = EState::DESCRIPTOR;
                    } else {
                        return false;
                    }
                    break;

                case EState::DESCRIPTOR:
                    if (!FeedDescriptor(task, event)) {
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
                        if (UseRdmaForCurrentEvent && section.IsRdmaCapable) {
                            type = static_cast<ui8>(EXdcCommand::DECLARE_SECTION_RDMA);
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
                task.Append<External>(data, len, &event.ZcTransferId);
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
            "EventActuallySerialized# %" PRIu32 " EventSerializedSize# %" PRIu32 " Type# 0x%08" PRIx32 " External# %" PRIi32,
            event.EventActuallySerialized, event.EventSerializedSize, event.Descr.Type, (ui32)External);

        return complete;
    }

    bool TEventOutputChannel::FeedPayload(TTcpPacketOutTask& task, TEventHolder& event, ssize_t rdmaDeviceIndex) {
        if (UseRdmaForCurrentEvent) {
            Y_ABORT_UNLESS(rdmaDeviceIndex >= 0);
        }
        for (;;) {
            // calculate inline or external part size (it may cover a few sections, not just single one)
            while (!PartLenRemain && RdmaCredsBuffer.CredsSize() == 0) {
                const auto& sections = SerializationInfo->Sections;
                if (!Params.UseExternalDataChannel || sections.empty()) {
                    // all data goes inline
                    IsPartInline = true;
                    IsPartRdma = false;
                    PartLenRemain = Max<size_t>();
                } else if (!Params.UseXdcShuffle) {
                    // when UseXdcShuffle feature is not supported by the remote side, we transfer whole event over XDC
                    IsPartInline = false;
                    IsPartRdma = false;
                    PartLenRemain = Max<size_t>();
                } else {
                    Y_ABORT_UNLESS(SectionIndex < sections.size());
                    IsPartInline = sections[SectionIndex].IsInline;
                    IsPartRdma = sections[SectionIndex].IsRdmaCapable;
                    while (SectionIndex < sections.size()
                            && IsPartInline == sections[SectionIndex].IsInline
                            && IsPartRdma == sections[SectionIndex].IsRdmaCapable) {
                        PartLenRemain += sections[SectionIndex].Size;
                        ++SectionIndex;
                    }
                }
            }

            // serialize bytes
            std::optional<bool> complete = false;
            if (IsPartInline) {
                complete = FeedInlinePayload(task, event);
            } else if (UseRdmaForCurrentEvent && IsPartRdma) {
                complete = FeedRdmaPayload(task, event, rdmaDeviceIndex, task.Params.ChecksumRdmaEvent);
            } else {
                complete = FeedExternalPayload(task, event);
            }
            if (!complete) { // no space to serialize
                return false;
            } else if (*complete) { // event serialized
                return true;
            }
        }
    }

    std::optional<bool> TEventOutputChannel::FeedInlinePayload(TTcpPacketOutTask& task, TEventHolder& event) {
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
        OutputQueueSize -= part.Size;

        return complete;
    }

    bool TEventOutputChannel::SerializeEventRdma(TEventHolder& event) {
        if (!event.Buffer && event.Event) {
            std::optional<TRope> rope = event.Event->SerializeToRope(GetDefaultRcBufAllocator());
            if (!rope) {
                return false; // serialization failed
            }
            event.Buffer = MakeIntrusive<TEventSerializedData>(
                std::move(*rope), event.Event->CreateSerializationInfo(Params.UseExternalDataChannel)
            );
            event.Event = nullptr;
            Iter = event.Buffer->GetBeginIter();
        }
        return true;
    }

    std::optional<bool> TEventOutputChannel::FeedRdmaPayload(TTcpPacketOutTask& task, TEventHolder& event, ssize_t rdmaDeviceIndex, bool checksumming) {
        Y_ABORT_UNLESS(rdmaDeviceIndex >= 0);

        Y_ABORT_UNLESS(event.Buffer);
        if (RdmaCredsBuffer.CredsSize() == 0) {
            for (; Iter.Valid() && PartLenRemain; ) {
                TRcBuf buf = Iter.GetChunk();
                auto memReg = NInterconnect::NRdma::TryExtractFromRcBuf(buf);
                if (memReg.Empty()) {
                    Y_ABORT_UNLESS(false, "RDMA section contains a non-RDMA chunk after RDMA preflight");
                }

                const char* data = Iter.ContiguousData();
                Y_ABORT_UNLESS(data >= buf.GetData() && data <= buf.GetData() + buf.GetSize());
                const size_t offset = data - buf.GetData();
                const size_t leftInChunk = buf.GetSize() - offset;
                const size_t chunkSize = Min(leftInChunk, PartLenRemain);

                if (checksumming) {
                    XXH3_64bits_update(&RdmaCumulativeChecksumState, data, chunkSize);
                }
                auto cred = RdmaCredsBuffer.AddCreds();
                cred->SetAddress(reinterpret_cast<ui64>(memReg.GetAddr()) + offset);
                cred->SetSize(chunkSize);
                cred->SetRkey(memReg.GetRKey(rdmaDeviceIndex));

                event.EventActuallySerialized += chunkSize;
                PartLenRemain -= chunkSize;
                Iter += chunkSize;
            }
        }
        Y_ABORT_UNLESS(PartLenRemain == 0);

        if (RdmaCredsBuffer.CredsSize() == 0) {
            RdmaCredPartPos = 0;
            return !Iter.Valid();
        }

        // Part = | TChannelPart | EXdcCommand::RDMA_READ | rdmaCreds.Size | rdmaCreds | checkSum |
        const size_t fixedPartSize = sizeof(TChannelPart) + sizeof(ui8) + sizeof(ui16) + sizeof(ui32);
        const size_t maxSerializedPart = Min<size_t>(task.GetInternalFreeAmount(), Max<ui16>());
        const ui32 minThreshold = fixedPartSize + RdmaCredsMinSizeSerialized;
        if (maxSerializedPart < minThreshold) {
            return std::nullopt;
        }

        auto calcPartCredLen = [] (size_t freeAmount, float credsPerByteAvg) -> size_t {
            return (freeAmount - fixedPartSize) * credsPerByteAvg;
        };

        const NActorsInterconnect::TRdmaCreds* rdmaCreds = &RdmaCredsBuffer;
        NActorsInterconnect::TRdmaCreds tmpCreds;

        bool lastPart = true;
        size_t partSize = 0;
        size_t credsSerializedSize = 0;

        size_t curPartCredLen = 0;
        for (;;) {
            if (Y_UNLIKELY(curPartCredLen || RdmaCredPartPos)) {
                if (!curPartCredLen) {
                    curPartCredLen = calcPartCredLen(maxSerializedPart, RdmaCredsPerByteAvg);
                    if (!curPartCredLen) {
                        return std::nullopt;
                    }
                }
                if (RdmaCredPartPos + curPartCredLen >= RdmaCredsBuffer.CredsSize()) {
                    curPartCredLen = RdmaCredsBuffer.CredsSize() - RdmaCredPartPos;
                    lastPart = true;
                } else {
                    lastPart = false;
                }

                tmpCreds.Clear();
                for (size_t i = 0, j = RdmaCredPartPos; i < curPartCredLen; ++i, ++j) {
                    tmpCreds.AddCreds()->CopyFrom(RdmaCredsBuffer.GetCreds(j));
                }
                rdmaCreds = &tmpCreds;
            }

            credsSerializedSize = rdmaCreds->ByteSizeLong();
            partSize = fixedPartSize + credsSerializedSize;

            if (Y_UNLIKELY(partSize > maxSerializedPart)) {
                RdmaCredsPerByteAvg = rdmaCreds->CredsSize() / (double)credsSerializedSize;
                size_t newLen = calcPartCredLen(maxSerializedPart, RdmaCredsPerByteAvg);
                if (newLen >= curPartCredLen) {
                    curPartCredLen = curPartCredLen ? curPartCredLen - 1 : newLen;
                } else {
                    curPartCredLen = newLen;
                }

                if (!curPartCredLen) {
                    return std::nullopt;
                }
            } else {
                if (Y_UNLIKELY(RdmaCredPartPos == 0 && curPartCredLen != 0)) {
                    Metrics->IncRdmaMultipartEvents();
                }
                RdmaCredPartPos += curPartCredLen;
                break;
            }
        }

        char buffer[partSize];
        TChannelPart *part = reinterpret_cast<TChannelPart*>(buffer);
        *part = {
            .ChannelFlags = static_cast<ui16>(ChannelId | TChannelPart::XdcFlag),
            .Size = static_cast<ui16>(partSize - sizeof(TChannelPart))
        };
        char *ptr = reinterpret_cast<char*>(part + 1);
        *ptr++ = static_cast<ui8>(EXdcCommand::RDMA_READ);
        WriteUnaligned<ui16>(ptr, credsSerializedSize);
        ptr += sizeof(ui16);

        ui32 payloadSz = 0;
        for (const auto& rdmaCred : rdmaCreds->GetCreds()) {
            payloadSz += rdmaCred.GetSize();
        }

        Y_ABORT_UNLESS(rdmaCreds->SerializePartialToArray(ptr, credsSerializedSize));
        ptr += credsSerializedSize;
        WriteUnaligned<ui32>(ptr, checksumming ? XXH3_64bits_digest(&RdmaCumulativeChecksumState) : 0);
        OutputQueueSize -= payloadSz;

        task.Write<false>(buffer, partSize);

        task.AttachRdmaPayloadSize(payloadSz);

        if (lastPart) {
            RdmaCredsBuffer.Clear();
            RdmaCredPartPos = 0;
            RdmaCredsPerByteAvg = 1.0 / RdmaCredsMinSizeSerialized;
        }

        return lastPart ? !Iter.Valid() : false;
    }

    std::optional<bool> TEventOutputChannel::FeedExternalPayload(TTcpPacketOutTask& task, TEventHolder& event) {
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

        WriteUnaligned<ui16>(ptr, bytesSerialized);
        ptr += sizeof(ui16);
        if (task.ChecksummingXxhash()) {
            XXH3_state_t state;
            XXH3_64bits_reset(&state);
            task.XdcStream.ScanLastBytes(bytesSerialized, [&state](TContiguousSpan span) {
                XXH3_64bits_update(&state, span.data(), span.size());
            });
            const ui32 cs = XXH3_64bits_digest(&state);
            WriteUnaligned<ui32>(ptr, cs);
        } else if (task.ChecksummingCrc32c()) {
            WriteUnaligned<ui32>(ptr, task.ExternalChecksum);
        }

        task.WriteBookmark(std::move(partBookmark), buffer, partSize);
        OutputQueueSize -= bytesSerialized;

        return complete;
    }

    void TEventOutputChannel::ProcessUndelivered(TEventHolderPool& pool, NInterconnect::IZcGuard* zg) {
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

        // Events in the NotYetConfirmed may be actualy not sended by kernel.
        // In case of enabled ZC we need to wait kernel send task to be completed before reusing buffers
        if (zg) {
            zg->ExtractToSafeTermination(NotYetConfirmed);
        }
        pool.Release(NotYetConfirmed);
        for (auto& item : Queue) {
            item.ForwardOnNondelivery(false);
        }
        pool.Release(Queue);
    }
}
