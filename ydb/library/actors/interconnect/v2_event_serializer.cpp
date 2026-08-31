#include "v2_event_serializer.h"

#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/rope.h>
#include <ydb/library/actors/protos/interconnect.pb.h>

#include <cstring>
#include <util/string/builder.h>
#include <util/string/hex.h>

namespace NActors {

    TRcBuf AllocateXdcSectionBuffer(size_t size, size_t headroom, size_t tailroom, size_t alignment) {
        if (alignment > 1) {
            Y_DEBUG_ABORT_UNLESS((alignment & (alignment - 1)) == 0);
            // Align the payload data pointer itself. TRopeAlignedBuffer gives us a 16-byte aligned base
            // buffer, but headroom may still shift the visible data away from the requested alignment, so
            // we always keep up to alignment - 1 bytes of extra slack and spend part of it as additional
            // headroom.
            const size_t extra = alignment - 1;
            TRcBuf buffer = TRcBuf(TRopeAlignedBuffer::Allocate(size + headroom + tailroom + extra));
            const uintptr_t ptr = reinterpret_cast<uintptr_t>(buffer.GetData()) + headroom;
            const size_t misalignment = ptr & (alignment - 1);
            const size_t shift = misalignment ? alignment - misalignment : 0;
            tailroom += extra - shift;
            buffer.TrimFront(size + tailroom);
            buffer.TrimBack(size);
            Y_DEBUG_ABORT_UNLESS(reinterpret_cast<uintptr_t>(buffer.GetData()) % alignment == 0);
            return buffer;
        }
        return TRcBuf::Uninitialized(size, headroom, tailroom);
    }

    TEventSerializer::TEventSerializer(bool checksumming, bool useExternalDataChannel)
        : Checksumming(checksumming)
        , UseExternalDataChannel(useExternalDataChannel)
    {}

    void TEventSerializer::Push(std::unique_ptr<IEventHandle> ev) {
        const ui16 channel = ev->GetChannel();
        if (GetQueue(channel).Events.Push(std::move(ev))) {
            // place this new quota into non-zero part of the heap
            Y_ABORT_UNLESS(channel != TChunkHeader::SystemChannel);
            PerChannelQuotaHeap.push_back(TPerChannelQuota{
                .Channel = channel,
                .Quota = DefaultQuota,
            });
            std::ranges::push_heap(PerChannelQuotaHeap, std::less<ui16>{}, &TPerChannelQuota::Quota);
        }
    }

    void TEventSerializer::Push(NActorsInterconnect::TSystemPayloadV2& systemRequest) {
        TString s;
        const bool success = systemRequest.SerializeToString(&s);
        Y_ABORT_UNLESS(success);
        SystemChannelQueue.SystemRequests.push_back(TRcBuf(std::move(s)));
        if (SystemChannelQueue.SystemRequests.size() == 1) {
            PerChannelQuotaHeap.push_back(TPerChannelQuota{
                .Channel = TChunkHeader::SystemChannel,
                .Quota = Max<ui16>(),
            });
            std::ranges::push_heap(PerChannelQuotaHeap, std::less<ui16>{}, &TPerChannelQuota::Quota);
        }
    }

    size_t TEventSerializer::ProduceOutputStream(TRcBuf& buffer, std::vector<TContiguousSpan> *out,
            size_t maxBytesToProduce) {
        return ProduceOutputStream(buffer, out, nullptr, nullptr, maxBytesToProduce);
    }

    size_t TEventSerializer::ProduceOutputStream(TRcBuf& buffer, std::vector<TContiguousSpan> *out,
            TRcBuf *xdcBuffer, std::vector<TContiguousSpan> *xdcOut, size_t maxBytesToProduce) {
        size_t totalBytesProduced = 0;
        ui64 mainBufferProduced = 0;
        ui64 xdcBufferProduced = 0;

        Y_ABORT_UNLESS(buffer.size() >= TEventSerializer::MinUsefulQuota);

        const ui64 mainProducedOnEntry = CumulativeProducedMain;
        const ui64 xdcProducedOnEntry = CumulativeProducedXdc;
        const size_t bufferSizeOnEntry = buffer.size();
        const size_t xdcBufferSizeOnEntry = xdcBuffer ? xdcBuffer->size() : 0;

        TStreamState main{
            .Buffer = buffer.UnsafeGetContiguousSpanMut(),
            .BufferOrig = buffer.UnsafeGetContiguousSpanMut(),
            .Out = out,
            .LastSpanEnd = out->empty() ? nullptr : out->back().data() + out->back().size(),
            .CumulativeProduced = &CumulativeProducedMain,
            .BufferProduced = &mainBufferProduced,
            .Enabled = true,
        };

        TStreamState xdcState;
        TStreamState *xdc = nullptr;
        if (xdcOut) {
            xdcState.Out = xdcOut;
            xdcState.LastSpanEnd = xdcOut->empty() ? nullptr : xdcOut->back().data() + xdcOut->back().size();
            xdcState.CumulativeProduced = &CumulativeProducedXdc;
            xdcState.BufferProduced = &xdcBufferProduced;
            xdcState.Enabled = true;
            if (xdcBuffer && xdcBuffer->size()) {
                xdcState.Buffer = xdcBuffer->UnsafeGetContiguousSpanMut();
                xdcState.BufferOrig = xdcState.Buffer;
            }
            xdc = &xdcState;
        }

        while (!PerChannelQuotaHeap.empty()) {
            if (PerChannelQuotaHeap.front().Quota < MinUsefulQuota) {
                for (auto& item : PerChannelQuotaHeap) {
                    Y_ABORT_UNLESS(item.Channel != TChunkHeader::SystemChannel);
                    item.Quota += DefaultQuota;
                }
            }

            TPerChannelQuota& q = PerChannelQuotaHeap.front();
            TPerChannelQueue& queue = GetQueue(q.Channel);
            const bool isSystemChannel = q.Channel == TChunkHeader::SystemChannel;
            const size_t numBytesProduced = ProduceOutputStreamForQueue(q.Channel, queue,
                Min<size_t>(maxBytesToProduce, q.Quota), main, xdc);
            if (!numBytesProduced) {
                break;
            }
            Y_ABORT_UNLESS(numBytesProduced <= q.Quota);
            totalBytesProduced += numBytesProduced;
            maxBytesToProduce -= numBytesProduced;

            std::ranges::pop_heap(PerChannelQuotaHeap, std::less<ui16>{}, &TPerChannelQuota::Quota);
            if (!queue.Events.Peek() && queue.SystemRequests.empty()) {
                PerChannelQuotaHeap.pop_back();
            } else {
                PerChannelQuotaHeap.back().Quota -= isSystemChannel ? 0 : numBytesProduced;
                std::ranges::push_heap(PerChannelQuotaHeap, std::less<ui16>{}, &TPerChannelQuota::Quota);
            }
        }

        Y_DEBUG_ABORT_UNLESS(mainProducedOnEntry + xdcProducedOnEntry + totalBytesProduced
            == CumulativeProducedMain + CumulativeProducedXdc);

        buffer.TrimFront(main.Buffer.size() - main.Buffer.size() % 64);
        const size_t scratchBytesUsed = bufferSizeOnEntry - buffer.size();

        if (mainBufferProduced) {
            RefcountItems.push_back({
                .MainEndOffset = mainBufferProduced,
                .XdcEndOffset = 0,
                .Scratch = buffer,
                .ScratchBytesUsed = scratchBytesUsed,
                .EventReceivedTimestamp = 0,
            });
            NumBytesInScratchBuffers += scratchBytesUsed;
        } else {
            Y_DEBUG_ABORT_UNLESS(scratchBytesUsed == 0);
        }

        if (xdc && xdcBuffer) {
            xdcBuffer->TrimFront(xdcState.Buffer.size() - xdcState.Buffer.size() % 64);
            const size_t xdcScratchUsed = xdcBufferSizeOnEntry - xdcBuffer->size();
            if (xdcBufferProduced) {
                RefcountItems.push_back({
                    .MainEndOffset = 0,
                    .XdcEndOffset = xdcBufferProduced,
                    .Scratch = *xdcBuffer,
                    .ScratchBytesUsed = xdcScratchUsed,
                    .EventReceivedTimestamp = 0,
                });
                NumBytesInScratchBuffers += xdcScratchUsed;
            } else {
                Y_DEBUG_ABORT_UNLESS(xdcScratchUsed == 0);
            }
        }

        return totalBytesProduced;
    }

    void TEventSerializer::CommitProducedBytes(size_t numMainBytes, size_t numXdcBytes,
            std::vector<ui64> *eventToWireTime,
            std::vector<std::unique_ptr<IEventBase>> *events,
            std::vector<TIntrusivePtr<TEventSerializedData>> *buffers) {
        CumulativeCommittedMain += numMainBytes;
        CumulativeCommittedXdc += numXdcBytes;
        Y_ABORT_UNLESS(CumulativeCommittedMain <= CumulativeProducedMain);
        Y_ABORT_UNLESS(CumulativeCommittedXdc <= CumulativeProducedXdc);
        const ui64 timestamp = GetCycleCountFast();
        while (!RefcountItems.empty()
                && RefcountItems.front().MainEndOffset <= CumulativeCommittedMain
                && RefcountItems.front().XdcEndOffset <= CumulativeCommittedXdc) {
            auto& front = RefcountItems.front();
            if (Y_LIKELY(eventToWireTime) && front.EventReceivedTimestamp) {
                eventToWireTime->push_back(timestamp - front.EventReceivedTimestamp);
            }
            NumBytesInScratchBuffers -= front.ScratchBytesUsed;
            if (events && front.Event) {
                events->push_back(std::move(front.Event));
            }
            if (buffers && front.Buffer) {
                buffers->push_back(std::move(front.Buffer));
            }
            RefcountItems.pop_front();
        }
    }

    bool TEventSerializer::HasExternalSections(const TEventSerializationInfo *info) {
        if (!info) {
            return false;
        }
        for (const auto& s : info->Sections) {
            if (!s.IsInline) {
                return true;
            }
        }
        return false;
    }

    void TEventSerializer::EnsureSection(TPerChannelQueue& queue) {
        if (!queue.UseXdcForEvent) {
            queue.CurrentIsInline = true;
            queue.SectionBytesRemain = Max<size_t>();
            return;
        }
        const auto& sections = queue.EvSerInfo->Sections;
        while (!queue.SectionBytesRemain && queue.SectionIndex < sections.size()) {
            queue.CurrentIsInline = sections[queue.SectionIndex].IsInline;
            while (queue.SectionIndex < sections.size()
                    && sections[queue.SectionIndex].IsInline == queue.CurrentIsInline) {
                queue.SectionBytesRemain += sections[queue.SectionIndex].Size;
                ++queue.SectionIndex;
            }
        }
    }

    void TEventSerializer::ResetEventState(TPerChannelQueue& queue) {
        if (queue.CoroutineChunkSerializer.GetCurrentEvent()) {
            queue.CoroutineChunkSerializer.Abort();
        }
        queue.SerializeStage = ESerializeStage::kInitial;
        queue.EventHeaderOffset = 0;
        queue.UseXdcForEvent = false;
        queue.CurrentIsInline = true;
        queue.SectionIndex = 0;
        queue.SectionBytesRemain = 0;
        queue.XdcDeclareIndex = 0;
        queue.EvSerInfo = nullptr;
        queue.EvSerInfoHolder = {};
        queue.Buffer = nullptr;
    }

    size_t TEventSerializer::ProduceOutputStreamForQueue(ui16 channel, TPerChannelQueue& queue, size_t maxBytesToProduce,
            TStreamState& main, TStreamState *xdc) {
        size_t numBytesProduced = 0;

        auto produceOutputSpan = [&](TStreamState& st, TContiguousSpan span, bool addToChecksum) {
            if (Y_UNLIKELY(addToChecksum)) {
                XXH3_64bits_update(&queue.ChecksumState, span.data(), span.size());
            }

            bool fromBuffer = st.BufferOrig.data() && span.data() >= st.BufferOrig.data()
                && span.data() + span.size() <= st.BufferOrig.data() + st.BufferOrig.size();

            if (!fromBuffer && (reinterpret_cast<uintptr_t>(span.data()) & 63) + span.size() <= 64
                    && st.Buffer.size() >= span.size()) {
                memcpy(st.Buffer.data(), span.data(), span.size());
                span = {st.Buffer.data(), span.size()};
                st.Buffer = st.Buffer.SubSpan(span.size(), Max<size_t>());
                fromBuffer = true;
            }

            Y_ABORT_UNLESS(span.size() <= maxBytesToProduce);
            maxBytesToProduce -= span.size();
            numBytesProduced += span.size();
            *st.CumulativeProduced += span.size();
            if (st.LastSpanEnd != span.data()) {
                st.Out->push_back(span);
            } else {
                Y_DEBUG_ABORT_UNLESS(!st.Out->empty());
                TContiguousSpan& lastSpan = st.Out->back();
                lastSpan = {lastSpan.data(), lastSpan.size() + span.size()};
            }
            st.LastSpanEnd = span.data() + span.size();

            (fromBuffer ? BytesCopied : BytesAliased) += span.size();

            if (fromBuffer) {
                Y_DEBUG_ABORT_UNLESS(*st.BufferProduced < *st.CumulativeProduced);
                *st.BufferProduced = *st.CumulativeProduced;
            }
        };

        auto takeInBuffer = [&](size_t numBytes) -> void* {
            Y_ABORT_UNLESS(numBytes <= maxBytesToProduce);
            Y_ABORT_UNLESS(numBytes <= main.Buffer.size());
            TMutableContiguousSpan res(main.Buffer.data(), numBytes);
            main.Buffer = main.Buffer.SubSpan(numBytes, Max<size_t>());
            produceOutputSpan(main, res, false);
            return res.data();
        };

        while (Y_UNLIKELY(!queue.SystemRequests.empty())) {
            auto& request = queue.SystemRequests.front();
            if (maxBytesToProduce < sizeof(TChunkHeader) + request.size() || main.Buffer.size() < sizeof(TChunkHeader)) {
                break;
            }
            *static_cast<TChunkHeader*>(takeInBuffer(sizeof(TChunkHeader))) = {
                .Length = static_cast<ui16>(request.size()),
                .TypeChannel = TChunkHeader::kSystem,
            };
            produceOutputSpan(main, {request.data(), request.size()}, false);
            RefcountItems.push_back({
                .MainEndOffset = CumulativeProducedMain,
                .XdcEndOffset = 0,
                .Scratch = std::move(request),
                .EventReceivedTimestamp = 0,
            });
            queue.SystemRequests.pop_front();
        }

        while (Min(main.Buffer.size(), maxBytesToProduce) >= MinUsefulQuota && queue.Events.Peek()) {
            IEventHandle& ev = *queue.Events.Peek();

            TChunkHeader *header = nullptr;
            auto ensureHeader = [&] {
                if (!header) {
                    header = static_cast<TChunkHeader*>(takeInBuffer(sizeof(TChunkHeader)));
                    *header = {
                        .Length = 0,
                        .TypeChannel = static_cast<ui16>(channel | TChunkHeader::kEventChunk),
                    };
                }
            };
            auto addEventChunkBytes = [&](const char *ptr, size_t numBytes) {
                ensureHeader();
                Y_DEBUG_ABORT_UNLESS(numBytes);
                Y_DEBUG_ABORT_UNLESS(header->Length + numBytes <= Max<ui16>());
                header->Length += numBytes;
                produceOutputSpan(main, {ptr, numBytes}, Checksumming);

                Y_ABORT_UNLESS(numBytes <= queue.SerializedBytesPending, "Type# 0x%08" PRIx32
                    " SerializedBytesPending# %zu CalculateSerializedSize# %zu CalculateSerializedSizeCached# %zu",
                    ev.Type, queue.SerializedBytesPending, ev.GetBase()->CalculateSerializedSize(),
                    ev.GetBase()->CalculateSerializedSizeCached());

                queue.SerializedBytesPending -= numBytes;
                if (queue.UseXdcForEvent) {
                    Y_ABORT_UNLESS(numBytes <= queue.SectionBytesRemain);
                    queue.SectionBytesRemain -= numBytes;
                }
            };

            auto emitXdcPush = [&](TContiguousSpan payload) {
                Y_ABORT_UNLESS(xdc);
                Y_ABORT_UNLESS(payload.size());
                Y_ABORT_UNLESS(payload.size() <= Max<ui16>());
                header = nullptr;
                *static_cast<TChunkHeader*>(takeInBuffer(sizeof(TChunkHeader))) = {
                    .Length = sizeof(ui16),
                    .TypeChannel = static_cast<ui16>(channel | TChunkHeader::kXdcPush),
                };
                const ui16 n = static_cast<ui16>(payload.size());
                memcpy(takeInBuffer(sizeof(ui16)), &n, sizeof(n));
                produceOutputSpan(*xdc, payload, Checksumming);
                Y_ABORT_UNLESS(payload.size() <= queue.SerializedBytesPending);
                queue.SerializedBytesPending -= payload.size();
                Y_ABORT_UNLESS(payload.size() <= queue.SectionBytesRemain);
                queue.SectionBytesRemain -= payload.size();
            };

            auto bodyFinished = [&] {
                if (queue.UseXdcForEvent) {
                    EnsureSection(queue);
                    return queue.SectionIndex == queue.EvSerInfo->Sections.size() && !queue.SectionBytesRemain;
                }
                return queue.SerializedBytesPending == 0;
            };

            const auto stageOnEntry = queue.SerializeStage;
            const size_t producedOnEntry = numBytesProduced;

            switch (queue.SerializeStage) {
                case ESerializeStage::kInitial:
                    if (ev.HasBuffer()) {
                        queue.SerializeStage = ESerializeStage::kBufferSerializer;
                        queue.Buffer = ev.ReleaseChainBuffer();
                        queue.Iter = queue.Buffer->GetBeginIter();
                        queue.EvSerInfo = &queue.Buffer->GetSerializationInfo();
                        queue.SerializedBytesPending = queue.Buffer->GetSize();
                    } else if (ev.HasEvent()) {
                        IEventBase *event = ev.GetBase();
                        queue.SerializeStage = ESerializeStage::kChunkSerializer;
                        queue.CoroutineChunkSerializer.SetSerializingEvent(event, /*withCachedSizes=*/ true,
                            /*withCords=*/ true);
                        queue.EvSerInfoHolder = event->CreateSerializationInfo(UseExternalDataChannel);
                        queue.EvSerInfo = &queue.EvSerInfoHolder;
                        queue.SerializedBytesPending = event->CalculateSerializedSizeCached();
                    } else {
                        queue.SerializeStage = ESerializeStage::kHeader;
                        queue.EvSerInfoHolder = {};
                        queue.EvSerInfo = &queue.EvSerInfoHolder;
                        queue.SerializedBytesPending = 0;
                    }
                    if (Checksumming) {
                        XXH3_64bits_reset(&queue.ChecksumState);
                    }

                    queue.EventHeader = {
                        .Type = ev.Type,
                        .Flags = ev.Flags | (queue.EvSerInfo->IsExtendedFormat ? IEventHandle::FlagExtendedFormat : 0),
                        .Cookie = ev.Cookie,
                        .Checksum = 0,
                        .Sender = ev.Sender,
                        .Recipient = ev.Recipient,
                    };
                    ev.TraceId.Serialize(&queue.EventHeader.TraceId);

                    queue.UseXdcForEvent = UseExternalDataChannel && xdc && HasExternalSections(queue.EvSerInfo);
                    queue.SectionIndex = 0;
                    queue.SectionBytesRemain = 0;
                    queue.XdcDeclareIndex = 0;
                    if (queue.UseXdcForEvent) {
                        queue.SerializeStage = ESerializeStage::kXdcDeclare;
                    }
                    break;

                case ESerializeStage::kXdcDeclare: {
                    const auto& sections = queue.EvSerInfo->Sections;
                    while (queue.XdcDeclareIndex < sections.size()) {
                        if (maxBytesToProduce < sizeof(TChunkHeader) + sizeof(TXdcSection)
                                || main.Buffer.size() < sizeof(TChunkHeader) + sizeof(TXdcSection)) {
                            break;
                        }
                        const size_t remain = (sections.size() - queue.XdcDeclareIndex) * sizeof(TXdcSection);
                        size_t n = Min(remain, maxBytesToProduce - sizeof(TChunkHeader),
                            main.Buffer.size() - sizeof(TChunkHeader), size_t(Max<ui16>()));
                        n = n / sizeof(TXdcSection) * sizeof(TXdcSection);
                        if (!n) {
                            break;
                        }
                        *static_cast<TChunkHeader*>(takeInBuffer(sizeof(TChunkHeader))) = {
                            .Length = static_cast<ui16>(n),
                            .TypeChannel = static_cast<ui16>(channel | TChunkHeader::kXdcDeclare),
                        };
                        char *p = static_cast<char*>(takeInBuffer(n));
                        const size_t count = n / sizeof(TXdcSection);
                        for (size_t i = 0; i < count; ++i) {
                            const auto& s = sections[queue.XdcDeclareIndex + i];
                            TXdcSection rec{
                                .Headroom = static_cast<ui32>(s.Headroom),
                                .Size = static_cast<ui32>(s.Size),
                                .Tailroom = static_cast<ui32>(s.Tailroom),
                                .Alignment = static_cast<ui32>(s.Alignment),
                                .Flags = static_cast<ui8>(s.IsInline ? TXdcSection::FlagInline : 0),
                            };
                            memcpy(p + i * sizeof(TXdcSection), &rec, sizeof(rec));
                        }
                        queue.XdcDeclareIndex += count;
                    }
                    if (queue.XdcDeclareIndex == sections.size()) {
                        queue.SectionIndex = 0;
                        queue.SectionBytesRemain = 0;
                        if (queue.Buffer) {
                            queue.SerializeStage = ESerializeStage::kBufferSerializer;
                        } else if (queue.CoroutineChunkSerializer.GetCurrentEvent()) {
                            queue.SerializeStage = ESerializeStage::kChunkSerializer;
                        } else {
                            queue.SerializeStage = ESerializeStage::kHeader;
                        }
                    }
                    break;
                }

                case ESerializeStage::kBufferSerializer: {
                    EnsureSection(queue);
                    if (bodyFinished()) {
                        queue.SerializeStage = ESerializeStage::kHeader;
                        Y_ABORT_UNLESS(queue.SerializedBytesPending == 0);
                        break;
                    }
                    if (queue.CurrentIsInline) {
                        while (maxBytesToProduce && queue.Iter.Valid() && queue.SectionBytesRemain) {
                            size_t numBytes = Min(maxBytesToProduce - (header ? 0 : sizeof(TChunkHeader)),
                                queue.Iter.ContiguousSize(), queue.SectionBytesRemain);
                            if (header && header->Length + numBytes > Max<ui16>()) {
                                numBytes = Max<ui16>() - header->Length;
                            }
                            if (!numBytes) {
                                break;
                            }
                            addEventChunkBytes(queue.Iter.ContiguousData(), numBytes);
                            queue.Iter += numBytes;
                        }
                    } else if (xdc) {
                        while (queue.Iter.Valid() && queue.SectionBytesRemain
                                && maxBytesToProduce > XdcPushFraming
                                && main.Buffer.size() >= XdcPushFraming) {
                            size_t n = Min(maxBytesToProduce - XdcPushFraming, queue.Iter.ContiguousSize(),
                                queue.SectionBytesRemain, size_t(Max<ui16>()));
                            if (!n) {
                                break;
                            }
                            emitXdcPush({queue.Iter.ContiguousData(), n});
                            queue.Iter += n;
                        }
                    } else {
                        break;
                    }
                    if (!queue.Iter.Valid()) {
                        queue.SerializeStage = ESerializeStage::kHeader;
                        Y_ABORT_UNLESS(queue.SerializedBytesPending == 0);
                    }
                    break;
                }

                case ESerializeStage::kChunkSerializer: {
                    UpdateTimestamp();
                    EnsureSection(queue);
                    // Live events must always FeedBuf until IsComplete(): that is what clears the
                    // coroutine's Event pointer. Skipping FeedBuf when the section table/pending
                    // count look empty (0-byte protobufs, 0-size tail sections) left Event set, and
                    // the next event on this channel hit SetSerializingEvent()'s Event == nullptr
                    // VERIFY on a real cluster.
                    if (queue.UseXdcForEvent
                            && queue.SectionIndex == queue.EvSerInfo->Sections.size()
                            && !queue.SectionBytesRemain
                            && !queue.CoroutineChunkSerializer.IsComplete()) {
                        queue.CurrentIsInline = true;
                        queue.SectionBytesRemain = Max<size_t>();
                    }

                    if (queue.CurrentIsInline) {
                        TMutableContiguousSpan span = main.Buffer.SubSpan(sizeof(TChunkHeader), Max<size_t>());
                        const size_t payloadLimit = Min(maxBytesToProduce - sizeof(TChunkHeader), queue.SectionBytesRemain);
                        if (payloadLimit) {
                            auto chunks = queue.CoroutineChunkSerializer.FeedBuf(&span, payloadLimit);
                            Y_DEBUG_ABORT_UNLESS(main.Buffer.data() + main.Buffer.size() == span.data() + span.size());
                            Y_DEBUG_ABORT_UNLESS(span.size() <= main.Buffer.size());
                            if (!chunks.empty()) {
                                ensureHeader();
                                main.Buffer = main.Buffer.SubSpan(main.Buffer.size() - span.size(), Max<size_t>());
                            }
                            for (const auto& chunk : chunks) {
                                addEventChunkBytes(chunk.Buf, chunk.Size);
                            }
                        }
                    } else if (xdc && maxBytesToProduce > XdcPushFraming && main.Buffer.size() >= XdcPushFraming) {
                        TMutableContiguousSpan span = xdc->Buffer;
                        const size_t payloadLimit = Min(maxBytesToProduce - XdcPushFraming, queue.SectionBytesRemain,
                            size_t(Max<ui16>()));
                        if (payloadLimit) {
                            auto chunks = queue.CoroutineChunkSerializer.FeedBuf(&span, payloadLimit);
                            xdc->Buffer = span;
                            size_t total = 0;
                            for (const auto& chunk : chunks) {
                                total += chunk.Size;
                            }
                            if (total) {
                                Y_ABORT_UNLESS(total <= Max<ui16>());
                                header = nullptr;
                                *static_cast<TChunkHeader*>(takeInBuffer(sizeof(TChunkHeader))) = {
                                    .Length = sizeof(ui16),
                                    .TypeChannel = static_cast<ui16>(channel | TChunkHeader::kXdcPush),
                                };
                                const ui16 n = static_cast<ui16>(total);
                                memcpy(takeInBuffer(sizeof(ui16)), &n, sizeof(n));
                                for (const auto& chunk : chunks) {
                                    produceOutputSpan(*xdc, {chunk.Buf, chunk.Size}, Checksumming);
                                }
                                Y_ABORT_UNLESS(total <= queue.SerializedBytesPending);
                                queue.SerializedBytesPending -= total;
                                Y_ABORT_UNLESS(total <= queue.SectionBytesRemain);
                                queue.SectionBytesRemain -= total;
                            }
                        }
                    } else {
                        SerializeEventTime += UpdateTimestamp();
                        break;
                    }

                    if (auto& cords = queue.CoroutineChunkSerializer.GetCords(); !cords.empty()) {
                        RefcountItems.push_back({
                            .MainEndOffset = CumulativeProducedMain,
                            .XdcEndOffset = CumulativeProducedXdc,
                            .Cords = std::exchange(cords, {}),
                        });
                    }

                    if (queue.CoroutineChunkSerializer.IsComplete()) {
                        queue.SerializeStage = ESerializeStage::kHeader;
                        Y_ABORT_UNLESS(queue.SerializedBytesPending == 0, "Type# 0x%08" PRIx32 " SerializedBytesPending# %zu"
                            " CalculateSerializedSize# %zu CalculateSerializedSizeCached# %zu", ev.Type,
                            queue.SerializedBytesPending, ev.GetBase()->CalculateSerializedSize(),
                            ev.GetBase()->CalculateSerializedSizeCached());
                    }

                    SerializeEventTime += UpdateTimestamp();
                    break;
                }

                case ESerializeStage::kHeader: {
                    if (Checksumming && !queue.EventHeaderOffset) {
                        XXH3_64bits_update(&queue.ChecksumState, &queue.EventHeader, sizeof(queue.EventHeader));
                        queue.EventHeader.Checksum = XXH3_64bits_digest(&queue.ChecksumState);
                    }

                    const size_t numDataBytes = Min(
                        main.Buffer.size() - sizeof(TChunkHeader),
                        maxBytesToProduce - sizeof(TChunkHeader),
                        sizeof(TEventHeader) - queue.EventHeaderOffset
                    );
                    Y_DEBUG_ABORT_UNLESS(numDataBytes);
                    *static_cast<TChunkHeader*>(takeInBuffer(sizeof(TChunkHeader))) = {
                        .Length = static_cast<ui16>(numDataBytes),
                        .TypeChannel = static_cast<ui16>(channel | TChunkHeader::kEventHeader),
                    };

                    void *ptr = takeInBuffer(numDataBytes);
                    memcpy(ptr, reinterpret_cast<const char*>(&queue.EventHeader) + queue.EventHeaderOffset, numDataBytes);
                    queue.EventHeaderOffset += numDataBytes;

                    if (queue.EventHeaderOffset == sizeof(TEventHeader)) {
                        RefcountItems.push_back({
                            .MainEndOffset = CumulativeProducedMain,
                            .XdcEndOffset = CumulativeProducedXdc,
                            .Buffer = std::exchange(queue.Buffer, nullptr),
                            .Event{ev.ReleaseBase().Release()},
                            .EventReceivedTimestamp = reinterpret_cast<const ui64&>(ev.OriginScopeId),
                        });
                        queue.Events.Pop();
                        ResetEventState(queue);
                    }
                    break;
                }
            }

            if (numBytesProduced == producedOnEntry && queue.SerializeStage == stageOnEntry) {
                break;
            }
        }

        return numBytesProduced;
    }

    ui64 TEventSerializer::UpdateTimestamp() {
        const ui64 prev = std::exchange(Timestamp, GetCycleCountFast());
        return Timestamp - prev;
    }

    TEventDeserializer::TEventDeserializer(TScopeId peerScopeId)
        : PeerScopeId(peerScopeId)
    {}

    TEventDeserializer::TPendingEvent& TEventDeserializer::CurrentEvent(TPerChannelQueue& queue) {
        if (queue.Pending.empty() || queue.Pending.back().HeaderComplete) {
            queue.Pending.emplace_back();
        }
        return queue.Pending.back();
    }

    void TEventDeserializer::ApplyXdcDeclare(TPendingEvent& ev, const char *data, size_t length) {
        Y_ABORT_UNLESS(length % sizeof(TXdcSection) == 0, "XDC declare length %zu is not a multiple of section size",
            length);
        Y_ABORT_UNLESS(!ev.HeaderComplete);
        Y_ABORT_UNLESS(!ev.InternalPayload);
        Y_ABORT_UNLESS(!ev.EventHeaderOffset);

        const size_t count = length / sizeof(TXdcSection);
        for (size_t i = 0; i < count; ++i) {
            TXdcSection rec;
            memcpy(&rec, data + i * sizeof(TXdcSection), sizeof(rec));
            const bool isInline = rec.Flags & TXdcSection::FlagInline;
            ev.EvSerInfo.Sections.push_back(TEventSectionInfo{
                rec.Headroom, rec.Size, rec.Tailroom, rec.Alignment, isInline, false});
            if (!isInline && rec.Size) {
                TRcBuf buffer = AllocateXdcSectionBuffer(rec.Size, rec.Headroom, rec.Tailroom, rec.Alignment);
                ev.XdcBuffers.push_back(buffer.UnsafeGetContiguousSpanMut());
                ev.ExternalPayload.Insert(ev.ExternalPayload.End(), TRope(std::move(buffer)));
                ev.XdcSizeLeft += rec.Size;
            }
        }
        ev.Declared = true;
        ev.EvSerInfo.IsExtendedFormat = true;
    }

    void TEventDeserializer::ApplyXdcPush(ui16 channel, TPendingEvent& ev, ui16 nbytes) {
        Y_ABORT_UNLESS(nbytes);
        Y_ABORT_UNLESS(ev.Declared);
        size_t remain = nbytes;
        while (remain) {
            Y_ABORT_UNLESS(!ev.XdcBuffers.empty(), "XDC push exceeds declared external size");
            auto& front = ev.XdcBuffers.front();
            const size_t take = Min<size_t>(remain, front.size());
            Y_ABORT_UNLESS(take);
            XdcInputQ.push_back(TXdcInputItem{
                .Channel = channel,
                .Span = TMutableContiguousSpan(front.data(), take),
            });
            front = TMutableContiguousSpan(front.data() + take, front.size() - take);
            if (!front.size()) {
                ev.XdcBuffers.pop_front();
            }
            remain -= take;
        }
    }

    TRope TEventDeserializer::Unshuffle(TPendingEvent& ev) {
        if (ev.EvSerInfo.Sections.empty()) {
            return std::exchange(ev.InternalPayload, {});
        }

        TRope payload;
        auto flushAccumulated = [&](TRope*& prev, size_t& accumSize) {
            if (accumSize) {
                prev->ExtractFront(accumSize, &payload);
                accumSize = 0;
            }
        };

        TRope *prev = nullptr;
        size_t accumSize = 0;
        for (const auto& s : ev.EvSerInfo.Sections) {
            TRope *rope = s.IsInline ? &ev.InternalPayload : &ev.ExternalPayload;
            if (s.IsInline && s.Alignment > 1 && s.Size) {
                flushAccumulated(prev, accumSize);
                auto it = rope->Begin();
                const bool alreadyAligned = it.Valid()
                    && it.ContiguousSize() >= s.Size
                    && reinterpret_cast<uintptr_t>(it.ContiguousData()) % s.Alignment == 0;
                if (alreadyAligned) {
                    rope->ExtractFront(s.Size, &payload);
                } else {
                    TRcBuf buffer = AllocateXdcSectionBuffer(s.Size, s.Headroom, s.Tailroom, s.Alignment);
                    const bool success = rope->ExtractFrontPlain(buffer.GetDataMut(), s.Size);
                    Y_ABORT_UNLESS(success);
                    payload.Insert(payload.End(), TRope(std::move(buffer)));
                }
            } else {
                if (rope != prev) {
                    flushAccumulated(prev, accumSize);
                    prev = rope;
                }
                accumSize += s.Size;
            }
        }
        flushAccumulated(prev, accumSize);
        Y_ABORT_UNLESS(!ev.InternalPayload);
        Y_ABORT_UNLESS(!ev.ExternalPayload);
        return payload;
    }

    void TEventDeserializer::TryDeliver(TPerChannelQueue& queue, IEventProcessor *eventProcessor, TActorId sessionId) {
        while (!queue.Pending.empty()) {
            auto& ev = queue.Pending.front();
            if (!ev.HeaderComplete || ev.XdcSizeLeft) {
                break;
            }

            TRope payload = Unshuffle(ev);

            if (ev.EventHeader.Checksum) {
                XXH3_state_t state;
                XXH3_64bits_reset(&state);
                for (auto iter = payload.begin(); iter.Valid(); iter.AdvanceToNextContiguousBlock()) {
                    XXH3_64bits_update(&state, iter.ContiguousData(), iter.ContiguousSize());
                }
                const ui64 expected = std::exchange(ev.EventHeader.Checksum, 0);
                XXH3_64bits_update(&state, &ev.EventHeader, sizeof(ev.EventHeader));
                const ui64 calculated = XXH3_64bits_digest(&state);
                Y_ABORT_UNLESS(calculated == expected);
            }

            ev.EvSerInfo.IsExtendedFormat = ev.EventHeader.Flags & IEventHandle::FlagExtendedFormat;
            eventProcessor->PushEvent(std::make_unique<IEventHandle>(
                sessionId,
                ev.EventHeader.Type,
                ev.EventHeader.Flags & ~IEventHandle::FlagExtendedFormat,
                ev.EventHeader.Recipient,
                ev.EventHeader.Sender,
                MakeIntrusive<TEventSerializedData>(
                    std::move(payload),
                    std::move(ev.EvSerInfo)),
                ev.EventHeader.Cookie,
                PeerScopeId,
                NWilson::TTraceId(ev.EventHeader.TraceId)));
            queue.Pending.pop_front();
        }
    }

    void TEventDeserializer::Push(TRcBuf buffer, IEventProcessor *eventProcessor, TActorId sessionId) {
        Accum.Insert(Accum.End(), std::move(buffer));

        while (Accum.size() >= sizeof(TChunkHeader)) {
            TChunkHeader header;
            Accum.begin().ExtractPlainDataAndAdvance(&header, sizeof(header));

            if (const size_t length = header.Length; Accum.size() >= sizeof(TChunkHeader) + length) {
                Accum.EraseFront(sizeof(header));

                switch (TPerChannelQueue& queue = GetQueue(header.GetChannel()); header.GetType()) {
                    case TChunkHeader::kEventChunk: {
                        auto& ev = CurrentEvent(queue);
                        Accum.ExtractFront(length, &ev.InternalPayload);
                        break;
                    }

                    case TChunkHeader::kXdcDeclare: {
                        TString bytes = TString::Uninitialized(length);
                        if (length) {
                            const bool ok = Accum.ExtractFrontPlain(bytes.Detach(), length);
                            Y_ABORT_UNLESS(ok);
                        }
                        if (queue.Pending.empty() || queue.Pending.back().HeaderComplete) {
                            queue.Pending.emplace_back();
                        } else {
                            auto& back = queue.Pending.back();
                            Y_ABORT_UNLESS(!back.InternalPayload && !back.EventHeaderOffset,
                                "XDC DECLARE after event body");
                        }
                        ApplyXdcDeclare(queue.Pending.back(), bytes.data(), length);
                        break;
                    }

                    case TChunkHeader::kXdcPush: {
                        Y_ABORT_UNLESS(length == sizeof(ui16));
                        ui16 nbytes = 0;
                        const bool ok = Accum.ExtractFrontPlain(&nbytes, sizeof(nbytes));
                        Y_ABORT_UNLESS(ok);
                        Y_ABORT_UNLESS(!queue.Pending.empty() && !queue.Pending.back().HeaderComplete);
                        ApplyXdcPush(header.GetChannel(), queue.Pending.back(), nbytes);
                        break;
                    }

                    case TChunkHeader::kEventHeader: {
                        auto& ev = CurrentEvent(queue);
                        if (ev.EventHeaderOffset + length > sizeof(TEventHeader)) {
                            Y_ABORT("unsupported header");
                        }

                        Accum.ExtractFrontPlain(reinterpret_cast<char*>(&ev.EventHeader) + ev.EventHeaderOffset, length);
                        ev.EventHeaderOffset += length;
                        if (ev.EventHeaderOffset == sizeof(TEventHeader)) {
                            ev.HeaderComplete = true;
                            TryDeliver(queue, eventProcessor, sessionId);
                        }
                        break;
                    }

                    case TChunkHeader::kSystem: {
                        TRopeStream stream(Accum.begin(), length);
                        NActorsInterconnect::TSystemPayloadV2 systemRequest;
                        const bool success = systemRequest.ParseFromZeroCopyStream(&stream);
                        Y_ABORT_UNLESS(success);
                        eventProcessor->Process(systemRequest);
                        Accum.EraseFront(length);
                        break;
                    }

                    default:
                        Y_ABORT("unsupported type");
                }
            } else {
                break;
            }
        }
    }

    size_t TEventDeserializer::PrepareXdcReadv(TIoVec *iov, size_t maxIov, size_t maxBytes) {
        size_t n = 0;
        size_t bytes = 0;
        for (auto& item : XdcInputQ) {
            if (n >= maxIov || bytes >= maxBytes) {
                break;
            }
            const size_t take = Min(item.Span.size(), maxBytes - bytes);
            if (!take) {
                break;
            }
            iov[n++] = TIoVec{.Data = item.Span.data(), .Size = take};
            bytes += take;
        }
        return n;
    }

    void TEventDeserializer::CommitXdcBytes(size_t n, IEventProcessor *eventProcessor, TActorId sessionId) {
        while (n) {
            Y_ABORT_UNLESS(!XdcInputQ.empty());
            auto& front = XdcInputQ.front();
            const size_t take = Min(n, front.Span.size());
            TPerChannelQueue& queue = GetQueue(front.Channel);
            TPendingEvent *ev = nullptr;
            for (auto& p : queue.Pending) {
                if (p.XdcSizeLeft) {
                    ev = &p;
                    break;
                }
            }
            Y_ABORT_UNLESS(ev);
            Y_ABORT_UNLESS(take <= ev->XdcSizeLeft);
            ev->XdcSizeLeft -= take;
            front.Span = TMutableContiguousSpan(front.Span.data() + take, front.Span.size() - take);
            if (!front.Span.size()) {
                XdcInputQ.pop_front();
            }
            n -= take;
            TryDeliver(queue, eventProcessor, sessionId);
        }
    }

} // NActors
