#include "v2_event_serializer.h"

#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/protos/interconnect.pb.h>

#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/string/hex.h>

namespace NActors {

    TEventSerializer::TEventSerializer(bool checksumming)
        : Checksumming(checksumming)
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

    size_t TEventSerializer::ProduceOutputStream(TRcBuf& buffer, std::vector<TContiguousSpan> *out, size_t maxBytesToProduce) {
        size_t totalBytesProduced = 0;
        ui64 bufferProduced = 0;

        Y_ABORT_UNLESS(buffer.size() >= TEventSerializer::MinUsefulQuota);

        const ui64 bytesProducedOnEntry = CumulativeProduced;
        const size_t bufferSizeOnEntry = buffer.size();

        TMutableContiguousSpan bufferSpan = buffer.UnsafeGetContiguousSpanMut();

        // we can't emit anything useful once the output buffer can't hold at least a chunk header along with a whole
        // some useful data, so we stop here to avoid spinning without making any progress
        while (!PerChannelQuotaHeap.empty()) {
            // if even the channel with the most quota can't emit a whole event header, replenish quota for every channel
            // back to the default value; this keeps the bandwidth distributed equally and guarantees that the channel we
            // are about to serve always has enough quota to make progress
            if (PerChannelQuotaHeap.front().Quota < MinUsefulQuota) {
                for (auto& item : PerChannelQuotaHeap) {
                    Y_ABORT_UNLESS(item.Channel != TChunkHeader::SystemChannel);
                    item.Quota += DefaultQuota;
                }
            }

            // get the channel/quota pair for the channel with the most quota available
            TPerChannelQuota& q = PerChannelQuotaHeap.front();

            // serialize part of data for this channel
            TPerChannelQueue& queue = GetQueue(q.Channel);
            const bool isSystemChannel = q.Channel == TChunkHeader::SystemChannel;
            const size_t numBytesProduced = ProduceOutputStreamForQueue(q.Channel, queue,
                Min<size_t>(maxBytesToProduce, q.Quota), bufferSpan, out, &bufferProduced);
            if (!numBytesProduced) { // in case we did not make any progress (not enough space in buffer)
                break;
            }
            Y_ABORT_UNLESS(numBytesProduced <= q.Quota);
            totalBytesProduced += numBytesProduced;
            maxBytesToProduce -= numBytesProduced;

            // update quota
            std::ranges::pop_heap(PerChannelQuotaHeap, std::less<ui16>{}, &TPerChannelQuota::Quota);
            if (!queue.Events.Peek() && queue.SystemRequests.empty()) {
                // we have serialized all the events avaiable in this queue, so we drop record from the quota heap
                PerChannelQuotaHeap.pop_back();
            } else {
                // adjust quota
                PerChannelQuotaHeap.back().Quota -= isSystemChannel ? 0 : numBytesProduced;
                std::ranges::push_heap(PerChannelQuotaHeap, std::less<ui16>{}, &TPerChannelQuota::Quota);
            }
        }

        Y_DEBUG_ABORT_UNLESS(bytesProducedOnEntry + totalBytesProduced == CumulativeProduced);

        buffer.TrimFront(bufferSpan.size() - bufferSpan.size() % 64); // align buffer on 64-byte boundary
        const size_t bufferSizeOnExit = buffer.size();
        Y_DEBUG_ABORT_UNLESS(bufferSizeOnExit <= bufferSizeOnEntry);

        const size_t scratchBytesUsed = bufferSizeOnEntry - bufferSizeOnExit;

        if (bufferProduced) {
            RefcountItems.push_back({
                .EndOffset = bufferProduced,
                .Scratch = buffer,
                .ScratchBytesUsed = scratchBytesUsed,
                .EventReceivedTimestamp = 0,
            });
            NumBytesInScratchBuffers += scratchBytesUsed;
        } else {
            Y_DEBUG_ABORT_UNLESS(scratchBytesUsed == 0);
        }

        return totalBytesProduced;
    }

    void TEventSerializer::CommitProducedBytes(size_t numBytes, std::vector<ui64> *eventToWireTime,
            std::vector<std::unique_ptr<IEventBase>> *events,
            std::vector<TIntrusivePtr<TEventSerializedData>> *buffers) {
        CumulativeCommitted += numBytes;
        Y_ABORT_UNLESS(CumulativeCommitted <= CumulativeProduced);
        const ui64 timestamp = GetCycleCountFast();
        while (!RefcountItems.empty() && RefcountItems.front().EndOffset <= CumulativeCommitted) {
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

    size_t TEventSerializer::ProduceOutputStreamForQueue(ui16 channel, TPerChannelQueue& queue, size_t maxBytesToProduce,
            TMutableContiguousSpan& buffer, std::vector<TContiguousSpan> *out, ui64 *bufferProduced) {
        const TContiguousSpan bufferSpan = buffer; // remember original buffer span
        size_t numBytesProduced = 0;
        const char *lastSpanEnd = out->empty() ? nullptr : [s = out->back()]{ return s.data() + s.size(); }();

        // this function is used to generate output span storing reference either to buffer, or to aliased memory range
        auto produceOutputSpan = [&](TContiguousSpan span, bool addToChecksum) {
            if (Y_UNLIKELY(addToChecksum)) {
                XXH3_64bits_update(&queue.ChecksumState, span.data(), span.size());
            }

            bool fromBuffer = span.data() >= bufferSpan.data() && span.data() + span.size() <= bufferSpan.data() + bufferSpan.size();

            if (!fromBuffer && (reinterpret_cast<uintptr_t>(span.data()) & 63) + span.size() <= 64 && buffer.size() >= span.size()) {
                // we got span referenced outside original buffer; check if we can copy it into the buffer, if it is
                // small enough and buffer has the space to do it
                memcpy(buffer.data(), span.data(), span.size());
                span = {buffer.data(), span.size()};
                buffer = buffer.SubSpan(span.size(), Max<size_t>());
                fromBuffer = true;
            }

            Y_ABORT_UNLESS(span.size() <= maxBytesToProduce);
            maxBytesToProduce -= span.size();
            numBytesProduced += span.size();
            CumulativeProduced += span.size();
            if (lastSpanEnd != span.data()) {
                out->push_back(span);
            } else {
                Y_DEBUG_ABORT_UNLESS(!out->empty());
                TContiguousSpan& lastSpan = out->back();
                lastSpan = {lastSpan.data(), lastSpan.size() + span.size()};
            }
            lastSpanEnd = span.data() + span.size();

            (fromBuffer ? BytesCopied : BytesAliased) += span.size();

            if (fromBuffer) {
                Y_DEBUG_ABORT_UNLESS(*bufferProduced < CumulativeProduced);
                *bufferProduced = CumulativeProduced;
            }
        };

        // this function allocated specified amount of space in provided buffer and returns reference to it, also
        // producing output span with allocated data
        auto takeInBuffer = [&](size_t numBytes) -> void* {
            Y_ABORT_UNLESS(numBytes <= maxBytesToProduce);
            Y_ABORT_UNLESS(numBytes <= buffer.size());
            TMutableContiguousSpan res(buffer.data(), numBytes);
            buffer = buffer.SubSpan(numBytes, Max<size_t>());
            produceOutputSpan(res, false);
            return res.data();
        };

        while (Y_UNLIKELY(!queue.SystemRequests.empty())) {
            auto& request = queue.SystemRequests.front();
            if (maxBytesToProduce < sizeof(TChunkHeader) + request.size() || buffer.size() < sizeof(TChunkHeader)) {
                break;
            }
            *static_cast<TChunkHeader*>(takeInBuffer(sizeof(TChunkHeader))) = {
                .Length = static_cast<ui16>(request.size()),
                .TypeChannel = TChunkHeader::kSystem,
            };
            produceOutputSpan({request.data(), request.size()}, false);
            RefcountItems.push_back({
                .EndOffset = CumulativeProduced,
                .Scratch = std::move(request),
                .EventReceivedTimestamp = 0,
            });
            queue.SystemRequests.pop_front();
        }

        while (Min(buffer.size(), maxBytesToProduce) >= MinUsefulQuota && queue.Events.Peek()) {
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
                produceOutputSpan({ptr, numBytes}, Checksumming);

                Y_ABORT_UNLESS(numBytes <= queue.SerializedBytesPending, "Type# 0x%08" PRIx32
                    " SerializedBytesPending# %zu CalculateSerializedSize# %zu CalculateSerializedSizeCached# %zu",
                    ev.Type, queue.SerializedBytesPending, ev.GetBase()->CalculateSerializedSize(),
                    ev.GetBase()->CalculateSerializedSizeCached());

                queue.SerializedBytesPending -= numBytes;
            };

            switch (queue.SerializeStage) {
                case ESerializeStage::kInitial:
                    // we are starting to serialize new event; decide which kind of serializer to use
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
                        queue.EvSerInfoHolder = event->CreateSerializationInfo(true);
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

                    // fill in event header
                    queue.EventHeader = {
                        .Type = ev.Type,
                        .Flags = ev.Flags | (queue.EvSerInfo->IsExtendedFormat ? IEventHandle::FlagExtendedFormat : 0),
                        .Cookie = ev.Cookie,
                        .Checksum = 0,
                        .Sender = ev.Sender,
                        .Recipient = ev.Recipient,
                    };
                    ev.TraceId.Serialize(&queue.EventHeader.TraceId);
                    break;

                case ESerializeStage::kBufferSerializer:
                    while (maxBytesToProduce && queue.Iter.Valid()) {
                        const size_t numBytes = Min(maxBytesToProduce - (header ? 0 : sizeof(TChunkHeader)),
                            queue.Iter.ContiguousSize());
                        addEventChunkBytes(queue.Iter.ContiguousData(), numBytes);
                        queue.Iter += numBytes;
                    }
                    if (!queue.Iter.Valid()) {
                        queue.SerializeStage = ESerializeStage::kHeader;
                        Y_ABORT_UNLESS(queue.SerializedBytesPending == 0);
                    }

                    break;

                case ESerializeStage::kChunkSerializer: {
                    UpdateTimestamp();

                    // reserve space for TChunkHeader which we write first thing if we have some data
                    TMutableContiguousSpan span = buffer.SubSpan(sizeof(TChunkHeader), Max<size_t>());
                    auto chunks = queue.CoroutineChunkSerializer.FeedBuf(&span, maxBytesToProduce - sizeof(TChunkHeader));
                    Y_DEBUG_ABORT_UNLESS(buffer.data() + buffer.size() == span.data() + span.size());
                    Y_DEBUG_ABORT_UNLESS(span.size() <= buffer.size()); // ensure span did not reduce
                    if (!chunks.empty()) {
                        ensureHeader();
                        buffer = buffer.SubSpan(buffer.size() - span.size(), Max<size_t>());
                    }
                    for (const auto& chunk : chunks) {
                        addEventChunkBytes(chunk.Buf, chunk.Size);
                    }
                    if (auto& cords = queue.CoroutineChunkSerializer.GetCords(); !cords.empty()) {
                        // retain ownership of cords provided for this serialization
                        RefcountItems.push_back({
                            .EndOffset = CumulativeProduced,
                            .Cords = std::exchange(cords, {}),
                        });
                    }

                    // check if we have finished serializing this event
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
                        buffer.size() - sizeof(TChunkHeader),
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
                            .EndOffset = CumulativeProduced,
                            .Buffer = std::exchange(queue.Buffer, nullptr),
                            .Event{ev.ReleaseBase().Release()},
                            .EventReceivedTimestamp = reinterpret_cast<const ui64&>(ev.OriginScopeId),
                        });
                        queue.Events.Pop();
                        queue.SerializeStage = ESerializeStage::kInitial;
                        queue.EventHeaderOffset = 0;
                    }
                    break;
                }
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

    void TEventDeserializer::Push(TRcBuf buffer, IEventProcessor *eventProcessor, TActorId sessionId) {
        // put incoming buffer to the queue's end
        Accum.Insert(Accum.End(), std::move(buffer));

        // parse accumulator
        while (Accum.size() >= sizeof(TChunkHeader)) {
            // extract next chunk's header
            TChunkHeader header;
            Accum.begin().ExtractPlainDataAndAdvance(&header, sizeof(header));

            // check if the whole chunks fits the accumulator
            if (const size_t length = header.Length; Accum.size() >= sizeof(TChunkHeader) + length) {
                // remove the just-parsed header
                Accum.EraseFront(sizeof(header));

                switch (TPerChannelQueue& queue = GetQueue(header.GetChannel()); header.GetType()) {
                    case TChunkHeader::kEventChunk:
                        Accum.ExtractFront(length, &queue.Accum);
                        break;

                    case TChunkHeader::kEventHeader:
                        if (queue.EventHeaderOffset + length > sizeof(TEventHeader)) {
                            Y_ABORT("unsupported header");
                        }

                        Accum.ExtractFrontPlain(reinterpret_cast<char*>(&queue.EventHeader) + queue.EventHeaderOffset, length);
                        queue.EventHeaderOffset += length;
                        if (queue.EventHeaderOffset == sizeof(TEventHeader)) {
                            queue.EventHeaderOffset = 0;

                            if (queue.EventHeader.Checksum) {
                                XXH3_state_t state;
                                XXH3_64bits_reset(&state);
                                for (auto iter = queue.Accum.begin(); iter.Valid(); iter.AdvanceToNextContiguousBlock()) {
                                    XXH3_64bits_update(&state, iter.ContiguousData(), iter.ContiguousSize());
                                }
                                const ui64 expected = std::exchange(queue.EventHeader.Checksum, 0);
                                XXH3_64bits_update(&state, &queue.EventHeader, sizeof(queue.EventHeader));
                                const ui64 calculated = XXH3_64bits_digest(&state);
                                Y_ABORT_UNLESS(calculated == expected);
                            }

                            queue.EvSerInfo.IsExtendedFormat = queue.EventHeader.Flags & IEventHandle::FlagExtendedFormat;
                            eventProcessor->PushEvent(std::make_unique<IEventHandle>(
                                sessionId,
                                queue.EventHeader.Type,
                                queue.EventHeader.Flags & ~IEventHandle::FlagExtendedFormat,
                                queue.EventHeader.Recipient,
                                queue.EventHeader.Sender,
                                MakeIntrusive<TEventSerializedData>(
                                    std::exchange(queue.Accum, {}),
                                    std::exchange(queue.EvSerInfo, {})),
                                queue.EventHeader.Cookie,
                                PeerScopeId,
                                NWilson::TTraceId(queue.EventHeader.TraceId)));
                        }

                        break;

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

} // NActors
