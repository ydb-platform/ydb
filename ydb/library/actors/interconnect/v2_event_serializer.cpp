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
        TPerChannelQueue& queue = GetQueue(channel);
        const bool first = queue.Events.empty();
        queue.Events.push_back(std::move(ev));
        if (first) {
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

    size_t TEventSerializer::ProduceOutputStream(TRcBuf& buffer, std::deque<TContiguousSpan> *out, size_t maxBytesToProduce) {
        size_t totalBytesProduced = 0;
        ui64 bufferProduced = 0;

        Y_ABORT_UNLESS(buffer.size() >= TEventSerializer::MinUsefulQuota);

        const ui64 bytesProducedOnEntry = CumulativeProduced;

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
                Min<size_t>(maxBytesToProduce, q.Quota), buffer, out, &bufferProduced);
            if (!numBytesProduced) { // in case we did not make any progress (not enough space in buffer)
                break;
            }
            Y_ABORT_UNLESS(numBytesProduced <= q.Quota);
            totalBytesProduced += numBytesProduced;
            maxBytesToProduce -= numBytesProduced;

            // update quota
            std::ranges::pop_heap(PerChannelQuotaHeap, std::less<ui16>{}, &TPerChannelQuota::Quota);
            if (queue.Events.empty() && queue.SystemRequests.empty()) {
                // we have serialized all the events avaiable in this queue, so we drop record from the quota heap
                PerChannelQuotaHeap.pop_back();
            } else {
                // adjust quota
                PerChannelQuotaHeap.back().Quota -= isSystemChannel ? 0 : numBytesProduced;
                std::ranges::push_heap(PerChannelQuotaHeap, std::less<ui16>{}, &TPerChannelQuota::Quota);
            }
        }

        Y_DEBUG_ABORT_UNLESS(bytesProducedOnEntry + totalBytesProduced == CumulativeProduced);

        if (bufferProduced) {
            RefcountItems.push_back({
                .EndOffset = bufferProduced,
                .Scratch = buffer,
                .EventReceivedTimestamp = 0,
            });
        }

        return totalBytesProduced;
    }

    void TEventSerializer::CommitProducedBytes(size_t numBytes, std::vector<ui64> *eventToWireTime) {
        CumulativeCommitted += numBytes;
        Y_ABORT_UNLESS(CumulativeCommitted <= CumulativeProduced);
        const ui64 timestamp = GetCycleCountFast();
        while (!RefcountItems.empty() && RefcountItems.front().EndOffset <= CumulativeCommitted) {
            auto& front = RefcountItems.front();
            if (Y_LIKELY(eventToWireTime) && front.EventReceivedTimestamp) {
                eventToWireTime->push_back(timestamp - front.EventReceivedTimestamp);
            }
            RefcountItems.pop_front();
        }
    }

    size_t TEventSerializer::ProduceOutputStreamForQueue(ui16 channel, TPerChannelQueue& queue, size_t maxBytesToProduce,
            TRcBuf& buffer, std::deque<TContiguousSpan> *out, ui64 *bufferProduced) {
        const TContiguousSpan bufferSpan = buffer.GetContiguousSpan(); // remember original buffer span
        size_t numBytesProduced = 0;

        // this function is used to generate output span storing reference either to buffer, or to aliased memory range
        auto produceOutputSpan = [&](TContiguousSpan span, bool addToChecksum) {
            if (addToChecksum) {
                XXH3_64bits_update(&queue.ChecksumState, span.data(), span.size());
            }

            if (span.data() + span.size() <= bufferSpan.data() || span.data() >= bufferSpan.data() + bufferSpan.size()) {
                // we got span referenced outside original buffer; check if we can copy it into the buffer, if it is
                // small enough and buffer has the space to do it
                const uintptr_t spanBegin = reinterpret_cast<uintptr_t>(span.data());
                const uintptr_t spanEnd = reinterpret_cast<uintptr_t>(span.data() + span.size() - 1);
                const uintptr_t mask = ~uintptr_t(63); // check if it fits the same cacheline
                if (buffer.size() >= span.size() && (spanBegin & mask) == (spanEnd & mask)) {
                    memcpy(buffer.UnsafeGetDataMut(), span.data(), span.size());
                    span = {buffer.data(), span.size()};
                    buffer.TrimFront(buffer.size() - span.size());
                }
            }

            Y_ABORT_UNLESS(span.size() <= maxBytesToProduce);
            maxBytesToProduce -= span.size();
            numBytesProduced += span.size();
            CumulativeProduced += span.size();
            if (out->empty()) {
                out->push_back(span);
            } else if (TContiguousSpan& last = out->back(); last.data() + last.size() != span.data()) {
                out->push_back(span);
            } else { // concatenate last span with the new one
                last = {last.data(), last.size() + span.size()};
            }

            if (span.data() + span.size() <= bufferSpan.data() || span.data() >= bufferSpan.data() + bufferSpan.size()) {
                BytesAliased += span.size();
            } else {
                BytesCopied += span.size();
            }
        };

        // this function allocated specified amount of space in provided buffer and returns reference to it, also
        // producing output span with allocated data
        auto takeInBuffer = [&](size_t numBytes) -> void* {
            Y_ABORT_UNLESS(numBytes <= maxBytesToProduce);
            Y_ABORT_UNLESS(numBytes <= buffer.size());
            TMutableContiguousSpan res(buffer.UnsafeGetDataMut(), numBytes);
            buffer.TrimFront(buffer.size() - numBytes);
            produceOutputSpan(res, false);
            *bufferProduced = CumulativeProduced;
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

        while (Min(buffer.size(), maxBytesToProduce) >= MinUsefulQuota && !queue.Events.empty()) {
            IEventHandle& ev = *queue.Events.front();

            TChunkHeader *header = nullptr;
            auto addEventChunkBytes = [&](const char *ptr, size_t numBytes) {
                Y_DEBUG_ABORT_UNLESS(numBytes);
                if (!header) {
                    header = static_cast<TChunkHeader*>(takeInBuffer(sizeof(TChunkHeader)));
                    *header = {
                        .Length = 0,
                        .TypeChannel = static_cast<ui16>(channel | TChunkHeader::kEventChunk),
                    };
                }
                Y_DEBUG_ABORT_UNLESS(header->Length + numBytes <= Max<ui16>());
                header->Length += numBytes;
                produceOutputSpan({ptr, numBytes}, Checksumming);
            };

            switch (queue.SerializeStage) {
                case ESerializeStage::kInitial:
                    // we are starting to serialize new event; decide which kind of serializer to use
                    if (ev.HasBuffer()) {
                        queue.SerializeStage = ESerializeStage::kBufferSerializer;
                        queue.Buffer = ev.ReleaseChainBuffer();
                        queue.Iter = queue.Buffer->GetBeginIter();
                        queue.EvSerInfo = &queue.Buffer->GetSerializationInfo();
                    } else if (ev.HasEvent()) {
                        IEventBase *event = ev.GetBase();
                        queue.SerializeStage = ESerializeStage::kChunkSerializer;
                        queue.CoroutineChunkSerializer.SetSerializingEvent(event, /*withCachedSizes=*/ false);
                        queue.EvSerInfoHolder = event->CreateSerializationInfo(true);
                        queue.EvSerInfo = &queue.EvSerInfoHolder;
                    } else {
                        queue.SerializeStage = ESerializeStage::kHeader;
                        queue.EvSerInfoHolder = {};
                        queue.EvSerInfo = &queue.EvSerInfoHolder;
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
                    UpdateTimestamp();

                    while (maxBytesToProduce && queue.Iter.Valid()) {
                        const size_t numBytes = Min(maxBytesToProduce - (header ? 0 : sizeof(TChunkHeader)),
                            queue.Iter.ContiguousSize());
                        addEventChunkBytes(queue.Iter.ContiguousData(), numBytes);
                        queue.Iter += numBytes;
                    }
                    if (!queue.Iter.Valid()) {
                        queue.SerializeStage = ESerializeStage::kHeader;
                    }

                    SerializeBufferTime += UpdateTimestamp();
                    break;

                case ESerializeStage::kChunkSerializer: {
                    UpdateTimestamp();

                    // serialize as much as we can
                    TMutableContiguousSpan span = buffer.UnsafeGetContiguousSpanMut().SubSpan(sizeof(TChunkHeader),
                        Max<size_t>()); // reserve space for TChunkHeader which we write first thing if we have some data
                    for (const auto [data, size] : queue.CoroutineChunkSerializer.FeedBuf(&span, maxBytesToProduce -
                            sizeof(TChunkHeader))) {
                        addEventChunkBytes(data, size);
                    }
                    Y_DEBUG_ABORT_UNLESS(buffer.data() + buffer.size() == span.data() + span.size());
                    Y_DEBUG_ABORT_UNLESS(span.size() <= buffer.size()); // ensure span did not reduce
                    if (header) {
                        buffer.TrimFront(span.size());
                    }

                    // check if we have finished serializing this event
                    if (queue.CoroutineChunkSerializer.IsComplete()) {
                        queue.SerializeStage = ESerializeStage::kHeader;
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
                        queue.Events.pop_front();
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
        return (Timestamp - prev) * Freq;
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
