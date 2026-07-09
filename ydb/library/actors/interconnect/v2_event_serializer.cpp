#include "v2_event_serializer.h"

namespace NActors {

    void TEventSerializer::Push(std::unique_ptr<IEventHandle> ev) {
        const ui16 channel = ev->GetChannel();
        TPerChannelQueue& queue = GetQueue(channel);
        const bool first = queue.Events.empty();
        queue.Events.push_back(std::move(ev));
        if (first) {
            // calculate quota for new channel: either it is default quota if we have no active transmissions, or the
            // greatest one amongst other channels
            const ui16 quota = PerChannelQuotaHeap.empty()
                ? DefaultQuota
                : PerChannelQuotaHeap.front().Quota;

            // this new quota must be nonzero
            Y_ABORT_UNLESS(quota);

            // place this new quota into non-zero part of the heap
            PerChannelQuotaHeap.push_back(TPerChannelQuota{
                .Channel = channel,
                .Quota = quota,
            });
            std::ranges::push_heap(PerChannelQuotaHeap, std::less<ui16>{}, &TPerChannelQuota::Quota);
        }
    }

    size_t TEventSerializer::ProduceOutputStream(TMutableContiguousSpan *buffer, std::vector<TContiguousSpan> *out) {
        size_t totalBytesProduced = 0;

        // we can't emit anything useful once the output buffer can't hold at least a chunk header along with a whole
        // some useful data, so we stop here to avoid spinning without making any progress
        while (!PerChannelQuotaHeap.empty() && buffer->size() >= MinUsefulQuota) {
            // if even the channel with the most quota can't emit a whole event header, replenish quota for every channel
            // back to the default value; this keeps the bandwidth distributed equally and guarantees that the channel we
            // are about to serve always has enough quota to make progress
            if (PerChannelQuotaHeap.front().Quota < MinUsefulQuota) {
                for (auto& item : PerChannelQuotaHeap) {
                    item.Quota = DefaultQuota;
                }
            }

            // get the channel/quota pair for the channel with the most quota available
            TPerChannelQuota& q = PerChannelQuotaHeap.front();

            // serialize part of data for this channel
            TPerChannelQueue& queue = GetQueue(q.Channel);
            const size_t numBytesProduced = ProduceOutputStreamForQueue(queue, q.Quota, buffer, out);
            Y_ABORT_UNLESS(numBytesProduced <= q.Quota);
            totalBytesProduced += numBytesProduced;

            // update quota
            std::ranges::pop_heap(PerChannelQuotaHeap, std::less<ui16>{}, &TPerChannelQuota::Quota);
            if (queue.Events.empty()) {
                // we have serialized all the events avaiable in this queue, so we drop record from the quota heap
                PerChannelQuotaHeap.pop_back();
            } else {
                // adjust quota
                PerChannelQuotaHeap.back().Quota -= numBytesProduced;
                std::ranges::push_heap(PerChannelQuotaHeap, std::less<ui16>{}, &TPerChannelQuota::Quota);
                Y_ABORT_UNLESS(numBytesProduced); // ensure we had progress
            }
        }

        return totalBytesProduced;
    }

    void TEventSerializer::CommitProducedBytes(size_t numBytes) {
        while (numBytes) {
            if (RefcountItems.empty()) {
                OverproducedBytes += numBytes;
                break;
            }

            TRefcountItem& item = RefcountItems.front();
            if (numBytes < item.NumBytesRemaining) {
                item.NumBytesRemaining -= numBytes;
                break;
            } else {
                numBytes -= item.NumBytesRemaining;
                RefcountItems.pop_front();
            }
        }
    }

    size_t TEventSerializer::ProduceOutputStreamForQueue(TPerChannelQueue& queue, size_t maxBytesToProduce,
            TMutableContiguousSpan *buffer, std::vector<TContiguousSpan> *out) {
        const TContiguousSpan bufferSpan = *buffer; // remember original buffer span
        size_t numBytesProduced = 0;

        // this function is used to generate output span storing reference either to buffer, or to aliased memory range
        auto produceOutputSpan = [&](TContiguousSpan span) {
            if (span.data() + span.size() <= bufferSpan.data() || span.data() >= bufferSpan.data() + bufferSpan.size()) {
                // we got span referenced outside original buffer; check if we can copy it into the buffer, if it is
                // small enough and buffer has the space to do it
                if (span.size() <= 64 && buffer->size() >= span.size()) {
                    memcpy(buffer->data(), span.data(), span.size());
                    span = {buffer->data(), span.size()};
                    *buffer = buffer->SubSpan(span.size(), Max<size_t>());
                }
            }

            Y_ABORT_UNLESS(span.size() <= maxBytesToProduce);
            maxBytesToProduce -= span.size();
            numBytesProduced += span.size();
            queue.EventProducedSize += span.size();
            if (out->empty()) {
                out->push_back(span);
            } else if (TContiguousSpan& last = out->back(); last.data() + last.size() != span.data()) {
                out->push_back(span);
            } else { // concatenate last span with the new one
                last = {last.data(), last.size() + span.size()};
            }
        };

        // this function allocated specified amount of space in provided buffer and returns reference to it, also
        // producing output span with allocated data
        auto takeInBuffer = [&](size_t numBytes) -> void* {
            Y_ABORT_UNLESS(numBytes <= maxBytesToProduce);
            Y_ABORT_UNLESS(numBytes <= buffer->size());
            TMutableContiguousSpan res = buffer->SubSpan(0, numBytes);
            *buffer = buffer->SubSpan(numBytes, Max<size_t>());
            produceOutputSpan(res);
            return res.data();
        };

        while (maxBytesToProduce && !queue.Events.empty()) {
            IEventHandle& ev = *queue.Events.front();

            // prepare chunk header depending on the state
            TChunkHeader *chunkHeader = nullptr;
            if (queue.SerializeStage != ESerializeStage::kInitial) {
                if (buffer->size() < sizeof(TChunkHeader) || maxBytesToProduce < MinUsefulQuota) {
                    break; // not even a chance to put something useful
                }

                // allocate chunk header and fill it in
                chunkHeader = static_cast<TChunkHeader*>(takeInBuffer(sizeof(TChunkHeader)));
                *chunkHeader = {
                    .TypeLength =
                        queue.SerializeStage == ESerializeStage::kHeader
                            ? TChunkHeader::kEventHeader
                            : TChunkHeader::kEventChunk,
                    .Channel = ev.GetChannel(),
                };
            }

            bool quit = false;

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
                        queue.CoroutineChunkSerializer.SetSerializingEvent(event);
                        queue.EvSerInfoHolder = event->CreateSerializationInfo(true);
                        queue.EvSerInfo = &queue.EvSerInfoHolder;
                    } else {
                        queue.SerializeStage = ESerializeStage::kHeader;
                        queue.EvSerInfoHolder = {};
                        queue.EvSerInfo = &queue.EvSerInfoHolder;
                    }

                    // fill in event header
                    queue.EventHeader = {
                        .Type = ev.Type,
                        .Flags = ev.Flags | (queue.EvSerInfo->IsExtendedFormat ? IEventHandle::FlagExtendedFormat : 0),
                        .Cookie = ev.Cookie,
                        .Sender = ev.Sender,
                        .Recipient = ev.Recipient,
                    };
                    ev.TraceId.Serialize(&queue.EventHeader.TraceId);
                    break;

                case ESerializeStage::kBufferSerializer:
                    while (maxBytesToProduce && queue.Iter.Valid()) {
                        const size_t numBytes = Min(maxBytesToProduce, queue.Iter.ContiguousSize());
                        produceOutputSpan(TContiguousSpan(queue.Iter.ContiguousData(), numBytes));
                        chunkHeader->TypeLength += numBytes;
                        queue.Iter += numBytes;
                    }
                    if (!queue.Iter.Valid()) {
                        queue.SerializeStage = ESerializeStage::kHeader;
                    }
                    break;

                case ESerializeStage::kChunkSerializer: {
                    // serialize as much as we can
                    bool progress = false;
                    for (const auto [data, size] : queue.CoroutineChunkSerializer.FeedBuf(buffer, maxBytesToProduce)) {
                        produceOutputSpan(TContiguousSpan(data, size));
                        chunkHeader->TypeLength += size;
                        Y_ABORT_UNLESS(size);
                        progress = true;
                    }

                    // check if we have finished serializing this event
                    if (queue.CoroutineChunkSerializer.IsComplete()) {
                        queue.SerializeStage = ESerializeStage::kHeader;
                    } else if (!progress) {
                        // we didn't serialize anything but the chunk header, so we should exit the loop after dropping
                        // the header
                        Y_ABORT_UNLESS(!chunkHeader->GetLength());
                        Y_ABORT_UNLESS(!buffer->size());
                        quit = true;
                    }
                    break;
                }

                case ESerializeStage::kHeader: {
                    const size_t numDataBytes = Min(
                        buffer->size(),
                        maxBytesToProduce,
                        sizeof(TEventHeader) - queue.EventHeaderOffset
                    );
                    chunkHeader->TypeLength += numDataBytes;
                    Y_ABORT_UNLESS(chunkHeader->GetLength() == numDataBytes);

                    void *ptr = takeInBuffer(numDataBytes);
                    memcpy(ptr, reinterpret_cast<const char*>(&queue.EventHeader) + queue.EventHeaderOffset, numDataBytes);
                    queue.EventHeaderOffset += numDataBytes;

                    if (queue.EventHeaderOffset == sizeof(TEventHeader)) {
                        RefcountItems.push_back({
                            .NumBytesRemaining = std::exchange(queue.EventProducedSize, 0),
                            .Buffer = std::exchange(queue.Buffer, nullptr),
                            .Event{ev.ReleaseBase().Release()},
                        });
                        CommitProducedBytes(std::exchange(OverproducedBytes, 0));
                        queue.Events.pop_front();
                        queue.SerializeStage = ESerializeStage::kInitial;
                        queue.EventHeaderOffset = 0;
                    }
                    break;
                }
            }

            if (chunkHeader && !chunkHeader->GetLength()) {
                // drop useless chunk header (when we have produced empty event, for instance); note that the chunk
                // header may have been concatenated with a preceding output span, so we trim it off the tail instead of
                // assuming it occupies a whole span
                Y_ABORT_UNLESS(!out->empty());
                TContiguousSpan& last = out->back();
                Y_ABORT_UNLESS(last.size() >= sizeof(TChunkHeader));
                Y_ABORT_UNLESS(last.data() + last.size() == buffer->data());
                Y_ABORT_UNLESS(last.data() + last.size() - sizeof(TChunkHeader) == reinterpret_cast<const char*>(chunkHeader));
                if (last.size() == sizeof(TChunkHeader)) {
                    out->pop_back();
                } else {
                    last = {last.data(), last.size() - sizeof(TChunkHeader)};
                }
                *buffer = {buffer->data() - sizeof(TChunkHeader), buffer->size() + sizeof(TChunkHeader)};
                maxBytesToProduce += sizeof(TChunkHeader);
                numBytesProduced -= sizeof(TChunkHeader);
                queue.EventProducedSize -= sizeof(TChunkHeader);
            } else if (chunkHeader) {
                // find chunk header in out
                for (size_t i = 0; i < out->size(); ++i) {
                    const void *begin = (*out)[i].data();
                    const void *end = (*out)[i].data() + (*out)[i].size();
                    if (chunkHeader >= begin && chunkHeader + 1 <= end) {
                        size_t numBytesAfter = (const char*)end - (const char*)(chunkHeader + 1);
                        while (++i < out->size()) {
                            const void *begin = (*out)[i].data();
                            const void *end = (*out)[i].data() + (*out)[i].size();
                            Y_ABORT_UNLESS(!(chunkHeader >= begin && chunkHeader + 1 <= end));

                            numBytesAfter += (*out)[i].size();
                        }
                        Y_ABORT_UNLESS(numBytesAfter == chunkHeader->GetLength());
                    }
                }
            }

            if (quit) {
                break;
            }
        }

        return numBytesProduced;
    }

    void TEventDeserializer::Push(TRcBuf buffer, IEventProcessor *eventProcessor) {
        // put incoming buffer to the queue's end
        Accum.Insert(Accum.End(), std::move(buffer));

        // parse accumulator
        while (Accum.size() >= sizeof(TChunkHeader)) {
            // extract next chunk's header
            TChunkHeader header;
            Accum.begin().ExtractPlainDataAndAdvance(&header, sizeof(header));

            // check if the whole chunks fits the accumulator
            if (const size_t length = header.GetLength(); Accum.size() >= sizeof(TChunkHeader) + length) {
                // remove the just-parsed header
                Accum.EraseFront(sizeof(header));

                TPerChannelQueue& queue = GetQueue(header.Channel);

                switch (header.TypeLength & TChunkHeader::TypeMask) {
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

                            queue.EvSerInfo.IsExtendedFormat = queue.EventHeader.Flags & IEventHandle::FlagExtendedFormat;
                            eventProcessor->PushEvent(std::make_unique<IEventHandle>(
                                TActorId(), // session id will be filled later
                                queue.EventHeader.Type,
                                queue.EventHeader.Flags & ~IEventHandle::FlagExtendedFormat,
                                queue.EventHeader.Recipient,
                                queue.EventHeader.Sender,
                                MakeIntrusive<TEventSerializedData>(
                                    std::exchange(queue.Accum, {}),
                                    std::exchange(queue.EvSerInfo, {})),
                                queue.EventHeader.Cookie,
                                TScopeId(),
                                NWilson::TTraceId(queue.EventHeader.TraceId)));
                        }

                        break;

                    default:
                        Y_ABORT("unsupported type");
                }
            } else {
                break;
            }
        }
    }

} // NActors
