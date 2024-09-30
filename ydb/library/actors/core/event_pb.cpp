#include "event_pb.h"

namespace NActors {
    bool TRopeStream::Next(const void** data, int* size) {
        *data = Iter.ContiguousData();
        *size = Iter.ContiguousSize();
        if (size_t(*size + TotalByteCount) > Size) {
            *size = Size - TotalByteCount;
            Iter += *size;
        } else if (Iter.Valid()) {
            Iter.AdvanceToNextContiguousBlock();
        }
        TotalByteCount += *size;
        return *size != 0;
    }

    void TRopeStream::BackUp(int count) {
        Y_ABORT_UNLESS(count <= TotalByteCount);
        Iter -= count;
        TotalByteCount -= count;
    }

    bool TRopeStream::Skip(int count) {
        if (static_cast<size_t>(TotalByteCount + count) > Size) {
            count = Size - TotalByteCount;
        }
        Iter += count;
        TotalByteCount += count;
        return static_cast<size_t>(TotalByteCount) != Size;
    }

    TCoroutineChunkSerializer::TCoroutineChunkSerializer()
        : TotalSerializedDataSize(0)
        , Stack(64 * 1024)
        , SelfClosure{this, TArrayRef(Stack.Begin(), Stack.End())}
        , InnerContext(SelfClosure)
    {}

    TCoroutineChunkSerializer::~TCoroutineChunkSerializer() {
        CancelFlag = true;
        Resume();
        Y_ABORT_UNLESS(Finished);
    }

    bool TCoroutineChunkSerializer::AllowsAliasing() const {
        return true;
    }

    void TCoroutineChunkSerializer::Produce(const void *data, size_t size) {
        Y_ABORT_UNLESS(size <= SizeRemain);
        SizeRemain -= size;
        TotalSerializedDataSize += size;

        if (!Chunks.empty()) {
            auto& last = Chunks.back();
            if (last.first + last.second == data) {
                last.second += size; // just extend the last buffer
                return;
            }
        }

        Chunks.emplace_back(static_cast<const char*>(data), size);
    }

    bool TCoroutineChunkSerializer::WriteAliasedRaw(const void* data, int size) {
        Y_ABORT_UNLESS(!CancelFlag);
        Y_ABORT_UNLESS(!AbortFlag);
        Y_ABORT_UNLESS(size >= 0);
        while (size) {
            if (const size_t bytesToAppend = Min<size_t>(size, SizeRemain)) {
                const void *produce = data;
                if ((reinterpret_cast<uintptr_t>(data) & 63) + bytesToAppend <= 64 &&
                        (Chunks.empty() || data != Chunks.back().first + Chunks.back().second)) {
                    memcpy(BufferPtr, data, bytesToAppend);
                    produce = BufferPtr;
                    BufferPtr += bytesToAppend;
                }
                Produce(produce, bytesToAppend);
                data = static_cast<const char*>(data) + bytesToAppend;
                size -= bytesToAppend;
            } else {
                InnerContext.SwitchTo(BufFeedContext);
                if (CancelFlag || AbortFlag) {
                    return false;
                }
            }
        }
        return true;
    }

    bool TCoroutineChunkSerializer::Next(void** data, int* size) {
        Y_ABORT_UNLESS(!CancelFlag);
        Y_ABORT_UNLESS(!AbortFlag);
        if (!SizeRemain) {
            InnerContext.SwitchTo(BufFeedContext);
            if (CancelFlag || AbortFlag) {
                return false;
            }
        }
        Y_ABORT_UNLESS(SizeRemain);
        *data = BufferPtr;
        *size = SizeRemain;
        BufferPtr += SizeRemain;
        Produce(*data, *size);
        return true;
    }

    void TCoroutineChunkSerializer::BackUp(int count) {
        if (!count) {
            return;
        }
        Y_ABORT_UNLESS(count > 0);
        Y_ABORT_UNLESS(!Chunks.empty());
        TChunk& buf = Chunks.back();
        Y_ABORT_UNLESS((size_t)count <= buf.second);
        Y_ABORT_UNLESS(buf.first + buf.second == BufferPtr, "buf# %p:%zu BufferPtr# %p SizeRemain# %zu NumChunks# %zu",
            buf.first, buf.second, BufferPtr, SizeRemain, Chunks.size());
        buf.second -= count;
        if (!buf.second) {
            Chunks.pop_back();
        }
        BufferPtr -= count;
        SizeRemain += count;
        TotalSerializedDataSize -= count;
    }

    void TCoroutineChunkSerializer::Resume() {
        TContMachineContext feedContext;
        BufFeedContext = &feedContext;
        feedContext.SwitchTo(&InnerContext);
        BufFeedContext = nullptr;
    }

    bool TCoroutineChunkSerializer::WriteRope(const TRope *rope) {
        for (auto iter = rope->Begin(); iter.Valid(); iter.AdvanceToNextContiguousBlock()) {
            if (!WriteAliasedRaw(iter.ContiguousData(), iter.ContiguousSize())) {
                return false;
            }
        }
        return true;
    }

    bool TCoroutineChunkSerializer::WriteString(const TString *s) {
        return WriteAliasedRaw(s->data(), s->length());
    }

    std::span<TCoroutineChunkSerializer::TChunk> TCoroutineChunkSerializer::FeedBuf(void* data, size_t size) {
        // fill in base params
        BufferPtr = static_cast<char*>(data);
        SizeRemain = size;
        Y_DEBUG_ABORT_UNLESS(size);

        // transfer control to the coroutine
        Y_ABORT_UNLESS(Event);
        Chunks.clear();
        Resume();

        return Chunks;
    }

    void TCoroutineChunkSerializer::SetSerializingEvent(const IEventBase *event) {
        Y_ABORT_UNLESS(Event == nullptr);
        Event = event;
        TotalSerializedDataSize = 0;
        AbortFlag = false;
    }

    void TCoroutineChunkSerializer::Abort() {
        Y_ABORT_UNLESS(Event);
        AbortFlag = true;
        Resume();
    }

    void TCoroutineChunkSerializer::DoRun() {
        while (!CancelFlag) {
            Y_ABORT_UNLESS(Event);
            SerializationSuccess = !AbortFlag && Event->SerializeToArcadiaStream(this);
            Event = nullptr;
            if (!CancelFlag) { // cancel flag may have been received during serialization
                InnerContext.SwitchTo(BufFeedContext);
            }
        }
        Finished = true;
        InnerContext.SwitchTo(BufFeedContext);
    }

    bool TAllocChunkSerializer::Next(void** pdata, int* psize) {
        if (Backup) {
            // we have some data in backup rope -- move the first chunk from the backup rope to the buffer and return
            // pointer to the buffer; it is safe to remove 'const' here as we uniquely own this buffer
            TRope::TIterator iter = Backup.Begin();
            *pdata = const_cast<char*>(iter.ContiguousData());
            *psize = iter.ContiguousSize();
            iter.AdvanceToNextContiguousBlock();
            Buffers->Append(Backup.Extract(Backup.Begin(), iter));
        } else {
            // no backup buffer, so we have to create new one
            auto item = TRopeAlignedBuffer::Allocate(4096);
            *pdata = item->GetBuffer();
            *psize = item->GetCapacity();
            Buffers->Append(TRope(std::move(item)));
        }
        return true;
    }

    void TAllocChunkSerializer::BackUp(int count) {
        Backup.Insert(Backup.Begin(), Buffers->EraseBack(count));
    }

    bool TAllocChunkSerializer::WriteAliasedRaw(const void*, int) {
        Y_ABORT_UNLESS(false);
        return false;
    }

    bool TAllocChunkSerializer::WriteRope(const TRope *rope) {
        Buffers->Append(TRope(*rope));
        return true;
    }

    bool TAllocChunkSerializer::WriteString(const TString *s) {
        Buffers->Append(*s);
        return true;
    }

    size_t SerializeNumber(size_t num, char *buffer) {
        char *begin = buffer;
        do {
            *buffer++ = (num & 0x7F) | (num >= 128 ? 0x80 : 0x00);
            num >>= 7;
        } while (num);
        return buffer - begin;
    }

    size_t DeserializeNumber(TRope::TConstIterator& iter, ui64& size) {
        size_t res = 0;
        size_t offset = 0;
        for (;;) {
            if (!iter.Valid()) {
                return Max<size_t>();
            }
            const char byte = *iter.ContiguousData();
            iter += 1;
            --size;
            res |= (static_cast<size_t>(byte) & 0x7F) << offset;
            offset += 7;
            if (!(byte & 0x80)) {
                break;
            }
        }
        return res;
    }

    bool SerializeToArcadiaStreamImpl(TChunkSerializer* chunker, const TVector<TRope> &payload) {
        // serialize payload first
        if (payload) {
            void *data;
            int size = 0;
            auto append = [&](const char *p, size_t len) {
                while (len) {
                    if (size) {
                        const size_t numBytesToCopy = std::min<size_t>(size, len);
                        memcpy(data, p, numBytesToCopy);
                        data = static_cast<char*>(data) + numBytesToCopy;
                        size -= numBytesToCopy;
                        p += numBytesToCopy;
                        len -= numBytesToCopy;
                    } else if (!chunker->Next(&data, &size)) {
                        return false;
                    }
                }
                return true;
            };
            auto appendNumber = [&](size_t number) {
                char buf[MaxNumberBytes];
                return append(buf, SerializeNumber(number, buf));
            };

            char marker = ExtendedPayloadMarker;
            append(&marker, 1);
            if (!appendNumber(payload.size())) {
                return false;
            }
            for (const TRope& rope : payload) {
                if (!appendNumber(rope.GetSize())) {
                    return false;
                }
            }
            if (size) {
                chunker->BackUp(std::exchange(size, 0));
            }
            for (const TRope& rope : payload) {
                if (!chunker->WriteRope(&rope)) {
                    return false;
                }
            }
        }

        return true;
    }


    void ParseExtendedFormatPayload(TRope::TConstIterator &iter, size_t &size, TVector<TRope> &payload, size_t &totalPayloadSize)
    {
        // check marker
        if (!iter.Valid() || (*iter.ContiguousData() != PayloadMarker && *iter.ContiguousData() != ExtendedPayloadMarker)) {
            Y_ABORT("invalid event");
        }

        const bool dataIsSeparate = *iter.ContiguousData() == ExtendedPayloadMarker; // ropes go after sizes

        auto fetchRope = [&](size_t len) {
            TRope::TConstIterator begin = iter;
            iter += len;
            size -= len;
            payload.emplace_back(begin, iter);
            totalPayloadSize += len;
        };

        // skip marker
        iter += 1;
        --size;
        // parse number of payload ropes
        size_t numRopes = DeserializeNumber(iter, size);
        if (numRopes == Max<size_t>()) {
            Y_ABORT("invalid event");
        }
        TStackVec<size_t, 16> ropeLens;
        if (dataIsSeparate) {
            ropeLens.reserve(numRopes);

        }
        while (numRopes--) {
            // parse length of the rope
            const size_t len = DeserializeNumber(iter, size);
            if (len == Max<size_t>() || size < len) {
                Y_ABORT("invalid event len# %zu size# %" PRIu64, len, size);
            }
            // extract the rope
            if (dataIsSeparate) {
                ropeLens.push_back(len);
            } else {
                fetchRope(len);
            }
        }
        for (size_t len : ropeLens) {
            fetchRope(len);
        }
    }

    ui32 CalculateSerializedSizeImpl(const TVector<TRope> &payload, ssize_t recordSize) {
        ssize_t result = recordSize;
        if (result >= 0 && payload) {
            ++result; // marker
            char buf[MaxNumberBytes];
            result += SerializeNumber(payload.size(), buf);
            size_t totalPayloadSize = 0;
            for (const TRope& rope : payload) {
                size_t ropeSize = rope.GetSize();
                totalPayloadSize += ropeSize;
                result += SerializeNumber(ropeSize, buf);
            }
            result += totalPayloadSize;
        }
        return result;
    }

    TEventSerializationInfo CreateSerializationInfoImpl(size_t preserializedSize, bool allowExternalDataChannel, const TVector<TRope> &payload, ssize_t recordSize) {
            TEventSerializationInfo info;
            info.IsExtendedFormat = static_cast<bool>(payload);

            if (allowExternalDataChannel) {
                if (payload) {
                    char temp[MaxNumberBytes];
                    size_t headerLen = 1 + SerializeNumber(payload.size(), temp);
                    for (const TRope& rope : payload) {
                        headerLen += SerializeNumber(rope.size(), temp);
                    }
                    info.Sections.push_back(TEventSectionInfo{0, headerLen, 0, 0, true});
                    for (const TRope& rope : payload) {
                        info.Sections.push_back(TEventSectionInfo{0, rope.size(), 0, 0, false});
                    }
                }

                const size_t byteSize = Max<ssize_t>(0, recordSize) + preserializedSize;
                info.Sections.push_back(TEventSectionInfo{0, byteSize, 0, 0, true}); // protobuf itself

#ifndef NDEBUG
                size_t total = 0;
                for (const auto& section : info.Sections) {
                    total += section.Size;
                }
                size_t serialized = CalculateSerializedSizeImpl(payload, recordSize);
                Y_ABORT_UNLESS(total == serialized, "total# %zu serialized# %zu byteSize# %zd payload.size# %zu", total,
                    serialized, byteSize, payload.size());
#endif
            }

            return info;
        }
}
