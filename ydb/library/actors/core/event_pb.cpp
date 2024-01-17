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
}
