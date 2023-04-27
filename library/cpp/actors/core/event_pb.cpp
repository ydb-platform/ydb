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
        Y_VERIFY(count <= TotalByteCount);
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
        Y_VERIFY(Finished);
    }

    bool TCoroutineChunkSerializer::AllowsAliasing() const {
        return true;
    }

    bool TCoroutineChunkSerializer::Produce(const void *data, size_t size) {
        Y_VERIFY(size <= SizeRemain);
        SizeRemain -= size;
        TotalSerializedDataSize += size;

        if (NumChunks) {
            auto& last = Chunks[NumChunks - 1];
            if (last.first + last.second == data) {
                last.second += size; // just extend the last buffer
                return true;
            }
        }

        if (NumChunks == MaxChunks) {
            InnerContext.SwitchTo(BufFeedContext);
            if (CancelFlag || AbortFlag) {
                return false;
            }
        }

        Y_VERIFY(NumChunks < MaxChunks);
        Chunks[NumChunks++] = {static_cast<const char*>(data), size};
        return true;
    }

    bool TCoroutineChunkSerializer::WriteAliasedRaw(const void* data, int size) {
        Y_VERIFY(size >= 0);
        while (size) {
            if (CancelFlag || AbortFlag) {
                return false;
            } else if (const size_t bytesToAppend = Min<size_t>(size, SizeRemain)) {
                if ((reinterpret_cast<uintptr_t>(data) & 63) + bytesToAppend <= 2 * 64 &&
                        (NumChunks == 0 || data != Chunks[NumChunks - 1].first + Chunks[NumChunks - 1].second)) {
                    memcpy(BufferPtr, data, bytesToAppend);

                    if (!Produce(BufferPtr, bytesToAppend)) {
                        return false;
                    }

                    BufferPtr += bytesToAppend;
                    data = static_cast<const char*>(data) + bytesToAppend;
                    size -= bytesToAppend;
                } else {
                    if (!Produce(data, bytesToAppend)) {
                        return false;
                    }
                    data = static_cast<const char*>(data) + bytesToAppend;
                    size -= bytesToAppend;
                }
            } else {
                InnerContext.SwitchTo(BufFeedContext);
            }
        }
        return true;
    }

    bool TCoroutineChunkSerializer::Next(void** data, int* size) {
        if (CancelFlag || AbortFlag) {
            return false;
        }
        if (!SizeRemain) {
            InnerContext.SwitchTo(BufFeedContext);
            if (CancelFlag || AbortFlag) {
                return false;
            }
        }
        Y_VERIFY(SizeRemain);
        *data = BufferPtr;
        *size = SizeRemain;
        BufferPtr += SizeRemain;
        return Produce(*data, *size);
    }

    void TCoroutineChunkSerializer::BackUp(int count) {
        if (!count) {
            return;
        }
        Y_VERIFY(count > 0);
        Y_VERIFY(NumChunks);
        TChunk& buf = Chunks[NumChunks - 1];
        Y_VERIFY((size_t)count <= buf.second);
        Y_VERIFY(buf.first + buf.second == BufferPtr);
        buf.second -= count;
        if (!buf.second) {
            --NumChunks;
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

    std::pair<TCoroutineChunkSerializer::TChunk*, TCoroutineChunkSerializer::TChunk*> TCoroutineChunkSerializer::FeedBuf(void* data, size_t size) {
        // fill in base params
        BufferPtr = static_cast<char*>(data);
        SizeRemain = size;
        Y_VERIFY_DEBUG(size);

        // transfer control to the coroutine
        Y_VERIFY(Event);
        NumChunks = 0;
        Resume();

        return {Chunks, Chunks + NumChunks};
    }

    void TCoroutineChunkSerializer::SetSerializingEvent(const IEventBase *event) {
        Y_VERIFY(Event == nullptr);
        Event = event;
        TotalSerializedDataSize = 0;
        AbortFlag = false;
    }

    void TCoroutineChunkSerializer::Abort() {
        Y_VERIFY(Event);
        AbortFlag = true;
        Resume();
    }

    void TCoroutineChunkSerializer::DoRun() {
        while (!CancelFlag) {
            Y_VERIFY(Event);
            SerializationSuccess = Event->SerializeToArcadiaStream(this);
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
        Y_VERIFY(false);
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
