#include "buffers.h"

#include <util/system/align.h>

namespace NKikimr {
namespace NPDisk {

//
// TBuffer
//

TBuffer::TBuffer(ui32 bufferSize, bool useHugePages)
    : FallbackData(bufferSize, useHugePages)
    , Buffer(FallbackData.Get())
    , BufferSize(bufferSize)
    , Pool(nullptr)
    , PopCount(0)
{
}

TBuffer::TBuffer(ui8* buffer, ui32 bufferSize, TBufferPool *pool)
    : Buffer(buffer)
    , BufferSize(bufferSize)
    , Pool(pool)
    , PopCount(0)
{}

TBuffer::~TBuffer() {
    Y_ABORT_UNLESS(!Pool);
}

void TBuffer::Exec(TActorSystem*) {
    ReturnToPool();
}

void TBuffer::Release(TActorSystem*) {
    ReturnToPool();
}

bool TBuffer::ReturnToPool() {
    if (Pool) {
        Pool->Push(this);
        return true;
    }

    delete this;
    return false;
}

ui8* TBuffer::Data() const {
    return Buffer;
}

ui32 TBuffer::Size() const {
    return BufferSize;
}

void TBuffer::RemoveFromPool() {
    Pool = nullptr;
    BufferSize = 0;
    Buffer = nullptr;
}

//
// TBufferPool
//

static constexpr size_t Alignment = 4096; // Block-device block size
static constexpr size_t PopRetries = 5;

TBufferPool::TBufferPool(ui32 bufferSize, ui32 buffersCount, TPDiskParams params)
    : BufferSize(bufferSize)
    , BuffersCount(buffersCount)
    , ReadRotation(0)
    , WriteRotation(0)
    , Params(params)
{
}

void TBufferPool::MarkUpPool(ui8 *alignedData) {
    AlignedData = alignedData;
    for (ui32 i = 0; i < BuffersCount; ++i) {
        TBuffer *buffer = new TBuffer(AlignedData + i * AlignUp((size_t)BufferSize, Alignment), BufferSize, this);
        Buffers.Push(buffer, AtomicIncrement(WriteRotation));
    }
}

ui32 TBufferPool::GetBufferSize() {
    return BufferSize;
}

TBuffer *TBufferPool::Pop() {
    size_t retry = 0;
    while (retry < PopRetries) {
        TBuffer* buffer = Buffers.Pop(AtomicIncrement(ReadRotation));
        if (buffer) {
            Y_VERIFY_S(buffer->PopCount == 0, "BufferPopCount# " << buffer->PopCount);
            buffer->PopCount++;
            NSan::Poison(buffer->Data(), buffer->Size());
            REQUEST_VALGRIND_MAKE_MEM_UNDEFINED(buffer->Data(), buffer->Size());
            return buffer;
        }
        if (Params.FailedAllocations) {
            *Params.FailedAllocations += 1;
        }
        Sleep(TDuration::MilliSeconds((retry + 1) * 10));
        ++retry;
    }
    if (Params.ActorSystem) {
        //LOG_NOTICE_S(*Params.ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << Params.PDiskId
        //        << "Failed to pop buffer from pool, retry# " << retry);
    }

    TBuffer *buffer = new TBuffer(BufferSize, UseHugePages);
    Y_VERIFY_S(buffer, "PDiskId# " << Params.PDiskId << "Cannot pop new buffer from PDisk's buffer pool");
    return buffer;
}

void TBufferPool::Push(TBuffer *buffer) {
    NSan::Poison(buffer->Data(), buffer->Size());
    REQUEST_VALGRIND_MAKE_MEM_UNDEFINED(buffer->Data(), buffer->Size());
    Y_VERIFY_S(buffer->PopCount == 1, "BufferPopCount# " << buffer->PopCount);
    buffer->PopCount--;
    Buffers.Push(buffer, AtomicIncrement(WriteRotation));
}

TBufferPool::~TBufferPool() {
    TBuffer *buffer = Buffers.Pop(AtomicIncrement(ReadRotation));
    while (buffer) {
        buffer->RemoveFromPool();
        delete buffer;
        buffer = Buffers.Pop(AtomicIncrement(ReadRotation));
    }
}

//
// TBufferPoolCommon
//
TBufferPoolCommon::TBufferPoolCommon(ui32 bufferSize, ui32 bufferCount, TBufferPool::TPDiskParams params)
    : TBufferPool(bufferSize, bufferCount, params)
{
    TBufferPool::UseHugePages = false;
    RawBuffer.Reset(new ui8[AlignUp((size_t)bufferSize, Alignment) * bufferCount + Alignment - 1]);
    ui8 *alignedData = (ui8*)AlignUp(RawBuffer.Get(), Alignment);
    Y_ABORT_UNLESS((ui64)alignedData % Alignment == 0);
    MarkUpPool(alignedData);
}

TBufferPoolCommon::~TBufferPoolCommon() {
}

TBufferPool *CreateBufferPool(ui64 size, ui32 bufferCount, bool UseHugePages, TBufferPool::TPDiskParams params) {
#ifdef _linux_
    if (UseHugePages) {
        return new TBufferPoolHugePages(size, bufferCount, params);
    } else
#endif
    {
        Y_UNUSED(UseHugePages);
        return new TBufferPoolCommon(size, bufferCount, params);
    }
}

} // NPDisk
} // NKikimr
