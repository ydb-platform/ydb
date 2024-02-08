#pragma once

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_completion.h>
#include <ydb/core/debug/valgrind_check.h>

#include <ydb/library/pdisk_io/spdk_state.h>
#include <ydb/library/actors/util/unordered_cache.h>

#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NPDisk {

//
// TAlignedData
//

struct TAlignedData {
    TArrayHolder<ui8> Holder;
    ui8 *AlignedBuffer = nullptr;
    ui32 BufferSize = 0;
    bool UseHugePages = false;
public:
    TAlignedData() = default;
    TAlignedData(TAlignedData &&data) = default;

    TAlignedData(ui32 size)
        : TAlignedData(size, false)
    {}

    TAlignedData(ui32 size, bool useHugePages)
        : AlignedBuffer(nullptr)
        , BufferSize(size)
        , UseHugePages(useHugePages)
    {
        constexpr intptr_t alignment = 4096; //block device block size / or 512 for newer linux versions
        if (UseHugePages) {
            auto spdkState = Singleton<TSpdkStateOSS>();
            if (AlignedBuffer) {
                spdkState->Free(AlignedBuffer);
            }
            AlignedBuffer = spdkState->Malloc(size, alignment);
        } else {
            Holder.Reset(new ui8[size + alignment - 1]);
            AlignedBuffer = (ui8*)((intptr_t)(Holder.Get() + alignment - 1) / alignment * alignment);
        }
        NSan::Poison(AlignedBuffer, size);
        REQUEST_VALGRIND_MAKE_MEM_UNDEFINED(AlignedBuffer, size);
    }

    ui8* Get() {
        return AlignedBuffer;
    }

    const ui8* Get() const {
        return AlignedBuffer;
    }

    ui32 Size() const {
        return BufferSize;
    }

    ~TAlignedData() {
        if (UseHugePages && AlignedBuffer) {
            auto spdkState = Singleton<TSpdkStateOSS>();
            spdkState->Free(AlignedBuffer);
        }
    }
};

//
// TBuffer
//

class TBufferPool;
class TReturnToPool;

struct TBuffer : TCompletionAction {
    using TPtr = THolder<TBuffer, TReturnToPool>;

    // Used only if buffer pool exhausted
    TAlignedData FallbackData;

    ui8 *Buffer;
    ui32 BufferSize;
    TBufferPool *Pool;
    i32 PopCount;

    TBuffer(ui8 *buffer, ui32 bufferSize, TBufferPool *pool);
    TBuffer(ui32 bufferSize, bool useHugePages);
    virtual ~TBuffer();
    void Exec(TActorSystem *actorSystem) override;
    void Release(TActorSystem *actorSystem) override;
    bool ReturnToPool();
    ui8* Data() const;
    ui32 Size() const;
    void RemoveFromPool();
};

class TReturnToPool {
public:
    static inline void Destroy(TBuffer *buffer) {
        if (!buffer->ReturnToPool()) {
            // buffer was allocated in heap
            delete buffer;
        }
    }
};


//
// TBufferPool
//
class TBufferPool {
public:
    struct TPDiskParams {
        ::NMonitoring::TDynamicCounters::TCounterPtr FailedAllocations = nullptr;
        TActorSystem *ActorSystem = nullptr;
        ui32 PDiskId = 0;

        TPDiskParams() = default;

        TPDiskParams(::NMonitoring::TDynamicCounters::TCounterPtr failedAllocations, TActorSystem *actorSystem,
                ui32 pDiskId)
            : FailedAllocations(failedAllocations)
            , ActorSystem(actorSystem)
            , PDiskId(pDiskId)
        {}
    };

private:
    ui32 BufferSize;
    ui32 BuffersCount;
    ui8 *AlignedData;
    TUnorderedCache<TBuffer*, 512, 3> Buffers;
    TAtomic ReadRotation;
    TAtomic WriteRotation;
    TPDiskParams Params;

protected:
    // Set by inherited classes
    bool UseHugePages;

public:
    TBufferPool(ui32 bufferSize, ui32 bufferCount, TPDiskParams params);
    void MarkUpPool(ui8 *alignedData);
    TBuffer *Pop();
    ui32 GetBufferSize();
    void Push(TBuffer *buffer);
    virtual ~TBufferPool() = 0;
};

//
// TBufferPoolCommon
//
class TBufferPoolCommon : public TBufferPool {
    TArrayHolder<ui8> RawBuffer;
public:
    TBufferPoolCommon(ui32 bufferSize, ui32 bufferCount, TBufferPool::TPDiskParams params);
    virtual ~TBufferPoolCommon();
};

//
// TBufferPoolHugePages
//
class TBufferPoolHugePages : public TBufferPool {
    ui8 *AlignedBuffer;
public:
    TBufferPoolHugePages(ui32 bufferSize, ui32 bufferCount, TPDiskParams params);
    virtual ~TBufferPoolHugePages();
};

TBufferPool *CreateBufferPool(ui64 size, ui32 bufferCount, bool UseHugePages, TBufferPool::TPDiskParams params);

} // NPDisk
} // NKikimr

