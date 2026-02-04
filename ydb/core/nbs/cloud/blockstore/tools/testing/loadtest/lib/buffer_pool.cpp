#include "buffer_pool.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/page_size.h>

#include <util/generic/singleton.h>
#include <util/memory/alloc.h>
#include <util/system/align.h>
#include <util/system/error.h>
#include <util/thread/lfstack.h>

#include <stdlib.h>

#if defined(_linux_)
#   include <sys/mman.h>
#   include <sys/types.h>
#elif defined(_win_)
#   include <util/system/winint.h>
#endif

namespace NYdb::NBS::NBlockStore::NLoadTest {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TBufferPool final
    : public IAllocator
{
private:
    const size_t PageSize = GetPlatformPageSize();
    static constexpr size_t NumBuckets = 1024;
    static constexpr size_t MaxSmallAlloc = 32*1024;

    TLockFreeStack<void*> Buckets[NumBuckets];

public:
    TBlock Allocate(size_t length) override
    {
        size_t allocBytes = AlignUp(length, PageSize);
        if (allocBytes <= MaxSmallAlloc) {
            void* ptr = DefaultAlloc(allocBytes);
            return { ptr, length };
        }

        size_t sizeIdx = allocBytes / PageSize;
        if (sizeIdx < NumBuckets) {
            void* ptr;
            if (Buckets[sizeIdx].Dequeue(&ptr)) {
                return { ptr, length };
            }
        }

        void* ptr = SystemAlloc(allocBytes);
        return { ptr, length };
    }

    void Release(const TBlock& block) override
    {
        size_t allocBytes = AlignUp(block.Len, PageSize);
        if (allocBytes <= MaxSmallAlloc) {
            DefaultFree(block.Data);
            return;
        }

        size_t sizeIdx = allocBytes / PageSize;
        if (sizeIdx < NumBuckets) {
            Buckets[sizeIdx].Enqueue(block.Data);
            return;
        }

        SystemFree(block.Data, allocBytes);
    }

private:
    static void* DefaultAlloc(size_t length)
    {
        void* ptr = malloc(length);
        Y_ABORT_UNLESS(ptr != nullptr, "malloc failed: %s", LastSystemErrorText());
        return ptr;
    }

    static void DefaultFree(void* ptr)
    {
        free(ptr);
    }

    static void* SystemAlloc(size_t length)
    {
#if defined(_linux_)
        int mapFlags = MAP_PRIVATE|MAP_ANON|MAP_POPULATE; // TODO: MAP_UNINITIALIZED
        void* ptr = mmap(nullptr, length, PROT_READ|PROT_WRITE, mapFlags, -1, 0);
        Y_ABORT_UNLESS(ptr != MAP_FAILED, "mmap failed: %s", LastSystemErrorText());
#elif defined(_win_)
        void* ptr = VirtualAlloc(nullptr, length, MEM_COMMIT, PAGE_READWRITE);
        Y_ABORT_UNLESS(ptr != nullptr, "VirtualAlloc failed: %s", LastSystemErrorText());
#else
        void* ptr = malloc(length);
        Y_ABORT_UNLESS(ptr != nullptr, "malloc failed: %s", LastSystemErrorText());
#endif
        return ptr;
    }

    static void SystemFree(void* ptr, size_t length)
    {
        Y_UNUSED(length);
#if defined(_linux_)
        int result = munmap(ptr, length);
        Y_ABORT_UNLESS(result == 0, "munmap failed: %s", LastSystemErrorText());
#elif defined(_win_)
        BOOL result = VirtualFree(ptr, 0, MEM_RELEASE);
        Y_ABORT_UNLESS(result != 0, "VirtualFree failed: %s", LastSystemErrorText());
#else
        free(ptr);
#endif
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IAllocator* BufferPool()
{
    return HugeSingletonWithPriority<TBufferPool, 0>();
}

}   // namespace NYdb::NBS::NBlockStore::NLoadTest
