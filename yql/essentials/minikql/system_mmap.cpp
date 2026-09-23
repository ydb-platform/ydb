#include "system_mmap.h"
#include "page_pool_constants.h"

#include <util/generic/singleton.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/align.h>
#include <util/system/defaults.h>
#include <util/system/error.h>
#include <util/system/info.h>
#include <util/system/yassert.h>

#if defined(_win_)
    #include <util/system/winint.h>
#elif defined(_unix_)
    #include <sys/types.h>
    #include <sys/mman.h>
#endif

namespace NKikimr {

namespace {

const size_t SystemPageSize = NSystemInfo::GetPageSize();

ui64 GetMaxMemoryMaps() {
    ui64 maxMapCount = 0;
#if defined(_unix_) && !defined(_darwin_)
    maxMapCount = FromString<ui64>(Strip(TFileInput("/proc/sys/vm/max_map_count").ReadAll()));
#endif
    return maxMapCount;
}

} // namespace

#ifdef _win_
void* TSystemMmap::Mmap(size_t size)
{
    if (auto res = ::VirtualAlloc(0, size, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE)) {
        return res;
    } else {
        return reinterpret_cast<void*>(-1);
    }
}

int TSystemMmap::Munmap(void* addr, size_t size) noexcept {
    Y_ABORT_UNLESS(AlignUp(addr, SystemPageSize) == addr, "Got unaligned address");
    Y_ABORT_UNLESS(AlignUp(size, SystemPageSize) == size, "Got unaligned size");
    return !::VirtualFree(addr, size, MEM_DECOMMIT);
}
#else
void* TSystemMmap::Mmap(size_t size)
{
    return ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
}

int TSystemMmap::Munmap(void* addr, size_t size) noexcept {
    Y_DEBUG_ABORT_UNLESS(AlignUp(addr, SystemPageSize) == addr, "Got unaligned address");
    Y_DEBUG_ABORT_UNLESS(AlignUp(size, SystemPageSize) == size, "Got unaligned size");

    if (size > MaxMidSize) {
        return ::munmap(addr, size);
    }

    // Unlock memory in case somewhere was called `mlockall(MCL_FUTURE)`.
    if (::munlock(addr, size) == -1) {
        switch (LastSystemError()) {
            case EAGAIN:
                [[fallthrough]];
                // The memory region was probably not locked - skip,
                // also since we can't distinguish from other kernel problems that may cause EAGAIN (not enough memory for structures?)
                // we rely on the failure of the following `madvise()` call.

            case EPERM:
                [[fallthrough]];
                // The most common case we get this error if we have no privileges, but also ignored error when called `mlockall()`
                // somewhere earlier. So ignore this.

            case EINVAL:
                // Something wrong with `addr` and `size` - we'll see the same error from the following `madvise()` call.
                break;

            case ENOMEM:
                // Locking or unlocking a region would result in the total number of mappings with distinct attributes
                // (e.g., locked versus unlocked) exceeding the allowed maximum.
                // NOTE: `madvise(MADV_DONTNEED)` can't return ENOMEM.
                return -1;
        }
    }

    /**
        There is at least a couple of drawbacks of using madvise instead of munmap:
        - more potential for use-after-free and memory corruption since we still may access unneeded regions by mistake,
        - actual RSS memory may be freed later after kernel gets some memory-pressure, and it may confuse system monitoring tools.

        But also there is a huge advantage: the number of memory maps used by process doesn't increase because of the "holes".

        The main source of the growth of number of memory regions is a clean-up of freed pages from page pools.
        Now we can safely invoke `TAlignedPagePool::DoCleanupGlobalFreeList()` whenever we want it.
     */
    return ::madvise(addr, size, MADV_DONTNEED);
}
#endif

TSystemMmap& TSystemMmap::GetInstance() {
    return *Singleton<TSystemMmap>();
}

size_t GetMemoryMapsCount() {
    size_t lineCount = 0;
    TString line;
#if defined(_unix_) && !defined(_darwin_)
    TFileInput file("/proc/self/maps");
    while (file.ReadLine(line)) {
        ++lineCount;
    }
#endif
    return lineCount;
}

TString GetMemoryMapsString() {
    TStringStream ss;
    ss << " (maps: " << GetMemoryMapsCount() << " vs " << GetMaxMemoryMaps() << ")";
    return ss.Str();
}

} // namespace NKikimr
