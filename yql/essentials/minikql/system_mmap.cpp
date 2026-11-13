#include "system_mmap.h"

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

TSystemError MakeMemoryError(int status, const char* operation, void* address, size_t size) {
    TSystemError error(status);
    error << operation << "(" << address << ", " << size << ") failed";
    if (status == ENOMEM) {
        error << GetMemoryMapsString();
    }
    return error;
}

} // namespace

#ifdef _win_
std::expected<void*, TSystemError> TSystemMmap::Mmap(size_t size) {
    if (void* address = ::VirtualAlloc(nullptr, size, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE)) {
        return address;
    }
    return std::unexpected(MakeMemoryError(LastSystemError(), "Mmap", /*address=*/nullptr, size));
}

std::expected<void, TSystemError> TSystemMmap::Munmap(void* addr, size_t size) {
    Y_ABORT_UNLESS(AlignUp(addr, SystemPageSize) == addr, "Got unaligned address");
    Y_ABORT_UNLESS(AlignUp(size, SystemPageSize) == size, "Got unaligned size");
    if (!::VirtualFree(addr, size, MEM_DECOMMIT)) {
        return std::unexpected(MakeMemoryError(LastSystemError(), "Munmap", addr, size));
    }
    return {};
}

std::expected<void, TSystemError> TSystemMmap::Freeze(void* addr, size_t size) {
    if (!::VirtualFree(addr, size, MEM_DECOMMIT)) {
        return std::unexpected(MakeMemoryError(LastSystemError(), "Freeze", addr, size));
    }
    return {};
}

std::expected<void, TSystemError> TSystemMmap::Unfreeze(void* addr, size_t size) {
    if (!::VirtualAlloc(addr, size, MEM_COMMIT, PAGE_READWRITE)) {
        return std::unexpected(MakeMemoryError(LastSystemError(), "Unfreeze", addr, size));
    }
    return {};
}
#else
std::expected<void*, TSystemError> TSystemMmap::Mmap(size_t size) {
    void* address = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
    if (address == MAP_FAILED) {
        return std::unexpected(MakeMemoryError(LastSystemError(), "Mmap", /*address=*/nullptr, size));
    }
    return address;
}

std::expected<void, TSystemError> TSystemMmap::Munmap(void* addr, size_t size) {
    Y_DEBUG_ABORT_UNLESS(AlignUp(addr, SystemPageSize) == addr, "Got unaligned address");
    Y_DEBUG_ABORT_UNLESS(AlignUp(size, SystemPageSize) == size, "Got unaligned size");
    if (::munmap(addr, size) == -1) {
        return std::unexpected(MakeMemoryError(LastSystemError(), "Munmap", addr, size));
    }
    return {};
}

std::expected<void, TSystemError> TSystemMmap::Freeze(void* addr, size_t size) {
    // MADV_DONTNEED rejects locked pages, including those locked by mlockall(MCL_FUTURE).
    if (::munlock(addr, size) == -1) {
        const int status = LastSystemError();
        if (status == ENOMEM) {
            return std::unexpected(MakeMemoryError(status, "Freeze", addr, size));
        }
    }
    if (::madvise(addr, size, MADV_DONTNEED) == -1) {
        return std::unexpected(MakeMemoryError(LastSystemError(), "Freeze", addr, size));
    }
    return {};
}

std::expected<void, TSystemError> TSystemMmap::Unfreeze(void*, size_t) {
    // MADV_DONTNEED preserves access permissions; pages are populated on demand.
    return {};
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
