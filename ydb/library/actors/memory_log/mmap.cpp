#include "memlog.h"

#if defined(_unix_)
#include <sys/mman.h>
#elif defined(_win_)
#include <util/system/winint.h>
#else
#error NO IMPLEMENTATION FOR THE PLATFORM
#endif

void TMemoryLog::TMMapArea::MMap(size_t amount) {
    Y_ABORT_UNLESS(amount > 0);

#if defined(_unix_)
    constexpr int mmapProt = PROT_READ | PROT_WRITE;
#if defined(_linux_)
    constexpr int mmapFlags = MAP_PRIVATE | MAP_ANON | MAP_POPULATE;
#else
    constexpr int mmapFlags = MAP_PRIVATE | MAP_ANON;
#endif

    BufPtr = ::mmap(nullptr, amount, mmapProt, mmapFlags, -1, 0);
    if (BufPtr == MAP_FAILED) {
        throw std::bad_alloc();
    }

#elif defined(_win_)
    Mapping = ::CreateFileMapping(
        (HANDLE)-1, nullptr, PAGE_READWRITE, 0, amount, nullptr);
    if (Mapping == NULL) {
        throw std::bad_alloc();
    }
    BufPtr = ::MapViewOfFile(Mapping, FILE_MAP_WRITE, 0, 0, amount);
    if (BufPtr == NULL) {
        throw std::bad_alloc();
    }
#endif

    Size = amount;
}

void TMemoryLog::TMMapArea::MUnmap() {
    if (BufPtr == nullptr) {
        return;
    }

#if defined(_unix_)
    int result = ::munmap(BufPtr, Size);
    Y_ABORT_UNLESS(result == 0);

#elif defined(_win_)
    BOOL result = ::UnmapViewOfFile(BufPtr);
    Y_ABORT_UNLESS(result != 0);

    result = ::CloseHandle(Mapping);
    Y_ABORT_UNLESS(result != 0);

    Mapping = 0;
#endif

    BufPtr = nullptr;
    Size = 0;
}
