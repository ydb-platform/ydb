#include <util/system/info.h>
#include <util/system/defaults.h>

#if defined(_win_)
#include <util/system/winint.h>
#elif defined(_unix_)
#include <sys/types.h>
#include <sys/mman.h>

#ifndef MAP_NOCORE
#define MAP_NOCORE 0
#endif
#else
#error todo
#endif

#include "remmap.h"

static const size_t REMMAP_PAGESIZE = NSystemInfo::GetPageSize();

#if defined(_unix_)
TRemmapAllocation::TRemmapAllocation()
    : Ptr_(nullptr)
    , Size_(0)
{
}

TRemmapAllocation::TRemmapAllocation(size_t size, char* base)
    : Ptr_(nullptr)
    , Size_(0)
{
    Alloc(size, base);
}

char* TRemmapAllocation::Alloc(size_t size, char* base) {
    assert(Ptr_ == nullptr);

    if (!size)
        return nullptr;

    const size_t HUGESIZE = size_t(16) << 30;
    Ptr_ = CommonMMap(HUGESIZE, base);

    if (Ptr_ != (char*)MAP_FAILED)
        munmap((void*)Ptr_, HUGESIZE);
    else
        Ptr_ = nullptr;

    Ptr_ = CommonMMap(AlignUp(size, REMMAP_PAGESIZE), Ptr_);
    if (Ptr_ == (char*)MAP_FAILED)
        Ptr_ = nullptr;

    Size_ = Ptr_ ? size : 0;
    return Ptr_;
}

char* TRemmapAllocation::Realloc(size_t newsize) {
    if (Ptr_ == nullptr)
        return Alloc(newsize);

    size_t realSize = AlignUp(Size_, REMMAP_PAGESIZE);
    size_t needSize = AlignUp(newsize, REMMAP_PAGESIZE);

    if (needSize > realSize) {
        char* part = Ptr_ + realSize;
        char* bunch = CommonMMap(needSize - realSize, part);
        if (bunch != (char*)MAP_FAILED && bunch != part)
            munmap(bunch, needSize - realSize);
        if (bunch == (char*)MAP_FAILED || bunch != part)
            return FullRealloc(newsize);
    } else if (needSize < realSize)
        munmap(Ptr_ + needSize, realSize - needSize);

    if ((Size_ = newsize) == 0)
        Ptr_ = nullptr;

    return Ptr_;
}

void TRemmapAllocation::Dealloc() {
    if (Ptr_ != nullptr)
        munmap(Ptr_, AlignUp(Size_, REMMAP_PAGESIZE));
    Ptr_ = nullptr;
    Size_ = 0;
}

char* TRemmapAllocation::FullRealloc(size_t newsize) {
    char* newPtr = CommonMMap(newsize);
    Y_ABORT_UNLESS(newPtr != MAP_FAILED, "mmap failed");

    size_t useful = Min(Size_, newsize), cur = 0;

    for (; cur + REMMAP_PAGESIZE < useful; cur += REMMAP_PAGESIZE) {
        memcpy((void*)&newPtr[cur], (void*)&Ptr_[cur], REMMAP_PAGESIZE);
        munmap((void*)&Ptr_[cur], REMMAP_PAGESIZE);
    }

    memcpy((void*)&newPtr[cur], (void*)&Ptr_[cur], useful - cur);
    munmap((void*)&Ptr_[cur], AlignUp(Size_ - cur, REMMAP_PAGESIZE));

    Size_ = newsize;
    return (Ptr_ = newPtr);
}

inline char* TRemmapAllocation::CommonMMap(size_t size, char* base) {
    return (char*)mmap((void*)base, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
}

#else
TRemmapAllocation::TRemmapAllocation()
    : Allocation_(0, false, NULL)
{
}

TRemmapAllocation::TRemmapAllocation(size_t size, char* base)
    : Allocation_(size, false, (void*)base)
{
}

char* TRemmapAllocation::Alloc(size_t size, char* base) {
    return (char*)Allocation_.Alloc(size, (void*)base);
}

char* TRemmapAllocation::Realloc(size_t newsize) {
    return FullRealloc(newsize);
}

void TRemmapAllocation::Dealloc() {
    Allocation_.Dealloc();
}

char* TRemmapAllocation::FullRealloc(size_t newsize) {
    TMappedAllocation other(newsize);
    memcpy(other.Ptr(), Allocation_.Ptr(), Min(other.MappedSize(), Allocation_.MappedSize()));
    Allocation_.swap(other);
    return Data();
}
#endif
