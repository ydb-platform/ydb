#pragma once

#include <util/system/yassert.h>
#include <util/system/align.h>
#include <util/system/info.h>
#include <util/system/filemap.h>
#include <util/memory/alloc.h>
#include <util/generic/noncopyable.h>

class TRemmapAllocation : TNonCopyable {
public:
    TRemmapAllocation();
    TRemmapAllocation(size_t size, char* base = nullptr);

    ~TRemmapAllocation() {
        Dealloc();
    }

    char* Alloc(size_t size, char* base = nullptr);
    char* Realloc(size_t newsize);
    void Dealloc();
    char* FullRealloc(size_t newsize);

#if defined(_unix_)
private:
    inline char* CommonMMap(size_t size, char* base = nullptr);

    char* Ptr_;
    size_t Size_;

public:
    inline void* Ptr() const {
        return (void*)Ptr_;
    }
    inline char* Data(ui32 pos = 0) const {
        return Ptr_ + pos;
    }
    inline size_t Size() const {
        return Size_;
    }
    inline void swap(TRemmapAllocation& other) {
        DoSwap(Ptr_, other.Ptr_);
        DoSwap(Size_, other.Size_);
    }

#else
private:
    TMappedAllocation Allocation_;

public:
    inline void* Ptr() const {
        return Allocation_.Ptr();
    }
    inline char* Data(ui32 pos = 0) const {
        return Allocation_.Data(pos);
    }
    inline size_t Size() const {
        return Allocation_.MappedSize();
    }
    inline void swap(TRemmapAllocation& other) {
        Allocation_.swap(other.Allocation_);
    }
#endif
};
