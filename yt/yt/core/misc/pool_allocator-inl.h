#ifndef POOL_ALLOCATOR_INL_H_
#error "Direct inclusion of this file is not allowed, include pool_allocator.h"
// For the sake of sane code completion.
#include "pool_allocator.h"
#endif

#include <library/cpp/yt/misc/tls.h>

#include <util/system/align.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TPoolAllocator::TAllocatedBlockHeader
{
    explicit TAllocatedBlockHeader(TPoolAllocator* pool)
        : Pool(pool)
    { }

    TPoolAllocator* Pool;
};

struct TPoolAllocator::TFreeBlockHeader
{
    explicit TFreeBlockHeader(TFreeBlockHeader* next)
        : Next(next)
    { }

    TFreeBlockHeader* Next;
};

////////////////////////////////////////////////////////////////////////////////

inline TPoolAllocator::TPoolAllocator(
    size_t blockSize,
    size_t blockAlignment,
    size_t chunkSize,
    TRefCountedTypeCookie cookie)
    : BlockSize_(blockSize)
    , BlockAlignment_(blockAlignment)
    , ChunkSize_(chunkSize)
    , Cookie_(cookie)
{ }

inline void* TPoolAllocator::Allocate() noexcept
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    if (Y_UNLIKELY(!FirstFree_)) {
        AllocateChunk();
    }

    auto* freeHeader = FirstFree_;
    FirstFree_ = freeHeader->Next;

    auto* ptr = reinterpret_cast<char*>(freeHeader + 1);
    auto* allocatedHeader = reinterpret_cast<TAllocatedBlockHeader*>(ptr) - 1;
    new(allocatedHeader) TAllocatedBlockHeader(this);
    return ptr;
}

inline void TPoolAllocator::Free(void* ptr) noexcept
{
    auto* header = static_cast<TAllocatedBlockHeader*>(ptr) - 1;
    header->Pool->DoFree(ptr);
}

template <std::derived_from<TPoolAllocator::TObjectBase> T, class... TArgs>
YT_PREVENT_TLS_CACHING std::unique_ptr<T> TPoolAllocator::New(TArgs&&... args)
{
    struct TChunkTag
    { };
    constexpr auto ChunkSize = 64_KB;
    thread_local TPoolAllocator Allocator(
        sizeof(T),
        alignof(T),
        ChunkSize,
        GetRefCountedTypeCookie<TChunkTag>());

    return std::unique_ptr<T>(new(&Allocator) T(std::forward<TArgs>(args)...));
}

inline void TPoolAllocator::DoFree(void* ptr)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    auto* header = static_cast<TFreeBlockHeader*>(ptr) - 1;
    new(header) TFreeBlockHeader(FirstFree_);
    FirstFree_ = header;
}

////////////////////////////////////////////////////////////////////////////////

inline void* TPoolAllocator::TObjectBase::operator new(size_t /*size*/, TPoolAllocator* allocator) noexcept
{
    return allocator->Allocate();
}

inline void* TPoolAllocator::TObjectBase::operator new(size_t /*size*/, void* where) noexcept
{
    return where;
}

inline void TPoolAllocator::TObjectBase::operator delete(void* ptr) noexcept
{
    TPoolAllocator::Free(ptr);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
