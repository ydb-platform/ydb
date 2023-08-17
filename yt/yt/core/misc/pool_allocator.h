#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/size_literals.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Allocates blocks of given fixed size with given alignment.
//! The underlying memory is acquired in chunks of fixed size.
/*!
 *  \note
 *  Thread affinity: single-threaded.
 */
class TPoolAllocator
{
public:
    //! Initializes a new allocator.
    /*!
     *  All allocated blocks will be #blockSize bytes and will obey #blockAlignment.
     *  Chunks of #chunkSize (approximately) tagged with #cookie will be used
     *  as an underlying storage.
     */
    TPoolAllocator(
        size_t blockSize,
        size_t blockAlignment,
        size_t chunkSize,
        TRefCountedTypeCookie cookie);

    //! Allocates a new block.
    void* Allocate() noexcept;

    //! Frees a block previously allocated via #Allocate.
    /*!
     *  The call must take place within the same thread the block was allocated.
     */
    static void Free(void* ptr) noexcept;

    //! A base for objects instantiated via #TPoolAllocator::New.
    class TObjectBase
    {
    public:
        void* operator new(size_t size, TPoolAllocator* allocator) noexcept;
        void* operator new(size_t size, void* where) noexcept;
        void operator delete(void* ptr) noexcept;

        [[deprecated("Use TPoolAllocator::New for instantiation")]]
        void* operator new(size_t size) noexcept;
        [[deprecated("Vectorized allocations are not supported")]]
        void* operator new[](size_t size) noexcept;
        [[deprecated("Vectorized allocations are not supported")]]
        void operator delete[](void* ptr) noexcept;
    };

    //! Allocates a new object of type #T (derived from #TPoolAllocator::TObjectBase)
    //! from a per-thread pool.
    /*!
     *  \note
     *  The object's disposal via |operator delete| must take place within
     *  the same thread the object was created.
     */
    template <std::derived_from<TObjectBase> T, class... TArgs>
    static std::unique_ptr<T> New(TArgs&&... args);

private:
    const size_t BlockSize_;
    const size_t BlockAlignment_;
    const size_t ChunkSize_;
    const TRefCountedTypeCookie Cookie_;

    DECLARE_THREAD_AFFINITY_SLOT(HomeThread);

    struct TAllocatedBlockHeader;
    struct TFreeBlockHeader;

    std::vector<TSharedRef> Chunks_;
    TFreeBlockHeader* FirstFree_ = nullptr;

    void DoFree(void* ptr);
    void AllocateChunk();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define POOL_ALLOCATOR_INL_H_
#include "pool_allocator-inl.h"
#undef POOL_ALLOCATOR_INL_H_
