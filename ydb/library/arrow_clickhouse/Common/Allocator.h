// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once

#include <string.h>

#ifdef NDEBUG
    #define ALLOCATOR_ASLR 0
#else
    #define ALLOCATOR_ASLR 1
#endif

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif
#if !defined(WIN32)
#include <sys/mman.h>
#endif

#include <cstdlib>
#include <algorithm>
#include <stdexcept>

#include <common/mremap.h>
//#include <common/getPageSize.h>

#include <Common/Allocator_fwd.h>

/// Required for older Darwin builds, that lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

namespace CH
{

/// Get memory page size
int64_t getPageSize();

/**
  * Many modern allocators (for example, tcmalloc) do not do a mremap for
  * realloc, even in case of large enough chunks of memory. Although this allows
  * you to increase performance and reduce memory consumption during realloc.
  * To fix this, we do mremap manually if the chunk of memory is large enough.
  * The threshold (64 MB) is chosen quite large, since changing the address
  * space is very slow, especially in the case of a large number of threads. We
  * expect that the set of operations mmap/something to do/mremap can only be
  * performed about 1000 times per second.
  *
  * P.S. This is also required, because tcmalloc can not allocate a chunk of
  * memory greater than 16 GB.
  *
  * P.P.S. Note that MMAP_THRESHOLD symbol is intentionally made weak. It allows
  * to override it during linkage when using ClickHouse as a library in
  * third-party applications which may already use own allocator doing mmaps
  * in the implementation of alloc/realloc.
  */
extern const size_t MMAP_THRESHOLD;

static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;


/** Responsible for allocating / freeing memory. Used, for example, in PODArray, Arena.
  * Also used in hash tables.
  * The interface is different from std::allocator
  * - the presence of the method realloc, which for large chunks of memory uses mremap;
  * - passing the size into the `free` method;
  * - by the presence of the `alignment` argument;
  * - the possibility of zeroing memory (used in hash tables);
  * - random hint address for mmap
  * - mmap_threshold for using mmap less or more
  */
template <bool clear_memory_, bool mmap_populate>
class Allocator
{
public:
    /// Allocate memory range.
    void * alloc(size_t size, size_t alignment = 0)
    {
        checkSize(size);
        return allocNoTrack(size, alignment);
    }

    /// Free memory range.
    void free(void * buf, size_t size)
    {
        try
        {
            checkSize(size);
            freeNoTrack(buf, size);
        }
        catch (...)
        {
            //DB::tryLogCurrentException("Allocator::free");
            throw;
        }
    }

    /** Enlarge memory range.
      * Data from old range is moved to the beginning of new range.
      * Address of memory range could change.
      */
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0)
    {
        checkSize(new_size);

        if (old_size == new_size)
        {
            /// nothing to do.
            /// BTW, it's not possible to change alignment while doing realloc.
        }
        else if (old_size < MMAP_THRESHOLD && new_size < MMAP_THRESHOLD
                 && alignment <= MALLOC_MIN_ALIGNMENT)
        {
            void * new_buf = ::realloc(buf, new_size);
            if (nullptr == new_buf)
                throw std::runtime_error("Allocator: Cannot realloc");

            buf = new_buf;
            if constexpr (clear_memory)
                if (new_size > old_size)
                    memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
        }
        else if (old_size >= MMAP_THRESHOLD && new_size >= MMAP_THRESHOLD)
        {
            // On apple and freebsd self-implemented mremap used (common/mremap.h)
            buf = clickhouse_mremap(buf, old_size, new_size, MREMAP_MAYMOVE,
                                    PROT_READ | PROT_WRITE, mmap_flags, -1, 0);
            if (MAP_FAILED == buf)
                throw std::runtime_error("Allocator: Cannot mremap memory");

            /// No need for zero-fill, because mmap guarantees it.
        }
        else if (new_size < MMAP_THRESHOLD)
        {
            void * new_buf = allocNoTrack(new_size, alignment);
            memcpy(new_buf, buf, std::min(old_size, new_size));
            freeNoTrack(buf, old_size);
            buf = new_buf;
        }
        else
        {
            /// Big allocs that requires a copy. MemoryTracker is called inside 'alloc', 'free' methods.

            void * new_buf = alloc(new_size, alignment);
            memcpy(new_buf, buf, std::min(old_size, new_size));
            free(buf, old_size);
            buf = new_buf;
        }

        return buf;
    }

protected:
    static constexpr size_t getStackThreshold()
    {
        return 0;
    }

    static constexpr bool clear_memory = clear_memory_;

    // Freshly mmapped pages are copy-on-write references to a global zero page.
    // On the first write, a page fault occurs, and an actual writable page is
    // allocated. If we are going to use this memory soon, such as when resizing
    // hash tables, it makes sense to pre-fault the pages by passing
    // MAP_POPULATE to mmap(). This takes some time, but should be faster
    // overall than having a hot loop interrupted by page faults.
    // It is only supported on Linux.
    static constexpr int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS
#if defined(OS_LINUX)
        | (mmap_populate ? MAP_POPULATE : 0)
#endif
        ;

private:
    void * allocNoTrack(size_t size, size_t alignment)
    {
        void * buf;
        size_t mmap_min_alignment = getPageSize();

        if (size >= MMAP_THRESHOLD)
        {
            if (alignment > mmap_min_alignment)
                throw std::runtime_error("Too large alignment: more than page size when allocating");

            buf = mmap(getMmapHint(), size, PROT_READ | PROT_WRITE,
                       mmap_flags, -1, 0);
            if (MAP_FAILED == buf)
                throw std::runtime_error("Allocator: Cannot mmap");

            /// No need for zero-fill, because mmap guarantees it.
        }
        else
        {
            if (alignment <= MALLOC_MIN_ALIGNMENT)
            {
                if constexpr (clear_memory)
                    buf = ::calloc(size, 1);
                else
                    buf = ::malloc(size);

                if (nullptr == buf)
                    throw std::runtime_error("Allocator: Cannot malloc");
            }
            else
            {
                buf = nullptr;
                int res = posix_memalign(&buf, alignment, size);

                if (0 != res)
                    throw std::runtime_error("Cannot allocate memory (posix_memalign)");

                if constexpr (clear_memory)
                    memset(buf, 0, size);
            }
        }
        return buf;
    }

    void freeNoTrack(void * buf, size_t size)
    {
        if (size >= MMAP_THRESHOLD)
        {
            if (0 != munmap(buf, size))
                throw std::runtime_error("Allocator: Cannot munmap");
        }
        else
        {
            ::free(buf);
        }
    }

    void checkSize(size_t size)
    {
        /// More obvious exception in case of possible overflow (instead of just "Cannot mmap").
        if (size >= 0x8000000000000000ULL)
            throw std::runtime_error("Too large size passed to allocator. It indicates an error.");
    }

#ifndef NDEBUG
    /// In debug builds, request mmap() at random addresses (a kind of ASLR), to
    /// reproduce more memory stomping bugs. Note that Linux doesn't do it by
    /// default. This may lead to worse TLB performance.
    void * getMmapHint()
    {
        //return reinterpret_cast<void *>(std::uniform_int_distribution<intptr_t>(0x100000000000UL, 0x700000000000UL)(thread_local_rng));
        return nullptr;
    }
#else
    void * getMmapHint()
    {
        return nullptr;
    }
#endif
};

/** When using AllocatorWithStackMemory, located on the stack,
  *  GCC 4.9 mistakenly assumes that we can call `free` from a pointer to the stack.
  * In fact, the combination of conditions inside AllocatorWithStackMemory does not allow this.
  */
#if !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
#endif

/** Allocator with optimization to place small memory ranges in automatic memory.
  */
template <typename Base, size_t _initial_bytes, size_t Alignment>
class AllocatorWithStackMemory : private Base
{
private:
    alignas(Alignment) char stack_memory[_initial_bytes];

public:
    static constexpr size_t initial_bytes = _initial_bytes;

    /// Do not use boost::noncopyable to avoid the warning about direct base
    /// being inaccessible due to ambiguity, when derived classes are also
    /// noncopiable (-Winaccessible-base).
    AllocatorWithStackMemory(const AllocatorWithStackMemory&) = delete;
    AllocatorWithStackMemory & operator = (const AllocatorWithStackMemory&) = delete;
    AllocatorWithStackMemory() = default;
    ~AllocatorWithStackMemory() = default;

    void * alloc(size_t size)
    {
        if (size <= initial_bytes)
        {
            if constexpr (Base::clear_memory)
                memset(stack_memory, 0, initial_bytes);
            return stack_memory;
        }

        return Base::alloc(size, Alignment);
    }

    void free(void * buf, size_t size)
    {
        if (size > initial_bytes)
            Base::free(buf, size);
    }

    void * realloc(void * buf, size_t old_size, size_t new_size)
    {
        /// Was in stack_memory, will remain there.
        if (new_size <= initial_bytes)
            return buf;

        /// Already was big enough to not fit in stack_memory.
        if (old_size > initial_bytes)
            return Base::realloc(buf, old_size, new_size, Alignment);

        /// Was in stack memory, but now will not fit there.
        void * new_buf = Base::alloc(new_size, Alignment);
        memcpy(new_buf, buf, old_size);
        return new_buf;
    }

protected:
    static constexpr size_t getStackThreshold()
    {
        return initial_bytes;
    }
};

// A constant that gives the number of initially available bytes in
// the allocator. Used to check that this number is in sync with the
// initial size of array or hash table that uses the allocator.
template<typename TAllocator>
constexpr size_t allocatorInitialBytes = 0;

template<typename Base, size_t initial_bytes, size_t Alignment>
constexpr size_t allocatorInitialBytes<AllocatorWithStackMemory<
    Base, initial_bytes, Alignment>> = initial_bytes;

/// Prevent implicit template instantiation of Allocator

extern template class Allocator<false, false>;
extern template class Allocator<true, false>;
extern template class Allocator<false, true>;
extern template class Allocator<true, true>;

}

#if !defined(__clang__)
#pragma GCC diagnostic pop
#endif
