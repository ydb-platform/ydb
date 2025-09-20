/*
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *
 * Copyright (c) 2011 Helge Bahmann
 * Copyright (c) 2013-2014, 2020-2025 Andrey Semashev
 */
/*!
 * \file   lock_pool.cpp
 *
 * This file contains implementation of the lock pool used to emulate atomic ops.
 */

#include <boost/predef/os/windows.h>
#if BOOST_OS_WINDOWS
// Include boost/winapi/config.hpp first to make sure target Windows version is selected by Boost.WinAPI
#include <boost/winapi/config.hpp>
#include <boost/predef/platform.h>
#endif
#include <boost/predef/architecture/x86.h>
#include <boost/predef/hardware/simd/x86.h>

#include <cstddef>
#include <cstring>
#include <cstdlib>
#include <new>
#include <limits>
#include <chrono>
#include <boost/config.hpp>
#include <boost/assert.hpp>
#include <boost/memory_order.hpp>
#include <boost/atomic/thread_pause.hpp>
#include <boost/atomic/capabilities.hpp>
#include <boost/atomic/detail/config.hpp>
#include <boost/atomic/detail/intptr.hpp>
#include <boost/atomic/detail/int_sizes.hpp>
#include <boost/atomic/detail/aligned_variable.hpp>
#include <boost/atomic/detail/core_operations.hpp>
#include <boost/atomic/detail/extra_operations.hpp>
#include <boost/atomic/detail/fence_operations.hpp>
#include <boost/atomic/detail/lock_pool.hpp>
#include <boost/atomic/detail/once_flag.hpp>
#include <boost/atomic/detail/type_traits/alignment_of.hpp>

#include <boost/align/aligned_alloc.hpp>

#include <boost/preprocessor/config/limits.hpp>
#include <boost/preprocessor/iteration/iterate.hpp>

#if BOOST_OS_WINDOWS
#include <type_traits>
#include <boost/winapi/basic_types.hpp>
#include <boost/winapi/thread.hpp>
#include <boost/winapi/wait_constants.hpp>
#include <boost/winapi/srw_lock.hpp>
#include <boost/winapi/condition_variable.hpp>
#include <boost/winapi/get_last_error.hpp>
#include <boost/winapi/error_codes.hpp>
#include <boost/atomic/detail/chrono.hpp>
#define BOOST_ATOMIC_USE_WINAPI
#else // BOOST_OS_WINDOWS
#include <time.h>
#include <unistd.h> // _POSIX_MONOTONIC_CLOCK
#include <cerrno>
#include <boost/atomic/detail/futex.hpp>
#if defined(BOOST_ATOMIC_DETAIL_HAS_FUTEX) && BOOST_ATOMIC_INT32_LOCK_FREE == 2
#define BOOST_ATOMIC_USE_FUTEX
#else // defined(BOOST_ATOMIC_DETAIL_HAS_FUTEX) && BOOST_ATOMIC_INT32_LOCK_FREE == 2
#include <pthread.h>
#define BOOST_ATOMIC_USE_PTHREAD
#endif // defined(BOOST_ATOMIC_DETAIL_HAS_FUTEX) && BOOST_ATOMIC_INT32_LOCK_FREE == 2
#endif // BOOST_OS_WINDOWS

#include "find_address.hpp"

#if BOOST_ARCH_X86 && (defined(BOOST_ATOMIC_USE_SSE2) || defined(BOOST_ATOMIC_USE_SSE41)) && defined(BOOST_ATOMIC_DETAIL_SIZEOF_POINTER) && \
    (\
        (BOOST_ATOMIC_DETAIL_SIZEOF_POINTER == 8 && BOOST_HW_SIMD_X86 < BOOST_HW_SIMD_X86_SSE4_1_VERSION) || \
        (BOOST_ATOMIC_DETAIL_SIZEOF_POINTER == 4 && BOOST_HW_SIMD_X86 < BOOST_HW_SIMD_X86_SSE2_VERSION) \
    )
#include "cpuid.hpp"
#define BOOST_ATOMIC_DETAIL_X86_USE_RUNTIME_DISPATCH
#endif

#include <boost/atomic/detail/header.hpp>

// Cache line size, in bytes
// NOTE: This constant is made as a macro because some compilers (gcc 4.4 for one) don't allow enums or namespace scope constants in alignment attributes
#if defined(__s390__) || defined(__s390x__)
#define BOOST_ATOMIC_CACHE_LINE_SIZE 256
#elif defined(powerpc) || defined(__powerpc__) || defined(__ppc__)
#define BOOST_ATOMIC_CACHE_LINE_SIZE 128
#else
#define BOOST_ATOMIC_CACHE_LINE_SIZE 64
#endif

namespace boost {
namespace atomics {
namespace detail {

//! \c find_address generic implementation
std::size_t find_address_generic(const volatile void* addr, const volatile void* const* addrs, std::size_t size)
{
    for (std::size_t i = 0u; i < size; ++i)
    {
        if (addrs[i] == addr)
            return i;
    }

    return size;
}

namespace lock_pool {

namespace {

#if BOOST_ARCH_X86 && (defined(BOOST_ATOMIC_USE_SSE2) || defined(BOOST_ATOMIC_USE_SSE41)) && \
    defined(BOOST_ATOMIC_DETAIL_SIZEOF_POINTER) && (BOOST_ATOMIC_DETAIL_SIZEOF_POINTER == 8 || BOOST_ATOMIC_DETAIL_SIZEOF_POINTER == 4)

using func_ptr_operations = atomics::detail::core_operations< sizeof(find_address_t*), false, false >;
static_assert(func_ptr_operations::is_always_lock_free, "Boost.Atomic unsupported target platform: native atomic operations not implemented for function pointers");

#if defined(BOOST_ATOMIC_DETAIL_X86_USE_RUNTIME_DISPATCH)
std::size_t find_address_dispatch(const volatile void* addr, const volatile void* const* addrs, std::size_t size);
#endif

union find_address_ptr
{
    find_address_t* as_ptr;
    func_ptr_operations::storage_type as_storage;
}
g_find_address =
{
#if defined(BOOST_ATOMIC_USE_SSE41) && BOOST_ATOMIC_DETAIL_SIZEOF_POINTER == 8 && BOOST_HW_SIMD_X86 >= BOOST_HW_SIMD_X86_SSE4_1_VERSION
    &find_address_sse41
#elif defined(BOOST_ATOMIC_USE_SSE2) && BOOST_ATOMIC_DETAIL_SIZEOF_POINTER == 4 && BOOST_HW_SIMD_X86 >= BOOST_HW_SIMD_X86_SSE2_VERSION
    &find_address_sse2
#else
    &find_address_dispatch
#endif
};

#if defined(BOOST_ATOMIC_DETAIL_X86_USE_RUNTIME_DISPATCH)

std::size_t find_address_dispatch(const volatile void* addr, const volatile void* const* addrs, std::size_t size)
{
    find_address_t* find_addr = &find_address_generic;

#if defined(BOOST_ATOMIC_USE_SSE2)
    // First, check the max available cpuid function
    std::uint32_t eax = 0u, ebx = 0u, ecx = 0u, edx = 0u;
    atomics::detail::cpuid(eax, ebx, ecx, edx);

    const std::uint32_t max_cpuid_function = eax;
    if (max_cpuid_function >= 1u)
    {
        // Obtain CPU features
        eax = 1u;
        ebx = ecx = edx = 0u;
        atomics::detail::cpuid(eax, ebx, ecx, edx);

        if ((edx & (1u << 26)) != 0u)
            find_addr = &find_address_sse2;

#if defined(BOOST_ATOMIC_USE_SSE41) && BOOST_ATOMIC_DETAIL_SIZEOF_POINTER == 8
        if ((ecx & (1u << 19)) != 0u)
            find_addr = &find_address_sse41;
#endif
    }
#endif // defined(BOOST_ATOMIC_USE_SSE2)

    find_address_ptr ptr = {};
    ptr.as_ptr = find_addr;
    func_ptr_operations::store(g_find_address.as_storage, ptr.as_storage, boost::memory_order_relaxed);

    return find_addr(addr, addrs, size);
}

#endif // defined(BOOST_ATOMIC_DETAIL_X86_USE_RUNTIME_DISPATCH)

inline std::size_t find_address(const volatile void* addr, const volatile void* const* addrs, std::size_t size)
{
    find_address_ptr ptr;
    ptr.as_storage = func_ptr_operations::load(g_find_address.as_storage, boost::memory_order_relaxed);
    return ptr.as_ptr(addr, addrs, size);
}

#else // BOOST_ARCH_X86 && ...

inline std::size_t find_address(const volatile void* addr, const volatile void* const* addrs, std::size_t size)
{
    return atomics::detail::find_address_generic(addr, addrs, size);
}

#endif // BOOST_ARCH_X86 && ...

struct wait_state;
struct lock_state;

//! Base class for a wait state
struct wait_state_base
{
    //! Number of waiters referencing this state
    std::size_t m_ref_count;
    //! Index of this wait state in the list
    std::size_t m_index;

    explicit wait_state_base(std::size_t index) noexcept :
        m_ref_count(0u),
        m_index(index)
    {
    }

    wait_state_base(wait_state_base const&) = delete;
    wait_state_base& operator= (wait_state_base const&) = delete;
};

//! List of wait states. Must be a POD structure.
struct wait_state_list
{
    //! List header
    struct header
    {
        //! List size
        std::size_t size;
        //! List capacity
        std::size_t capacity;
    };

    /*!
     * \brief Pointer to the list header
     *
     * The list buffer consists of three adjacent areas: header object, array of atomic pointers and array of pointers to the wait_state structures.
     * Each of the arrays have header.capacity elements, of which the first header.size elements correspond to the currently ongoing wait operations
     * and the rest are spare elements. Spare wait_state structures may still be allocated (in which case the wait_state pointer is not null) and
     * can be reused on future requests. Spare atomic pointers are null and unused.
     *
     * This memory layout was designed to optimize wait state lookup by atomic address and also support memory pooling to reduce dynamic memory allocations.
     */
    header* m_header;
    //! The flag indicates that memory pooling is disabled. Set on process cleanup.
    bool m_free_memory;

    //! Buffer alignment, in bytes
    static constexpr std::size_t buffer_alignment = 16u;
    //! Alignment of pointer arrays in the buffer, in bytes. This should align atomic pointers to the vector size used in \c find_address implementation.
    static constexpr std::size_t entries_alignment = atomics::detail::alignment_of< void* >::value < 16u ? 16u : atomics::detail::alignment_of< void* >::value;
    //! Offset from the list header to the beginning of the array of atomic pointers in the buffer, in bytes
    static constexpr std::size_t entries_offset = (sizeof(header) + entries_alignment - 1u) & ~static_cast< std::size_t >(entries_alignment - 1u);
    //! Initial buffer capacity, in elements. This should be at least as large as a vector size used in \c find_address implementation.
    static constexpr std::size_t initial_capacity = (16u / sizeof(void*)) < 2u ? 2u : (16u / sizeof(void*));

    //! Returns a pointer to the array of atomic pointers
    static const volatile void** get_atomic_pointers(header* p) noexcept
    {
        BOOST_ASSERT(p != nullptr);
        return reinterpret_cast< const volatile void** >(reinterpret_cast< unsigned char* >(p) + entries_offset);
    }

    //! Returns a pointer to the array of atomic pointers
    const volatile void** get_atomic_pointers() const noexcept
    {
        return get_atomic_pointers(m_header);
    }

    //! Returns a pointer to the array of pointers to the wait states
    static wait_state** get_wait_states(const volatile void** ptrs, std::size_t capacity) noexcept
    {
        return reinterpret_cast< wait_state** >(const_cast< void** >(ptrs + capacity));
    }

    //! Returns a pointer to the array of pointers to the wait states
    static wait_state** get_wait_states(header* p) noexcept
    {
        return get_wait_states(get_atomic_pointers(p), p->capacity);
    }

    //! Returns a pointer to the array of pointers to the wait states
    wait_state** get_wait_states() const noexcept
    {
        return get_wait_states(m_header);
    }

    //! Finds an element with the given pointer to the atomic object
    wait_state* find(const volatile void* addr) const noexcept
    {
        wait_state* ws = nullptr;
        if (BOOST_LIKELY(m_header != nullptr))
        {
            const volatile void* const* addrs = get_atomic_pointers();
            const std::size_t size = m_header->size;
            std::size_t pos = find_address(addr, addrs, size);
            if (pos < size)
                ws = get_wait_states()[pos];
        }

        return ws;
    }

    //! Finds an existing element with the given pointer to the atomic object or allocates a new one. Returns nullptr in case of failure.
    wait_state* find_or_create(const volatile void* addr) noexcept;
    //! Releases the previously created wait state
    void erase(wait_state* w) noexcept;

    //! Deallocates spare entries and the list buffer if no allocated entries are left
    void free_spare() noexcept;
    //! Allocates new buffer for the list entries. Returns nullptr in case of failure.
    static header* allocate_buffer(std::size_t new_capacity, header* old_header = nullptr) noexcept;
};

#define BOOST_ATOMIC_WAIT_STATE_LIST_INIT { nullptr, false }

// In the platform-specific definitions below, lock_state must be a POD structure and wait_state must derive from wait_state_base.

#if defined(BOOST_ATOMIC_USE_PTHREAD)

//! State of a wait operation associated with an atomic object
struct wait_state :
    public wait_state_base
{
    //! Condition variable
    pthread_cond_t m_cond;
    //! Clock used by the condition variable
#if defined(_POSIX_MONOTONIC_CLOCK) && (_POSIX_MONOTONIC_CLOCK > 0)
    static constexpr clockid_t m_clock_id = CLOCK_MONOTONIC;
#elif defined(_POSIX_MONOTONIC_CLOCK) && (_POSIX_MONOTONIC_CLOCK == 0)
    clockid_t m_clock_id = CLOCK_REALTIME;
#else
    static constexpr clockid_t m_clock_id = CLOCK_REALTIME;
#endif

    explicit wait_state(std::size_t index) noexcept :
        wait_state_base(index)
    {
        pthread_condattr_t* pattr = nullptr;
#if defined(_POSIX_MONOTONIC_CLOCK) && (_POSIX_MONOTONIC_CLOCK >= 0)
        pthread_condattr_t attr;
        if (BOOST_LIKELY(is_clock_monotonic_supported()))
        {
            BOOST_VERIFY(pthread_condattr_init(&attr) == 0);
            BOOST_VERIFY(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
            pattr = &attr;
        }
#endif // defined(_POSIX_MONOTONIC_CLOCK) && (_POSIX_MONOTONIC_CLOCK >= 0)

        BOOST_VERIFY(pthread_cond_init(&m_cond, pattr) == 0);

#if defined(_POSIX_MONOTONIC_CLOCK) && (_POSIX_MONOTONIC_CLOCK >= 0)
        if (BOOST_LIKELY(pattr != nullptr))
        {
#if (_POSIX_MONOTONIC_CLOCK == 0)
            m_clock_id = CLOCK_MONOTONIC;
#endif // (_POSIX_MONOTONIC_CLOCK >= 0)
            pthread_condattr_destroy(pattr);
        }
#endif // defined(_POSIX_MONOTONIC_CLOCK) && (_POSIX_MONOTONIC_CLOCK >= 0)
    }

    ~wait_state() noexcept
    {
        pthread_cond_destroy(&m_cond);
    }

    //! Blocks in the wait operation until notified
    void wait(lock_state& state) noexcept;
    //! Blocks in the wait operation until notified or timeout
    bool wait_until(lock_state& state, clockid_t clock_id, timespec const& abs_timeout) noexcept;
    //! Blocks in the wait operation until notified or timeout
    bool wait_for(lock_state& state, std::chrono::nanoseconds rel_timeout) noexcept;

    //! Wakes up one thread blocked in the wait operation
    void notify_one(lock_state&) noexcept
    {
        BOOST_VERIFY(pthread_cond_signal(&m_cond) == 0);
    }
    //! Wakes up all threads blocked in the wait operation
    void notify_all(lock_state&) noexcept
    {
        BOOST_VERIFY(pthread_cond_broadcast(&m_cond) == 0);
    }

#if defined(_POSIX_MONOTONIC_CLOCK) && (_POSIX_MONOTONIC_CLOCK >= 0)
private:
    //! Returns \c true if CLOCK_MONOTONIC is supported
    static bool is_clock_monotonic_supported() noexcept
    {
#if (_POSIX_MONOTONIC_CLOCK > 0)
        return true;
#else // (_POSIX_MONOTONIC_CLOCK > 0)
        static const bool supported = []() noexcept -> bool
        {
            timespec ts{};
            return clock_gettime(CLOCK_MONOTONIC, &ts) == 0;
        }();
        return supported;
#endif // (_POSIX_MONOTONIC_CLOCK > 0)
    }
#endif // defined(_POSIX_MONOTONIC_CLOCK) && (_POSIX_MONOTONIC_CLOCK >= 0)
};

//! Lock pool entry
struct lock_state
{
    //! Mutex
    pthread_mutex_t m_mutex;
    //! Wait states
    wait_state_list m_wait_states;

    //! Locks the mutex for a short duration
    void short_lock() noexcept
    {
        long_lock();
    }

    //! Locks the mutex for a long duration
    void long_lock() noexcept
    {
        for (unsigned int i = 0u; i < 5u; ++i)
        {
            if (BOOST_LIKELY(pthread_mutex_trylock(&m_mutex) == 0))
                return;

            atomics::thread_pause();
        }

        BOOST_VERIFY(pthread_mutex_lock(&m_mutex) == 0);
    }

    //! Unlocks the mutex
    void unlock() noexcept
    {
        BOOST_VERIFY(pthread_mutex_unlock(&m_mutex) == 0);
    }
};

#define BOOST_ATOMIC_LOCK_STATE_INIT { PTHREAD_MUTEX_INITIALIZER, BOOST_ATOMIC_WAIT_STATE_LIST_INIT }

//! Blocks in the wait operation until notified
inline void wait_state::wait(lock_state& state) noexcept
{
    BOOST_VERIFY(pthread_cond_wait(&m_cond, &state.m_mutex) == 0);
}

//! Blocks in the wait operation until notified or timeout
inline bool wait_state::wait_until(lock_state& state, clockid_t clock_id, timespec const& abs_timeout) noexcept
{
#if defined(BOOST_ATOMIC_HAS_PTHREAD_COND_CLOCKWAIT)

    if (BOOST_LIKELY(
#if defined(_POSIX_MONOTONIC_CLOCK) && (_POSIX_MONOTONIC_CLOCK >= 0)
        clock_id == CLOCK_MONOTONIC ||
#endif
        clock_id == CLOCK_REALTIME))
    {
        return pthread_cond_clockwait(&m_cond, &state.m_mutex, clock_id, &abs_timeout) == ETIMEDOUT;
    }

#else // defined(BOOST_ATOMIC_HAS_PTHREAD_COND_CLOCKWAIT)

    if (BOOST_LIKELY(clock_id == m_clock_id))
    {
        return pthread_cond_timedwait(&m_cond, &state.m_mutex, &abs_timeout) == ETIMEDOUT;
    }

#endif // defined(BOOST_ATOMIC_HAS_PTHREAD_COND_CLOCKWAIT)

    timespec ts{};
    while (true)
    {
        BOOST_VERIFY(clock_gettime(clock_id, &ts) == 0);

        if (ts.tv_sec > abs_timeout.tv_sec || (ts.tv_sec == abs_timeout.tv_sec && ts.tv_nsec >= abs_timeout.tv_nsec))
            return true;

        std::chrono::nanoseconds rel_timeout =
            std::chrono::seconds(static_cast< std::chrono::seconds::rep >(abs_timeout.tv_sec) - static_cast< std::chrono::seconds::rep >(ts.tv_sec)) +
            std::chrono::nanoseconds(static_cast< std::chrono::nanoseconds::rep >(abs_timeout.tv_nsec) - static_cast< std::chrono::nanoseconds::rep >(ts.tv_nsec));

        if (!wait_for(state, rel_timeout)) // may return true before rel_timeout expires if we reach time_t capacity
            break;
    }

    return false;
}

//! Blocks in the wait operation until notified or timeout
inline bool wait_state::wait_for(lock_state& state, std::chrono::nanoseconds rel_timeout) noexcept
{
    timespec ts{};
    BOOST_VERIFY(clock_gettime(m_clock_id, &ts) == 0);

    std::chrono::nanoseconds::rep nsec = rel_timeout.count() + ts.tv_nsec;
    std::chrono::nanoseconds::rep sec = static_cast< std::chrono::nanoseconds::rep >(ts.tv_sec) + nsec / 1000000000;
    if (BOOST_LIKELY(sec <= (std::numeric_limits< decltype(ts.tv_sec) >::max)()))
    {
        ts.tv_sec = static_cast< decltype(ts.tv_sec) >(sec);
        ts.tv_nsec = static_cast< decltype(ts.tv_nsec) >(nsec % 1000000000);
    }
    else
    {
        ts.tv_sec = (std::numeric_limits< decltype(ts.tv_sec) >::max)();
        ts.tv_nsec = static_cast< decltype(ts.tv_nsec) >(999999999);
    }

    return pthread_cond_timedwait(&m_cond, &state.m_mutex, &ts) == ETIMEDOUT;
}

#elif defined(BOOST_ATOMIC_USE_FUTEX)

using futex_operations = atomics::detail::core_operations< 4u, false, false >;
// The storage type must be a 32-bit object, as required by futex API
static_assert(futex_operations::is_always_lock_free && sizeof(futex_operations::storage_type) == 4u,
    "Boost.Atomic unsupported target platform: native atomic operations not implemented for 32-bit integers");
using futex_extra_operations = atomics::detail::extra_operations< futex_operations, futex_operations::storage_size, futex_operations::is_signed >;

namespace mutex_bits {

//! The bit indicates a locked mutex
constexpr futex_operations::storage_type locked = 1u;
//! The bit indicates that there is at least one thread blocked waiting for the mutex to be released
constexpr futex_operations::storage_type contended = 1u << 1u;
//! The lowest bit of the counter bits used to mitigate ABA problem. This and any higher bits in the mutex state constitute the counter.
constexpr futex_operations::storage_type counter_one = 1u << 2u;

} // namespace mutex_bits

//! State of a wait operation associated with an atomic object
struct wait_state :
    public wait_state_base
{
    //! Condition variable futex. Used as the counter of notify calls.
    BOOST_ATOMIC_DETAIL_ALIGNED_VAR(futex_operations::storage_alignment, futex_operations::storage_type, m_cond);
    //! Number of currently blocked waiters
    futex_operations::storage_type m_waiter_count;

    explicit wait_state(std::size_t index) noexcept :
        wait_state_base(index),
        m_cond(0u),
        m_waiter_count(0u)
    {
    }

    //! Blocks in the wait operation until notified
    void wait(lock_state& state) noexcept;
    //! Blocks in the wait operation until notified or timeout
    bool wait_until(lock_state& state, clockid_t clock_id, timespec const& abs_timeout) noexcept;
    //! Blocks in the wait operation until notified or timeout
    bool wait_for(lock_state& state, std::chrono::nanoseconds rel_timeout) noexcept;

    //! Wakes up one thread blocked in the wait operation
    void notify_one(lock_state& state) noexcept;
    //! Wakes up all threads blocked in the wait operation
    void notify_all(lock_state& state) noexcept;
};

//! Lock pool entry
struct lock_state
{
    //! Mutex futex
    BOOST_ATOMIC_DETAIL_ALIGNED_VAR(futex_operations::storage_alignment, futex_operations::storage_type, m_mutex);
    //! Wait states
    wait_state_list m_wait_states;

    //! Locks the mutex for a short duration
    void short_lock() noexcept
    {
        long_lock();
    }

    //! Locks the mutex for a long duration
    void long_lock() noexcept
    {
        for (unsigned int i = 0u; i < 10u; ++i)
        {
            futex_operations::storage_type prev_state = futex_operations::load(m_mutex, boost::memory_order_relaxed);
            if (BOOST_LIKELY((prev_state & mutex_bits::locked) == 0u))
            {
                futex_operations::storage_type new_state = prev_state | mutex_bits::locked;
                if (BOOST_LIKELY(futex_operations::compare_exchange_strong(m_mutex, prev_state, new_state, boost::memory_order_acquire, boost::memory_order_relaxed)))
                    return;
            }

            atomics::thread_pause();
        }

        lock_slow_path();
    }

    //! Locks the mutex for a long duration
    void lock_slow_path() noexcept
    {
        futex_operations::storage_type prev_state = futex_operations::load(m_mutex, boost::memory_order_relaxed);
        while (true)
        {
            if (BOOST_LIKELY((prev_state & mutex_bits::locked) == 0u))
            {
                futex_operations::storage_type new_state = prev_state | mutex_bits::locked;
                if (BOOST_LIKELY(futex_operations::compare_exchange_weak(m_mutex, prev_state, new_state, boost::memory_order_acquire, boost::memory_order_relaxed)))
                    return;
            }
            else
            {
                futex_operations::storage_type new_state = prev_state | mutex_bits::contended;
                if (BOOST_LIKELY(futex_operations::compare_exchange_weak(m_mutex, prev_state, new_state, boost::memory_order_relaxed, boost::memory_order_relaxed)))
                {
                    atomics::detail::futex_wait(&m_mutex, new_state, BOOST_ATOMIC_DETAIL_FUTEX_PRIVATE_FLAG);
                    prev_state = futex_operations::load(m_mutex, boost::memory_order_relaxed);
                }
            }
        }
    }

    //! Unlocks the mutex
    void unlock() noexcept
    {
        futex_operations::storage_type prev_state = futex_operations::load(m_mutex, boost::memory_order_relaxed);
        futex_operations::storage_type new_state;
        while (true)
        {
            new_state = (prev_state & (~mutex_bits::locked)) + mutex_bits::counter_one;
            if (BOOST_LIKELY(futex_operations::compare_exchange_weak(m_mutex, prev_state, new_state, boost::memory_order_release, boost::memory_order_relaxed)))
                break;
        }

        if ((prev_state & mutex_bits::contended) != 0u)
        {
            int woken_count = atomics::detail::futex_signal(&m_mutex, BOOST_ATOMIC_DETAIL_FUTEX_PRIVATE_FLAG);
            if (woken_count == 0)
            {
                prev_state = new_state;
                new_state &= ~mutex_bits::contended;
                futex_operations::compare_exchange_strong(m_mutex, prev_state, new_state, boost::memory_order_relaxed, boost::memory_order_relaxed);
            }
        }
    }
};

#if !defined(BOOST_ATOMIC_DETAIL_NO_CXX11_ALIGNAS)
#define BOOST_ATOMIC_LOCK_STATE_INIT { 0u, BOOST_ATOMIC_WAIT_STATE_LIST_INIT }
#else
#define BOOST_ATOMIC_LOCK_STATE_INIT { { 0u }, BOOST_ATOMIC_WAIT_STATE_LIST_INIT }
#endif

//! Blocks in the wait operation until notified
inline void wait_state::wait(lock_state& state) noexcept
{
    const futex_operations::storage_type prev_cond = futex_operations::load(m_cond, boost::memory_order_relaxed);
    ++m_waiter_count;

    state.unlock();

    do
    {
        int err = atomics::detail::futex_wait(&m_cond, prev_cond, BOOST_ATOMIC_DETAIL_FUTEX_PRIVATE_FLAG);
        if (err < 0)
        {
            err = errno;
            if (BOOST_LIKELY(err != EINTR))
                break;
        }
    }
    while (futex_operations::load(m_cond, boost::memory_order_relaxed) == prev_cond);

    state.long_lock();

    --m_waiter_count;
}

//! Blocks in the wait operation until notified or timeout
inline bool wait_state::wait_until(lock_state& state, clockid_t clock_id, timespec const& abs_timeout) noexcept
{
    const futex_operations::storage_type prev_cond = futex_operations::load(m_cond, boost::memory_order_relaxed);
    ++m_waiter_count;

    state.unlock();

    bool timed_out = false;

#if defined(BOOST_ATOMIC_DETAIL_FUTEX_WAIT_BITSET) && \
    ((defined(_POSIX_MONOTONIC_CLOCK) && (_POSIX_MONOTONIC_CLOCK >= 0)) || defined(BOOST_ATOMIC_DETAIL_FUTEX_CLOCK_REALTIME))

    if (BOOST_LIKELY(
#if defined(_POSIX_MONOTONIC_CLOCK) && (_POSIX_MONOTONIC_CLOCK >= 0)
        clock_id == CLOCK_MONOTONIC ||
#endif
#if defined(BOOST_ATOMIC_DETAIL_FUTEX_CLOCK_REALTIME)
        clock_id == CLOCK_REALTIME
#else
        false
#endif
        ))
    {
        futex_timespec ts(abs_timeout);

        int flags = BOOST_ATOMIC_DETAIL_FUTEX_PRIVATE_FLAG;
#if defined(BOOST_ATOMIC_DETAIL_FUTEX_CLOCK_REALTIME)
        if (clock_id == CLOCK_REALTIME)
            flags |= BOOST_ATOMIC_DETAIL_FUTEX_CLOCK_REALTIME;
#endif
        do
        {
            int err = atomics::detail::futex_wait_until(&m_cond, prev_cond, ts, flags);
            if (err < 0)
            {
                err = errno;
                if (BOOST_LIKELY(err != EINTR))
                {
                    timed_out = err == ETIMEDOUT;
                    break;
                }
            }
        }
        while (futex_operations::load(m_cond, boost::memory_order_relaxed) == prev_cond);

        goto finish;
    }

#endif // defined(BOOST_ATOMIC_DETAIL_FUTEX_WAIT_BITSET) && ...

    {
        timespec now{};
        if (BOOST_LIKELY(clock_gettime(clock_id, &now) == 0))
        {
            futex_timespec ts{};
            do
            {
                std::chrono::nanoseconds::rep nsec = std::chrono::nanoseconds(
                    std::chrono::seconds(static_cast< std::chrono::seconds::rep >(abs_timeout.tv_sec) - static_cast< std::chrono::seconds::rep >(now.tv_sec)) +
                    std::chrono::nanoseconds(static_cast< std::chrono::nanoseconds::rep >(abs_timeout.tv_nsec) - static_cast< std::chrono::nanoseconds::rep >(now.tv_nsec))
                ).count();
                if (nsec <= 0)
                {
                    timed_out = true;
                    goto finish; // to avoid "unused label" warnings
                }

                const std::chrono::nanoseconds::rep sec = nsec / 1000000000;
                if (BOOST_LIKELY(sec <= (std::numeric_limits< decltype(ts.tv_sec) >::max)()))
                {
                    ts.tv_sec = static_cast< decltype(ts.tv_sec) >(sec);
                    ts.tv_nsec = static_cast< decltype(ts.tv_nsec) >(nsec % 1000000000);
                }
                else
                {
                    ts.tv_sec = (std::numeric_limits< decltype(ts.tv_sec) >::max)();
                    ts.tv_nsec = static_cast< decltype(ts.tv_nsec) >(999999999);
                }

                atomics::detail::futex_wait_for(&m_cond, prev_cond, ts, BOOST_ATOMIC_DETAIL_FUTEX_PRIVATE_FLAG);

                BOOST_VERIFY(clock_gettime(clock_id, &now) == 0);
            }
            while (futex_operations::load(m_cond, boost::memory_order_relaxed) == prev_cond);
        }
    }

finish:
    state.long_lock();

    --m_waiter_count;

    return timed_out;
}

//! Blocks in the wait operation until notified or timeout
inline bool wait_state::wait_for(lock_state& state, std::chrono::nanoseconds rel_timeout) noexcept
{
    const futex_operations::storage_type prev_cond = futex_operations::load(m_cond, boost::memory_order_relaxed);
    ++m_waiter_count;

    state.unlock();

    bool timed_out = false;

    futex_timespec ts{};

    const std::chrono::nanoseconds::rep sec = rel_timeout.count() / 1000000000;
    if (BOOST_LIKELY(sec <= (std::numeric_limits< decltype(ts.tv_sec) >::max)()))
    {
        ts.tv_sec = static_cast< decltype(ts.tv_sec) >(sec);
        ts.tv_nsec = static_cast< decltype(ts.tv_nsec) >(rel_timeout.count() % 1000000000);
    }
    else
    {
        ts.tv_sec = (std::numeric_limits< decltype(ts.tv_sec) >::max)();
        ts.tv_nsec = static_cast< decltype(ts.tv_nsec) >(999999999);
    }

    int err = atomics::detail::futex_wait_for(&m_cond, prev_cond, ts, BOOST_ATOMIC_DETAIL_FUTEX_PRIVATE_FLAG);
    if (err < 0)
    {
        err = errno;
        timed_out = err == ETIMEDOUT;
    }

    state.long_lock();

    --m_waiter_count;

    return timed_out;
}

//! Wakes up one thread blocked in the wait operation
inline void wait_state::notify_one(lock_state& state) noexcept
{
    futex_extra_operations::opaque_add(m_cond, 1u, boost::memory_order_relaxed);

    if (BOOST_LIKELY(m_waiter_count > 0u))
    {
        // Move one blocked thread to the mutex futex and mark the mutex contended so that the thread is unblocked on unlock()
        atomics::detail::futex_requeue(&m_cond, &state.m_mutex, BOOST_ATOMIC_DETAIL_FUTEX_PRIVATE_FLAG, 0u, 1u);
        futex_extra_operations::opaque_or(state.m_mutex, mutex_bits::contended, boost::memory_order_relaxed);
    }
}

//! Wakes up all threads blocked in the wait operation
inline void wait_state::notify_all(lock_state& state) noexcept
{
    futex_extra_operations::opaque_add(m_cond, 1u, boost::memory_order_relaxed);

    if (BOOST_LIKELY(m_waiter_count > 0u))
    {
        // Move blocked threads to the mutex futex and mark the mutex contended so that a thread is unblocked on unlock()
        atomics::detail::futex_requeue(&m_cond, &state.m_mutex, BOOST_ATOMIC_DETAIL_FUTEX_PRIVATE_FLAG, 0u);
        futex_extra_operations::opaque_or(state.m_mutex, mutex_bits::contended, boost::memory_order_relaxed);
    }
}

#else

//! State of a wait operation associated with an atomic object
struct wait_state :
    public wait_state_base
{
    //! Condition variable
    boost::winapi::CONDITION_VARIABLE_ m_cond;

    explicit wait_state(std::size_t index) noexcept :
        wait_state_base(index)
    {
        boost::winapi::InitializeConditionVariable(&m_cond);
    }

    //! Blocks in the wait operation until notified
    void wait(lock_state& state) noexcept;
    //! Blocks in the wait operation until notified or timeout
    bool wait_for(lock_state& state, std::chrono::nanoseconds rel_timeout) noexcept;

    //! Wakes up one thread blocked in the wait operation
    void notify_one(lock_state&) noexcept
    {
        boost::winapi::WakeConditionVariable(&m_cond);
    }
    //! Wakes up all threads blocked in the wait operation
    void notify_all(lock_state&) noexcept
    {
        boost::winapi::WakeAllConditionVariable(&m_cond);
    }
};

//! Lock pool entry
struct lock_state
{
    //! Mutex
    boost::winapi::SRWLOCK_ m_mutex;
    //! Wait states
    wait_state_list m_wait_states;

    //! Locks the mutex for a short duration
    void short_lock() noexcept
    {
        long_lock();
    }

    //! Locks the mutex for a long duration
    void long_lock() noexcept
    {
        // Presumably, AcquireSRWLockExclusive already implements spinning internally, so there's no point in doing this ourselves.
        boost::winapi::AcquireSRWLockExclusive(&m_mutex);
    }

    //! Unlocks the mutex
    void unlock() noexcept
    {
        boost::winapi::ReleaseSRWLockExclusive(&m_mutex);
    }
};

#define BOOST_ATOMIC_LOCK_STATE_INIT { BOOST_WINAPI_SRWLOCK_INIT, BOOST_ATOMIC_WAIT_STATE_LIST_INIT }

//! Blocks in the wait operation until notified
inline void wait_state::wait(lock_state& state) noexcept
{
    boost::winapi::SleepConditionVariableSRW(&m_cond, &state.m_mutex, boost::winapi::infinite, 0u);
}

//! Blocks in the wait operation until notified or timeout
inline bool wait_state::wait_for(lock_state& state, std::chrono::nanoseconds rel_timeout) noexcept
{
    using common_type = std::common_type< std::chrono::milliseconds::rep, boost::winapi::DWORD_ >::type;

    boost::winapi::DWORD_ tout = boost::winapi::max_non_infinite_wait;
    std::chrono::milliseconds::rep msec = detail::chrono::ceil< std::chrono::milliseconds >(rel_timeout).count();
    if (BOOST_LIKELY(static_cast< common_type >(msec) < static_cast< common_type >(tout)))
        tout = static_cast< boost::winapi::DWORD_ >(msec);
    return !boost::winapi::SleepConditionVariableSRW(&m_cond, &state.m_mutex, tout, 0u) &&
        boost::winapi::GetLastError() == boost::winapi::ERROR_TIMEOUT_;
}

#endif

enum
{
    tail_size = sizeof(lock_state) % BOOST_ATOMIC_CACHE_LINE_SIZE,
    padding_size = tail_size > 0 ? BOOST_ATOMIC_CACHE_LINE_SIZE - tail_size : 0u
};

template< unsigned int PaddingSize >
struct BOOST_ALIGNMENT(BOOST_ATOMIC_CACHE_LINE_SIZE) padded_lock_state
{
    lock_state state;
    // The additional padding is needed to avoid false sharing between locks
    char padding[PaddingSize];
};

template< >
struct BOOST_ALIGNMENT(BOOST_ATOMIC_CACHE_LINE_SIZE) padded_lock_state< 0u >
{
    lock_state state;
};

using padded_lock_state_t = padded_lock_state< padding_size >;

#if !defined(BOOST_ATOMIC_LOCK_POOL_SIZE_LOG2)
#define BOOST_ATOMIC_LOCK_POOL_SIZE_LOG2 8
#endif
#if (BOOST_ATOMIC_LOCK_POOL_SIZE_LOG2) < 0
#error "Boost.Atomic: BOOST_ATOMIC_LOCK_POOL_SIZE_LOG2 macro value is negative"
#endif
#define BOOST_ATOMIC_DETAIL_LOCK_POOL_SIZE (1ull << (BOOST_ATOMIC_LOCK_POOL_SIZE_LOG2))

//! Lock pool size. Must be a power of two.
constexpr std::size_t lock_pool_size = static_cast< std::size_t >(1u) << (BOOST_ATOMIC_LOCK_POOL_SIZE_LOG2);

static padded_lock_state_t g_lock_pool[lock_pool_size] =
{
#if BOOST_ATOMIC_DETAIL_LOCK_POOL_SIZE > 256u
#if (BOOST_ATOMIC_DETAIL_LOCK_POOL_SIZE / 256u) > BOOST_PP_LIMIT_ITERATION
#error "Boost.Atomic: BOOST_ATOMIC_LOCK_POOL_SIZE_LOG2 macro value is too large"
#endif
#define BOOST_PP_ITERATION_PARAMS_1 (3, (1, (BOOST_ATOMIC_DETAIL_LOCK_POOL_SIZE / 256u), "lock_pool_init256.ipp"))
#else // BOOST_ATOMIC_DETAIL_LOCK_POOL_SIZE > 256u
#define BOOST_PP_ITERATION_PARAMS_1 (3, (1, BOOST_ATOMIC_DETAIL_LOCK_POOL_SIZE, "lock_pool_init1.ipp"))
#endif // BOOST_ATOMIC_DETAIL_LOCK_POOL_SIZE > 256u
#include BOOST_PP_ITERATE()
#undef BOOST_PP_ITERATION_PARAMS_1
};

//! Pool cleanup function
void cleanup_lock_pool()
{
    for (std::size_t i = 0u; i < lock_pool_size; ++i)
    {
        lock_state& state = g_lock_pool[i].state;
        state.long_lock();
        state.m_wait_states.m_free_memory = true;
        state.m_wait_states.free_spare();
        state.unlock();
    }
}

static_assert(once_flag_operations::is_always_lock_free, "Boost.Atomic unsupported target platform: native atomic operations not implemented for bytes");
static once_flag g_pool_cleanup_registered = {};

//! Returns index of the lock pool entry for the given pointer value
BOOST_FORCEINLINE std::size_t get_lock_index(atomics::detail::uintptr_t h) noexcept
{
    return h & (lock_pool_size - 1u);
}

//! Finds an existing element with the given pointer to the atomic object or allocates a new one
inline wait_state* wait_state_list::find_or_create(const volatile void* addr) noexcept
{
    if (BOOST_UNLIKELY(m_header == nullptr))
    {
        m_header = allocate_buffer(initial_capacity);
        if (BOOST_UNLIKELY(m_header == nullptr))
            return nullptr;
    }
    else
    {
        wait_state* ws = this->find(addr);
        if (BOOST_LIKELY(ws != nullptr))
            return ws;

        if (BOOST_UNLIKELY(m_header->size == m_header->capacity))
        {
            header* new_header = allocate_buffer(m_header->capacity * 2u, m_header);
            if (BOOST_UNLIKELY(new_header == nullptr))
                return nullptr;
            boost::alignment::aligned_free(static_cast< void* >(m_header));
            m_header = new_header;
        }
    }

    const std::size_t index = m_header->size;
    BOOST_ASSERT(index < m_header->capacity);

    wait_state** pw = get_wait_states() + index;
    wait_state* w = *pw;
    if (BOOST_UNLIKELY(w == nullptr))
    {
        w = new (std::nothrow) wait_state(index);
        if (BOOST_UNLIKELY(w == nullptr))
            return nullptr;
        *pw = w;
    }

    get_atomic_pointers()[index] = addr;

    ++m_header->size;

    return w;
}

//! Releases the previously created wait state
inline void wait_state_list::erase(wait_state* w) noexcept
{
    BOOST_ASSERT(m_header != nullptr);

    const volatile void** pa = get_atomic_pointers();
    wait_state** pw = get_wait_states();

    std::size_t index = w->m_index;

    BOOST_ASSERT(index < m_header->size);
    BOOST_ASSERT(pw[index] == w);

    std::size_t last_index = m_header->size - 1u;

    if (index != last_index)
    {
        pa[index] = pa[last_index];
        pa[last_index] = nullptr;

        wait_state* last_w = pw[last_index];
        pw[index] = last_w;
        pw[last_index] = w;

        last_w->m_index = index;
        w->m_index = last_index;
    }
    else
    {
        pa[index] = nullptr;
    }

    --m_header->size;

    if (BOOST_UNLIKELY(m_free_memory))
        free_spare();
}

//! Allocates new buffer for the list entries
wait_state_list::header* wait_state_list::allocate_buffer(std::size_t new_capacity, header* old_header) noexcept
{
    if (BOOST_UNLIKELY(once_flag_operations::load(g_pool_cleanup_registered.m_flag, boost::memory_order_relaxed) == 0u))
    {
        if (once_flag_operations::exchange(g_pool_cleanup_registered.m_flag, 1u, boost::memory_order_relaxed) == 0u)
            std::atexit(&cleanup_lock_pool);
    }

    const std::size_t new_buffer_size = entries_offset + new_capacity * sizeof(void*) * 2u;

    void* p = boost::alignment::aligned_alloc(buffer_alignment, new_buffer_size);
    if (BOOST_UNLIKELY(p == nullptr))
        return nullptr;

    header* h = new (p) header;
    const volatile void** a = new (get_atomic_pointers(h)) const volatile void*[new_capacity];
    wait_state** w = new (get_wait_states(a, new_capacity)) wait_state*[new_capacity];

    if (BOOST_LIKELY(old_header != nullptr))
    {
        BOOST_ASSERT(new_capacity >= old_header->capacity);

        h->size = old_header->size;

        const volatile void** old_a = get_atomic_pointers(old_header);
        std::memcpy(a, old_a, old_header->size * sizeof(const volatile void*));
        std::memset(a + old_header->size, 0, (new_capacity - old_header->size) * sizeof(const volatile void*));

        wait_state** old_w = get_wait_states(old_a, old_header->capacity);
        std::memcpy(w, old_w, old_header->capacity * sizeof(wait_state*)); // copy spare wait state pointers
        std::memset(w + old_header->capacity, 0, (new_capacity - old_header->capacity) * sizeof(wait_state*));
    }
    else
    {
        std::memset(p, 0, new_buffer_size);
    }

    h->capacity = new_capacity;

    return h;
}

//! Deallocates spare entries and the list buffer if no allocated entries are left
void wait_state_list::free_spare() noexcept
{
    if (BOOST_LIKELY(m_header != nullptr))
    {
        wait_state** ws = get_wait_states();
        for (std::size_t i = m_header->size, n = m_header->capacity; i < n; ++i)
        {
            wait_state* w = ws[i];
            if (!w)
                break;

            delete w;
            ws[i] = nullptr;
        }

        if (m_header->size == 0u)
        {
            boost::alignment::aligned_free(static_cast< void* >(m_header));
            m_header = nullptr;
        }
    }
}

} // namespace


BOOST_ATOMIC_DECL void* short_lock(atomics::detail::uintptr_t h) noexcept
{
    lock_state& ls = g_lock_pool[get_lock_index(h)].state;
    ls.short_lock();
    return &ls;
}

BOOST_ATOMIC_DECL void* long_lock(atomics::detail::uintptr_t h) noexcept
{
    lock_state& ls = g_lock_pool[get_lock_index(h)].state;
    ls.long_lock();
    return &ls;
}

BOOST_ATOMIC_DECL void unlock(void* vls) noexcept
{
    static_cast< lock_state* >(vls)->unlock();
}


BOOST_ATOMIC_DECL void* allocate_wait_state(void* vls, const volatile void* addr) noexcept
{
    BOOST_ASSERT(vls != nullptr);

    lock_state* ls = static_cast< lock_state* >(vls);

    // Note: find_or_create may fail to allocate memory. However, C++20 specifies that wait/notify operations
    // are noexcept, so allocate_wait_state must succeed. To implement this we return nullptr in case of failure and test for nullptr
    // in other wait/notify functions so that all of them become nop (which is a conforming, though inefficient behavior).
    wait_state* ws = ls->m_wait_states.find_or_create(addr);

    if (BOOST_LIKELY(ws != nullptr))
        ++ws->m_ref_count;

    return ws;
}

BOOST_ATOMIC_DECL void free_wait_state(void* vls, void* vws) noexcept
{
    BOOST_ASSERT(vls != nullptr);

    wait_state* ws = static_cast< wait_state* >(vws);
    if (BOOST_LIKELY(ws != nullptr))
    {
        if (--ws->m_ref_count == 0u)
        {
            lock_state* ls = static_cast< lock_state* >(vls);
            ls->m_wait_states.erase(ws);
        }
    }
}

BOOST_ATOMIC_DECL void wait(void* vls, void* vws) noexcept
{
    BOOST_ASSERT(vls != nullptr);

    lock_state* ls = static_cast< lock_state* >(vls);
    wait_state* ws = static_cast< wait_state* >(vws);
    if (BOOST_LIKELY(ws != nullptr))
    {
        ws->wait(*ls);
    }
    else
    {
        // A conforming wait operation must unlock and lock the mutex to allow a notify to complete
        ls->unlock();
        atomics::detail::wait_some();
        ls->long_lock();
    }
}

#if !defined(BOOST_WINDOWS)
BOOST_ATOMIC_DECL bool wait_until(void* vls, void* vws, clockid_t clock_id, timespec const& abs_timeout) noexcept
{
    BOOST_ASSERT(vls != nullptr);

    lock_state* ls = static_cast< lock_state* >(vls);
    wait_state* ws = static_cast< wait_state* >(vws);
    if (BOOST_LIKELY(ws != nullptr))
    {
        return ws->wait_until(*ls, clock_id, abs_timeout);
    }
    else
    {
        // A conforming wait operation must unlock and lock the mutex to allow a notify to complete
        ls->unlock();
        atomics::detail::wait_some();
        ls->long_lock();

        timespec ts{};
        return clock_gettime(clock_id, &ts) == 0 &&
            (ts.tv_sec > abs_timeout.tv_sec || (ts.tv_sec == abs_timeout.tv_sec && ts.tv_nsec >= abs_timeout.tv_nsec));
    }
}
#endif // !defined(BOOST_WINDOWS)

BOOST_ATOMIC_DECL bool wait_for(void* vls, void* vws, std::chrono::nanoseconds rel_timeout) noexcept
{
    BOOST_ASSERT(vls != nullptr);

    lock_state* ls = static_cast< lock_state* >(vls);
    wait_state* ws = static_cast< wait_state* >(vws);
    if (BOOST_LIKELY(ws != nullptr))
    {
        return ws->wait_for(*ls, rel_timeout);
    }
    else
    {
        // A conforming wait operation must unlock and lock the mutex to allow a notify to complete
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        ls->unlock();
        atomics::detail::wait_some();
        ls->long_lock();
        return (std::chrono::steady_clock::now() - start) >= rel_timeout;
    }
}

BOOST_ATOMIC_DECL void notify_one(void* vls, const volatile void* addr) noexcept
{
    BOOST_ASSERT(vls != nullptr);

    lock_state* ls = static_cast< lock_state* >(vls);
    wait_state* ws = ls->m_wait_states.find(addr);
    if (BOOST_LIKELY(ws != nullptr))
        ws->notify_one(*ls);
}

BOOST_ATOMIC_DECL void notify_all(void* vls, const volatile void* addr) noexcept
{
    BOOST_ASSERT(vls != nullptr);

    lock_state* ls = static_cast< lock_state* >(vls);
    wait_state* ws = ls->m_wait_states.find(addr);
    if (BOOST_LIKELY(ws != nullptr))
        ws->notify_all(*ls);
}


BOOST_ATOMIC_DECL void thread_fence() noexcept
{
#if BOOST_ATOMIC_THREAD_FENCE == 2
    atomics::detail::fence_operations::thread_fence(memory_order_seq_cst);
#else
    // Emulate full fence by locking/unlocking a mutex
    lock_pool::unlock(lock_pool::short_lock(0u));
#endif
}

BOOST_ATOMIC_DECL void signal_fence() noexcept
{
    // This function is intentionally non-inline, even if empty. This forces the compiler to treat its call as a compiler barrier.
#if BOOST_ATOMIC_SIGNAL_FENCE == 2
    atomics::detail::fence_operations::signal_fence(memory_order_seq_cst);
#endif
}

} // namespace lock_pool
} // namespace detail
} // namespace atomics
} // namespace boost

#include <boost/atomic/detail/footer.hpp>
