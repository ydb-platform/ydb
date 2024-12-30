#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/threading/public.h>

#include <library/cpp/yt/misc/preprocessor.h>

#include <atomic>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

/*!
 * Allows to annotate certain functions with thread affinity.
 * The checks are performed at run-time to ensure that each function
 * invocation that is annotated with a particular affinity slot
 * takes place in one thread.
 *
 * The usage is as follows.
 * - For each thread that may invoke your functions declare a slot with
 *   \code
 *   DECLARE_THREAD_AFFINITY_SLOT(Thread);
 *   \endcode
 * - Write
 *   \code
 *   YT_ASSERT_THREAD_AFFINITY(Thread);
 *   \endcode
 *   at the beginning of each function in the group.
 *
 * Please refer to the unit test for an actual usage example
 * (unittests/thread_affinity_ut.cpp).
 */
class TThreadAffinitySlot
{
public:
    //! Checks if the slot matches the given thread id.
    void Check(NThreading::TThreadId threadId);

    //! Checks if the slot matches the current thread id.
    void Check();

    //! Returns thread id used for affinity check
    //! or #InvalidThreadId if bound thread is still undefined.
    NThreading::TThreadId GetBoundThreadId() const;

private:
    std::atomic<NThreading::TThreadId> BoundId_ = NThreading::InvalidThreadId;
};

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK

#define DECLARE_THREAD_AFFINITY_SLOT(slot) \
    mutable ::NYT::NConcurrency::TThreadAffinitySlot PP_CONCAT(slot, _Slot)

#define YT_ASSERT_THREAD_AFFINITY(slot) \
    PP_CONCAT(slot, _Slot).Check()

#define YT_ASSERT_SPINLOCK_AFFINITY(spinLock) \
    YT_VERIFY((spinLock).IsLocked());

#define YT_ASSERT_READER_SPINLOCK_AFFINITY(spinLock) \
    YT_VERIFY((spinLock).IsLockedByReader());

#define YT_ASSERT_WRITER_SPINLOCK_AFFINITY(spinLock) \
    YT_VERIFY((spinLock).IsLockedByWriter());

#define YT_ASSERT_INVOKER_AFFINITY(invoker) \
    YT_VERIFY(::NYT::NConcurrency::VerifyInvokerAffinity(invoker))

#define YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(invoker) \
    YT_VERIFY(::NYT::NConcurrency::VerifySerializedInvokerAffinity(invoker))

#define YT_ASSERT_INVOKERS_AFFINITY(...) \
    YT_VERIFY(::NYT::NConcurrency::VerifyInvokersAffinity(__VA_ARGS__))

#define YT_ASSERT_INVOKER_POOL_AFFINITY(invokerPool) \
    YT_VERIFY(::NYT::NConcurrency::VerifyInvokerPoolAffinity(invokerPool))

#define YT_ASSERT_INVOKER_THREAD_AFFINITY(invoker, slot) \
    PP_CONCAT(slot, _Slot).Check((invoker)->GetThreadId());

#else

// Expand macros to null but take care of the trailing semicolon.
#define DECLARE_THREAD_AFFINITY_SLOT(slot)               struct PP_CONCAT(TNullThreadAffinitySlot_,  __LINE__) { }
#define YT_ASSERT_THREAD_AFFINITY(slot)                     do { } while (false)
#define YT_ASSERT_SPINLOCK_AFFINITY(spinLock)               do { } while (false)
#define YT_ASSERT_READER_SPINLOCK_AFFINITY(spinLock)        do { } while (false)
#define YT_ASSERT_WRITER_SPINLOCK_AFFINITY(spinLock)        do { } while (false)
#define YT_ASSERT_INVOKER_AFFINITY(invoker)                 do { } while (false)
#define YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(invoker)      do { } while (false)
#define YT_ASSERT_INVOKERS_AFFINITY(...)                    do { } while (false)
#define YT_ASSERT_INVOKER_POOL_AFFINITY(invokerPool)        do { } while (false)
#define YT_ASSERT_INVOKER_THREAD_AFFINITY(invoker, slot)    do { } while (false)

#endif

//! This is a mere declaration and intentionally does not check anything.
#define YT_ASSERT_THREAD_AFFINITY_ANY() do { } while (false)

// Temporary compatibility. Remove after removing all usages of VERIFY_*.
#define VERIFY_THREAD_AFFINITY(slot) YT_ASSERT_THREAD_AFFINITY(slot)
#define VERIFY_SPINLOCK_AFFINITY(spinLock) YT_ASSERT_SPINLOCK_AFFINITY(spinLock)
#define VERIFY_READER_SPINLOCK_AFFINITY(spinLock) YT_ASSERT_READER_SPINLOCK_AFFINITY(spinLock)
#define VERIFY_WRITER_SPINLOCK_AFFINITY(spinLock) YT_ASSERT_WRITER_SPINLOCK_AFFINITY(spinLock)
#define VERIFY_INVOKER_AFFINITY(invoker) YT_ASSERT_INVOKER_AFFINITY(invoker)
#define VERIFY_SERIALIZED_INVOKER_AFFINITY(invoker) YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(invoker)
#define VERIFY_INVOKERS_AFFINITY(...) YT_ASSERT_INVOKERS_AFFINITY(__VA_ARGS__)
#define VERIFY_INVOKER_POOL_AFFINITY(invokerPool) YT_ASSERT_INVOKER_POOL_AFFINITY(invokerPool)
#define VERIFY_INVOKER_THREAD_AFFINITY(invoker, slot) YT_ASSERT_INVOKER_THREAD_AFFINITY(invoker, slot)
#define VERIFY_THREAD_AFFINITY_ANY() YT_ASSERT_THREAD_AFFINITY_ANY()

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define THREAD_AFFINITY_INL_H_
#include "thread_affinity-inl.h"
#undef THREAD_AFFINITY_INL_H_
