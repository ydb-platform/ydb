#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/hazard_ptr.h>

#include <library/cpp/yt/memory/new.h>

#include <library/cpp/yt/threading/event_count.h>

#include <atomic>
#include <thread>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

// For a type with EnableHazard the destructor runs immediately when the last strong
// reference is dropped, while the memory deallocation is deferred to the hazard
// pointer manager and performed during a reclamation pass. Reclamation is what gets
// held back while a hazard pointer protects the object, so the test observes the
// deferred TAllocator::Free rather than the destructor.
class TTrackedAllocator
{
public:
    explicit TTrackedAllocator(std::atomic<int>* freedCounter)
        : FreedCounter_(freedCounter)
    { }

    void* Allocate(size_t size)
    {
        size += sizeof(TTrackedAllocator*);
        auto* ptr = ::malloc(size);
        auto* header = static_cast<TTrackedAllocator**>(ptr);
        *header = this;
        return header + 1;
    }

    static void Free(void* ptr)
    {
        auto* header = static_cast<TTrackedAllocator**>(ptr) - 1;
        auto* allocator = *header;
        allocator->FreedCounter_->fetch_add(1, std::memory_order::release);
        ::free(header);
    }

private:
    std::atomic<int>* const FreedCounter_;
};

class TTrackedObject final
{
public:
    using TAllocator = TTrackedAllocator;
    static constexpr bool EnableHazard = true;
};

////////////////////////////////////////////////////////////////////////////////

// Reproduces the scenario in which a hazard-managed object is retired on a worker
// thread while it is still protected by a hazard pointer held by another thread.
// The reclamation pass that runs as the worker parks cannot free the object yet, so
// it stays in the worker's thread-local retire list. The protection is then released
// and no further work is submitted, leaving the worker idle.
//
// Invariant under test: the retired object is reclaimed eventually regardless, i.e.
// an idle worker keeps retrying maintenance and never retains hazard pointers forever.
void TestHazardPointersAreNeverRetainedForever(const IInvokerPtr& invoker)
{
    // The counter and allocator have static storage duration. This sidesteps the
    // tension between leak detection and use-after-free: static storage is never a leak
    // (LeakSanitizer treats statics as roots), yet it outlives any deferred reclamation,
    // so the object's Free can run after this function returns — even on a regression —
    // without touching destroyed storage. The counter is reset per invocation because
    // this helper runs once per test case; on the success path the previous invocation
    // already observed full reclamation, so no stale Free can race this reset.
    static std::atomic<int> freed;
    static TTrackedAllocator allocator(&freed);
    freed.store(0, std::memory_order::release);

    auto object = New<TTrackedObject>(&allocator);
    auto* rawObject = object.Get();

    NThreading::TEvent hazardAcquired;
    NThreading::TEvent releaseHazard;
    NThreading::TEvent protectorFinished;

    // The protector holds a hazard pointer to the object across its retirement,
    // forcing the retiring worker thread to leave it pending in its thread-local
    // retire list.
    std::thread protector([&] {
        auto hazardPtr = THazardPtr<TTrackedObject>::Acquire([&] { return rawObject; });
        YT_VERIFY(hazardPtr);
        hazardAcquired.NotifyAll();

        releaseHazard.Wait();
        hazardPtr.Reset();
        // NB: Intentionally do not reclaim here. Reclamation must be driven by the
        // (now idle) worker thread that retired the object, not by this thread.
        protectorFinished.NotifyAll();
    });

    hazardAcquired.Wait();

    // Drop the last strong reference on a worker thread so the object is retired
    // there while it is still protected.
    WaitFor(BIND([&, object = std::move(object)] () mutable {
        object.Reset();
    }).AsyncVia(invoker).Run())
        .ThrowOnError();

    // Let the worker finish maintenance and park; the object stays pinned meanwhile.
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(500));
    EXPECT_EQ(0, freed.load(std::memory_order::acquire))
        << "object memory was reclaimed while still protected by a hazard pointer";

    // Release the protection. The pool is now idle and nothing else is submitted to it.
    releaseHazard.NotifyAll();
    protectorFinished.Wait();
    protector.join();

    // Even though the pool stays idle, the retired object must be reclaimed; otherwise
    // the hazard pointer (and the memory it pins) would be retained forever.
    WaitForPredicate(
        [&] { return freed.load(std::memory_order::acquire) == 1; },
        "hazard-retired object was retained forever on an idle thread");
}

////////////////////////////////////////////////////////////////////////////////

class THazardPtrReclamationTest
    : public ::testing::Test
{ };

TEST_W(THazardPtrReclamationTest, ActionQueue)
{
    auto queue = New<TActionQueue>("TestAQ");
    TestHazardPointersAreNeverRetainedForever(queue->GetInvoker());
    queue->Shutdown();
}

TEST_W(THazardPtrReclamationTest, SingleThreadPool)
{
    auto pool = CreateThreadPool(1, "TestTP1");
    TestHazardPointersAreNeverRetainedForever(pool->GetInvoker());
    pool->Shutdown();
}

TEST_W(THazardPtrReclamationTest, MultiThreadPool)
{
    auto pool = CreateThreadPool(4, "TestTP4");
    TestHazardPointersAreNeverRetainedForever(pool->GetInvoker());
    pool->Shutdown();
}

TEST_W(THazardPtrReclamationTest, FairShareThreadPool)
{
    auto pool = CreateFairShareThreadPool(4, "TestFS");
    TestHazardPointersAreNeverRetainedForever(pool->GetInvoker("pool"));
    pool->Shutdown();
}

TEST_W(THazardPtrReclamationTest, SingleThreadTwoLevelFairShareThreadPool)
{
    auto pool = CreateTwoLevelFairShareThreadPool(1, "TestTL1");
    TestHazardPointersAreNeverRetainedForever(pool->GetInvoker("pool", "tag"));
    pool->Shutdown();
}

TEST_W(THazardPtrReclamationTest, MultiThreadTwoLevelFairShareThreadPool)
{
    auto pool = CreateTwoLevelFairShareThreadPool(4, "TestTL4");
    TestHazardPointersAreNeverRetainedForever(pool->GetInvoker("pool", "tag"));
    pool->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
