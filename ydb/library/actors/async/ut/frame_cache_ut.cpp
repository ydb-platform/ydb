#include <ydb/library/actors/testlib/scoped_allocation_cache.h>
#include "common.h"

#include <ydb/library/actors/async/wait_for_event.h>

#include <algorithm>
#include <cstring>
#include <thread>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/thread_context.h>
#include <util/system/event.h>

namespace NAsyncTest {
namespace {

    struct TCaptureFrame {
        static constexpr bool IsActorAwareAwaiter = true;
        void*& Address;

        bool await_ready() const noexcept { return false; }
        bool await_suspend(std::coroutine_handle<> handle) const noexcept {
            Address = handle.address();
            return false;
        }
        void await_resume() const noexcept {}
    };

    struct TThrowOnMove {
        bool Fail = true;
        TThrowOnMove() = default;
        TThrowOnMove(const TThrowOnMove&) = default;
        Y_NO_INLINE TThrowOnMove(TThrowOnMove&& other)
            : Fail(other.Fail)
        {
            // Keep a runtime-dependent throwing move: an unconditionally
            // throwing constructor lets the compiler remove frame allocation.
            if (Fail) {
                throw TTestException();
            }
        }
    };

    struct TCacheActorState {
        void* Root = nullptr;
        void* Child = nullptr;
        std::coroutine_handle<> Bridge;
        size_t RootDestroyed = 0;
        size_t ChildDestroyed = 0;
        size_t Finished = 0;
        size_t Caught = 0;
        int Value = 0;
    };

    class TCacheActor : public TAsyncTestActor {
    public:
        TCacheActorState& Counts;

        TCacheActor(TState& state, TCacheActorState& counts)
            : TAsyncTestActor(state)
            , Counts(counts)
        {}

        void Root(bool generic = false, bool throws = false) {
            Y_DEFER { ++Counts.RootDestroyed; };
            co_await TCaptureFrame{Counts.Root};
            try {
                Counts.Value = co_await Child(generic, throws);
            } catch (const TTestException&) {
                ++Counts.Caught;
            }
            ++Counts.Finished;
        }

        Y_NO_INLINE async<int> Child(bool generic, bool throws) {
            Y_DEFER { ++Counts.ChildDestroyed; };
            co_await TCaptureFrame{Counts.Child};
            if (generic) {
                co_await TSuspendStdAwaiterWithoutCancel{&Counts.Bridge};
            } else {
                co_await ActorWaitForEvent<TEvents::TEvWakeup>(0);
            }
            if (throws) {
                throw TTestException();
            }
            co_return 42;
        }

        Y_NO_INLINE async<void> LazyVoid() { co_return; }
        Y_NO_INLINE async<int> LazyValue() { co_return 42; }
        Y_NO_INLINE async<int> ConstMember() const { co_return 42; }
        Y_NO_INLINE async<void> ThrowingParameter(TThrowOnMove value) {
            Y_UNUSED(value);
            co_return;
        }
        Y_NO_INLINE void ThrowingRootParameter(TThrowOnMove value) {
            Y_UNUSED(value);
            co_return;
        }
    };

    struct TFixture {
        TAllocationCache<TAsyncFrameCacheTag> Cache;
        TScopedAllocationCache<TAsyncFrameCacheTag> Binding;
        TAsyncTestActor::TState State;
        TCacheActorState Counts;
        TAsyncTestActorRuntime Runtime;
        TCacheActor* Self;
        TAsyncTestActorRuntime::TAsyncActorOperations Actor;

        explicit TFixture(bool enabled = true)
            : Cache(enabled ? TAsyncFrameCache::DefaultSizeBytes : 0)
            , Binding(&Cache)
            , Self(new TCacheActor(State, Counts))
            , Actor(Runtime, Runtime.Register(Self))
        {
            Actor.Step();
        }
    };

    Y_NO_INLINE async<int> FreeWithActor(IActor& actor) {
        Y_UNUSED(actor);
        co_return 1;
    }

    Y_NO_INLINE async<int> FreeWithoutActor() { co_return 1; }

    struct TWorkerState {
        TManualEvent Ready;
        size_t Budget = 0;
        bool Shared = false;
        bool Destroyed = false;
        bool LazyReused = false;
        size_t DestroyedFrames = 0;
    };

    class TWorkerCacheActor : public TActorBootstrapped<TWorkerCacheActor> {
        TWorkerState& State;
    public:
        explicit TWorkerCacheActor(TWorkerState& state) : State(state) {}
        ~TWorkerCacheActor() { State.Destroyed = true; }

        void Bootstrap() {
            Become(&TWorkerCacheActor::StateWork);
            auto* cache = TAllocationCache<TAsyncFrameCacheTag>::GetCurrent();
            UNIT_ASSERT(cache);
            State.Budget = cache->GetSizeBytes();
            State.Shared = TlsThreadContext->IsShared();
            Root();
            void* first;
            {
                auto lazy = FreeWithoutActor();
                first = lazy.GetHandle().address();
            }
            {
                auto lazy = FreeWithoutActor();
                State.LazyReused = lazy.GetHandle().address() == first;
            }
            State.Ready.Signal();
        }

        Y_NO_INLINE void Root() {
            Y_DEFER { ++State.DestroyedFrames; };
            co_await ActorWaitForEvent<TEvents::TEvWakeup>(0);
        }
        STFUNC(StateWork) { Y_UNUSED(ev); }
    };

    void CheckRealWorker(size_t budget, bool io, bool shared) {
        THolder<TActorSystemSetup> setup(new TActorSystemSetup);
        setup->RegisterSubSystem(std::make_unique<TAsyncFrameCache>(budget));
        setup->Scheduler = new TBasicSchedulerThread;
        if (io) {
            setup->CpuManager.IO.push_back(TIOExecutorPoolConfig{});
        } else {
            TBasicExecutorPoolConfig pool;
            pool.PoolName = "cache-test";
            pool.MinThreadCount = pool.MaxThreadCount = pool.DefaultThreadCount = 1;
            pool.HasSharedThread = shared;
            pool.AllThreadsAreShared = shared;
            setup->CpuManager.Basic.push_back(pool);
        }
        TWorkerState a, b;
        TActorSystem system(setup);
        system.Start();
        Y_DEFER { system.Stop(); system.Cleanup(); };
        system.Register(new TWorkerCacheActor(a));
        UNIT_ASSERT(a.Ready.WaitT(TDuration::Seconds(10)));
        system.Register(new TWorkerCacheActor(b));
        UNIT_ASSERT(b.Ready.WaitT(TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(a.Budget, budget);
        UNIT_ASSERT_VALUES_EQUAL(b.Budget, budget);
        UNIT_ASSERT_VALUES_EQUAL(a.Shared, shared);
        UNIT_ASSERT_VALUES_EQUAL(b.Shared, shared);
        if (budget) {
            UNIT_ASSERT(a.LazyReused && b.LazyReused);
        }
        // The actor system sums the idle frames retained by its own workers.
        // The suspended roots are live, so only the released lazy frames count.
        const auto cached = system.GetSubSystem<TAllocationCacheSubSystem>()->GetCachedStats(TAsyncFrameCache::FamilyId());
        if (budget) {
            UNIT_ASSERT(cached.CachedFrames >= 1);
            UNIT_ASSERT(cached.CachedBytes >= cached.CachedFrames * TAsyncFrameCacheTag::MinAllocationSize);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(cached.CachedFrames, 0);
            UNIT_ASSERT_VALUES_EQUAL(cached.CachedBytes, 0);
        }
        // Both suspended roots outlive the worker-local caches.
        system.Stop();
        system.Cleanup();
        UNIT_ASSERT(a.Destroyed && b.Destroyed);
        UNIT_ASSERT_VALUES_EQUAL(a.DestroyedFrames, 1);
        UNIT_ASSERT_VALUES_EQUAL(b.DestroyedFrames, 1);
    }

} // namespace

Y_UNIT_TEST_SUITE(AsyncFrameCache) {
    Y_UNIT_TEST(BasicWorkerShutdown) { CheckRealWorker(4194304, false, false); }
    Y_UNIT_TEST(IOWorkerShutdown) { CheckRealWorker(4096, true, false); }
    Y_UNIT_TEST(SharedWorkerShutdown) { CheckRealWorker(4096, false, true); }
    Y_UNIT_TEST(DisabledWorkerShutdown) { CheckRealWorker(0, false, false); }

    Y_UNIT_TEST(DefaultCustomAndZeroBudgets) {
        UNIT_ASSERT_VALUES_EQUAL(TAllocationCache<TAsyncFrameCacheTag>(TAsyncFrameCache::DefaultSizeBytes).GetSizeBytes(), 4194304);
        UNIT_ASSERT_VALUES_EQUAL(TAsyncFrameCache::DefaultSizeBytes, 4194304);
        for (size_t budget : {size_t(0), size_t(1023), size_t(1024), size_t(2047), size_t(2048)}) {
            TAllocationCache<TAsyncFrameCacheTag> cache(budget);
            TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
            auto* a = TAsyncFrameCache::Allocate(100);
            auto* b = TAsyncFrameCache::Allocate(100);
            TAsyncFrameCache::Free(a, 100);
            TAsyncFrameCache::Free(b, 100);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, std::min(budget / 1024, size_t(2)) * 1024);
            auto* large = cache.Allocate(65537);
            TAsyncFrameCache::Free(large, 65537);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, std::min(budget / 1024, size_t(2)) * 1024);
        }
    }

    Y_UNIT_TEST(BinBoundariesAndLifo) {
        TAllocationCache<TAsyncFrameCacheTag> cache(TAsyncFrameCache::DefaultSizeBytes);
        TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
        for (size_t capacity = 1024; capacity <= 65536; capacity *= 2) {
            const auto retainedBefore = cache.GetStats().CachedBytes;
            const auto lower = capacity == 1024 ? size_t(1) : capacity / 2 + 1;
            auto* first = cache.Allocate(lower);
            auto* second = cache.Allocate(capacity);
            UNIT_ASSERT(first != second);
            TAsyncFrameCache::Free(first, lower);
            TAsyncFrameCache::Free(second, capacity);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, retainedBefore + 2 * capacity);
            UNIT_ASSERT_VALUES_EQUAL(cache.Allocate(lower), second);
            UNIT_ASSERT_VALUES_EQUAL(cache.Allocate(capacity), first);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, retainedBefore);
            TAsyncFrameCache::Free(first, capacity);
            TAsyncFrameCache::Free(second, lower);
        }
        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().SizeClasses, 7);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, 2 * (1024 + 2048 + 4096 + 8192 + 16384 + 32768 + 65536));
        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().HeapAllocations, 14);
    }

    Y_UNIT_TEST(Over64KiBAlwaysBypassesCache) {
        TAllocationCache<TAsyncFrameCacheTag> cache(TAsyncFrameCache::DefaultSizeBytes);
        TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
        for (size_t size : {size_t(65537), size_t(131072)}) {
            auto* frame = cache.Allocate(size);
            TAsyncFrameCache::Free(frame, size);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedFrames, 0);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().HeapAllocations, 2);
    }

    Y_UNIT_TEST(RoundedCapacityFillsDefaultBudget) {
        TAllocationCache<TAsyncFrameCacheTag> cache(TAsyncFrameCache::DefaultSizeBytes);
        TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
        std::array<void*, 65> frames;
        for (auto& frame : frames) {
            frame = cache.Allocate(32769);
        }
        for (auto* frame : frames) {
            TAsyncFrameCache::Free(frame, 32769);
        }
        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, 4 * 1024 * 1024);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedFrames, 64);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().SizeClasses, 1);
    }

    Y_UNIT_TEST(AlignmentAndSmallFrames) {
        TAllocationCache<TAsyncFrameCacheTag> cache(TAsyncFrameCache::DefaultSizeBytes);
        TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
        for (size_t size : {size_t(0), size_t(1), size_t(7), size_t(8), size_t(1023), size_t(1024)}) {
            auto* frame = cache.Allocate(size);
            UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(frame) % __STDCPP_DEFAULT_NEW_ALIGNMENT__, 0);
            const auto before = cache.GetStats().CachedBytes;
            TAsyncFrameCache::Free(frame, size);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, before + 1024);
        }
    }

    Y_UNIT_TEST(UncachedAllocationsCanEnterWorkerBins) {
        UNIT_ASSERT(!TAllocationCache<TAsyncFrameCacheTag>::GetCurrent());
        auto* outside = TAsyncFrameCache::Allocate(1025);
        void* disabled;
        {
            TAllocationCache<TAsyncFrameCacheTag> cache(0);
            TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
            disabled = TAsyncFrameCache::Allocate(1);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().HeapAllocations, 1);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, 0);
        }
        TAllocationCache<TAsyncFrameCacheTag> cache(3072);
        TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
        TAsyncFrameCache::Free(outside, 1025);
        TAsyncFrameCache::Free(disabled, 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, 3072);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().SizeClasses, 2);
        auto* large = cache.Allocate(2048);
        auto* small = cache.Allocate(1024);
        UNIT_ASSERT_VALUES_EQUAL(large, outside);
        UNIT_ASSERT_VALUES_EQUAL(small, disabled);
        static_cast<char*>(large)[2047] = 1;
        static_cast<char*>(small)[1023] = 1;
        TAsyncFrameCache::Free(large, 2048);
        TAsyncFrameCache::Free(small, 1024);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().HeapAllocations, 0);
    }

    Y_UNIT_TEST(FreeingThreadOwnsRetentionAfterOriginExits) {
        void* frame = nullptr;
        std::thread([&] {
            TAllocationCache<TAsyncFrameCacheTag> cache(1024);
            TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
            frame = TAsyncFrameCache::Allocate(100);
        }).join();
        std::thread([&] {
            TAllocationCache<TAsyncFrameCacheTag> cache(1024);
            TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
            TAsyncFrameCache::Free(frame, 100);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, 1024);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().HeapAllocations, 0);
            UNIT_ASSERT_VALUES_EQUAL(cache.Allocate(100), frame);
        }).join();
        // No worker binding: direct heap deletion, even after both caches exit.
        UNIT_ASSERT(!TAllocationCache<TAsyncFrameCacheTag>::GetCurrent());
        TAsyncFrameCache::Free(frame, 100);
    }

    Y_UNIT_TEST(ReturnToAnotherThreadWhileOriginLives) {
        TAllocationCache<TAsyncFrameCacheTag> origin(1024);
        TScopedAllocationCache<TAsyncFrameCacheTag> binding(&origin);
        void* frame = TAsyncFrameCache::Allocate(100);
        std::thread([&] {
            TAllocationCache<TAsyncFrameCacheTag> destination(1024);
            TScopedAllocationCache<TAsyncFrameCacheTag> destinationBinding(&destination);
            TAsyncFrameCache::Free(frame, 100);
            UNIT_ASSERT_VALUES_EQUAL(destination.GetStats().CachedBytes, 1024);
            UNIT_ASSERT_VALUES_EQUAL(destination.GetStats().HeapAllocations, 0);
        }).join();
        UNIT_ASSERT_VALUES_EQUAL(origin.GetStats().CachedBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(origin.GetStats().HeapAllocations, 1);
    }

    Y_UNIT_TEST(IndependentThreadBudgetsAndScopedRestoration) {
        TAllocationCache<TAsyncFrameCacheTag> cache(1024);
        TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
        TAsyncFrameCache::Free(cache.Allocate(100), 100);
        std::thread([&] {
            UNIT_ASSERT(!TAllocationCache<TAsyncFrameCacheTag>::GetCurrent());
            TAllocationCache<TAsyncFrameCacheTag> other(2048);
            {
                TScopedAllocationCache<TAsyncFrameCacheTag> otherBinding(&other);
                TAsyncFrameCache::Free(other.Allocate(200), 200);
                UNIT_ASSERT_VALUES_EQUAL(other.GetStats().CachedBytes, 1024);
            }
            UNIT_ASSERT(!TAllocationCache<TAsyncFrameCacheTag>::GetCurrent());
        }).join();
        UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedBytes, 1024);
        {
            TAllocationCache<TAsyncFrameCacheTag> other(0);
            TScopedAllocationCache<TAsyncFrameCacheTag> nested(&other);
            UNIT_ASSERT_VALUES_EQUAL(TAllocationCache<TAsyncFrameCacheTag>::GetCurrent(), &other);
        }
        UNIT_ASSERT_VALUES_EQUAL(TAllocationCache<TAsyncFrameCacheTag>::GetCurrent(), &cache);
    }

    Y_UNIT_TEST(CachedStatsFollowIdleBlocksAndBudget) {
        TAllocationCache<TAsyncFrameCacheTag> cache(1024);
        auto* first = cache.Allocate(1);
        auto* overflow = cache.Allocate(1);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCachedStats().CachedFrames, 0);
        cache.Release(first, 1);
        cache.Release(overflow, 1); // Cannot retain a second rounded block.
        cache.Release(cache.Allocate(65537), 65537); // Never retained.
        auto stats = cache.GetCachedStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.CachedFrames, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.CachedBytes, 1024);

        auto* reused = cache.Allocate(1000);
        UNIT_ASSERT_VALUES_EQUAL(reused, first);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCachedStats().CachedFrames, 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetCachedStats().CachedBytes, 0);
        cache.Release(reused, 1000);

        TAllocationCache<TAsyncFrameCacheTag> disabled(0);
        disabled.Release(disabled.Allocate(1), 1);
        UNIT_ASSERT_VALUES_EQUAL(disabled.GetCachedStats().CachedFrames, 0);

        TAllocationCache<TAsyncFrameCacheTag> large(2048);
        large.Release(large.Allocate(1025), 1025);
        TAllocationCacheProcessStats total;
        total.Add(cache.GetCachedStats());
        total.Add(disabled.GetCachedStats());
        total.Add(large.GetCachedStats());
        UNIT_ASSERT_VALUES_EQUAL(total.CachedFrames, 2);
        UNIT_ASSERT_VALUES_EQUAL(total.CachedBytes, 3072);
    }

    Y_UNIT_TEST(CachedStatsSampledWhileOwnerChurns) {
        // Only the owner thread allocates and releases; other threads may sample.
        TAllocationCache<TAsyncFrameCacheTag> cache(3072);
        TManualEvent start;
        std::thread owner([&] {
            start.WaitI();
            for (size_t i = 0; i < 20000; ++i) {
                auto* small = cache.Allocate(1);
                auto* large = cache.Allocate(1025);
                cache.Release(small, 1);
                cache.Release(large, 1025);
                cache.Release(cache.Allocate(1024), 1024);
            }
        });
        Y_DEFER { start.Signal(); owner.join(); };
        start.Signal();
        for (size_t i = 0; i < 20000; ++i) {
            const auto stats = cache.GetCachedStats();
            UNIT_ASSERT(stats.CachedFrames <= 2);
            UNIT_ASSERT(stats.CachedBytes >= stats.CachedFrames * 1024);
            UNIT_ASSERT(stats.CachedBytes <= stats.CachedFrames * 2048);
        }
    }

#if defined(_asan_enabled_)
    Y_UNIT_TEST(AsanIdleFramesAndRequestedBoundaries) {
        UNIT_ASSERT(!TAllocationCache<TAsyncFrameCacheTag>::GetCurrent());
        for (const auto& [size, capacity] : {std::pair<size_t, size_t>{0, 1024},
                {1, 1024}, {7, 1024}, {8, 1024}, {9, 1024}, {1023, 1024}, {1024, 1024},
                {1025, 2048}, {2047, 2048}, {2048, 2048}, {32769, 65536}, {65535, 65536}, {65536, 65536}}) {
            auto checkLive = [&](void* frame, size_t requested) {
                UNIT_ASSERT(!__asan_region_is_poisoned(frame, requested));
                auto* bytes = static_cast<char*>(frame);
                // Includes the heap redzone when the request fills the bin.
                UNIT_ASSERT(__asan_address_is_poisoned(bytes + requested));
                if (requested < capacity) {
                    UNIT_ASSERT(__asan_address_is_poisoned(bytes + capacity - 1));
                }
            };
            auto* frame = TAsyncFrameCache::Allocate(size);
            checkLive(frame, size);
            TAllocationCache<TAsyncFrameCacheTag> cache(TAsyncFrameCache::DefaultSizeBytes);
            TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
            TAsyncFrameCache::Free(frame, size);
            for (size_t offset = 0; offset < capacity; ++offset) {
                UNIT_ASSERT(__asan_address_is_poisoned(static_cast<char*>(frame) + offset));
            }
            UNIT_ASSERT_VALUES_EQUAL(cache.GetStats().CachedFrames, 1);
            UNIT_ASSERT_VALUES_EQUAL(cache.Allocate(capacity), frame);
            checkLive(frame, capacity);
            cache.Release(frame, capacity);
            const auto smaller = capacity == 1024 ? size_t(1) : capacity / 2 + 1;
            UNIT_ASSERT_VALUES_EQUAL(cache.Allocate(smaller), frame);
            checkLive(frame, smaller);
            cache.Release(frame, smaller);
        }
        TAllocationCache<TAsyncFrameCacheTag> disabled(0);
        TScopedAllocationCache<TAsyncFrameCacheTag> binding(&disabled);
        auto* frame = TAsyncFrameCache::Allocate(1);
        UNIT_ASSERT(!__asan_address_is_poisoned(frame));
        UNIT_ASSERT(__asan_address_is_poisoned(static_cast<char*>(frame) + 1));
        TAsyncFrameCache::Free(frame, 1);
    }
#endif

#if defined(_msan_enabled_)
    Y_UNIT_TEST(MsanReusedFramesAreUninitialized) {
        TAllocationCache<TAsyncFrameCacheTag> cache(TAsyncFrameCache::DefaultSizeBytes);
        for (size_t size : {size_t(1), size_t(7), size_t(8), size_t(1024), size_t(1025), size_t(65536)}) {
            auto* frame = cache.Allocate(size);
            UNIT_ASSERT_VALUES_EQUAL(__msan_test_shadow(frame, size), 0);
            std::memset(frame, 0x55, size);
            UNIT_ASSERT_VALUES_EQUAL(__msan_test_shadow(frame, size), -1);
            cache.Release(frame, size);
            UNIT_ASSERT_VALUES_EQUAL(__msan_test_shadow(frame, size), 0);
            UNIT_ASSERT(cache.GetStats().CachedFrames);
            UNIT_ASSERT_VALUES_EQUAL(cache.Allocate(size), frame);
            UNIT_ASSERT_VALUES_EQUAL(__msan_test_shadow(frame, size), 0);
            std::memset(frame, 0xaa, size);
            cache.Release(frame, size);
        }
    }
#endif

    Y_UNIT_TEST(ReuseAcrossActorsAndLazyFrameOutlivesActor) {
        TFixture f;
        auto* second = new TCacheActor(f.State, f.Counts);
        TAsyncTestActorRuntime::TAsyncActorOperations actor(f.Runtime, f.Runtime.Register(second));
        actor.Step();
        void* address;
        {
            auto lazy = f.Self->LazyValue();
            address = lazy.GetHandle().address();
        }
        {
            auto lazy = second->LazyValue();
            UNIT_ASSERT_VALUES_EQUAL(lazy.GetHandle().address(), address);
            f.Runtime.CleanupNode();
            // Destruction of an unstarted member coroutine does not access this.
        }
        UNIT_ASSERT(f.Cache.GetStats().CachedFrames >= 1);
    }

    Y_UNIT_TEST(RootAndNestedFramesReusedAfterCompletion) {
        TFixture f;
        f.Actor.RunSync([&] { f.Self->Root(); });
        void* root = f.Counts.Root;
        void* child = f.Counts.Child;
        UNIT_ASSERT(root && child && root != child);
        f.Actor.Receive(new TEvents::TEvWakeup);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.Value, 42);
        const auto allocations = f.Cache.GetStats().HeapAllocations;
        f.Actor.RunSync([&] { f.Self->Root(); });
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.Root, root);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.Child, child);
        UNIT_ASSERT_VALUES_EQUAL(f.Cache.GetStats().HeapAllocations, allocations);
        f.Actor.Receive(new TEvents::TEvWakeup);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.Finished, 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.RootDestroyed, 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.ChildDestroyed, 2);
    }

    Y_UNIT_TEST(ZeroBudgetUsesHeap) {
        TFixture f(false);
        f.Actor.RunSync([&] { f.Self->Root(); });
        UNIT_ASSERT_VALUES_EQUAL(f.Cache.GetStats().HeapAllocations, 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Cache.GetStats().CachedBytes, 0);
        f.Actor.Receive(new TEvents::TEvWakeup);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.Finished, 1);
    }

    Y_UNIT_TEST(UnstartedMembersAndFreeFunctions) {
        TFixture f;
        f.Actor.RunSync([&] {
            {
                auto child = f.Self->LazyVoid();
                UNIT_ASSERT(child.GetHandle());
            }
            {
                auto child = f.Self->LazyValue();
                UNIT_ASSERT(child.GetHandle());
            }
            {
                auto child = FreeWithActor(*f.Self);
                UNIT_ASSERT(child.GetHandle());
            }
            const auto allocations = f.Cache.GetStats().HeapAllocations;
            auto free = FreeWithoutActor();
            auto member = f.Self->ConstMember();
            auto lambda = []() Y_NO_INLINE -> async<int> { co_return 1; };
            auto closure = lambda();
            UNIT_ASSERT(free.GetHandle() && member.GetHandle() && closure.GetHandle());
            UNIT_ASSERT(f.Cache.GetStats().HeapAllocations > allocations);
        });
    }

    Y_UNIT_TEST(ParameterCopyFailureReturnsAllocatedFrame) {
        TFixture f;
        f.Actor.RunSync([&] {
            TThrowOnMove value;
            UNIT_ASSERT_EXCEPTION(f.Self->ThrowingParameter(value), TTestException);
            UNIT_ASSERT_VALUES_EQUAL(f.Cache.GetStats().HeapAllocations, 1);
            UNIT_ASSERT_EXCEPTION(f.Self->ThrowingRootParameter(value), TTestException);
            UNIT_ASSERT(f.Cache.GetStats().CachedFrames >= 1);
        });
    }

    Y_UNIT_TEST(NestedExceptionReturnsBothFrames) {
        TFixture f;
        f.Actor.RunSync([&] { f.Self->Root(false, true); });
        f.Actor.Receive(new TEvents::TEvWakeup);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.Caught, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.RootDestroyed, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.ChildDestroyed, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Cache.GetStats().CachedFrames, 2);
    }

    Y_UNIT_TEST(PassAwayReturnsSuspendedRootAndChild) {
        TFixture f;
        f.Actor.RunSync([&] { f.Self->Root(); });
        f.Actor.Poison();
        UNIT_ASSERT(f.State.Destroyed);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.Finished, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.RootDestroyed, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.ChildDestroyed, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Cache.GetStats().CachedFrames, 2);
    }

    Y_UNIT_TEST(PassAwayWaitsForGenericCompletion) {
        TFixture f;
        f.Actor.RunSync([&] { f.Self->Root(true); });
        f.Actor.Poison();
        UNIT_ASSERT(!f.State.Destroyed);
        f.Counts.Bridge.resume();
        UNIT_ASSERT(!f.State.Destroyed);
        f.Actor.Step();
        UNIT_ASSERT(f.State.Destroyed);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.RootDestroyed, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Counts.ChildDestroyed, 1);
    }

    Y_UNIT_TEST(ForcedCleanupThenLateGenericCompletion) {
        TAsyncTestActor::TState state;
        TCacheActorState counts;
        {
            TAsyncTestActorRuntime runtime;
            {
                TAllocationCache<TAsyncFrameCacheTag> cache(TAsyncFrameCache::DefaultSizeBytes);
                TScopedAllocationCache<TAsyncFrameCacheTag> binding(&cache);
                auto* self = new TCacheActor(state, counts);
                TAsyncTestActorRuntime::TAsyncActorOperations actor(runtime, runtime.Register(self));
                actor.Step();
                actor.RunSync([&] { self->Root(true); });
                // The generic awaiter receives a separately allocated bridge adapter,
                // which can survive forced destruction of both actor frames and their cache.
                UNIT_ASSERT(counts.Bridge && counts.Root && counts.Child);
                UNIT_ASSERT(counts.Bridge.address() != counts.Root);
                UNIT_ASSERT(counts.Bridge.address() != counts.Child);
                runtime.CleanupNode();
                UNIT_ASSERT(state.Destroyed);
                UNIT_ASSERT_VALUES_EQUAL(counts.Finished, 0);
                UNIT_ASSERT_VALUES_EQUAL(counts.RootDestroyed, 1);
                UNIT_ASSERT_VALUES_EQUAL(counts.ChildDestroyed, 1);
            } // Drop cached memory before the bridge completes without worker TLS.
            // As in the generic-awaiter tests, the runtime destructor cleans up
            // the late event; a cleaned mailbox cannot be dispatched again.
            counts.Bridge.resume();
        }
        UNIT_ASSERT_VALUES_EQUAL(counts.Finished, 0);
        UNIT_ASSERT_VALUES_EQUAL(counts.RootDestroyed, 1);
        UNIT_ASSERT_VALUES_EQUAL(counts.ChildDestroyed, 1);
    }
}

} // namespace NAsyncTest
