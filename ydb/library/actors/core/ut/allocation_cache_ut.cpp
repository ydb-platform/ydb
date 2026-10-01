#include <ydb/library/actors/testlib/scoped_allocation_cache.h>
#include "subsystems/allocation_cache.h"
#include "actor_bootstrapped.h"
#include "subsystems/stats.h"

#include <util/system/event.h>
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <thread>

using namespace NActors;

namespace {

struct TSmallTag {
    static constexpr const char* Name = "Small";
    static constexpr size_t MinAllocationSize = 64;
    static constexpr size_t MaxAllocationSize = 512;
};

struct TOtherTag : TSmallTag {
    static constexpr const char* Name = "Other";
};
struct TAbsentTag : TSmallTag {};

using TSmallFrontend = TAllocationCacheFrontend<TSmallTag>;
using TOtherFrontend = TAllocationCacheFrontend<TOtherTag>;

THolder<TActorSystemSetup> MakeSetup(size_t smallBudget = 128, size_t otherBudget = 512) {
    auto setup = MakeHolder<TActorSystemSetup>();
    setup->NodeId = 1;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0] = new TBasicExecutorPool(0, 1, 10, "cache-test");
    setup->Scheduler = new TBasicSchedulerThread;
    // Deliberately register frontends first. Resolution supplies their core.
    setup->RegisterSubSystem(std::make_unique<TSmallFrontend>(smallBudget));
    setup->RegisterSubSystem(std::make_unique<TOtherFrontend>(otherBudget));
    return setup;
}

class TWorkerBinding {
public:
    explicit TWorkerBinding(TAllocationCacheWorker* worker)
        : Previous(TlsThreadContext)
        , PreviousWorker(TAllocationCacheWorker::GetCurrent())
        , Context(0, nullptr, nullptr)
    {
        TlsThreadContext = &Context;
        TAllocationCacheWorker::SetCurrent(worker);
    }

    ~TWorkerBinding() { TlsThreadContext = Previous; TAllocationCacheWorker::SetCurrent(PreviousWorker); }

private:
    TThreadContext* Previous;
    TAllocationCacheWorker* PreviousWorker;
    TThreadContext Context;
};

struct TWorkerResult {
    TManualEvent Ready;
    bool HasCache = false;
};

class TFamilyActor : public TActorBootstrapped<TFamilyActor> {
public:
    explicit TFamilyActor(TWorkerResult* result) : Result(result) {}

    void Bootstrap() {
        Result->HasCache = TAllocationCache<TSmallTag>::GetCurrent() != nullptr;
        auto* block = TSmallFrontend::Allocate(1);
        TSmallFrontend::Free(block, 1);
        Result->Ready.Signal();
        PassAway();
    }

private:
    TWorkerResult* Result;
};

class TMissingDependency : public ISubSystem {};

class TUnavailableStats : public TActorSystemStatsSubSystem {
public:
    TSubSystemDependencies GetDependencies() const override {
        return DependsOn<TMissingDependency>();
    }
    void GetPoolStats(ui32, TExecutorPoolStats&, TVector<TExecutorThreadStats>&) const override {}
    void GetPoolStats(ui32, TExecutorPoolStats&, TVector<TExecutorThreadStats>&,
        TVector<TExecutorThreadStats>&) const override {}
    void GetExecutorPoolState(i16, TExecutorPoolState&) const override {}
    void GetExecutorPoolStates(std::vector<TExecutorPoolState>&) const override {}
    void GetHarmonizerStats(THarmonizerStats&) const override {}
};

} // namespace

Y_UNIT_TEST_SUITE(AllocationCacheSubsystem) {
    Y_UNIT_TEST(FamilySnapshotsIncludeAllWorkersAndEmptyFamilies) {
        auto setup = MakeSetup();
        TActorSystem system(setup);
        system.Start();
        auto* core = system.GetSubSystem<TAllocationCacheSubSystem>();
        auto first = core->CreateWorker();
        auto second = core->CreateWorker();
        first->Get<TSmallTag>()->Release(first->Get<TSmallTag>()->Allocate(1), 1);
        second->Get<TSmallTag>()->Release(second->Get<TSmallTag>()->Allocate(1), 1);
        std::vector<TAllocationCacheFamilyStats> stats;
        core->GetFamilyStats(&stats);
        UNIT_ASSERT_VALUES_EQUAL(stats.size(), 3);
        for (const auto& family : stats) {
            UNIT_ASSERT_VALUES_EQUAL(family.Stats.CachedFrames, family.Name == "Small" ? 2 : 0);
            UNIT_ASSERT_VALUES_EQUAL(family.Stats.CachedBytes, family.Name == "Small" ? 128 : 0);
        }
        first.reset();
        second.reset();
        core->GetFamilyStats(&stats);
        for (const auto& family : stats) {
            UNIT_ASSERT_VALUES_EQUAL(family.Stats.CachedBytes, 0);
        }
        system.Stop();
        system.Cleanup();
    }

    Y_UNIT_TEST(RealWorkersWithoutMetricsAndRepeatedWorkerIds) {
        auto setup = MakeSetup();
        setup->RegisterSubSystem(std::unique_ptr<TActorSystemStatsSubSystem>(new TUnavailableStats));
        setup->ExecutorsCount = 2;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[2]);
        setup->Executors[0] = new TBasicExecutorPool(0, 1, 10, "first");
        setup->Executors[1] = new TBasicExecutorPool(1, 1, 10, "second");
        TActorSystem system(setup);
        system.Start();
        Y_DEFER { system.Stop(); system.Cleanup(); };
        UNIT_ASSERT(!system.GetSubSystem<TActorSystemStatsSubSystem>());
        TWorkerResult first, second;
        system.Register(new TFamilyActor(&first), TMailboxType::HTSwap, 0);
        system.Register(new TFamilyActor(&second), TMailboxType::HTSwap, 1);
        UNIT_ASSERT(first.Ready.WaitT(TDuration::Seconds(10)));
        UNIT_ASSERT(second.Ready.WaitT(TDuration::Seconds(10)));
        UNIT_ASSERT(first.HasCache && second.HasCache);
        // Both pools have worker id 0, but own separate physical caches.
        UNIT_ASSERT_VALUES_EQUAL(system.GetSubSystem<TAllocationCacheSubSystem>()->GetCachedStats(TSmallFrontend::FamilyId()).CachedBytes, 128);
    }

    Y_UNIT_TEST(SharedPhysicalWorkerIsCountedOnce) {
        auto setup = MakeSetup();
        setup->ExecutorsCount = 0;
        setup->Executors.Reset();
        TBasicExecutorPoolConfig pool;
        pool.PoolName = "shared-cache-test";
        pool.MinThreadCount = pool.MaxThreadCount = pool.DefaultThreadCount = 1;
        pool.HasSharedThread = true;
        pool.AllThreadsAreShared = true;
        setup->CpuManager.Basic.push_back(pool);
        TActorSystem system(setup);
        system.Start();
        Y_DEFER { system.Stop(); system.Cleanup(); };
        TWorkerResult first, second;
        system.Register(new TFamilyActor(&first));
        system.Register(new TFamilyActor(&second));
        UNIT_ASSERT(first.Ready.WaitT(TDuration::Seconds(10)));
        UNIT_ASSERT(second.Ready.WaitT(TDuration::Seconds(10)));
        UNIT_ASSERT(first.HasCache && second.HasCache);
        UNIT_ASSERT_VALUES_EQUAL(system.GetSubSystem<TAllocationCacheSubSystem>()->GetCachedStats(TSmallFrontend::FamilyId()).CachedBytes, 64);
    }

    Y_UNIT_TEST(AutomaticRegistrationAndIsolatedBudgets) {
        auto setup = MakeSetup();
        TActorSystem system(setup);
        system.Start();
        Y_DEFER { system.Stop(); system.Cleanup(); };
        auto* core = system.GetSubSystem<TAllocationCacheSubSystem>();
        UNIT_ASSERT(core);
        UNIT_ASSERT_VALUES_EQUAL(core->GetWorkerBudget(), 128 + 512 + 4_MB);
        auto worker = core->CreateWorker();
        TWorkerBinding binding(worker.get());
        UNIT_ASSERT(!worker->Get<TAbsentTag>());
        UNIT_ASSERT_VALUES_EQUAL(worker->Get<TSmallTag>()->GetSizeBytes(), 128);
        UNIT_ASSERT_VALUES_EQUAL(worker->Get<TOtherTag>()->GetSizeBytes(), 512);
        void* small = TSmallFrontend::Allocate(65);
        void* overflow = TSmallFrontend::Allocate(1);
        void* other = TOtherFrontend::Allocate(257);
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(small) % __STDCPP_DEFAULT_NEW_ALIGNMENT__, 0);
        TSmallFrontend::Free(small, 65);
        TSmallFrontend::Free(overflow, 1);
        TOtherFrontend::Free(other, 257);
        UNIT_ASSERT_VALUES_EQUAL(core->GetCachedStats(TSmallFrontend::FamilyId()).CachedBytes, 128);
        UNIT_ASSERT_VALUES_EQUAL(core->GetCachedStats(TOtherFrontend::FamilyId()).CachedBytes, 512);
        UNIT_ASSERT_VALUES_EQUAL(TSmallFrontend::Allocate(65), small);
        TSmallFrontend::Free(small, 65);
        UNIT_ASSERT_VALUES_EQUAL(system.GetSubSystem<TAllocationCacheSubSystem>()->GetCachedStats(TAsyncFrameCacheFrontend::FamilyId()).CachedBytes, 0);
    }

    Y_UNIT_TEST(ScopedCoroutineBindingPreservesOtherFamilies) {
        auto setup = MakeSetup();
        TActorSystem system(setup);
        system.Start();
        Y_DEFER { system.Stop(); system.Cleanup(); };
        auto* core = system.GetSubSystem<TAllocationCacheSubSystem>();
        auto worker = core->CreateWorker();
        TWorkerBinding binding(worker.get());
        auto* small = TAllocationCache<TSmallTag>::GetCurrent();
        auto* coroutine = TAsyncFrameCache::GetCurrent();
        TAsyncFrameCache scoped(1024);
        {
            TScopedAllocationCache<TAsyncFrameCacheTag> guard(&scoped);
            UNIT_ASSERT_VALUES_EQUAL(TAsyncFrameCache::GetCurrent(), &scoped);
            UNIT_ASSERT_VALUES_EQUAL(TAllocationCache<TSmallTag>::GetCurrent(), small);
        }
        UNIT_ASSERT_VALUES_EQUAL(TAsyncFrameCache::GetCurrent(), coroutine);
        auto* block = TSmallFrontend::Allocate(65);
        TSmallFrontend::Free(block, 65);
        TAllocationCacheWorker::SetCurrent(nullptr);
        worker.reset();
        UNIT_ASSERT_VALUES_EQUAL(core->GetCachedStats(TSmallFrontend::FamilyId()).CachedBytes, 0);
    }

    Y_UNIT_TEST(AbsentAndDisabledUseCompatibleFallback) {
        TSubSystems subsystems;
        RegisterSubSystem(subsystems, std::make_unique<TSmallFrontend>(1024));
        const auto order = ResolveSubSystemDependencies(subsystems);
        UNIT_ASSERT(order);
        UNIT_ASSERT(!GetSubSystem<TSmallFrontend>(subsystems));
        UNIT_ASSERT(!TAllocationCache<TSmallTag>::GetCurrent());
        void* outside = TSmallFrontend::Allocate(65);
        auto setup = MakeSetup(0);
        TActorSystem system(setup);
        system.Start();
        Y_DEFER { system.Stop(); system.Cleanup(); };
        auto worker = system.GetSubSystem<TAllocationCacheSubSystem>()->CreateWorker();
        TWorkerBinding binding(worker.get());
        TSmallFrontend::Free(outside, 65);
        void* disabled = TSmallFrontend::Allocate(65);
        TSmallFrontend::Free(disabled, 65);
        UNIT_ASSERT_VALUES_EQUAL(worker->Get<TSmallTag>()->GetCachedStats().CachedBytes, 0);
        // An unregistered tag in a context with other families still falls back.
        auto* absent = TAllocationCacheFrontend<TAbsentTag>::Allocate(70);
        TAllocationCacheFrontend<TAbsentTag>::Free(absent, 70);
    }

    Y_UNIT_TEST(CrossThreadAndCrossSystemTransfer) {
        auto firstSetup = MakeSetup(0);
        auto secondSetup = MakeSetup(256);
        TActorSystem first(firstSetup), second(secondSetup);
        first.Start();
        second.Start();
        Y_DEFER { first.Stop(); first.Cleanup(); second.Stop(); second.Cleanup(); };
        auto origin = first.GetSubSystem<TAllocationCacheSubSystem>()->CreateWorker();
        auto destination = second.GetSubSystem<TAllocationCacheSubSystem>()->CreateWorker();
        void* block;
        {
            TWorkerBinding binding(origin.get());
            block = TSmallFrontend::Allocate(65);
        }
        origin.reset();
        first.Stop();
        first.Cleanup();
        std::thread freeing([&] {
            TWorkerBinding binding(destination.get());
            TSmallFrontend::Free(block, 65);
            UNIT_ASSERT_VALUES_EQUAL(TSmallFrontend::Allocate(65), block);
            TSmallFrontend::Free(block, 65);
        });
        freeing.join();
        UNIT_ASSERT_VALUES_EQUAL(second.GetSubSystem<TAllocationCacheSubSystem>()->GetCachedStats(TSmallFrontend::FamilyId()).CachedBytes, 128);
    }

    Y_UNIT_TEST(LateFreeAfterWorkerAndSubsystemDestruction) {
        void* block;
        std::unique_ptr<TAllocationCacheWorker> worker;
        {
            auto setup = MakeSetup();
            TActorSystem system(setup);
            system.Start();
            worker = system.GetSubSystem<TAllocationCacheSubSystem>()->CreateWorker();
            {
                TWorkerBinding binding(worker.get());
                block = TSmallFrontend::Allocate(65);
                auto* idle = TOtherFrontend::Allocate(1);
                TOtherFrontend::Free(idle, 1);
            }
            TAllocationCacheProcessStats stats;
            worker->GetCachedStats(TOtherFrontend::FamilyId(), &stats);
            UNIT_ASSERT_VALUES_EQUAL(stats.CachedBytes, 64);
            worker.reset();
            system.Stop();
            system.Cleanup();
        }
        TSmallFrontend::Free(block, 65);
    }

    Y_UNIT_TEST(StatisticsDuringWorkerMutationAndDestruction) {
        auto setup = MakeSetup();
        TActorSystem system(setup);
        system.Start();
        Y_DEFER { system.Stop(); system.Cleanup(); };
        auto* core = system.GetSubSystem<TAllocationCacheSubSystem>();
        std::atomic<bool> finished = false;
        std::thread owner([&] {
            for (size_t i = 0; i < 1000; ++i) {
                auto worker = core->CreateWorker();
                TWorkerBinding binding(worker.get());
                for (size_t j = 0; j < 100; ++j) {
                    auto* block = TSmallFrontend::Allocate(65);
                    TSmallFrontend::Free(block, 65);
                }
            }
            finished.store(true);
        });
        while (!finished.load()) {
            const auto stats = core->GetCachedStats(TSmallFrontend::FamilyId());
            UNIT_ASSERT_VALUES_EQUAL(stats.CachedBytes, stats.CachedFrames * 128);
            UNIT_ASSERT(stats.CachedBytes <= 128);
        }
        owner.join();
        UNIT_ASSERT_VALUES_EQUAL(core->GetCachedStats(TSmallFrontend::FamilyId()).CachedBytes, 0);
    }
}
