#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "hfunc.h"
#include "scheduler_basic.h"

#include <library/cpp/actors/util/should_continue.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/actors/protos/unittests.pb.h>

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

struct TEvMsg : public NActors::TEventBase<TEvMsg, 10347> {
    DEFINE_SIMPLE_LOCAL_EVENT(TEvMsg, "ExecutorPoolTest: Msg");
};

////////////////////////////////////////////////////////////////////////////////

inline ui64 DoTimedWork(ui64 workUs) {
    ui64 startUs = ThreadCPUTime();
    ui64 endUs = startUs + workUs;
    ui64 nowUs = startUs;
    do {
        ui64 endTs = GetCycleCountFast() + Us2Ts(endUs - nowUs);
        while (GetCycleCountFast() <= endTs) {}
        nowUs = ThreadCPUTime();
    } while (nowUs <= endUs);
    return nowUs - startUs;
}

class TTestSenderActor : public IActorCallback {
private:
    using EActivityType = IActor::EActivityType ;
    using EActorActivity = IActor::EActorActivity;

private:
    TAtomic Counter;
    TActorId Receiver;

    std::function<void(void)> Action;

public:
    TTestSenderActor(std::function<void(void)> action = [](){},
                     EActivityType activityType =  EActorActivity::OTHER)
        : IActorCallback(static_cast<TReceiveFunc>(&TTestSenderActor::Execute), activityType)
        , Action(action)
    {}

    void Start(TActorId receiver, size_t count) {
        AtomicSet(Counter, count);
        Receiver = receiver;
    }

    void Stop() {
        while (true) {
            if (GetCounter() == 0) {
                break;
            }

            Sleep(TDuration::MilliSeconds(1));
        }
    }

    size_t GetCounter() const {
        return AtomicGet(Counter);
    }

private:
    STFUNC(Execute) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvMsg, Handle);
        }
    }

    void Handle(TEvMsg::TPtr &ev) {
        Y_UNUSED(ev);
        Action();
        TAtomicBase count = AtomicDecrement(Counter);
        Y_VERIFY(count != Max<TAtomicBase>());
        if (count) {
            Send(Receiver, new TEvMsg());
        }
    }
};

// Single cpu balancer that switches pool on every activation; not thread-safe
struct TRoundRobinBalancer: public IBalancer {
    TCpuState* State;
    TMap<TPoolId, TPoolId> NextPool;

    bool AddCpu(const TCpuAllocation& cpuAlloc, TCpuState* cpu) override {
        State = cpu;
        TPoolId prev = cpuAlloc.AllowedPools.rbegin()->PoolId;
        for (auto& p : cpuAlloc.AllowedPools) {
            NextPool[prev] = p.PoolId;
            prev = p.PoolId;
        }
        return true;
    }

    bool TryLock(ui64) override { return true; }
    void SetPoolStats(TPoolId, const TBalancerStats&) override {}
    void Unlock() override {}

    void Balance() override {
        TPoolId assigned;
        TPoolId current;
        State->Load(assigned, current);
        State->AssignPool(NextPool[assigned]);
    }

    ui64 GetPeriodUs() override {
        return 1000;
    }
};

void AddUnitedPool(THolder<TActorSystemSetup>& setup, ui32 concurrency = 0) {
    TUnitedExecutorPoolConfig united;
    united.PoolId = setup->GetExecutorsCount();
    united.Concurrency = concurrency;
    setup->CpuManager.United.emplace_back(std::move(united));
}

THolder<TActorSystemSetup> GetActorSystemSetup(ui32 cpuCount) {
    auto setup = MakeHolder<NActors::TActorSystemSetup>();
    setup->NodeId = 1;
    setup->CpuManager.UnitedWorkers.CpuCount = cpuCount;
    setup->CpuManager.UnitedWorkers.NoRealtime = true; // unavailable in test environment
    setup->Scheduler = new TBasicSchedulerThread(NActors::TSchedulerConfig(512, 0));
    return setup;
}

Y_UNIT_TEST_SUITE(UnitedExecutorPool) {

#ifdef _linux_

    Y_UNIT_TEST(OnePoolManyCpus) {
        const size_t msgCount = 1e4;
        auto setup = GetActorSystemSetup(4);
        AddUnitedPool(setup);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        auto begin = TInstant::Now();

        auto actor = new TTestSenderActor();
        auto actorId = actorSystem.Register(actor);
        actor->Start(actor->SelfId(), msgCount);
        actorSystem.Send(actorId, new TEvMsg());

        while (actor->GetCounter()) {
            auto now = TInstant::Now();
            UNIT_ASSERT_C(now - begin < TDuration::Seconds(5), "Counter is " << actor->GetCounter());

            Sleep(TDuration::MilliSeconds(1));
        }

        TVector<TExecutorThreadStats> stats;
        TExecutorPoolStats poolStats;
        actorSystem.GetPoolStats(0, poolStats, stats);
        // Sum all per-thread counters into the 0th element
        for (ui32 idx = 1; idx < stats.size(); ++idx) {
            stats[0].Aggregate(stats[idx]);
        }

        UNIT_ASSERT_VALUES_EQUAL(stats[0].SentEvents, msgCount - 1);
        UNIT_ASSERT_VALUES_EQUAL(stats[0].ReceivedEvents, msgCount);
        //UNIT_ASSERT_VALUES_EQUAL(stats[0].PreemptedEvents, 0); // depends on execution time and system load, so may be non-zero
        UNIT_ASSERT_VALUES_EQUAL(stats[0].NonDeliveredEvents, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats[0].EmptyMailboxActivation, 0);
        //UNIT_ASSERT_VALUES_EQUAL(stats[0].CpuUs, 0); // depends on total duration of test, so undefined
        UNIT_ASSERT(stats[0].ElapsedTicks > 0);
        //UNIT_ASSERT(stats[0].ParkedTicks == 0); // per-pool parked time does not make sense for united pools
        UNIT_ASSERT_VALUES_EQUAL(stats[0].BlockedTicks, 0);
        UNIT_ASSERT(stats[0].ActivationTimeHistogram.TotalSamples >= msgCount / TBasicExecutorPoolConfig::DEFAULT_EVENTS_PER_MAILBOX);
        UNIT_ASSERT_VALUES_EQUAL(stats[0].EventDeliveryTimeHistogram.TotalSamples, msgCount);
        UNIT_ASSERT_VALUES_EQUAL(stats[0].EventProcessingCountHistogram.TotalSamples, msgCount);
        UNIT_ASSERT(stats[0].EventProcessingTimeHistogram.TotalSamples > 0);
        UNIT_ASSERT(stats[0].ElapsedTicksByActivity[NActors::TActorTypeOperator::GetOtherActivityIndex()] > 0);
        UNIT_ASSERT_VALUES_EQUAL(stats[0].ReceivedEventsByActivity[NActors::TActorTypeOperator::GetOtherActivityIndex()], msgCount);
        UNIT_ASSERT_VALUES_EQUAL(stats[0].ActorsAliveByActivity[NActors::TActorTypeOperator::GetOtherActivityIndex()], 1);
        UNIT_ASSERT_VALUES_EQUAL(stats[0].ScheduledEventsByActivity[NActors::TActorTypeOperator::GetOtherActivityIndex()], 0);
        UNIT_ASSERT_VALUES_EQUAL(stats[0].PoolActorRegistrations, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats[0].PoolDestroyedActors, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats[0].PoolAllocatedMailboxes, 4095); // one line
        UNIT_ASSERT(stats[0].MailboxPushedOutByTime + stats[0].MailboxPushedOutByEventCount + stats[0].MailboxPushedOutBySoftPreemption >= msgCount / TBasicExecutorPoolConfig::DEFAULT_EVENTS_PER_MAILBOX);
    }

    Y_UNIT_TEST(ManyPoolsOneSharedCpu) {
        const size_t msgCount = 1e4;
        const size_t pools = 4;
        auto setup = GetActorSystemSetup(1);
        for (size_t pool = 0; pool < pools; pool++) {
            AddUnitedPool(setup);
        }
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        auto begin = TInstant::Now();

        TVector<TTestSenderActor*> actors;
        for (size_t pool = 0; pool < pools; pool++) {
            auto actor = new TTestSenderActor();
            auto actorId = actorSystem.Register(actor, TMailboxType::HTSwap, pool);
            actor->Start(actor->SelfId(), msgCount);
            actorSystem.Send(actorId, new TEvMsg());
            actors.push_back(actor);
        }

        while (true) {
            size_t left = 0;
            for (auto actor : actors) {
                left += actor->GetCounter();
            }
            if (left == 0) {
                break;
            }
            auto now = TInstant::Now();
            UNIT_ASSERT_C(now - begin < TDuration::Seconds(5), "left " << left);
            Sleep(TDuration::MilliSeconds(1));
        }

        for (size_t pool = 0; pool < pools; pool++) {
            TVector<TExecutorThreadStats> stats;
            TExecutorPoolStats poolStats;
            actorSystem.GetPoolStats(pool, poolStats, stats);
            // Sum all per-thread counters into the 0th element
            for (ui32 idx = 1; idx < stats.size(); ++idx) {
                stats[0].Aggregate(stats[idx]);
            }

            UNIT_ASSERT_VALUES_EQUAL(stats[0].ReceivedEvents, msgCount);
            UNIT_ASSERT_VALUES_EQUAL(stats[0].PoolActorRegistrations, 1);
        }
    }

    Y_UNIT_TEST(ManyPoolsOneAssignedCpu) {
        const size_t msgCount = 1e4;
        const size_t pools = 4;
        auto setup = GetActorSystemSetup(1);
        setup->Balancer.Reset(new TRoundRobinBalancer());
        for (size_t pool = 0; pool < pools; pool++) {
            AddUnitedPool(setup);
        }
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        auto begin = TInstant::Now();

        TVector<TTestSenderActor*> actors;
        for (size_t pool = 0; pool < pools; pool++) {
            auto actor = new TTestSenderActor();
            auto actorId = actorSystem.Register(actor, TMailboxType::HTSwap, pool);
            actor->Start(actor->SelfId(), msgCount);
            actorSystem.Send(actorId, new TEvMsg());
            actors.push_back(actor);
        }

        while (true) {
            size_t left = 0;
            for (auto actor : actors) {
                left += actor->GetCounter();
            }
            if (left == 0) {
                break;
            }
            auto now = TInstant::Now();
            UNIT_ASSERT_C(now - begin < TDuration::Seconds(5), "left " << left);
            Sleep(TDuration::MilliSeconds(1));
        }

        for (size_t pool = 0; pool < pools; pool++) {
            TVector<TExecutorThreadStats> stats;
            TExecutorPoolStats poolStats;
            actorSystem.GetPoolStats(pool, poolStats, stats);
            // Sum all per-thread counters into the 0th element
            for (ui32 idx = 1; idx < stats.size(); ++idx) {
                stats[0].Aggregate(stats[idx]);
            }

            UNIT_ASSERT_VALUES_EQUAL(stats[0].ReceivedEvents, msgCount);
            UNIT_ASSERT_VALUES_EQUAL(stats[0].PoolActorRegistrations, 1);
        }
    }

    Y_UNIT_TEST(ManyPoolsOneCpuSlowEvents) {
        const size_t msgCount = 3;
        const size_t pools = 4;
        auto setup = GetActorSystemSetup(1);
        for (size_t pool = 0; pool < pools; pool++) {
            AddUnitedPool(setup);
        }
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        auto begin = TInstant::Now();

        TVector<TTestSenderActor*> actors;
        for (size_t pool = 0; pool < pools; pool++) {
            auto actor = new TTestSenderActor([]() {
                DoTimedWork(100'000);
            });
            auto actorId = actorSystem.Register(actor, TMailboxType::HTSwap, pool);
            actor->Start(actor->SelfId(), msgCount);
            actorSystem.Send(actorId, new TEvMsg());
            actors.push_back(actor);
        }

        while (true) {
            size_t left = 0;
            for (auto actor : actors) {
                left += actor->GetCounter();
            }
            if (left == 0) {
                break;
            }
            auto now = TInstant::Now();
            UNIT_ASSERT_C(now - begin < TDuration::Seconds(15), "left " << left);
            Sleep(TDuration::MilliSeconds(1));
        }

        for (size_t pool = 0; pool < pools; pool++) {
            TVector<TExecutorThreadStats> stats;
            TExecutorPoolStats poolStats;
            actorSystem.GetPoolStats(pool, poolStats, stats);
            // Sum all per-thread counters into the 0th element
            for (ui32 idx = 1; idx < stats.size(); ++idx) {
                stats[0].Aggregate(stats[idx]);
            }

            UNIT_ASSERT_VALUES_EQUAL(stats[0].ReceivedEvents, msgCount);
            UNIT_ASSERT_VALUES_EQUAL(stats[0].PreemptedEvents, msgCount); // every 100ms event should be preempted
            UNIT_ASSERT_VALUES_EQUAL(stats[0].PoolActorRegistrations, 1);
        }
    }

#endif

}
