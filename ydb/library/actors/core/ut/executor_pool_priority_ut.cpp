#include "actor_bootstrapped.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"
#include "subsystems/stats.h"
#include "thread_context.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

using namespace NActors;

namespace {

class TCallbackActor : public TActor<TCallbackActor> {
    const std::function<void(const TActorContext&)> Callback;

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == TEvents::TEvPoison::EventType) {
            PassAway();
            return;
        }
        Callback(ActorContext());
    }

public:
    explicit TCallbackActor(std::function<void(const TActorContext&)> callback)
        : TActor(&TThis::StateWork)
        , Callback(std::move(callback))
    {}
};

struct TFixture {
    std::mutex Mutex;
    std::condition_variable Changed;
    bool Stopping = false;
    ui32 Running = 0;
    std::set<ui32> Released;
    std::vector<ui32> Order;
    std::unique_ptr<TActorSystem> System;

    TFixture(ui32 threads, bool waker, bool shared = false, bool priority = true) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        TBasicExecutorPoolConfig config;
        config.PoolName = "PriorityTest";
        config.Threads = threads;
        config.MinThreadCount = threads;
        config.MaxThreadCount = threads;
        config.DefaultThreadCount = threads;
        config.SpinThreshold = 0;
        config.EventsPerMailbox = 1; // every remaining event requires rescheduling
        config.AllThreadsAreShared = shared;
        config.EnableWaker = waker;
        config.UsePriority = priority;
        setup->CpuManager.Basic.push_back(config);
        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 0)));
        System = std::make_unique<TActorSystem>(setup);
        System->Start();
    }

    ~TFixture() {
        {
            std::lock_guard lock(Mutex);
            Stopping = true;
            Changed.notify_all();
        }
        System->Stop();
    }

    template<class TPredicate>
    void Wait(TPredicate predicate) {
        std::unique_lock lock(Mutex);
        UNIT_ASSERT_C(Changed.wait_for(lock, std::chrono::seconds(30), predicate), "executor made no progress");
    }

    TActorId Register(std::function<void(const TActorContext&)> callback,
            EMailboxPriority priority = EMailboxPriority::Normal) {
        THolder<IActor> actor(new TCallbackActor(std::move(callback)));
        actor->SetMailboxPriority(priority);
        return System->Register(actor.Release());
    }

    void Send(TActorId actor) {
        System->Send(actor, new TEvents::TEvWakeup);
    }

    void Record(ui32 value) {
        std::lock_guard lock(Mutex);
        Order.push_back(value);
        Changed.notify_all();
    }

    TActorId Gate(ui32 id) {
        const auto actor = Register([this, id](const auto&) {
            std::unique_lock lock(Mutex);
            ++Running;
            Changed.notify_all();
            Changed.wait(lock, [&] { return Stopping || Released.contains(id); });
            --Running;
            Changed.notify_all();
        });
        Send(actor);
        return actor;
    }

    void Release(ui32 id) {
        std::lock_guard lock(Mutex);
        Released.insert(id);
        Changed.notify_all();
    }
};

void CheckRepeatedReadyWork(bool waker, bool shared, bool priority = true) {
    TFixture env(1, waker, shared, priority);
    const auto high = env.Register([&](const auto&) { env.Record(1); }, EMailboxPriority::High);
    const auto normal = env.Register([&](const auto&) { env.Record(0); });
    for (ui32 round = 0; round < 16; ++round) {
        env.Gate(round);
        env.Wait([&] { return env.Running == 1; });
        // An existing Normal backlog, followed by several events in one High
        // mailbox. Each event must survive a quantum/requeue with its priority.
        for (ui32 i = 0; i < 16; ++i) {
            env.Send(normal);
        }
        for (ui32 i = 0; i < 3; ++i) {
            env.Send(high);
        }
        env.Release(round);
        env.Wait([&] { return env.Order.size() == (round + 1) * 19; });
        std::lock_guard lock(env.Mutex);
        for (ui32 i = 0; i < 3 && priority; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(env.Order[round * 19 + i], 1);
        }
        if (!priority) {
            UNIT_ASSERT_VALUES_EQUAL(env.Order[round * 19], 0);
        }
    }
}

template<ESendingType Sending>
void CheckCapturedWork(bool waker, bool shared) {
    TFixture env(1, waker, shared);
    const auto normal = env.Register([&](const auto&) { env.Record(0); });
    const auto high = env.Register([&](const auto&) { env.Record(1); }, EMailboxPriority::High);
    // Generate a same-pool send while High is already queued. A Tail or Lazy
    // capture must not hide the priority decision from the queue.
    const auto sender = env.Register([&](const TActorContext& ctx) {
        {
            std::unique_lock lock(env.Mutex);
            ++env.Running;
            env.Changed.notify_all();
            env.Changed.wait(lock, [&] { return env.Stopping || env.Released.contains(0); });
        }
        ctx.Send<Sending>(normal, new TEvents::TEvWakeup);
    });
    env.Send(sender);
    env.Wait([&] { return env.Running == 1; });
    env.Send(high);
    env.Release(0);
    env.Wait([&] { return env.Order.size() == 2; });
    std::lock_guard lock(env.Mutex);
    UNIT_ASSERT_VALUES_EQUAL(env.Order[0], 1);
    UNIT_ASSERT_VALUES_EQUAL(env.Order[1], 0);
}

void CheckAllWorkersAvailable(bool waker, bool shared) {
    TFixture env(4, waker, shared);
    for (ui32 i = 0; i < 4; ++i) {
        env.Gate(i);
    }
    env.Wait([&] { return env.Running == 4; });
    const auto high = env.Register([&](const auto&) { env.Record(1); }, EMailboxPriority::High);
    for (ui32 i = 4; i < 8; ++i) {
        env.Gate(i); // more Normal work, all queued
    }
    env.Send(high);
    env.Release(0);
    env.Wait([&] { return env.Order.size() == 1 && env.Running == 4; });
    // High got a worker, then all four workers returned to Normal. No reserve.
    std::lock_guard lock(env.Mutex);
    UNIT_ASSERT_VALUES_EQUAL(env.Order[0], 1);
}

void CheckConcurrentSenders(bool waker, bool shared) {
    TFixture env(4, waker, shared);
    std::vector<TActorId> actors;
    for (ui32 i = 0; i < 16; ++i) {
        actors.push_back(env.Register([&, i](const auto&) { env.Record(i); },
            i % 2 ? EMailboxPriority::High : EMailboxPriority::Normal));
    }
    for (ui32 round = 0; round < 8; ++round) {
        std::vector<std::thread> senders;
        for (ui32 producer = 0; producer < 4; ++producer) {
            senders.emplace_back([&, producer] {
                for (ui32 event = 0; event < 256; ++event) {
                    env.Send(actors[(producer + event) % actors.size()]);
                }
            });
        }
        for (auto& sender : senders) {
            sender.join();
        }
        env.Wait([&] { return env.Order.size() == (round + 1) * 1024; });
    }
    std::lock_guard lock(env.Mutex);
    std::vector<ui32> counts(16);
    for (ui32 actor : env.Order) {
        ++counts[actor];
    }
    for (ui32 count : counts) {
        UNIT_ASSERT_VALUES_EQUAL(count, 512);
    }
}

void CheckSameMailboxInheritance(bool waker, bool highParent) {
    TActorId child;
    TFixture env(1, waker);
    const auto parent = env.Register([&](const TActorContext& ctx) {
        THolder<IActor> actor(new TCallbackActor([&](const auto&) { env.Record(1); }));
        // The child's requested class deliberately differs from its parent's.
        actor->SetMailboxPriority(highParent ? EMailboxPriority::Normal : EMailboxPriority::High);
        const auto id = ctx.RegisterWithSameMailbox(actor.Release());
        std::lock_guard lock(env.Mutex);
        child = id;
        env.Changed.notify_all();
    }, highParent ? EMailboxPriority::High : EMailboxPriority::Normal);
    env.Send(parent);
    env.Wait([&] { return bool(child); });
    UNIT_ASSERT_VALUES_EQUAL(parent.Hint(), child.Hint());
    env.Gate(0);
    env.Wait([&] { return env.Running == 1; });
    env.Send(child);
    env.Send(env.Register([&](const auto&) { env.Record(0); },
        highParent ? EMailboxPriority::Normal : EMailboxPriority::High));
    env.Release(0);
    env.Wait([&] { return env.Order.size() == 2; });
    std::lock_guard lock(env.Mutex);
    UNIT_ASSERT_VALUES_EQUAL(env.Order[0], highParent ? 1 : 0);
    UNIT_ASSERT_VALUES_EQUAL(env.Order[1], highParent ? 0 : 1);
}

void CheckMailboxReuse(bool waker, bool shared) {
    std::vector<TActorId> children;
    TFixture env(1, waker, shared);
    const auto parent = env.Register([&](const TActorContext& ctx) {
        std::lock_guard lock(env.Mutex);
        THolder<IActor> actor(new TCallbackActor([&](const auto&) { env.Record(1); }));
        if (children.empty()) {
            actor->SetMailboxPriority(EMailboxPriority::High);
        }
        // Use the registration overload with this worker's cache explicitly:
        // the public ctx.Register path currently allocates from the table.
        children.push_back(TlsThreadContext->Pool()->Register(actor.Release(),
            TlsThreadContext->WorkerContext.MailboxCache, 0, ctx.SelfID));
        env.Changed.notify_all();
    });
    env.Send(parent);
    env.Wait([&] { return children.size() == 1; });
    // The only worker handles High's death before the Normal parent. Its next
    // registration reuses the just-freed mailbox from that worker's cache.
    env.System->Send(children[0], new TEvents::TEvPoison);
    env.Send(parent);
    env.Wait([&] { return children.size() == 2; });
    UNIT_ASSERT_VALUES_EQUAL(children[0].Hint(), children[1].Hint());
    UNIT_ASSERT(children[0] != children[1]);
    env.Gate(0);
    env.Wait([&] { return env.Running == 1; });
    env.Send(children[1]);
    env.Send(env.Register([&](const auto&) { env.Record(0); }, EMailboxPriority::High));
    env.Release(0);
    env.Wait([&] { return env.Order.size() == 2; });
    std::lock_guard lock(env.Mutex);
    UNIT_ASSERT_VALUES_EQUAL(env.Order[0], 0);
    UNIT_ASSERT_VALUES_EQUAL(env.Order[1], 1);
}

class TTrackedActor : public TCallbackActor {
    std::atomic<ui32>& Destroyed;
public:
    TTrackedActor(std::atomic<ui32>& destroyed, std::atomic<ui32>& handled)
        : TCallbackActor([&handled](const auto&) { ++handled; })
        , Destroyed(destroyed)
    {}

    ~TTrackedActor() override {
        ++Destroyed;
    }
};

struct TTrackedEvent : TEventLocal<TTrackedEvent, TEvents::ES_PRIVATE> {
    std::atomic<ui32>& Destroyed;

    explicit TTrackedEvent(std::atomic<ui32>& destroyed) : Destroyed(destroyed) {}

    ~TTrackedEvent() override {
        ++Destroyed;
    }
};

void CheckShutdownWithQueuedWork(bool waker) {
    std::atomic<ui32> actorsDestroyed{0}, eventsDestroyed{0}, handled{0};
    TFixture env(1, waker);
    env.Gate(0);
    env.Wait([&] { return env.Running == 1; });
    for (ui32 i = 0; i < 16; ++i) {
        THolder<IActor> actor(new TTrackedActor(actorsDestroyed, handled));
        if (i % 2) {
            actor->SetMailboxPriority(EMailboxPriority::High);
        }
        const auto id = env.System->Register(actor.Release());
        env.System->Send(id, new TTrackedEvent(eventsDestroyed));
    }
    // Stop queue consumption before opening the running gate. Both priority
    // queues remain nonempty, so cleanup must own every pending actor/event.
    env.System->GetBasicExecutorPools()[0]->PrepareStop();
    env.Release(0);
    env.System->Stop();
    env.System->Cleanup();
    UNIT_ASSERT_VALUES_EQUAL(handled.load(), 0);
    UNIT_ASSERT_VALUES_EQUAL(actorsDestroyed.load(), 16);
    UNIT_ASSERT_VALUES_EQUAL(eventsDestroyed.load(), 16);
}

Y_UNIT_TEST_SUITE(PriorityExecutorPool) {
    Y_UNIT_TEST(OldestQueuedActivationStats) {
        for (bool waker : {false, true}) {
            for (bool shared : {false, true}) {
                TFixture env(1, waker, shared);
                const auto readStats = [&] {
                    TExecutorPoolStats poolStats;
                    TVector<TExecutorThreadStats> ownStats, sharedStats;
                    GetActorSystemStats(*env.System).GetPoolStats(0, poolStats, ownStats, sharedStats);
                    UNIT_ASSERT(poolStats.HasPriorityActivationQueues);
                    return poolStats;
                };
                const auto gate = [&](ui32 id, EMailboxPriority priority) {
                    const auto actor = env.Register([&, id](const auto&) {
                        std::unique_lock lock(env.Mutex);
                        env.Order.push_back(id);
                        env.Changed.notify_all();
                        env.Changed.wait(lock, [&] { return env.Stopping || env.Released.contains(id); });
                    }, priority);
                    env.Send(actor);
                };

                const auto initial = readStats();
                UNIT_ASSERT_VALUES_EQUAL(initial.OldestNormalActivationTs, 0);
                UNIT_ASSERT_VALUES_EQUAL(initial.OldestHighActivationTs, 0);
                gate(0, EMailboxPriority::Normal);
                env.Wait([&] { return env.Order.size() == 1; });
                const auto running = readStats();
                UNIT_ASSERT_VALUES_EQUAL(running.OldestNormalActivationTs, 0);
                UNIT_ASSERT_VALUES_EQUAL(running.OldestHighActivationTs, 0);

                const ui64 beforeNormal = GetCycleCountFast();
                gate(3, EMailboxPriority::Normal);
                const auto firstNormal = readStats();
                UNIT_ASSERT(firstNormal.OldestNormalActivationTs >= beforeNormal);
                UNIT_ASSERT(firstNormal.OldestNormalActivationTs <= GetCycleCountFast());
                const ui64 beforeNextNormal = GetCycleCountFast();
                gate(4, EMailboxPriority::Normal);

                const ui64 beforeHigh = GetCycleCountFast();
                gate(1, EMailboxPriority::High);
                const auto firstHigh = readStats();
                UNIT_ASSERT_VALUES_EQUAL(firstHigh.OldestNormalActivationTs, firstNormal.OldestNormalActivationTs);
                UNIT_ASSERT(firstHigh.OldestHighActivationTs >= beforeHigh);
                UNIT_ASSERT(firstHigh.OldestHighActivationTs <= GetCycleCountFast());
                const ui64 beforeNextHigh = GetCycleCountFast();
                gate(2, EMailboxPriority::High);
                const auto queued = readStats();
                UNIT_ASSERT_VALUES_EQUAL(queued.OldestNormalActivationTs, firstNormal.OldestNormalActivationTs);
                UNIT_ASSERT_VALUES_EQUAL(queued.OldestHighActivationTs, firstHigh.OldestHighActivationTs);

                env.Release(0);
                env.Wait([&] { return env.Order.size() == 2; });
                const auto nextHigh = readStats();
                UNIT_ASSERT(nextHigh.OldestHighActivationTs >= beforeNextHigh);
                UNIT_ASSERT(nextHigh.OldestHighActivationTs <= GetCycleCountFast());
                UNIT_ASSERT_VALUES_EQUAL(nextHigh.OldestNormalActivationTs, firstNormal.OldestNormalActivationTs);

                env.Release(1);
                env.Wait([&] { return env.Order.size() == 3; });
                const auto emptyHigh = readStats();
                UNIT_ASSERT_VALUES_EQUAL(emptyHigh.OldestHighActivationTs, 0);
                UNIT_ASSERT_VALUES_EQUAL(emptyHigh.OldestNormalActivationTs, firstNormal.OldestNormalActivationTs);

                env.Release(2);
                env.Wait([&] { return env.Order.size() == 4; });
                const auto nextNormal = readStats();
                UNIT_ASSERT(nextNormal.OldestNormalActivationTs >= beforeNextNormal);
                UNIT_ASSERT(nextNormal.OldestNormalActivationTs <= GetCycleCountFast());
                UNIT_ASSERT_VALUES_EQUAL(nextNormal.OldestHighActivationTs, 0);

                env.Release(3);
                env.Wait([&] { return env.Order.size() == 5; });
                const auto empty = readStats();
                UNIT_ASSERT_VALUES_EQUAL(empty.OldestNormalActivationTs, 0);
                UNIT_ASSERT_VALUES_EQUAL(empty.OldestHighActivationTs, 0);
                std::lock_guard lock(env.Mutex);
                UNIT_ASSERT_VALUES_EQUAL(env.Order, (std::vector<ui32>{0, 1, 2, 3, 4}));
            }
        }
    }

    Y_UNIT_TEST(RepeatedFreshContinuationsRing) {
        CheckRepeatedReadyWork(false, false);
    }

    Y_UNIT_TEST(RepeatedFreshContinuationsWaker) {
        CheckRepeatedReadyWork(true, false);
    }

    Y_UNIT_TEST(SharedRepeatedFreshContinuationsRing) {
        CheckRepeatedReadyWork(false, true);
    }

    Y_UNIT_TEST(SharedRepeatedFreshContinuationsWaker) {
        CheckRepeatedReadyWork(true, true);
    }

    Y_UNIT_TEST(OrdinaryPoolKeepsItsSchedulingRing) {
        CheckRepeatedReadyWork(false, false, false);
    }

    Y_UNIT_TEST(OrdinaryPoolKeepsItsSchedulingWaker) {
        CheckRepeatedReadyWork(true, false, false);
    }

    Y_UNIT_TEST(CommonCannotBypassHighRing) {
        CheckCapturedWork<ESendingType::Common>(false, false);
    }

    Y_UNIT_TEST(CommonCannotBypassHighWaker) {
        CheckCapturedWork<ESendingType::Common>(true, false);
    }

    Y_UNIT_TEST(LazyCannotBypassHighRing) {
        CheckCapturedWork<ESendingType::Lazy>(false, false);
    }

    Y_UNIT_TEST(LazyCannotBypassHighWaker) {
        CheckCapturedWork<ESendingType::Lazy>(true, false);
    }

    Y_UNIT_TEST(TailCannotBypassHighRing) {
        CheckCapturedWork<ESendingType::Tail>(false, false);
    }

    Y_UNIT_TEST(TailCannotBypassHighWaker) {
        CheckCapturedWork<ESendingType::Tail>(true, false);
    }

    Y_UNIT_TEST(SharedTailCannotBypassHighRing) {
        CheckCapturedWork<ESendingType::Tail>(false, true);
    }

    Y_UNIT_TEST(SharedTailCannotBypassHighWaker) {
        CheckCapturedWork<ESendingType::Tail>(true, true);
    }

    Y_UNIT_TEST(AllWorkersServeNormalWorkRing) {
        CheckAllWorkersAvailable(false, false);
    }

    Y_UNIT_TEST(AllWorkersServeNormalWorkWaker) {
        CheckAllWorkersAvailable(true, false);
    }

    Y_UNIT_TEST(AllSharedWorkersServeNormalWorkRing) {
        CheckAllWorkersAvailable(false, true);
    }

    Y_UNIT_TEST(AllSharedWorkersServeNormalWorkWaker) {
        CheckAllWorkersAvailable(true, true);
    }

    Y_UNIT_TEST(ConcurrentProducersLoseNoEventsRing) {
        CheckConcurrentSenders(false, false);
    }

    Y_UNIT_TEST(ConcurrentProducersLoseNoEventsWaker) {
        CheckConcurrentSenders(true, false);
    }

    Y_UNIT_TEST(SharedConcurrentProducersLoseNoEventsRing) {
        CheckConcurrentSenders(false, true);
    }

    Y_UNIT_TEST(SharedConcurrentProducersLoseNoEventsWaker) {
        CheckConcurrentSenders(true, true);
    }

    Y_UNIT_TEST(SameMailboxInheritsHighRing) {
        CheckSameMailboxInheritance(false, true);
    }

    Y_UNIT_TEST(SameMailboxInheritsHighWaker) {
        CheckSameMailboxInheritance(true, true);
    }

    Y_UNIT_TEST(SameMailboxCannotBePromotedRing) {
        CheckSameMailboxInheritance(false, false);
    }

    Y_UNIT_TEST(SameMailboxCannotBePromotedWaker) {
        CheckSameMailboxInheritance(true, false);
    }

    Y_UNIT_TEST(ReusedMailboxDoesNotKeepHighRing) {
        CheckMailboxReuse(false, false);
    }

    Y_UNIT_TEST(ReusedMailboxDoesNotKeepHighWaker) {
        CheckMailboxReuse(true, false);
    }

    Y_UNIT_TEST(SharedReusedMailboxDoesNotKeepHighRing) {
        CheckMailboxReuse(false, true);
    }

    Y_UNIT_TEST(SharedReusedMailboxDoesNotKeepHighWaker) {
        CheckMailboxReuse(true, true);
    }

    Y_UNIT_TEST(ShutdownOwnsBothQueuesRing) {
        CheckShutdownWithQueuedWork(false);
    }

    Y_UNIT_TEST(ShutdownOwnsBothQueuesWaker) {
        CheckShutdownWithQueuedWork(true);
    }
}

} // namespace
