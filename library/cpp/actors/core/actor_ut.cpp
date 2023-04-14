#include "actor.cpp"
#include "events.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"
#include "actor_bootstrapped.h"

#include <library/cpp/actors/testlib/test_runtime.h>
#include <library/cpp/actors/util/threadparkpad.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/rwlock.h>
#include <util/system/hp_timer.h>

using namespace NActors;

struct TTestEndDecorator : TDecorator {
    TThreadParkPad* Pad;
    TAtomic* ActorsAlive;

    TTestEndDecorator(THolder<IActor>&& actor, TThreadParkPad* pad, TAtomic* actorsAlive)
        : TDecorator(std::move(actor))
        , Pad(pad)
        , ActorsAlive(actorsAlive)
    {
        AtomicIncrement(*ActorsAlive);
    }

    ~TTestEndDecorator() {
        if (AtomicDecrement(*ActorsAlive) == 0) {
            Pad->Unpark();
        }
    }
};

Y_UNIT_TEST_SUITE(ActorBenchmark) {
    static constexpr bool DefaultNoRealtime = true;
    static constexpr ui32 DefaultSpinThreshold = 1000000;
    static constexpr ui32 TotalEventsAmount = 1000;

    class TDummyActor : public TActor<TDummyActor> {
    public:
        TDummyActor() : TActor<TDummyActor>(&TDummyActor::StateFunc) {}
        STFUNC(StateFunc) {
            (void)ev;
        }
    };

    enum class ERole {
        Leader,
        Follower
    };

    class TSendReceiveActor : public TActorBootstrapped<TSendReceiveActor> {
    public:
        static constexpr auto ActorActivityType() {
            return ACTORLIB_COMMON;
        }

        TSendReceiveActor(double* elapsedTime, TActorId receiver, bool allocation, ERole role, ESendingType sendingType, ui32 neighbours = 0)
            : EventsCounter(TotalEventsAmount)
            , ElapsedTime(elapsedTime)
            , Receiver(receiver)
            , AllocatesMemory(allocation)
            , Role(role)
            , SendingType(sendingType)
            , MailboxNeighboursCount(neighbours)
        {}

        void Bootstrap(const TActorContext &ctx) {
            if (!Receiver && Role == ERole::Leader) {
                this->Receiver = SelfId();
            } else {
                EventsCounter /= 2; // We want to measure CPU requirement for one-way send
            }
            Timer.Reset();
            Become(&TThis::StateFunc);
            for (ui32 i = 0; i < MailboxNeighboursCount; ++i) {
                ctx.RegisterWithSameMailbox(new TDummyActor());
            }
            if (Role == ERole::Leader) {
                TAutoPtr<IEventHandle> ev = new IEventHandle(Receiver, SelfId(), new TEvents::TEvPing());
                SpecialSend(ev, ctx);
            }
        }

        void SpecialSend(TAutoPtr<IEventHandle> ev, const TActorContext &ctx) {
            --EventsCounter;
            if (SendingType == ESendingType::Lazy) {
                ctx.Send<ESendingType::Lazy>(ev);
            } else if (SendingType == ESendingType::Tail) {
                ctx.Send<ESendingType::Tail>(ev);
            } else {
                ctx.Send(ev);
            }
        }

        bool CheckWorkIsDone() {
            if (EventsCounter == 0) {
                if (ElapsedTime != nullptr) {
                    *ElapsedTime = Timer.Passed() / TotalEventsAmount;
                }
                PassAway();
                return true;
            }
            return false;
        }

        STFUNC(StateFunc) {
            if (CheckWorkIsDone())
                return;

            auto ctx(ActorContext());
            if (AllocatesMemory) {
                SpecialSend(new IEventHandle(ev->Sender, SelfId(), new TEvents::TEvPing()), ctx);
            } else {
                std::swap(*const_cast<TActorId*>(&ev->Sender), *const_cast<TActorId*>(&ev->Recipient));
                ev->DropRewrite();
                SpecialSend(ev, ctx);
            }

            CheckWorkIsDone();
        }

    private:
        THPTimer Timer;
        ui64 EventsCounter;
        double* ElapsedTime;
        TActorId Receiver;
        bool AllocatesMemory;
        ERole Role;
        ESendingType SendingType;
        ui32 MailboxNeighboursCount;
    };

    void AddBasicPool(THolder<TActorSystemSetup>& setup, ui32 threads, bool activateEveryEvent) {
        TBasicExecutorPoolConfig basic;
        basic.PoolId = setup->GetExecutorsCount();
        basic.PoolName = TStringBuilder() << "b" << basic.PoolId;
        basic.Threads = threads;
        basic.SpinThreshold = DefaultSpinThreshold;
        basic.TimePerMailbox = TDuration::Hours(1);
        if (activateEveryEvent) {
            basic.EventsPerMailbox = 1;
        } else {
            basic.EventsPerMailbox = Max<ui32>();
        }
        setup->CpuManager.Basic.emplace_back(std::move(basic));
    }

    void AddUnitedPool(THolder<TActorSystemSetup>& setup, ui32 concurrency, bool activateEveryEvent) {
        TUnitedExecutorPoolConfig united;
        united.PoolId = setup->GetExecutorsCount();
        united.PoolName = TStringBuilder() << "u" << united.PoolId;
        united.Concurrency = concurrency;
        united.TimePerMailbox = TDuration::Hours(1);
        if (activateEveryEvent) {
            united.EventsPerMailbox = 1;
        } else {
            united.EventsPerMailbox = Max<ui32>();
        }
        setup->CpuManager.United.emplace_back(std::move(united));
    }

    THolder<TActorSystemSetup> GetActorSystemSetup(ui32 unitedCpuCount, bool preemption) {
        auto setup = MakeHolder<NActors::TActorSystemSetup>();
        setup->NodeId = 1;
        setup->CpuManager.UnitedWorkers.CpuCount = unitedCpuCount;
        setup->CpuManager.UnitedWorkers.SpinThresholdUs = DefaultSpinThreshold;
        setup->CpuManager.UnitedWorkers.NoRealtime = DefaultNoRealtime;
        if (preemption) {
            setup->CpuManager.UnitedWorkers.PoolLimitUs = 500;
            setup->CpuManager.UnitedWorkers.EventLimitUs = 100;
            setup->CpuManager.UnitedWorkers.LimitPrecisionUs = 100;
        } else {
            setup->CpuManager.UnitedWorkers.PoolLimitUs = 100'000'000'000;
            setup->CpuManager.UnitedWorkers.EventLimitUs = 10'000'000'000;
            setup->CpuManager.UnitedWorkers.LimitPrecisionUs = 10'000'000'000;
        }
        setup->Scheduler = new TBasicSchedulerThread(NActors::TSchedulerConfig(512, 0));
        return setup;
    }

    enum class EPoolType {
        Basic,
        United
    };

    THolder<TActorSystemSetup> InitActorSystemSetup(EPoolType poolType, ui32 poolsCount, ui32 threads, bool activateEveryEvent, bool preemption) {
        if (poolType == EPoolType::Basic) {
            THolder<TActorSystemSetup> setup = GetActorSystemSetup(0, false);
            for (ui32 i = 0; i < poolsCount; ++i) {
                AddBasicPool(setup, threads, activateEveryEvent);
            }
            return setup;
        } else if (poolType == EPoolType::United) {
            THolder<TActorSystemSetup> setup = GetActorSystemSetup(poolsCount * threads, preemption);
            for (ui32 i = 0; i < poolsCount; ++i) {
                AddUnitedPool(setup, threads, activateEveryEvent);
            }
            return setup;
        }
        Y_FAIL();
    }

    double BenchSendReceive(bool allocation, NActors::TMailboxType::EType mType, EPoolType poolType, ESendingType sendingType) {
        THolder<TActorSystemSetup> setup = InitActorSystemSetup(poolType, 1, 1, false, false);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        double elapsedTime = 0;
        THolder<IActor> endActor{
            new TTestEndDecorator(THolder(
                new TSendReceiveActor(&elapsedTime, {}, allocation, ERole::Leader, sendingType)), &pad, &actorsAlive)};

        actorSystem.Register(endActor.Release(), mType);

        pad.Park();
        actorSystem.Stop();

        return 1e9 * elapsedTime;
    }

    double BenchSendActivateReceive(ui32 poolsCount, ui32 threads, bool allocation, EPoolType poolType, ESendingType sendingType) {
        THolder<TActorSystemSetup> setup = InitActorSystemSetup(poolType, poolsCount, threads, true, false);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        double elapsedTime = 0;
        ui32 followerPoolId = 0;

        ui32 leaderPoolId = poolsCount == 1 ? 0 : 1;
        TActorId followerId = actorSystem.Register(
            new TSendReceiveActor(nullptr, {}, allocation, ERole::Follower, ESendingType::Common), TMailboxType::HTSwap, followerPoolId);
        THolder<IActor> leader{
            new TTestEndDecorator(THolder(
                new TSendReceiveActor(&elapsedTime, followerId, allocation, ERole::Leader, sendingType)), &pad, &actorsAlive)};
        actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);

        pad.Park();
        actorSystem.Stop();

        return 1e9 * elapsedTime;
    }

    double BenchSendActivateReceiveWithMailboxNeighbours(ui32 MailboxNeighbourActors, EPoolType poolType, ESendingType sendingType) {
        THolder<TActorSystemSetup> setup = InitActorSystemSetup(poolType, 1, 1, false, false);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        double elapsedTime = 0;

        TActorId followerId = actorSystem.Register(
            new TSendReceiveActor(nullptr, {}, false, ERole::Follower, ESendingType::Common, MailboxNeighbourActors), TMailboxType::HTSwap);
        THolder<IActor> leader{
            new TTestEndDecorator(THolder(
                new TSendReceiveActor(&elapsedTime, followerId, false, ERole::Leader, sendingType, MailboxNeighbourActors)), &pad, &actorsAlive)};
        actorSystem.Register(leader.Release(), TMailboxType::HTSwap);

        pad.Park();
        actorSystem.Stop();

        return 1e9 * elapsedTime;
    }

    double BenchContentedThreads(ui32 threads, ui32 actorsPairsCount, EPoolType poolType, ESendingType sendingType) {
        THolder<TActorSystemSetup> setup = InitActorSystemSetup(poolType, 1, threads, true, false);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        THPTimer Timer;

        TVector<double> dummy(actorsPairsCount);
        Timer.Reset();
        for (ui32 i = 0; i < actorsPairsCount; ++i) {
            ui32 followerPoolId = 0;
            ui32 leaderPoolId = 0;
            TActorId followerId = actorSystem.Register(
                new TSendReceiveActor(nullptr, {}, true, ERole::Follower, ESendingType::Common), TMailboxType::HTSwap, followerPoolId);
            THolder<IActor> leader{
                new TTestEndDecorator(THolder(
                    new TSendReceiveActor(&dummy[i], followerId, true, ERole::Leader, sendingType)), &pad, &actorsAlive)};
            actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);
        }

        pad.Park();
        auto elapsedTime = Timer.Passed() / TotalEventsAmount;
        actorSystem.Stop();

        return 1e9 * elapsedTime;
    }

    auto Mean(const TVector<double>& data) {
        return Accumulate(data.begin(), data.end(), 0.0) / data.size();
    }

    auto Deviation(const TVector<double>& data) {
        auto mean = Mean(data);
        double deviation = 0.0;
        for (const auto& x : data) {
            deviation += (x - mean) * (x - mean);
        }
        return std::sqrt(deviation / data.size());
    }

    struct TStats {
        double Mean;
        double Deviation;
        TString ToString() {
            return TStringBuilder() << Mean << " ± " << Deviation << " ns " << std::ceil(Deviation / Mean * 1000) / 10.0 << "%";
        }
    };

    template <typename Func>
    TStats CountStats(Func func, ui32 itersCount = 5) {
        TVector<double> elapsedTimes;
        for (ui32 i = 0; i < itersCount; ++i) {
            auto elapsedTime = func();
            elapsedTimes.push_back(elapsedTime);
        }
        return {Mean(elapsedTimes), Deviation(elapsedTimes)};
    }

    TVector<NActors::TMailboxType::EType> MailboxTypes = {
        TMailboxType::Simple,
        TMailboxType::Revolving,
        TMailboxType::HTSwap,
        TMailboxType::ReadAsFilled,
        TMailboxType::TinyReadAsFilled
    };

    Y_UNIT_TEST(SendReceive1Pool1ThreadAlloc) {
        for (const auto& mType : MailboxTypes) {
            auto stats = CountStats([mType] {
                return BenchSendReceive(true, mType, EPoolType::Basic, ESendingType::Common);
            });
            Cerr << stats.ToString() << " " << mType << Endl;
            stats = CountStats([mType] {
                return BenchSendReceive(true, mType, EPoolType::Basic, ESendingType::Lazy);
            });
            Cerr << stats.ToString() << " " << mType << " Lazy" << Endl;
            stats = CountStats([mType] {
                return BenchSendReceive(true, mType, EPoolType::Basic, ESendingType::Tail);
            });
            Cerr << stats.ToString() << " " << mType << " Tail" << Endl;
        }
    }

    Y_UNIT_TEST(SendReceive1Pool1ThreadAllocUnited) {
        for (const auto& mType : MailboxTypes) {
            auto stats = CountStats([mType] {
                return BenchSendReceive(true, mType, EPoolType::United, ESendingType::Common);
            });
            Cerr << stats.ToString() << " " << mType << Endl;
            stats = CountStats([mType] {
                return BenchSendReceive(true, mType, EPoolType::United, ESendingType::Lazy);
            });
            Cerr << stats.ToString() << " " << mType << " Lazy" << Endl;
            stats = CountStats([mType] {
                return BenchSendReceive(true, mType, EPoolType::United, ESendingType::Tail);
            });
            Cerr << stats.ToString() << " " << mType << " Tail" << Endl;
        }
    }

    Y_UNIT_TEST(SendReceive1Pool1ThreadNoAlloc) {
        for (const auto& mType : MailboxTypes) {
            auto stats = CountStats([mType] {
                return BenchSendReceive(false, mType, EPoolType::Basic, ESendingType::Common);
            });
            Cerr << stats.ToString() << " " << mType << Endl;
            stats = CountStats([mType] {
                return BenchSendReceive(false, mType, EPoolType::Basic, ESendingType::Lazy);
            });
            Cerr << stats.ToString() << " " << mType << " Lazy" << Endl;
            stats = CountStats([mType] {
                return BenchSendReceive(false, mType, EPoolType::Basic, ESendingType::Tail);
            });
            Cerr << stats.ToString() << " " << mType << " Tail" << Endl;
        }
    }

    Y_UNIT_TEST(SendReceive1Pool1ThreadNoAllocUnited) {
        for (const auto& mType : MailboxTypes) {
            auto stats = CountStats([mType] {
                return BenchSendReceive(false, mType, EPoolType::United, ESendingType::Common);
            });
            Cerr << stats.ToString() << " " << mType << Endl;
            stats = CountStats([mType] {
                return BenchSendReceive(false, mType, EPoolType::United, ESendingType::Lazy);
            });
            Cerr << stats.ToString() << " " << mType << " Lazy" << Endl;
            stats = CountStats([mType] {
                return BenchSendReceive(false, mType, EPoolType::United, ESendingType::Tail);
            });
            Cerr << stats.ToString() << " " << mType << " Tail" << Endl;
        }
    }

    void RunBenchSendActivateReceive(ui32 poolsCount, ui32 threads, bool allocation, EPoolType poolType) {
        auto stats = CountStats([=] {
            return BenchSendActivateReceive(poolsCount, threads, allocation, poolType, ESendingType::Common);
        });
        Cerr << stats.ToString() << Endl;
        stats = CountStats([=] {
            return BenchSendActivateReceive(poolsCount, threads, allocation, poolType, ESendingType::Lazy);
        });
        Cerr << stats.ToString() << " Lazy" << Endl;
        stats = CountStats([=] {
            return BenchSendActivateReceive(poolsCount, threads, allocation, poolType, ESendingType::Tail);
        });
        Cerr << stats.ToString() << " Tail" << Endl;
    }

    Y_UNIT_TEST(SendActivateReceive1Pool1ThreadAlloc) {
        RunBenchSendActivateReceive(1, 1, true, EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive1Pool1ThreadAllocUnited) {
        RunBenchSendActivateReceive(1, 1, true, EPoolType::United);
    }

    Y_UNIT_TEST(SendActivateReceive1Pool1ThreadNoAlloc) {
        RunBenchSendActivateReceive(1, 1, false, EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive1Pool1ThreadNoAllocUnited) {
        RunBenchSendActivateReceive(1, 1, false, EPoolType::United);
    }

    Y_UNIT_TEST(SendActivateReceive1Pool2ThreadsAlloc) {
        RunBenchSendActivateReceive(1, 2, true, EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive1Pool2ThreadsAllocUnited) {
        RunBenchSendActivateReceive(1, 2, true, EPoolType::United);
    }

    Y_UNIT_TEST(SendActivateReceive1Pool2ThreadsNoAlloc) {
        RunBenchSendActivateReceive(1, 2, false, EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive1Pool2ThreadsNoAllocUnited) {
        RunBenchSendActivateReceive(1, 2, false, EPoolType::United);
    }

    Y_UNIT_TEST(SendActivateReceive2Pool1ThreadAlloc) {
        RunBenchSendActivateReceive(2, 1, true, EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive2Pool1ThreadAllocUnited) {
        RunBenchSendActivateReceive(2, 1, true, EPoolType::United);
    }

    Y_UNIT_TEST(SendActivateReceive2Pool1ThreadNoAlloc) {
        RunBenchSendActivateReceive(2, 1, false, EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive2Pool1ThreadNoAllocUnited) {
        RunBenchSendActivateReceive(2, 1, false, EPoolType::United);
    }

    void RunBenchContentedThreads(ui32 threads, EPoolType poolType) {
        for (ui32 actorPairs = 1; actorPairs <= 2 * threads; actorPairs++) {
            auto stats = CountStats([threads, actorPairs, poolType] {
                return BenchContentedThreads(threads, actorPairs, poolType, ESendingType::Common);
            });
            Cerr << stats.ToString() << " actorPairs: " << actorPairs << Endl;
            stats = CountStats([threads, actorPairs, poolType] {
                return BenchContentedThreads(threads, actorPairs, poolType, ESendingType::Lazy);
            });
            Cerr << stats.ToString() << " actorPairs: " << actorPairs << " Lazy"<< Endl;
            stats = CountStats([threads, actorPairs, poolType] {
                return BenchContentedThreads(threads, actorPairs, poolType, ESendingType::Tail);
            });
            Cerr << stats.ToString() << " actorPairs: " << actorPairs << " Tail"<< Endl;
        }
    }

    Y_UNIT_TEST(SendActivateReceive1Pool1Threads)       { RunBenchContentedThreads(1, EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool1ThreadsUnited) { RunBenchContentedThreads(1, EPoolType::United); }
    Y_UNIT_TEST(SendActivateReceive1Pool2Threads)       { RunBenchContentedThreads(2, EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool2ThreadsUnited) { RunBenchContentedThreads(2, EPoolType::United); }
    Y_UNIT_TEST(SendActivateReceive1Pool3Threads)       { RunBenchContentedThreads(3, EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool3ThreadsUnited) { RunBenchContentedThreads(3, EPoolType::United); }
    Y_UNIT_TEST(SendActivateReceive1Pool4Threads)       { RunBenchContentedThreads(4, EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool4ThreadsUnited) { RunBenchContentedThreads(4, EPoolType::United); }
    Y_UNIT_TEST(SendActivateReceive1Pool5Threads)       { RunBenchContentedThreads(5, EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool5ThreadsUnited) { RunBenchContentedThreads(5, EPoolType::United); }
    Y_UNIT_TEST(SendActivateReceive1Pool6Threads)       { RunBenchContentedThreads(6, EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool6ThreadsUnited) { RunBenchContentedThreads(6, EPoolType::United); }
    Y_UNIT_TEST(SendActivateReceive1Pool7Threads)       { RunBenchContentedThreads(7, EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool7ThreadsUnited) { RunBenchContentedThreads(7, EPoolType::United); }
    Y_UNIT_TEST(SendActivateReceive1Pool8Threads)       { RunBenchContentedThreads(8, EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool8ThreadsUnited) { RunBenchContentedThreads(8, EPoolType::United); }

    Y_UNIT_TEST(SendActivateReceiveCSV) {
        Cout << "threads,actorPairs,msgs_per_sec" << Endl;
        for (ui32 threads = 1; threads <= 32; threads *= 2) {
            for (ui32 actorPairs = 1; actorPairs <= 2 * 32; actorPairs *= 2) {
                auto stats = CountStats([threads, actorPairs] {
                    return BenchContentedThreads(threads, actorPairs, EPoolType::Basic, ESendingType::Common);
                }, 3);
                Cout << threads << "," << actorPairs << "," << actorPairs * 1e9 / stats.Mean << Endl;
            }
        }
    }

    Y_UNIT_TEST(SendActivateReceiveWithMailboxNeighbours) {
        TVector<ui32> NeighbourActors = {0, 1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64, 128, 256};
        for (const auto& neighbour : NeighbourActors) {
            auto stats = CountStats([neighbour] {
                return BenchSendActivateReceiveWithMailboxNeighbours(neighbour, EPoolType::Basic, ESendingType::Common);
            });
            Cerr << stats.ToString() << " neighbourActors: " << neighbour << Endl;
            stats = CountStats([neighbour] {
                return BenchSendActivateReceiveWithMailboxNeighbours(neighbour, EPoolType::Basic, ESendingType::Lazy);
            });
            Cerr << stats.ToString() << " neighbourActors: " << neighbour << " Lazy" << Endl;
            stats = CountStats([neighbour] {
                return BenchSendActivateReceiveWithMailboxNeighbours(neighbour, EPoolType::Basic, ESendingType::Tail);
            });
            Cerr << stats.ToString() << " neighbourActors: " << neighbour << " Tail" << Endl;
        }
    }

    Y_UNIT_TEST(SendActivateReceiveWithMailboxNeighboursUnited) {
        TVector<ui32> NeighbourActors = {0, 1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64, 128, 256};
        for (const auto& neighbour : NeighbourActors) {
            auto stats = CountStats([neighbour] {
                return BenchSendActivateReceiveWithMailboxNeighbours(neighbour, EPoolType::United, ESendingType::Common);
            });
            Cerr << stats.ToString() << " neighbourActors: " << neighbour << Endl;
            stats = CountStats([neighbour] {
                return BenchSendActivateReceiveWithMailboxNeighbours(neighbour, EPoolType::United, ESendingType::Lazy);
            });
            Cerr << stats.ToString() << " neighbourActors: " << neighbour << " Lazy" << Endl;
            stats = CountStats([neighbour] {
                return BenchSendActivateReceiveWithMailboxNeighbours(neighbour, EPoolType::United, ESendingType::Tail);
            });
            Cerr << stats.ToString() << " neighbourActors: " << neighbour << " Tail" << Endl;
        }
    }
}

Y_UNIT_TEST_SUITE(TestDecorator) {
    struct TPingDecorator : TDecorator {
        TAutoPtr<IEventHandle> SavedEvent = nullptr;
        ui64* Counter;

        TPingDecorator(THolder<IActor>&& actor, ui64* counter)
            : TDecorator(std::move(actor))
            , Counter(counter)
        {
        }

        bool DoBeforeReceiving(TAutoPtr<IEventHandle>& ev, const TActorContext&) override {
            *Counter += 1;
            if (ev->Type != TEvents::THelloWorld::Pong) {
                TAutoPtr<IEventHandle> pingEv = new IEventHandle(SelfId(), SelfId(), new TEvents::TEvPing());
                SavedEvent = ev;
                Actor->Receive(pingEv);
            } else {
                Actor->Receive(SavedEvent);
            }
            return false;
        }
    };

    struct TPongDecorator : TDecorator {
        ui64* Counter;

        TPongDecorator(THolder<IActor>&& actor, ui64* counter)
            : TDecorator(std::move(actor))
            , Counter(counter)
        {
        }

        bool DoBeforeReceiving(TAutoPtr<IEventHandle>& ev, const TActorContext&) override {
            *Counter += 1;
            if (ev->Type == TEvents::THelloWorld::Ping) {
                TAutoPtr<IEventHandle> pongEv = new IEventHandle(SelfId(), SelfId(), new TEvents::TEvPong());
                Send(SelfId(), new TEvents::TEvPong());
                return false;
            }
            return true;
        }
    };

    struct TTestActor : TActorBootstrapped<TTestActor> {
        static constexpr char ActorName[] = "TestActor";

        void Bootstrap()
        {
            const auto& activityTypeIndex = GetActivityType();
            Y_ENSURE(activityTypeIndex < GetActivityTypeCount());
            Y_ENSURE(GetActivityTypeName(activityTypeIndex) == "TestActor");
            PassAway();
        }
    };

    Y_UNIT_TEST(Basic) {
        THolder<TActorSystemSetup> setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 0;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);

        ui64 ts = GetCycleCountFast();
        THolder<IHarmonizer> harmonizer(MakeHarmonizer(ts));
        for (ui32 i = 0; i < setup->ExecutorsCount; ++i) {
            setup->Executors[i] = new TBasicExecutorPool(i, 1, 10, "basic", harmonizer.Get());
            harmonizer->AddPool(setup->Executors[i].Get());
        }
        setup->Scheduler = new TBasicSchedulerThread;

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        THolder<IActor> innerActor = MakeHolder<TTestActor>();
        ui64 pongCounter = 0;
        THolder<IActor> pongActor = MakeHolder<TPongDecorator>(std::move(innerActor), &pongCounter);
        ui64 pingCounter = 0;
        THolder<IActor> pingActor = MakeHolder<TPingDecorator>(std::move(pongActor), &pingCounter);

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;

        THolder<IActor> endActor = MakeHolder<TTestEndDecorator>(std::move(pingActor), &pad, &actorsAlive);
        actorSystem.Register(endActor.Release(), TMailboxType::HTSwap);

        pad.Park();
        actorSystem.Stop();
        UNIT_ASSERT(pongCounter == 2 && pingCounter == 2);
    }

    Y_UNIT_TEST(LocalProcessKey) {
        static constexpr char ActorName[] = "TestActor";

        UNIT_ASSERT((TEnumProcessKey<TActorActivityTag, IActor::EActorActivity>::GetName(IActor::INTERCONNECT_PROXY_TCP) == "INTERCONNECT_PROXY_TCP"));

        UNIT_ASSERT((TLocalProcessKey<TActorActivityTag, ActorName>::GetName() == ActorName));
        UNIT_ASSERT((TEnumProcessKey<TActorActivityTag, IActor::EActorActivity>::GetIndex(IActor::INTERCONNECT_PROXY_TCP) == IActor::INTERCONNECT_PROXY_TCP));
    }
}

Y_UNIT_TEST_SUITE(TestStateFunc) {
    struct TTestActorWithExceptionsStateFunc : TActor<TTestActorWithExceptionsStateFunc> {
        static constexpr char ActorName[] = "TestActorWithExceptionsStateFunc";

        TTestActorWithExceptionsStateFunc()
            : TActor<TTestActorWithExceptionsStateFunc>(&TTestActorWithExceptionsStateFunc::StateFunc)
        {
        }

        STRICT_STFUNC_EXC(StateFunc,
            hFunc(TEvents::TEvWakeup, Handle),
            ExceptionFunc(yexception, HandleException)
            ExceptionFuncEv(std::exception, HandleException)
            AnyExceptionFunc(HandleException)
        )

        void Handle(TEvents::TEvWakeup::TPtr& ev) {
            Owner = ev->Sender;
            switch (ev->Get()->Tag) {
            case ETag::NoException:
                SendResponse(ETag::NoException);
                break;
            case ETag::YException:
                Cerr << "Throw yexception" << Endl;
                throw yexception();
            case ETag::StdException:
                Cerr << "Throw std::exception" << Endl;
                throw std::runtime_error("trololo");
            case ETag::OtherException:
                Cerr << "Throw trash" << Endl;
                throw TString("1");
            default:
                UNIT_ASSERT(false);
            }
        }

        void HandleException(const yexception&) {
            Cerr << "Handle yexception" << Endl;
            SendResponse(ETag::YException);
        }

        void HandleException(const std::exception&, TAutoPtr<::NActors::IEventHandle>& ev) {
            Cerr << "Handle std::exception from event with type " << ev->Type << Endl;
            SendResponse(ETag::StdException);
        }

        void HandleException() {
            Cerr << "Handle trash" << Endl;
            SendResponse(ETag::OtherException);
        }

        enum ETag : ui64 {
            NoException,
            YException,
            StdException,
            OtherException,
        };

        void SendResponse(ETag tag) {
            Send(Owner, new TEvents::TEvWakeup(tag));
        }

        TActorId Owner;
    };

    Y_UNIT_TEST(StateFuncWithExceptions) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto sender = runtime.AllocateEdgeActor();
        auto testActor = runtime.Register(new TTestActorWithExceptionsStateFunc());
        for (ui64 tag = 0; tag < 4; ++tag) {
            runtime.Send(new IEventHandle(testActor, sender, new TEvents::TEvWakeup(tag)), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Tag, tag);
        }
    }
}
