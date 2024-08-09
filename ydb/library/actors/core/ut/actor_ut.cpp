#include "actor.h"
#include "events.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"
#include "actor_bootstrapped.h"
#include "actor_benchmark_helper.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/util/threadparkpad.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/rwlock.h>
#include <util/system/hp_timer.h>

using namespace NActors;
using namespace NActors::NTests;

Y_UNIT_TEST_SUITE(ActorBenchmark) {

    using TActorBenchmark = ::NActors::NTests::TActorBenchmark<>;
    using TSettings = TActorBenchmark::TSettings;
    using TSendReceiveActorParams = TActorBenchmark::TSendReceiveActorParams;

    Y_UNIT_TEST(WithOnlyOneSharedExecutors) {
        return;
        THolder<TActorSystemSetup> setup =  TActorBenchmark::GetActorSystemSetup();
        TActorBenchmark::AddBasicPool(setup, 1, 1, true);

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        THPTimer Timer;

        ui64 eventsPerPair = 1000;

        Timer.Reset();
        ui32 followerPoolId = 0;
        ui32 leaderPoolId = 0;
        TActorId followerId = actorSystem.Register(
            new TActorBenchmark::TSendReceiveActor(
                TSendReceiveActorParams{.OtherEvents=eventsPerPair / 2, .Allocation=true, .ToSchedule=false}
            ),
            TMailboxType::HTSwap,
            followerPoolId
        );
        THolder<IActor> leader{
            new TTestEndDecorator(THolder(new TActorBenchmark::TSendReceiveActor(
                TSendReceiveActorParams{.OwnEvents=eventsPerPair / 2, .Receivers={followerId}, .Allocation=true}
            )),
            &pad,
            &actorsAlive)
        };
        actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);

        pad.Park();
        auto elapsedTime = Timer.Passed() /  (TSettings::TotalEventsAmountPerThread * 4);
        actorSystem.Stop();

        TExecutorThreadStats aggregated;
        TVector<TExecutorThreadStats> stats;
        TVector<TExecutorThreadStats> sharedStats;
        TExecutorPoolStats poolStats;
        actorSystem.GetPoolStats(0, poolStats, stats, sharedStats);
        // Sum all per-thread counters into the 0th element
        for (auto &stat : stats) {
            aggregated.Aggregate(stat);
        }
        for (auto &stat : sharedStats) {
            aggregated.Aggregate(stat);
        }

        Cerr << "Completed " << 1e9 * elapsedTime << Endl;
        Cerr << "Elapsed " << Ts2Us(aggregated.ElapsedTicks) << "us" << Endl;
    }


    Y_UNIT_TEST(WithOnlyOneNotSharedExecutors) {
        THolder<TActorSystemSetup> setup =  TActorBenchmark::GetActorSystemSetup();
        TActorBenchmark::AddBasicPool(setup, 1, 1, false);

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        THPTimer Timer;

        ui64 eventsPerPair = 1000;
        //ui64 eventsPerPair = 100000000;

        Timer.Reset();
        ui32 followerPoolId = 0;
        ui32 leaderPoolId = 0;
        TActorId followerId = actorSystem.Register(
            new TActorBenchmark::TSendReceiveActor(
                TSendReceiveActorParams{.OtherEvents=eventsPerPair / 2, .Allocation=true}
            ),
            TMailboxType::HTSwap,
            followerPoolId
        );
        THolder<IActor> leader{
            new TTestEndDecorator(THolder(new TActorBenchmark::TSendReceiveActor(
                TSendReceiveActorParams{.OwnEvents=eventsPerPair / 2, .Receivers={followerId}, .Allocation=true}
            )),
            &pad,
            &actorsAlive)
        };
        actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);

        pad.Park();
        auto elapsedTime = Timer.Passed() /  (TSettings::TotalEventsAmountPerThread * 4);
        actorSystem.Stop();

        TExecutorThreadStats aggregated;
        TVector<TExecutorThreadStats> stats;
        TVector<TExecutorThreadStats> sharedStats;
        TExecutorPoolStats poolStats;
        actorSystem.GetPoolStats(0, poolStats, stats, sharedStats);
        // Sum all per-thread counters into the 0th element
        for (auto &stat : stats) {
            aggregated.Aggregate(stat);
        }
        for (auto &stat : sharedStats) {
            aggregated.Aggregate(stat);
        }

        Cerr << "Completed " << 1e9 * elapsedTime << Endl;
        Cerr << "Elapsed " << Ts2Us(aggregated.ElapsedTicks) << "us" << Endl;
    }

    Y_UNIT_TEST(WithOnlyOneSharedAndOneCommonExecutors) {
        return;
        THolder<TActorSystemSetup> setup =  TActorBenchmark::GetActorSystemSetup();
        TActorBenchmark::AddBasicPool(setup, 2, true, true);

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        THPTimer Timer;

        ui64 eventsPerPair = 1000;

        Timer.Reset();
        ui32 followerPoolId = 0;
        ui32 leaderPoolId = 0;
        for (ui32 idx = 0; idx < 50; ++idx) {
            TActorId followerId = actorSystem.Register(
                new TActorBenchmark::TSendReceiveActor(
                    TSendReceiveActorParams{.OtherEvents=eventsPerPair / 2, .Allocation=true}
                ),
                TMailboxType::HTSwap,
                followerPoolId
            );
            THolder<IActor> leader{
                new TTestEndDecorator(THolder(new TActorBenchmark::TSendReceiveActor(
                    TSendReceiveActorParams{.OwnEvents=eventsPerPair / 2, .Receivers={followerId}, .Allocation=true}
                )),
                &pad,
                &actorsAlive)
            };
            actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);
        }

        pad.Park();
        auto elapsedTime = Timer.Passed() /  (TSettings::TotalEventsAmountPerThread * 4);
        actorSystem.Stop();

        Cerr << "Completed " << 1e9 * elapsedTime << Endl;
    }

    Y_UNIT_TEST(WithSharedExecutors) {
        return;
        THolder<TActorSystemSetup> setup =  TActorBenchmark::GetActorSystemSetup();
         TActorBenchmark::AddBasicPool(setup, 2, 1, false);
         TActorBenchmark::AddBasicPool(setup, 2, 1, true);

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        THPTimer Timer;

        ui64 eventsPerPair = 100000 * 4 / 60;
        //ui64 eventsPerPair = TSettings::TotalEventsAmountPerThread * 4 / 60;

        Timer.Reset();
        for (ui32 i = 0; i < 50; ++i) {
            ui32 followerPoolId = 0;
            ui32 leaderPoolId = 0;
            TActorId followerId = actorSystem.Register(
                new TActorBenchmark::TSendReceiveActor(
                    TSendReceiveActorParams{.OtherEvents=eventsPerPair / 2, .Allocation=true}
                ),
                TMailboxType::HTSwap,
                followerPoolId
            );
            THolder<IActor> leader{
                new TTestEndDecorator(
                    THolder(new TActorBenchmark::TSendReceiveActor(
                        TSendReceiveActorParams{.OwnEvents=eventsPerPair / 2, .Receivers={followerId}, .Allocation=true}
                    )),
                    &pad,
                    &actorsAlive
                )
            };
            actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);
        }
        for (ui32 i = 0; i < 10; ++i) {
            ui32 followerPoolId = 1;
            ui32 leaderPoolId = 1;
            TActorId followerId = actorSystem.Register(
                new TActorBenchmark::TSendReceiveActor(
                    TSendReceiveActorParams{.OtherEvents=eventsPerPair / 2, .Allocation=true}
                ),
                TMailboxType::HTSwap,
                followerPoolId
            );
            THolder<IActor> leader{
                new TTestEndDecorator(
                    THolder(new TActorBenchmark::TSendReceiveActor(
                        TSendReceiveActorParams{.OwnEvents=eventsPerPair / 2, .Receivers={followerId}, .Allocation=true}
                    )),
                    &pad,
                    &actorsAlive
                )
            };
            actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);
        }

        pad.Park();
        auto elapsedTime = Timer.Passed() /  (4 * TSettings::TotalEventsAmountPerThread);
        actorSystem.Stop();

        TExecutorThreadStats aggregated;
        TVector<TExecutorThreadStats> stats;
        TVector<TExecutorThreadStats> sharedStats;
        TExecutorPoolStats poolStats;
        actorSystem.GetPoolStats(0, poolStats, stats, sharedStats);
        // Sum all per-thread counters into the 0th element
        for (auto &stat : stats) {
            aggregated.Aggregate(stat);
        }
        for (auto &stat : sharedStats) {
            aggregated.Aggregate(stat);
        }

        Cerr << "Completed " << 1e9 * elapsedTime << Endl;
        Cerr << "Elapsed " << Ts2Us(aggregated.ElapsedTicks) << "us" << Endl;
    }

    Y_UNIT_TEST(WithoutSharedExecutors) {
        THolder<TActorSystemSetup> setup =  TActorBenchmark::GetActorSystemSetup();
        TActorBenchmark::AddBasicPool(setup, 2, 1, 0);
        TActorBenchmark::AddBasicPool(setup, 2, 1, 0);

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        THPTimer Timer;

        ui64 eventsPerPair = 100000 * 4 / 60;
        //ui64 eventsPerPair = TSettings::TotalEventsAmountPerThread * 4 / 60;

        Timer.Reset();
        for (ui32 i = 0; i < 50; ++i) {
            ui32 followerPoolId = 0;
            ui32 leaderPoolId = 0;
            TActorId followerId = actorSystem.Register(
                new TActorBenchmark::TSendReceiveActor(
                    TSendReceiveActorParams{.OtherEvents=eventsPerPair / 2, .Allocation=true}
                ),
                TMailboxType::HTSwap,
                followerPoolId
            );
            THolder<IActor> leader{
                new TTestEndDecorator(
                    THolder(new TActorBenchmark::TSendReceiveActor(
                        TSendReceiveActorParams{.OwnEvents=eventsPerPair / 2, .Receivers={followerId}, .Allocation=true}
                    )),
                    &pad,
                    &actorsAlive
                )
            };
            actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);
        }
        for (ui32 i = 0; i < 10; ++i) {
            ui32 followerPoolId = 1;
            ui32 leaderPoolId = 1;
            TActorId followerId = actorSystem.Register(
                new TActorBenchmark::TSendReceiveActor(
                    TSendReceiveActorParams{.OtherEvents=eventsPerPair / 2, .Allocation=true}
                ),
                TMailboxType::HTSwap,
                followerPoolId
            );
            THolder<IActor> leader{
                new TTestEndDecorator(
                    THolder(new TActorBenchmark::TSendReceiveActor(
                        TSendReceiveActorParams{.OwnEvents=eventsPerPair / 2, .Receivers={followerId}, .Allocation=true}
                    )),
                    &pad,
                    &actorsAlive
                )
            };
            actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);
        }

        pad.Park();
        auto elapsedTime = Timer.Passed() /  (4 * TSettings::TotalEventsAmountPerThread);
        actorSystem.Stop();

        TExecutorThreadStats aggregated;
        TVector<TExecutorThreadStats> stats;
        TVector<TExecutorThreadStats> sharedStats;
        TExecutorPoolStats poolStats;
        actorSystem.GetPoolStats(0, poolStats, stats, sharedStats);
        // Sum all per-thread counters into the 0th element
        for (auto &stat : stats) {
            aggregated.Aggregate(stat);
        }
        for (auto &stat : sharedStats) {
            aggregated.Aggregate(stat);
        }

        Cerr << "Completed " << 1e9 * elapsedTime << Endl;
        Cerr << "Elapsed " << Ts2Us(aggregated.ElapsedTicks) << "us" << Endl;
    }

    Y_UNIT_TEST(SendReceive1Pool1ThreadAlloc) {
        for (const auto& mType : TSettings::MailboxTypes) {
            auto stats =  TActorBenchmark::CountStats([mType] {
                return  TActorBenchmark::BenchSendReceive(true, mType, TActorBenchmark::EPoolType::Basic, ESendingType::Common);
            });
            Cerr << stats.ToString() << " " << mType << Endl;
            stats =  TActorBenchmark::CountStats([mType] {
                return  TActorBenchmark::BenchSendReceive(true, mType, TActorBenchmark::EPoolType::Basic, ESendingType::Lazy);
            });
            Cerr << stats.ToString() << " " << mType << " Lazy" << Endl;
            stats =  TActorBenchmark::CountStats([mType] {
                return  TActorBenchmark::BenchSendReceive(true, mType, TActorBenchmark::EPoolType::Basic, ESendingType::Tail);
            });
            Cerr << stats.ToString() << " " << mType << " Tail" << Endl;
        }
    }

    Y_UNIT_TEST(SendReceive1Pool1ThreadNoAlloc) {
        for (const auto& mType : TSettings::MailboxTypes) {
            auto stats =  TActorBenchmark::CountStats([mType] {
                return  TActorBenchmark::BenchSendReceive(false, mType, TActorBenchmark::EPoolType::Basic, ESendingType::Common);
            });
            Cerr << stats.ToString() << " " << mType << Endl;
            stats =  TActorBenchmark::CountStats([mType] {
                return  TActorBenchmark::BenchSendReceive(false, mType, TActorBenchmark::EPoolType::Basic, ESendingType::Lazy);
            });
            Cerr << stats.ToString() << " " << mType << " Lazy" << Endl;
            stats =  TActorBenchmark::CountStats([mType] {
                return  TActorBenchmark::BenchSendReceive(false, mType, TActorBenchmark::EPoolType::Basic, ESendingType::Tail);
            });
            Cerr << stats.ToString() << " " << mType << " Tail" << Endl;
        }
    }

    Y_UNIT_TEST(SendActivateReceive1Pool1ThreadAlloc) {
         TActorBenchmark::RunBenchSendActivateReceive(1, 1, true, TActorBenchmark::EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive1Pool1ThreadNoAlloc) {
         TActorBenchmark::RunBenchSendActivateReceive(1, 1, false, TActorBenchmark::EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive1Pool2ThreadsAlloc) {
         TActorBenchmark::RunBenchSendActivateReceive(1, 2, true, TActorBenchmark::EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive1Pool2ThreadsNoAlloc) {
         TActorBenchmark::RunBenchSendActivateReceive(1, 2, false, TActorBenchmark::EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive2Pool1ThreadAlloc) {
         TActorBenchmark::RunBenchSendActivateReceive(2, 1, true, TActorBenchmark::EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive2Pool1ThreadNoAlloc) {
         TActorBenchmark::RunBenchSendActivateReceive(2, 1, false, TActorBenchmark::EPoolType::Basic);
    }

    Y_UNIT_TEST(SendActivateReceive1Pool1Threads)       {  TActorBenchmark::RunBenchContentedThreads(1, TActorBenchmark::EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool2Threads)       {  TActorBenchmark::RunBenchContentedThreads(2, TActorBenchmark::EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool3Threads)       {  TActorBenchmark::RunBenchContentedThreads(3, TActorBenchmark::EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool4Threads)       {  TActorBenchmark::RunBenchContentedThreads(4, TActorBenchmark::EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool5Threads)       {  TActorBenchmark::RunBenchContentedThreads(5, TActorBenchmark::EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool6Threads)       {  TActorBenchmark::RunBenchContentedThreads(6, TActorBenchmark::EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool7Threads)       {  TActorBenchmark::RunBenchContentedThreads(7, TActorBenchmark::EPoolType::Basic);  }
    Y_UNIT_TEST(SendActivateReceive1Pool8Threads)       {  TActorBenchmark::RunBenchContentedThreads(8, TActorBenchmark::EPoolType::Basic);  }

    Y_UNIT_TEST(SendActivateReceiveCSV) {
        std::vector<ui32> threadsList;
        for (ui32 threads = 1; threads <= 32; threads *= 2) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList;
        for (ui32 actorPairs = 1; actorPairs <= 2 * 32; actorPairs *= 2) {
            actorPairsList.push_back(actorPairs);
        }
        TActorBenchmark::RunSendActivateReceiveCSV(threadsList, actorPairsList, {1}, TDuration::MilliSeconds(100));
    }

    Y_UNIT_TEST(SendActivateReceiveWithMailboxNeighbours) {
        TVector<ui32> NeighbourActors = {0, 1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64, 128, 256};
        for (const auto& neighbour : NeighbourActors) {
            auto stats =  TActorBenchmark::CountStats([neighbour] {
                return  TActorBenchmark::BenchSendActivateReceiveWithMailboxNeighbours(neighbour, TActorBenchmark::EPoolType::Basic, ESendingType::Common);
            });
            Cerr << stats.ToString() << " neighbourActors: " << neighbour << Endl;
            stats =  TActorBenchmark::CountStats([neighbour] {
                return  TActorBenchmark::BenchSendActivateReceiveWithMailboxNeighbours(neighbour, TActorBenchmark::EPoolType::Basic, ESendingType::Lazy);
            });
            Cerr << stats.ToString() << " neighbourActors: " << neighbour << " Lazy" << Endl;
            stats =  TActorBenchmark::CountStats([neighbour] {
                return  TActorBenchmark::BenchSendActivateReceiveWithMailboxNeighbours(neighbour, TActorBenchmark::EPoolType::Basic, ESendingType::Tail);
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

        UNIT_ASSERT((TEnumProcessKey<TActorActivityTag, IActor::EActorActivity>::GetName(IActor::EActivityType::INTERCONNECT_PROXY_TCP) == "INTERCONNECT_PROXY_TCP"));
        UNIT_ASSERT((TLocalProcessKey<TActorActivityTag, ActorName>::GetName() == ActorName));
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

        UNIT_ASSERT_VALUES_EQUAL(420, 42);
    }
}
