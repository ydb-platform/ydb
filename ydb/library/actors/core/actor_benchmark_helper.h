#include "actor.h"
#include "events.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_basic.h"
#include "actor_bootstrapped.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/util/threadparkpad.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/chunk_queue/queue.h>

#include <util/generic/algorithm.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/rwlock.h>
#include <util/system/hp_timer.h>
#include <vector>

namespace NActors::NTests {

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
        auto alive = AtomicDecrement(*ActorsAlive);
        if (alive == 0) {
            Pad->Unpark();
        }
    }
};


struct TActorBenchmarkSettings {
    static constexpr bool DefaultNoRealtime = true;
    static constexpr ui32 DefaultSpinThreshold = 1'000'000;
    static constexpr ui32 TotalEventsAmountPerThread = 1'000;

    static constexpr auto MailboxTypes = {
        TMailboxType::Simple,
        TMailboxType::Revolving,
        TMailboxType::HTSwap,
        TMailboxType::ReadAsFilled,
        TMailboxType::TinyReadAsFilled
    };
};


template <typename TSettings_ = TActorBenchmarkSettings>
struct TActorBenchmark {
    using TSettings = TSettings_;

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

    struct TEvOwnedPing : TEvents::TEvPing {
        TEvOwnedPing(TActorId owner)
            : TEvPing()
            , Owner(owner)
        {}

        TActorId Owner;
    };

    struct TEventSharedCounters {
        TEventSharedCounters(ui32 count)
            : NotStarted(count)
            , Finished(0)
            , Counters(count)
            , StartedCounters(count)
            , EndedCounters(count)
        {
            for (ui32 idx = 0; idx < count; ++idx) {
                Counters[idx].store(0);
                StartedCounters[idx].store(0);
                EndedCounters[idx].store(0);
            }
        }

        std::atomic<ui64> NotStarted = 0;
        std::atomic<ui64> Finished = 0;
        std::vector<NThreading::TPadded<std::atomic<ui64>>> Counters;
        std::vector<NThreading::TPadded<std::atomic<ui64>>> StartedCounters;
        std::vector<NThreading::TPadded<std::atomic<ui64>>> EndedCounters;
        std::atomic<ui64> StartTimeTs = 0;
        std::atomic<ui64> EndTimeTs = 0;
        std::atomic<bool> DoStop = false;
    };

    struct TSendReceiveActorParams {
        ui64 OwnEvents = 0;
        ui64 OtherEvents = 0;
        bool EndlessSending = false;
        double *ElapsedTime = nullptr;
        std::vector<TActorId> Receivers;
        bool Allocation = false;
        ESendingType SendingType = ESendingType::Common;
        ui32 Neighbours = 0;
        TEventSharedCounters *SharedCounters;
        ui32 InFlight = 1;
        bool ToSchedule = false;
        TDuration DelayForScheduling = TDuration::MicroSeconds(1);
    };

    class TSendReceiveActor : public TActorBootstrapped<TSendReceiveActor> {
    public:
        static constexpr auto ActorActivityType() {
            return IActorCallback::EActivityType::ACTORLIB_COMMON;
        }

        TSendReceiveActor(const TSendReceiveActorParams &params, ui32 idx=0)
            : OwnEventsCounter(params.OwnEvents)
            , OtherEventsCounter(params.OtherEvents)
            , ElapsedTime(params.ElapsedTime)
            , Receivers(params.Receivers)
            , AllocatesMemory(params.Allocation)
            , SendingType(params.SendingType)
            , MailboxNeighboursCount(params.Neighbours)
            , SharedCounters(params.SharedCounters)
            , PairIdx(idx)
            , EndlessSending(params.EndlessSending)
            , IsLeader(OwnEventsCounter)
            , InFlight(params.InFlight)
            , ToSchedule(params.ToSchedule)
            , DelayForScheduling(params.DelayForScheduling)
        {}

        ~TSendReceiveActor() {
        }

        void StoreCounters(std::vector<NThreading::TPadded<std::atomic<ui64>>> &dest) {
            for (ui32 idx = 0; idx < dest.size(); ++idx) {
                dest[idx].store(SharedCounters->Counters[idx]);
            }
        }

        void Bootstrap(const TActorContext &ctx) {
            if (SharedCounters && IsLeader) {
                ui32 count = --SharedCounters->NotStarted;
                if (!count) {
                    SharedCounters->StartTimeTs = GetCycleCountFast();
                    StoreCounters(SharedCounters->StartedCounters);
                }
            }
            if (Receivers.empty() && OwnEventsCounter) {
                Receivers.push_back(this->SelfId());
            }
            Timer.Reset();
            this->Become(&TSendReceiveActor::StateFunc);
            for (ui32 i = 0; i < MailboxNeighboursCount; ++i) {
                ctx.RegisterWithSameMailbox(new TDummyActor());
            }
            for (TActorId receiver : Receivers) {
                for (ui32 eventIdx = 0; eventIdx < InFlight; ++eventIdx) {
                    TAutoPtr<IEventHandle> ev = new IEventHandle(receiver, this->SelfId(), new TEvOwnedPing(this->SelfId()));
                    SpecialSend(ev, ctx, true);
                }
            }
        }

        void SpecialSend(TAutoPtr<IEventHandle> ev, const TActorContext &ctx, bool own) {
            EventsCounter++;
            if (own) {
                --OwnEventsCounter;
            }
            if (ToSchedule) {
                TActivationContext::Schedule(DelayForScheduling, ev.Release());
            } else if (SendingType == ESendingType::Lazy) {
                ctx.Send<ESendingType::Lazy>(ev);
            } else if (SendingType == ESendingType::Tail) {
                ctx.Send<ESendingType::Tail>(ev);
            } else {
                ctx.Send(ev);
            }
        }

        void Stop() {
            if (SharedCounters && IsLeader) {
                if (!SharedCounters->NotStarted++) {
                    StoreCounters(SharedCounters->EndedCounters);
                    SharedCounters->EndTimeTs = GetCycleCountFast();
                }
            }
            if (ElapsedTime != nullptr) {
                if (Receivers.size() && Receivers[0] != this->SelfId()) {
                    *ElapsedTime = Timer.Passed() / EventsCounter;
                } else {
                    *ElapsedTime = Timer.Passed() * 2 / EventsCounter;
                }
            }
            this->PassAway();
        }

        bool CheckWorkIsDone() {
            if (OwnEventsCounter || OtherEventsCounter || EndlessSending) {
                return false;
            }
            Stop();
            return true;
        }

        STFUNC(StateFunc) {
            ++EventsCounter;
            ui32 counter = ++ReceiveTurn;
            if (SharedCounters) {
                if (counter % 128 == 0) {
                    if (IsLeader) {
                        SharedCounters->Counters[PairIdx].store(EventsCounter);
                    }
                    if (SharedCounters->DoStop) {
                        Stop();
                        return;
                    }
                }
            }
            bool own = ev->Get<TEvOwnedPing>()->Owner == this->SelfId();
            if (!own) {
                --OtherEventsCounter;
            }
            if (CheckWorkIsDone())
                return;

            auto ctx(this->ActorContext());
            if (AllocatesMemory) {
                SpecialSend(new IEventHandle(ev->Sender, this->SelfId(), new TEvOwnedPing(ev->Get<TEvOwnedPing>()->Owner)), ctx, own);
            } else {
                std::swap(*const_cast<TActorId*>(&ev->Sender), *const_cast<TActorId*>(&ev->Recipient));
                ev->DropRewrite();
                SpecialSend(ev, ctx, own);
            }

            CheckWorkIsDone();
        }

    private:
        THPTimer Timer;
        ui64 OwnEventsCounter;
        ui64 OtherEventsCounter;
        double* ElapsedTime;
        std::vector<TActorId> Receivers;
        bool AllocatesMemory;
        ESendingType SendingType;
        ui32 MailboxNeighboursCount;
        ui32 EventsCounter = 0;
        TEventSharedCounters *SharedCounters;
        ui32 PairIdx = 0;
        bool EndlessSending = false;
        bool IsLeader = false;
        ui32 InFlight = 1;
        ui32 ReceiveTurn = 0;
        bool ToSchedule = false;
        TDuration DelayForScheduling = TDuration::MicroSeconds(1);
    };

    static void AddBasicPool(THolder<TActorSystemSetup>& setup, ui32 threads, bool activateEveryEvent, bool hasSharedThread) {
        TBasicExecutorPoolConfig basic;
        basic.PoolId = setup->GetExecutorsCount();
        basic.PoolName = TStringBuilder() << "b" << basic.PoolId;
        basic.Threads = threads;
        basic.SpinThreshold = TSettings::DefaultSpinThreshold;
        basic.TimePerMailbox = TDuration::Hours(1);
        basic.HasSharedThread = hasSharedThread;
        basic.SoftProcessingDurationTs = Us2Ts(100);
        if (activateEveryEvent) {
            basic.EventsPerMailbox = 1;
        }
        setup->CpuManager.Basic.emplace_back(std::move(basic));
    }

    static THolder<TActorSystemSetup> GetActorSystemSetup() {
        auto setup = MakeHolder<NActors::TActorSystemSetup>();
        setup->NodeId = 1;
        setup->Scheduler = new TBasicSchedulerThread(NActors::TSchedulerConfig(512, 0));
        return setup;
    }

    enum class EPoolType {
        Basic,
    };

    static THolder<TActorSystemSetup> InitActorSystemSetup(EPoolType poolType, ui32 poolsCount, ui32 threads, bool activateEveryEvent) {
        if (poolType == EPoolType::Basic) {
            THolder<TActorSystemSetup> setup = GetActorSystemSetup();
            for (ui32 i = 0; i < poolsCount; ++i) {
                AddBasicPool(setup, threads, activateEveryEvent, 0);
            }
            return setup;
        }
        Y_ABORT();
    }

    static double BenchSendReceive(bool allocation, NActors::TMailboxType::EType mType, EPoolType poolType, ESendingType sendingType) {
        THolder<TActorSystemSetup> setup = InitActorSystemSetup(poolType, 1, 1, false);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        double elapsedTime = 0;
        THolder<IActor> endActor{new TTestEndDecorator(
            THolder(new TSendReceiveActor(
                TSendReceiveActorParams{
                    .OwnEvents=TSettings::TotalEventsAmountPerThread,
                    .OtherEvents=0,
                    .ElapsedTime=&elapsedTime,
                    .Allocation=allocation,
                    .SendingType=sendingType,
                }
            )),
            &pad,
            &actorsAlive
        )};

        actorSystem.Register(endActor.Release(), mType);

        pad.Park();
        actorSystem.Stop();

        return 1e9 * elapsedTime;
    }

    static double BenchSendActivateReceive(ui32 poolsCount, ui32 threads, bool allocation, EPoolType poolType, ESendingType sendingType) {
        THolder<TActorSystemSetup> setup = InitActorSystemSetup(poolType, poolsCount, threads, true);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        double elapsedTime = 0;
        ui32 followerPoolId = 0;

        ui32 leaderPoolId = poolsCount == 1 ? 0 : 1;
        ui64 eventsPerPair = TSettings::TotalEventsAmountPerThread;

        TActorId followerId = actorSystem.Register(
            new TActorBenchmark::TSendReceiveActor(
                TSendReceiveActorParams{.OtherEvents=eventsPerPair / 2, .Allocation=allocation}
            ),
            TMailboxType::HTSwap,
            followerPoolId
        );
        THolder<IActor> leader{
            new TTestEndDecorator(
                THolder(new TActorBenchmark::TSendReceiveActor(
                    TSendReceiveActorParams{
                        .OwnEvents=eventsPerPair / 2,
                        .ElapsedTime=&elapsedTime,
                        .Receivers={followerId},
                        .Allocation=allocation,
                        .SendingType=sendingType,
                    }
                )),
                &pad,
                &actorsAlive
            )
        };
        actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);

        pad.Park();
        actorSystem.Stop();

        return 1e9 * elapsedTime;
    }

   static double BenchSendActivateReceiveWithMailboxNeighbours(ui32 MailboxNeighbourActors, EPoolType poolType, ESendingType sendingType) {
        THolder<TActorSystemSetup> setup = InitActorSystemSetup(poolType, 1, 1, false);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        double elapsedTime = 0;

        ui64 eventsPerPair = TSettings::TotalEventsAmountPerThread;

        TActorId followerId = actorSystem.Register(
            new TActorBenchmark::TSendReceiveActor(
                TSendReceiveActorParams{
                    .OtherEvents=eventsPerPair / 2,
                    .Allocation=false,
                    .Neighbours=MailboxNeighbourActors,
                }
            ),
            TMailboxType::HTSwap
        );
        THolder<IActor> leader{
            new TTestEndDecorator(
                THolder(new TActorBenchmark::TSendReceiveActor(
                    TSendReceiveActorParams{
                        .OwnEvents=eventsPerPair / 2,
                        .ElapsedTime=&elapsedTime,
                        .Receivers={followerId},
                        .Allocation=false,
                        .SendingType=sendingType,
                        .Neighbours=MailboxNeighbourActors,
                    }
                )),
                &pad,
                &actorsAlive
            )
        };
        actorSystem.Register(leader.Release(), TMailboxType::HTSwap);

        pad.Park();
        actorSystem.Stop();

        return 1e9 * elapsedTime;
    }

    struct TBenchResult {
        double ElapsedTime;
        ui64 SentEvents;
        ui64 MinPairSentEvents;
        ui64 MaxPairSentEvents;
    };

    static auto BenchContentedThreads(ui32 threads, ui32 actorsPairsCount, EPoolType poolType, ESendingType sendingType, TDuration testDuration = TDuration::Zero(), ui32 inFlight = 1) {
        THolder<TActorSystemSetup> setup = InitActorSystemSetup(poolType, 1, threads, false);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;

        TEventSharedCounters sharedCounters(actorsPairsCount);

        ui64 totalEvents = TSettings::TotalEventsAmountPerThread * threads;
        ui64 eventsPerPair = totalEvents / actorsPairsCount;

        for (ui32 i = 0; i < actorsPairsCount; ++i) {
            ui32 followerPoolId = 0;
            ui32 leaderPoolId = 0;
            TActorId followerId = actorSystem.Register(
                new TSendReceiveActor(
                    TSendReceiveActorParams{
                        .OtherEvents = eventsPerPair / 2,
                        .EndlessSending = bool(testDuration),
                        .Allocation = false,
                        .SharedCounters = &sharedCounters,
                    }
                ),
                TMailboxType::HTSwap,
                followerPoolId
            );
            THolder<IActor> leader{
                new TTestEndDecorator(
                    THolder(new TSendReceiveActor(TSendReceiveActorParams{
                        .OwnEvents = eventsPerPair / 2,
                        .EndlessSending = bool(testDuration),
                        .Receivers={followerId},
                        .Allocation = false,
                        .SendingType=sendingType,
                        .SharedCounters=&sharedCounters,
                        .InFlight = inFlight
                    }, i)),
                    &pad,
                    &actorsAlive
                )
            };
            actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);
        }

        if (testDuration) {
            Sleep(testDuration);
            for (ui32 idx = 0; idx < actorsPairsCount; ++idx) {
                sharedCounters.EndedCounters[idx].store(sharedCounters.Counters[idx]);
            }
            sharedCounters.EndTimeTs = GetCycleCountFast();
        } else {
            pad.Park();
        }
        actorSystem.Stop();

        ui64 sentEvents = sharedCounters.EndedCounters[0] - sharedCounters.StartedCounters[0];
        ui64 minSentEvents = sentEvents;
        ui64 maxSentEvents = sentEvents;
        for (ui32 pairIdx = 1; pairIdx < actorsPairsCount; ++pairIdx) {
            ui64 count = sharedCounters.EndedCounters[pairIdx] - sharedCounters.StartedCounters[pairIdx];
            sentEvents += count;
            minSentEvents = ::Min(minSentEvents, count);
            maxSentEvents = ::Max(maxSentEvents, count);
        }

        return TBenchResult {
            .ElapsedTime = 1000 * Ts2Us(sharedCounters.EndTimeTs - sharedCounters.StartTimeTs),
            .SentEvents = sentEvents,
            .MinPairSentEvents = minSentEvents,
            .MaxPairSentEvents = maxSentEvents
        };
    }

    static auto BenchStarContentedThreads(ui32 threads, ui32 actorsPairsCount, EPoolType poolType, ESendingType sendingType, TDuration testDuration = TDuration::Zero(), ui32 starMultiply=10) {
        THolder<TActorSystemSetup> setup = InitActorSystemSetup(poolType, 1, threads, true);
        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;

        TEventSharedCounters sharedCounters(actorsPairsCount);

        ui64 totalEvents = TSettings::TotalEventsAmountPerThread * threads;
        ui64 eventsPerPair = totalEvents / actorsPairsCount;

        for (ui32 i = 0; i < actorsPairsCount; ++i) {
            ui32 followerPoolId = 0;
            ui32 leaderPoolId = 0;
            std::vector<TActorId> receivers;
            for (ui32 idx = 0; idx < starMultiply; ++idx) {
                TActorId followerId = actorSystem.Register(
                    new TSendReceiveActor(
                        TSendReceiveActorParams{
                            .OtherEvents = eventsPerPair / 2 / starMultiply,
                            .EndlessSending = bool(testDuration),
                            .Allocation = false,
                            .SharedCounters = &sharedCounters,
                        }
                    ),
                    TMailboxType::HTSwap,
                    followerPoolId
                );
                receivers.push_back(followerId);
            }
            THolder<IActor> leader{
                new TTestEndDecorator(
                    THolder(new TSendReceiveActor(TSendReceiveActorParams{
                        .OwnEvents = eventsPerPair / 2,
                        .EndlessSending = bool(testDuration),
                        .Receivers=receivers,
                        .Allocation = false,
                        .SendingType=sendingType,
                        .SharedCounters=&sharedCounters,
                    }, i)),
                    &pad,
                    &actorsAlive
                )
            };
            actorSystem.Register(leader.Release(), TMailboxType::HTSwap, leaderPoolId);
        }

        if (testDuration) {
            Sleep(testDuration);
            for (ui32 idx = 0; idx < actorsPairsCount; ++idx) {
                sharedCounters.EndedCounters[idx].store(sharedCounters.Counters[idx]);
            }
            sharedCounters.EndTimeTs = GetCycleCountFast();
        } else {
            pad.Park();
        }
        actorSystem.Stop();

        ui64 sentEvents = sharedCounters.EndedCounters[0] - sharedCounters.StartedCounters[0];
        ui64 minSentEvents = sentEvents;
        ui64 maxSentEvents = sentEvents;
        for (ui32 pairIdx = 1; pairIdx < actorsPairsCount; ++pairIdx) {
            ui64 count = sharedCounters.EndedCounters[pairIdx] - sharedCounters.StartedCounters[pairIdx];
            sentEvents += count;
            minSentEvents = ::Min(minSentEvents, count);
            maxSentEvents = ::Max(maxSentEvents, count);
        }

        return TBenchResult {
            .ElapsedTime = 1000 * Ts2Us(sharedCounters.EndTimeTs - sharedCounters.StartTimeTs),
            .SentEvents = sentEvents,
            .MinPairSentEvents = minSentEvents,
            .MaxPairSentEvents = maxSentEvents
        };
    }


    static auto Mean(const std::vector<double>& data) {
        return Accumulate(data.begin(), data.end(), 0.0) / data.size();
    }

    static auto Deviation(const std::vector<double>& data) {
        auto mean = Mean(data);
        double deviation = 0.0;
        for (const auto& x : data) {
            deviation += (x - mean) * (x - mean);
        }
        return std::sqrt(deviation / data.size());
    }

    static double Min(const std::vector<double>& data) {
        return *std::min_element(data.begin(), data.end());
    }

    static double Max(const std::vector<double>& data) {
        return *std::max_element(data.begin(), data.end());
    }

    template <auto Measurment>
    struct TStats {
        double Mean;
        double Deviation;
        double Min;
        double Max;

        TStats(const std::vector<double> &data)
            : Mean(TActorBenchmark::Mean(data))
            , Deviation(TActorBenchmark::Deviation(data))
            , Min(TActorBenchmark::Min(data))
            , Max(TActorBenchmark::Max(data))
        {
        }

        TString ToString() {
            return TStringBuilder() << Mean << " ± " << Deviation << " " << Measurment()
                << " " << std::ceil(Deviation / Mean * 1000) / 10.0 << "%"
                << " min " << Min << " " << Measurment()  << " max " << Max << " " << Measurment();
        }
    };

    static constexpr auto EmptyMsr = []{return "";};
    static constexpr auto NsMsr = []{return "ns";};

    struct TStatsBenchResult {
        TStats<NsMsr> ElapsedTime;
        TStats<EmptyMsr> SentEvents;
        TStats<EmptyMsr> MinPairSentEvents;
        TStats<EmptyMsr> MaxPairSentEvents;

        TString ToString() {
            return TStringBuilder() << ElapsedTime.ToString() << Endl << SentEvents.ToString() << Endl << MinPairSentEvents.ToString() << Endl << MaxPairSentEvents.ToString();
        }
    };

    template <typename Func>
    static auto CountStats(Func func, ui32 itersCount = 5) {
        if constexpr (std::is_same_v<double, std::decay_t<decltype(func())>>) {
            std::vector<double> elapsedTimes;
            for (ui32 i = 0; i < itersCount; ++i) {
                auto elapsedTime = func();
                elapsedTimes.push_back(elapsedTime);
            }
            return TStats<NsMsr>(elapsedTimes);
        } else {
            std::vector<double> elapsedTimes;
            std::vector<double> sentEvents;
            std::vector<double> minPairSentEvents;
            std::vector<double> maxPairSentEvents;
            for (ui32 i = 0; i < itersCount; ++i) {
                TBenchResult result = func();
                elapsedTimes.push_back(result.ElapsedTime);
                sentEvents.push_back(result.SentEvents);
                minPairSentEvents.push_back(result.MinPairSentEvents);
                maxPairSentEvents.push_back(result.MaxPairSentEvents);
            }
            return TStatsBenchResult {
                .ElapsedTime = TStats<NsMsr>(elapsedTimes),
                .SentEvents = TStats<EmptyMsr>(sentEvents),
                .MinPairSentEvents = TStats<EmptyMsr>(minPairSentEvents),
                .MaxPairSentEvents = TStats<EmptyMsr>(maxPairSentEvents),
            };
        }
    }

    static void RunBenchSendActivateReceive(ui32 poolsCount, ui32 threads, bool allocation, EPoolType poolType) {
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

    static void RunBenchContentedThreads(ui32 threads, EPoolType poolType) {
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

    static void RunSendActivateReceiveCSV(const std::vector<ui32> &threadsList, const std::vector<ui32> &actorPairsList, const std::vector<ui32> &inFlights, TDuration subtestDuration) {
        Cout << "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs" << Endl;
        for (ui32 threads : threadsList) {
            for (ui32 actorPairs : actorPairsList) {
                for (ui32 inFlight : inFlights) {
                    auto stats = CountStats([threads, actorPairs, inFlight, subtestDuration] {
                        return BenchContentedThreads(threads, actorPairs, EPoolType::Basic, ESendingType::Common, subtestDuration, inFlight);
                    }, 3);
                    double elapsedSeconds = stats.ElapsedTime.Mean / 1e9;
                    ui64 eventsPerSecond = stats.SentEvents.Mean / elapsedSeconds;
                    Cout << threads << "," << actorPairs << "," << inFlight << "," << eventsPerSecond << "," << elapsedSeconds << "," << stats.MinPairSentEvents.Min << "," << stats.MaxPairSentEvents.Max << Endl;
                }
            }
        }
    }


    static void RunStarSendActivateReceiveCSV(const std::vector<ui32> &threadsList, const std::vector<ui32> &actorPairsList, const std::vector<ui32> &starsList) {
        Cout << "threads,actorPairs,star_multiply,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs" << Endl;
        for (ui32 threads : threadsList) {
            for (ui32 actorPairs : actorPairsList) {
                for (ui32 stars : starsList) {
                    auto stats = CountStats([threads, actorPairs, stars] {
                        return BenchStarContentedThreads(threads, actorPairs, EPoolType::Basic, ESendingType::Common, TDuration::Seconds(1), stars);
                    }, 3);
                    double elapsedSeconds = stats.ElapsedTime.Mean / 1e9;
                    ui64 eventsPerSecond = stats.SentEvents.Mean / elapsedSeconds;
                    Cout << threads << "," << actorPairs << "," << stars << "," << eventsPerSecond << "," << elapsedSeconds << "," << stats.MinPairSentEvents.Min << "," << stats.MaxPairSentEvents.Max << Endl;
                }
            }
        }
    }
};

} // NActors::NTests
