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

Y_UNIT_TEST_SUITE(ActorBasic) {

    using TActorBenchmark = ::NActors::NTests::TActorBenchmark<>;
    using TSettings = TActorBenchmark::TSettings;
    using TSendReceiveActorParams = TActorBenchmark::TSendReceiveActorParams;

    Y_UNIT_TEST(ActorSendReceive) {
        THolder<TActorSystemSetup> setup =  TActorBenchmark::GetActorSystemSetup();
        TActorBenchmark::AddBasicPool(setup, 1, 1, false);

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TThreadParkPad pad;
        TAtomic actorsAlive = 0;
        THPTimer Timer;

        //ui64 eventsPerPair = 100000000;

        Timer.Reset();
        ui32 followerPoolId = 0;
        ui32 leaderPoolId = 0;
        TActorId followerId = actorSystem.Register(
            new TActorBenchmark::TSendReceiveActor(
                TSendReceiveActorParams{.OtherEvents=2, .Allocation=true}
            ),
            TMailboxType::HTSwap,
            followerPoolId
        );
        THolder<IActor> leader{
            new TTestEndDecorator(THolder(new TActorBenchmark::TSendReceiveActor(
                TSendReceiveActorParams{.OwnEvents=2, .Receivers={followerId}, .Allocation=true}
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

        Cerr << "Completed " << 1e9 * elapsedTime << Endl;
        Cerr << "Elapsed " << Ts2Us(aggregated.ElapsedTicks) << "us" << Endl;
    }

}
