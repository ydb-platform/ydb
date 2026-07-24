#include "actor_liveness_checker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors {
namespace {

    class TTargetActor
        : public TActorBootstrapped<TTargetActor>
    {
    public:
        void Bootstrap() {
            Become(&TThis::StateFunc);
        }

    private:
        STRICT_STFUNC(StateFunc,
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    };

    struct TCheckResult {
        TVector<TActorLivenessCheckTarget> DeadTargets;
        bool Complete = false;
    };

    class TCheckObserver
        : public TActorBootstrapped<TCheckObserver>
    {
    public:
        explicit TCheckObserver(TCheckResult& result)
            : Result(result)
        {
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }

    private:
        STRICT_STFUNC(StateFunc,
            hFunc(TEvents::TEvActorDead, Handle)
            hFunc(TEvents::TEvGone, Handle)
        )

        void Handle(TEvents::TEvActorDead::TPtr& ev) {
            Result.DeadTargets.push_back({
                .ActorId = ev->Sender,
                .Cookie = ev->Cookie,
            });
        }

        void Handle(TEvents::TEvGone::TPtr&) {
            Result.Complete = true;
            PassAway();
        }

    private:
        TCheckResult& Result;
    };

}

Y_UNIT_TEST_SUITE(TActorLivenessCheckerTest) {
    Y_UNIT_TEST(KeepsLiveActor) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();

        const TActorId target = runtime.Register(new TTargetActor());
        TCheckResult result;
        const TActorId observer = runtime.Register(new TCheckObserver(result));
        runtime.Register(CreateActorLivenessChecker(
            {{
                .ActorId = target,
                .Cookie = 10,
            }},
            observer));

        TDispatchOptions options;
        options.CustomFinalCondition = [&result] {
            return result.Complete;
        };
        runtime.DispatchEvents(options);

        UNIT_ASSERT(result.Complete);
        UNIT_ASSERT(result.DeadTargets.empty());
    }

    Y_UNIT_TEST(ReportsDeadActorWithCookie) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();

        const TActorId target(runtime.GetNodeId(), 0, Max<ui64>(), 0);
        constexpr ui64 Cookie = 42;
        TCheckResult result;
        const TActorId observer = runtime.Register(new TCheckObserver(result));
        runtime.Register(CreateActorLivenessChecker(
            {{
                .ActorId = target,
                .Cookie = Cookie,
            }},
            observer));

        TDispatchOptions options;
        options.CustomFinalCondition = [&result] {
            return result.Complete;
        };
        runtime.DispatchEvents(options);

        UNIT_ASSERT(result.Complete);
        UNIT_ASSERT_VALUES_EQUAL(result.DeadTargets.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.DeadTargets.front().ActorId, target);
        UNIT_ASSERT_VALUES_EQUAL(result.DeadTargets.front().Cookie, Cookie);
    }

    Y_UNIT_TEST(KeepsRemoteActorWhenLivenessIsUnsure) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();

        const TActorId target(runtime.GetNodeId() + 1, 0, 1, 0);
        TCheckResult result;
        const TActorId observer = runtime.Register(new TCheckObserver(result));
        runtime.Register(CreateActorLivenessChecker(
            {{
                .ActorId = target,
                .Cookie = 10,
            }},
            observer));

        TDispatchOptions options;
        options.CustomFinalCondition = [&result] {
            return result.Complete;
        };
        runtime.DispatchEvents(options);

        UNIT_ASSERT(result.Complete);
        UNIT_ASSERT(result.DeadTargets.empty());
    }
}

}
