#include "actor_liveness_checker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

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
        TVector<TActorLivenessCheckTarget> AliveTargets;
        TVector<TActorLivenessCheckTarget> DeadTargets;
        TVector<TActorLivenessCheckTarget> UnsureTargets;
        size_t FinishCount = 0;
    };

    class TTestActorLivenessChecker
        : public TActorLivenessChecker
    {
    public:
        TTestActorLivenessChecker(
                TVector<TActorLivenessCheckTarget> targets,
                TCheckResult& result)
            : TActorLivenessChecker(std::move(targets))
            , Result(result)
        {
        }

    private:
        void OnAlive(const TActorLivenessCheckTarget& target) override {
            Result.AliveTargets.push_back(target);
        }

        void OnDead(const TActorLivenessCheckTarget& target) override {
            Result.DeadTargets.push_back(target);
        }

        void OnUnsure(const TActorLivenessCheckTarget& target) override {
            Result.UnsureTargets.push_back(target);
        }

        void OnFinish() override {
            ++Result.FinishCount;
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
        runtime.Register(new TTestActorLivenessChecker(
            {{
                .ActorId = target,
                .Cookie = 10,
            }},
            result));

        TDispatchOptions options;
        options.CustomFinalCondition = [&result] {
            return result.FinishCount;
        };
        runtime.DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(result.FinishCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(result.AliveTargets.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.AliveTargets.front().ActorId, target);
        UNIT_ASSERT_VALUES_EQUAL(result.AliveTargets.front().Cookie, 10);
        UNIT_ASSERT(result.DeadTargets.empty());
        UNIT_ASSERT(result.UnsureTargets.empty());
    }

    Y_UNIT_TEST(ReportsDeadActorWithCookie) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();

        const TActorId target(runtime.GetNodeId(), 0, Max<ui64>(), 0);
        constexpr ui64 Cookie = 42;
        TCheckResult result;
        runtime.Register(new TTestActorLivenessChecker(
            {{
                .ActorId = target,
                .Cookie = Cookie,
            }},
            result));

        TDispatchOptions options;
        options.CustomFinalCondition = [&result] {
            return result.FinishCount;
        };
        runtime.DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(result.FinishCount, 1);
        UNIT_ASSERT(result.AliveTargets.empty());
        UNIT_ASSERT_VALUES_EQUAL(result.DeadTargets.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.DeadTargets.front().ActorId, target);
        UNIT_ASSERT_VALUES_EQUAL(result.DeadTargets.front().Cookie, Cookie);
        UNIT_ASSERT(result.UnsureTargets.empty());
    }

    Y_UNIT_TEST(KeepsRemoteActorWhenLivenessIsUnsure) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();

        const TActorId target(runtime.GetNodeId() + 1, 0, 1, 0);
        constexpr ui64 Cookie = 10;
        TCheckResult result;
        runtime.Register(new TTestActorLivenessChecker(
            {{
                .ActorId = target,
                .Cookie = Cookie,
            }},
            result));

        TDispatchOptions options;
        options.CustomFinalCondition = [&result] {
            return result.FinishCount;
        };
        runtime.DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(result.FinishCount, 1);
        UNIT_ASSERT(result.AliveTargets.empty());
        UNIT_ASSERT(result.DeadTargets.empty());
        UNIT_ASSERT_VALUES_EQUAL(result.UnsureTargets.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.UnsureTargets.front().ActorId, target);
        UNIT_ASSERT_VALUES_EQUAL(result.UnsureTargets.front().Cookie, Cookie);
    }
}

}
