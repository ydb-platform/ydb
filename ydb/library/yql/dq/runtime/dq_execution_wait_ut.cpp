#include "dq_execution_wait.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NDq;
using namespace NKikimr::NMiniKQL;

namespace {
struct TExecution {
    ui32 Enqueues = 0;
    std::shared_ptr<TTaskSchedulingHandle> Task = std::make_shared<TTaskSchedulingHandle>(
        NActors::TActorId{}, NActors::TActorId{}, [this](auto) { ++Enqueues; }, [] {});
    TTaskExecutionWaitObserver Observer{Task};

    void Start() {
        auto generation = Task->TryDispatch();
        UNIT_ASSERT(generation && Task->Start(*generation));
        Observer.BeginRun();
        Task->BeginProbe();
    }
};
} // namespace

Y_UNIT_TEST_SUITE(DqExecutionWait) {
    Y_UNIT_TEST(SpillFutureWakesOnlyItsOwnDependency) {
        TExecution t;
        auto promise = NThreading::NewPromise<void>();
        const auto future = promise.GetFuture();
        t.Task->RequestExecution();
        t.Start();
        t.Observer.AwaitFuture(10, &future, EExecutionWaitKind::SpillWrite, future);
        t.Task->Complete({.RunnerInvoked = true});
        UNIT_ASSERT(t.Task->GetSnapshot().Waits[0].Reason == ETaskWaitReason::SpillWrite);
        UNIT_ASSERT(t.Task->GetSnapshot().Waits[0].Precision == ETaskWaitPrecision::Exact);
        t.Task->Signal(InputDependency(1));
        t.Task->Signal(SpillingDependency);
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 1);
        promise.SetValue();
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        t.Task->Cancel();
    }

    Y_UNIT_TEST(FutureCompletingBeforeParkingIsNotLost) {
        TExecution t;
        auto promise = NThreading::NewPromise<void>();
        const auto future = promise.GetFuture();
        t.Task->RequestExecution();
        t.Start();
        t.Observer.AwaitFuture(10, &future, EExecutionWaitKind::SpillRead, future);
        promise.SetValue();
        t.Task->Complete({.RunnerInvoked = true});
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        t.Task->Cancel();
    }

    Y_UNIT_TEST(PendingFutureSurvivesAnUnrelatedExplicitContinuation) {
        TExecution t;
        auto promise = NThreading::NewPromise<void>();
        const auto future = promise.GetFuture();
        t.Task->RequestExecution();
        t.Start();
        t.Observer.AwaitFuture(10, &future, EExecutionWaitKind::SpillRead, future);
        t.Task->Complete({.RunnerInvoked = true});
        t.Task->RequestExecution();
        t.Start();
        t.Observer.AwaitFuture(10, &future, EExecutionWaitKind::SpillRead, future);
        t.Task->Complete({.RunnerInvoked = true});
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        promise.SetValue();
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 3);
        t.Task->Cancel();
    }

    Y_UNIT_TEST(NestedSpillWaitRetainsRightInputContext) {
        TExecution t;
        auto promise = NThreading::NewPromise<void>();
        const auto future = promise.GetFuture();
        t.Task->RequestExecution();
        t.Start();
        TExecutionInputScope right(&t.Observer, &t, EExecutionInputSide::Right);
        t.Observer.AwaitFuture(10, &future, EExecutionWaitKind::SpillRead, future);
        right.Yielded();
        t.Task->Complete({.RunnerInvoked = true});
        const auto waits = t.Task->GetSnapshot().Waits;
        UNIT_ASSERT_VALUES_EQUAL(waits.size(), 1);
        UNIT_ASSERT(waits[0].Reason == ETaskWaitReason::SpillRead);
        UNIT_ASSERT_VALUES_EQUAL(waits[0].InputPath.size(), 1);
        UNIT_ASSERT(waits[0].InputPath[0].Side == ETaskInputSide::Right);
        t.Task->Cancel();
        promise.SetValue();
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 1);
    }
}
