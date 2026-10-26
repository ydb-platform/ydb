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
        // A root adapter, unlike an arbitrary leaf, owns the whole contract.
        t.Task->SealWait(ETaskWaitCoverage::Complete);
        t.Observer.EndRun();
        t.Task->Complete({.RunnerInvoked = true});
        UNIT_ASSERT(t.Task->GetSnapshot().Waits[0].Reason == ETaskWaitReason::SpillWrite);
        UNIT_ASSERT(t.Task->GetSnapshot().Waits[0].Precision == ETaskWaitPrecision::Exact);
        UNIT_ASSERT(t.Task->GetSnapshot().Classification.Coverage == ETaskWaitCoverage::Complete);
        t.Task->Signal(InputDependency(1));
        t.Task->Signal(SpillingDependency);
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 1);
        promise.SetValue();
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        t.Task->Cancel();
    }

    Y_UNIT_TEST(ExactLeafDoesNotCertifyOpaqueParentOrLoseLegacySpillWakeup) {
        for (bool beforeParking : {false, true}) {
            TExecution t;
            auto promise = NThreading::NewPromise<void>();
            auto future = promise.GetFuture();
            t.Task->RequestExecution();
            t.Start();
            t.Observer.AwaitFuture(10, &future, EExecutionWaitKind::SpillRead, future);
            if (beforeParking) { t.Task->Signal(SpillingDependency); }
            t.Observer.EndRun();
            t.Task->Complete({.RunnerInvoked = true});
            const auto classification = t.Task->GetSnapshot().Classification;
            UNIT_ASSERT(classification.Coverage == ETaskWaitCoverage::Partial);
            UNIT_ASSERT(classification.LegacyWakeups);
            if (!beforeParking) { t.Task->Signal(SpillingDependency); }
            UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
            UNIT_ASSERT(t.Task->GetSnapshot().Classification.Readiness == ETaskReadiness::Recheck);
            t.Task->Cancel();
            promise.SetValue();
            UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        }
    }

    Y_UNIT_TEST(AggregationOverJoinRetainsLeafSpillAndBothInputFrames) {
        TExecution t;
        auto promise = NThreading::NewPromise<void>();
        auto future = promise.GetFuture();
        t.Task->RequestExecution();
        t.Start();
        const int aggregation = 0, join = 0;
        TExecutionInputScope aggregate(&t.Observer, &aggregation, EExecutionInputSide::None, "BlockCombineHashed");
        TExecutionInputScope build(&t.Observer, &join, EExecutionInputSide::Build, "HashJoin");
        t.Observer.AwaitFuture(reinterpret_cast<ui64>(&join), &future, EExecutionWaitKind::SpillRead, future);
        build.Yielded();
        aggregate.Yielded();
        t.Observer.ReportWork(reinterpret_cast<ui64>(&aggregation), "BlockCombineHashed", EExecutionWorkKind::Consume, "accumulate");
        t.Observer.EndRun();
        t.Task->Complete({.RunnerInvoked = true});
        auto snapshot = t.Task->GetSnapshot();
        UNIT_ASSERT(snapshot.Classification.Work == ETaskWorkKind::Consume);
        UNIT_ASSERT(snapshot.Classification.Readiness == ETaskReadiness::Waiting);
        UNIT_ASSERT(snapshot.Classification.Coverage == ETaskWaitCoverage::Partial);
        const auto& wait = snapshot.Waits.front();
        UNIT_ASSERT(wait.Reason == ETaskWaitReason::SpillRead);
        UNIT_ASSERT_VALUES_EQUAL(wait.OperatorName, "HashJoin");
        UNIT_ASSERT_VALUES_EQUAL(wait.InputPath.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(wait.InputPath[0].OperatorName, "HashJoin");
        UNIT_ASSERT(wait.InputPath[0].Side == ETaskInputSide::Build);
        UNIT_ASSERT_VALUES_EQUAL(wait.InputPath[1].OperatorName, "BlockCombineHashed");
        promise.SetValue();
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        UNIT_ASSERT(t.Task->GetSnapshot().Classification.Readiness == ETaskReadiness::Recheck);
        t.Start();
        t.Observer.ReportWork(reinterpret_cast<ui64>(&aggregation), "BlockCombineHashed", EExecutionWorkKind::Emit, "emit");
        t.Observer.MakeRunnable();
        t.Observer.EndRun();
        t.Task->Complete({.OutputRows = 5, .RunnerInvoked = true});
        snapshot = t.Task->GetSnapshot();
        UNIT_ASSERT(snapshot.Classification.Work == ETaskWorkKind::Emit);
        UNIT_ASSERT(snapshot.Classification.Readiness == ETaskReadiness::Runnable);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Classification.Phase, "emit");
        t.Task->Cancel();
    }

    Y_UNIT_TEST(PartialLeafReportCannotHideAnOpaqueSiblingInput) {
        for (bool beforeParking : {false, true}) {
            TExecution t;
            auto promise = NThreading::NewPromise<void>();
            auto future = promise.GetFuture();
            t.Task->RequestExecution();
            t.Start();
            t.Task->Await(InputDependency(42), t.Task->Observe(InputDependency(42)));
            t.Observer.AwaitFuture(10, &future, EExecutionWaitKind::SpillRead, future);
            if (beforeParking) { t.Task->Signal(InputDependency(42)); }
            t.Observer.EndRun();
            t.Task->Complete({.RunnerInvoked = true});
            if (!beforeParking) { t.Task->Signal(InputDependency(42)); }
            UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
            UNIT_ASSERT(t.Task->GetSnapshot().Classification.Readiness == ETaskReadiness::Recheck);
            t.Task->Cancel();
            promise.SetValue();
        }
    }

    Y_UNIT_TEST(OpaqueChildYieldIsNotInventedInputReadiness) {
        TExecution t;
        t.Task->RequestExecution();
        t.Start();
        TExecutionInputScope aggregate(&t.Observer, &t, EExecutionInputSide::None, "BlockCombineAll");
        aggregate.Yielded();
        t.Observer.EndRun();
        t.Task->Complete({.RunnerInvoked = true});
        auto snapshot = t.Task->GetSnapshot();
        UNIT_ASSERT(snapshot.Classification.Readiness == ETaskReadiness::OpaqueYield);
        UNIT_ASSERT(snapshot.Classification.Coverage == ETaskWaitCoverage::Unknown);
        UNIT_ASSERT(snapshot.Waits[0].Reason == ETaskWaitReason::Unknown);
        t.Task->Signal(SpillingDependency);
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        UNIT_ASSERT(t.Task->GetSnapshot().Classification.Readiness == ETaskReadiness::Recheck);
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
        t.Observer.EndRun();
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
        t.Observer.EndRun();
        t.Task->Complete({.RunnerInvoked = true});
        t.Task->RequestExecution();
        t.Start();
        t.Observer.AwaitFuture(10, &future, EExecutionWaitKind::SpillRead, future);
        t.Observer.EndRun();
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
        t.Observer.EndRun();
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
