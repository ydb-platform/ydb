#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/formats/arrow/program/filter.h>
#include <ydb/core/formats/arrow/program/projection.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/program_observer.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/adapters/read_report_finalizer.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/operations.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr::NOlap::NStats::NScan {
namespace {

NColumnShardStats::TScanIdentity ProgramIdentity() {
    NColumnShardStats::TScanIdentity identity;
    identity.SetTabletId(42);
    identity.SetScanAttemptId("program-attempt");
    return identity;
}

const NColumnShardStats::TOperation* FindOperation(const NColumnShardStats::TOperation& operation, const TString& name) {
    for (const auto& child : operation.GetSubOperations()) {
        if (const auto* found = FindOperation(child, name)) {
            return found;
        }
    }
    if (operation.GetName() == name) {
        return &operation;
    }
    return nullptr;
}

const NColumnShardStats::TOperation* FindOperation(const NColumnShardStats::TReport& report, const TString& name) {
    for (const auto& operation : report.GetOperations()) {
        if (const auto* found = FindOperation(operation, name)) {
            return found;
        }
    }
    return nullptr;
}

struct TProgramEnvironment {
    std::vector<TStatsUpdate> Updates;

    std::unique_ptr<TLocalStats> Local() {
        return TLocalStats::Create(ProgramIdentity(), MakeOutput(), [this](TStatsUpdate&& update) {
            Updates.emplace_back(std::move(update));
        });
    }

    std::shared_ptr<const NColumnShardStats::TReport> Snapshot() const {
        TScanStats collector(ProgramIdentity());
        for (const auto& update : Updates) {
            UNIT_ASSERT(collector.Apply(update));
        }
        auto report = std::move(collector).Finish(NColumnShardStats::SUCCESS);
        if (report) {
            TReadReportFinalizer(*report).Finalize();
        }
        return report;
    }
};

const NColumnShardStats::TOperation* FindPhase(const NColumnShardStats::TReport& report, const TString& name) {
    for (const auto& root : report.GetOperations()) {
        for (const auto& child : root.GetSubOperations()) {
            if (child.GetKey() == "phase/" + name) {
                return &child;
            }
        }
    }
    return nullptr;
}

}   // namespace

std::vector<TCallHandle> CollectAncestors(TCallHandle call) {
    std::vector<TCallHandle> result;
    for (; call; call = call.GetParent()) {
        if (call.GetNode()) {
            result.push_back(call);
        }
    }
    std::reverse(result.begin(), result.end());
    return result;
}

Y_UNIT_TEST_SUITE(TProgramObserverTest) {
    Y_UNIT_TEST(MaterializedScalarRowsAreNotReplacedByAnEarlierAppliedFilter) {
        TProgramEnvironment env;
        auto observer = std::make_shared<TProgramObserver>(env.Local());
        NArrow::NSSA::TAccessorsCollection resources(7);
        resources.AddFilter(NArrow::TColumnFilter(std::vector<bool>{ true, true, true, false, false, false, false }));
        UNIT_ASSERT_VALUES_EQUAL(resources.GetFilter().GetFilteredCountIfKnown().value(), 3);
        resources.AddCalculated(1, arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3)));
        UNIT_ASSERT_VALUES_EQUAL(resources.GetRecordsCountActualOptional().value(), 1);
        NArrow::NSSA::TProjectionProcessor projection({ NArrow::NSSA::TColumnChainInfo(1) });
        observer->OnEnter(1, projection, resources);
        observer->OnExecute(1, resources);
        observer->OnResult(1, resources, NArrow::NSSA::TExecutionResult::Done());
        observer->OnExit(1, resources, false);
        observer->FinishObservedExecution();
        const auto report = env.Snapshot();
        const auto* operation = FindOperation(*report, "Projection");
        UNIT_ASSERT(operation);
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatistics().GetInputRows().GetValue(), 1);
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatistics().GetOutputRows().GetValue(), 1);
    }

    Y_UNIT_TEST(ReenteredProcessorKeepsOneOperationAndBothCalls) {
        TProgramEnvironment env;
        auto observer = std::make_shared<TProgramObserver>(env.Local());
        NArrow::NSSA::TProjectionProcessor projection({ NArrow::NSSA::TColumnChainInfo(1) });
        NArrow::NSSA::TAccessorsCollection resources(7);
        for (ui64 call = 0; call < 2; ++call) {
            observer->OnEnter(1, projection, resources);
            observer->OnExecute(1, resources);
            observer->OnResult(1, resources, NArrow::NSSA::TExecutionResult::Done());
            observer->OnExit(1, resources, false);
        }
        observer->FinishObservedExecution();
        const auto report = env.Snapshot();
        const auto* phase = FindPhase(*report, "Projection");
        UNIT_ASSERT(phase);
        UNIT_ASSERT_VALUES_EQUAL(phase->SubOperationsSize(), 1);
        const auto& operation = phase->GetSubOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetCallsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetInputRows().GetValue(), 14);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetOutputRows().GetValue(), 14);
    }

    Y_UNIT_TEST(KnownStopClosesOnlyPhaseFragment) {
        TProgramEnvironment env;
        auto observer = std::make_shared<TProgramObserver>(env.Local());
        NArrow::NSSA::TProjectionProcessor projection({ NArrow::NSSA::TColumnChainInfo(1) });
        NArrow::NSSA::TAccessorsCollection resources(7);
        observer->OnEnter(1, projection, resources);
        observer->OnExecute(1, resources);
        const auto calls = CollectAncestors(observer->GetCurrentCallHandle());
        UNIT_ASSERT_VALUES_EQUAL(calls.size(), 2);
        observer->OnResult(1, resources, NArrow::NSSA::TExecutionResult::Pending(std::make_shared<NArrow::NSSA::IAsyncJob>()));
        observer->OnExit(1, resources, false);
        observer->FinishObservedExecution();
        const auto report = env.Snapshot();
        const auto* phase = FindPhase(*report, "Projection");
        const auto* leaf = FindOperation(*report, "Projection");
        UNIT_ASSERT(phase && leaf);
        UNIT_ASSERT_VALUES_EQUAL(phase->GetCallsCount(), 1);
        UNIT_ASSERT(!phase->HasUnfinished());
        UNIT_ASSERT(!leaf->HasCallsCount());
        UNIT_ASSERT_VALUES_EQUAL(leaf->GetUnfinished().GetCallsCount(), 1);
        const auto survivingCall = calls.back();
        UNIT_ASSERT(survivingCall.GetNode());
        observer.reset();
        UNIT_ASSERT(!survivingCall.GetNode());
        survivingCall.Resume();
        const auto afterDestruction = env.Snapshot();
        const auto* lostLeaf = FindOperation(*afterDestruction, "Projection");
        UNIT_ASSERT(!lostLeaf || !lostLeaf->HasCallsCount());
        for (const auto& call : calls) {
            UNIT_ASSERT(!call.GetNode());
        }
    }

    Y_UNIT_TEST(PhaseFragmentsCloseBeforeOtherPhaseWork) {
        TProgramEnvironment env;
        auto observer = std::make_shared<TProgramObserver>(env.Local(), THashMap<ui32, TString>{}, THashMap<ui32, TString>{},
            THashMap<ui64, EProgramPhase>{ { 1, EProgramPhase::Filter }, { 2, EProgramPhase::Projection }, { 3, EProgramPhase::Filter } });
        NArrow::NSSA::TFilterProcessor filter(NArrow::NSSA::TColumnChainInfo(1));
        NArrow::NSSA::TProjectionProcessor projection({ NArrow::NSSA::TColumnChainInfo(1) });
        NArrow::NSSA::TAccessorsCollection resources(7);
        observer->OnEnter(1, filter, resources);
        observer->OnExecute(1, resources);
        observer->OnResult(1, resources, NArrow::NSSA::TExecutionResult::Done());
        observer->OnExit(1, resources, false);
        observer->OnEnter(2, projection, resources);
        observer->OnExecute(2, resources);
        const auto middle = env.Snapshot();
        const auto* completed = FindPhase(*middle, "Filter");
        const auto* active = FindPhase(*middle, "Projection");
        UNIT_ASSERT(completed && active);
        UNIT_ASSERT_VALUES_EQUAL(completed->GetCallsCount(), 1);
        UNIT_ASSERT(!completed->HasUnfinished());
        UNIT_ASSERT_VALUES_EQUAL(active->GetUnfinished().GetCallsCount(), 1);
        observer->OnResult(2, resources, NArrow::NSSA::TExecutionResult::Done());
        observer->OnExit(2, resources, false);
        observer->OnEnter(3, filter, resources);
        observer->OnExecute(3, resources);
        observer->OnResult(3, resources, NArrow::NSSA::TExecutionResult::Done());
        observer->OnExit(3, resources, false);
        observer->FinishObservedExecution();
        const auto final = env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(FindPhase(*final, "Filter")->GetCallsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(FindPhase(*final, "Projection")->GetCallsCount(), 1);
        UNIT_ASSERT(!FindPhase(*final, "Filter")->HasUnfinished());
        UNIT_ASSERT(!FindPhase(*final, "Projection")->HasUnfinished());
    }

    Y_UNIT_TEST(SemanticPhasesAreReaderSiblings) {
        TProgramEnvironment env;
        auto observer = std::make_shared<TProgramObserver>(env.Local(), THashMap<ui32, TString>{}, THashMap<ui32, TString>{},
            THashMap<ui64, EProgramPhase>{ { 1, EProgramPhase::Filter }, { 2, EProgramPhase::Projection } });
        NArrow::NSSA::TFilterProcessor filter(NArrow::NSSA::TColumnChainInfo(1));
        NArrow::NSSA::TProjectionProcessor projection({ NArrow::NSSA::TColumnChainInfo(1) });
        NArrow::NSSA::TAccessorsCollection resources(7);
        observer->OnEnter(2, projection, resources);
        observer->OnEnter(1, filter, resources);
        observer->OnExecute(1, resources);
        observer->OnResult(1, resources, NArrow::NSSA::TExecutionResult::Done());
        observer->OnExit(1, resources, false);
        observer->OnExecute(2, resources);
        observer->OnResult(2, resources, NArrow::NSSA::TExecutionResult::Done());
        observer->OnExit(2, resources, false);
        observer->FinishObservedExecution();
        const auto report = env.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(report->GetOperations().size(), 1);
        const auto& root = report->GetOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(root.GetSubOperations().size(), 2);
        for (const auto& phase : root.GetSubOperations()) {
            UNIT_ASSERT(phase.GetName() == "Filter" || phase.GetName() == "Projection");
            UNIT_ASSERT_VALUES_EQUAL(phase.GetCallsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(phase.GetSubOperations().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(phase.GetSubOperations(0).GetCallsCount(), 1);
        }
    }

    Y_UNIT_TEST(ControlEnterIsNotAnExecutedCall) {
        TProgramEnvironment env;
        auto observer = std::make_shared<TProgramObserver>(env.Local());
        NArrow::NSSA::TFilterProcessor filter(NArrow::NSSA::TColumnChainInfo(1));
        NArrow::NSSA::TAccessorsCollection resources(0);
        observer->OnEnter(1, filter, resources);
        UNIT_ASSERT(!observer->GetCurrentCallHandle());
        observer->OnExit(1, resources, true);
        const auto report = env.Snapshot();
        const auto* operation = FindOperation(*report, "Filter");
        UNIT_ASSERT(!operation || (!operation->HasCallsCount() && !operation->HasUnfinished()));
    }

    Y_UNIT_TEST(PendingExitResumeKeepsOneCall) {
        TProgramEnvironment env;
        auto observer = std::make_shared<TProgramObserver>(env.Local());
        UNIT_ASSERT(observer);
        NArrow::NSSA::TProjectionProcessor projection({ NArrow::NSSA::TColumnChainInfo(1) });
        NArrow::NSSA::TAccessorsCollection resources(7);
        constexpr ui64 nodeId = (ui64(1) << 40) + 3;

        observer->OnEnter(nodeId, projection, resources);
        auto* stableNode = observer->GetCurrentNode();
        observer->OnExecute(nodeId, resources);
        const auto scopes = CollectAncestors(observer->GetCurrentCallHandle());
        UNIT_ASSERT_VALUES_EQUAL(scopes.size(), 2);
        observer->OnResult(nodeId, resources, NArrow::NSSA::TExecutionResult::Pending(std::make_shared<NArrow::NSSA::IAsyncJob>()));
        observer->OnExit(nodeId, resources, false);
        UNIT_ASSERT_VALUES_EQUAL(CollectAncestors(observer->GetCurrentCallHandle()).size(), 1);
        UNIT_ASSERT(scopes[0].GetNode());
        {
            const auto snapshot = env.Snapshot();
            const auto* operation = FindOperation(*snapshot, "Projection");
            UNIT_ASSERT(operation);
            UNIT_ASSERT(!operation->HasCallsCount());
            UNIT_ASSERT_VALUES_EQUAL(operation->GetUnfinished().GetCallsCount(), 1);
        }

        observer->OnEnter(nodeId, projection, resources);
        UNIT_ASSERT_VALUES_EQUAL(observer->GetCurrentNode(), stableNode);
        observer->OnExecute(nodeId, resources);
        observer->OnResult(nodeId, resources, NArrow::NSSA::TExecutionResult::Done());
        observer->OnExit(nodeId, resources, false);
        UNIT_ASSERT(!scopes[1].GetNode());
        UNIT_ASSERT(scopes[0].GetNode());
        observer->FinishObservedExecution();
        UNIT_ASSERT(!scopes[0].GetNode());
        const auto snapshot = env.Snapshot();
        const auto* operation = FindOperation(*snapshot, "Projection");
        UNIT_ASSERT(operation);
        UNIT_ASSERT_VALUES_EQUAL(operation->GetCallsCount(), 1);
        UNIT_ASSERT(!operation->HasUnfinished());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatistics().GetInputRows().GetValue(), 7);
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatistics().GetOutputRows().GetValue(), 7);
        UNIT_ASSERT(!operation->GetStatistics().GetInputBytes().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatistics().GetInputBytes().GetCoverage().GetMeasured(), 0);
        UNIT_ASSERT_VALUES_EQUAL(operation->GetKey(), "ssa/1099511627779");
    }

    Y_UNIT_TEST(FailedChildDoesNotFinishParent) {
        TProgramEnvironment env;
        auto observer = std::make_shared<TProgramObserver>(env.Local());
        NArrow::NSSA::TFilterProcessor filter(NArrow::NSSA::TColumnChainInfo(1));
        NArrow::NSSA::TProjectionProcessor child({ NArrow::NSSA::TColumnChainInfo(1) });
        NArrow::NSSA::TAccessorsCollection resources(7);
        observer->OnEnter(1, filter, resources);
        observer->OnExecute(1, resources);
        observer->OnEnter(2, child, resources);
        observer->OnExecute(2, resources);
        observer->OnResult(2, resources, TConclusionStatus::Fail("native failure"));
        observer->OnExit(2, resources, false);

        const auto snapshot = env.Snapshot();
        const auto* parent = FindOperation(*snapshot, "Filter");
        const auto* completed = FindOperation(*snapshot, "Projection");
        UNIT_ASSERT(parent && completed);
        UNIT_ASSERT(!parent->HasCallsCount());
        UNIT_ASSERT_VALUES_EQUAL(parent->GetUnfinished().GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(completed->GetCallsCount(), 1);
        UNIT_ASSERT(!completed->HasUnfinished());
    }

    Y_UNIT_TEST(NamesAndPredicatesAreOwnedMetadata) {
        TProgramEnvironment env;
        THashMap<ui32, TString> names{ { 1, "value" } };
        THashMap<ui32, TString> expressions{ { 1, "greater(value, literal(7))" } };
        auto observer = std::make_shared<TProgramObserver>(env.Local(), std::move(names), std::move(expressions));
        NArrow::NSSA::TFilterProcessor filter(NArrow::NSSA::TColumnChainInfo(1));
        NArrow::NSSA::TAccessorsCollection resources(0);
        observer->OnEnter(1, filter, resources);
        observer->OnExecute(1, resources);
        observer->OnResult(1, resources, NArrow::NSSA::TExecutionResult::Done());
        observer->OnExit(1, resources, false);
        const auto snapshot = env.Snapshot();
        const auto* operation = FindOperation(*snapshot, "Filter");
        UNIT_ASSERT(operation);
        UNIT_ASSERT(operation->GetFilter().GetKind() == NColumnShardStats::PREDICATE);
        UNIT_ASSERT_VALUES_EQUAL(operation->GetFilter().GetPredicate(), "greater(value, literal(7))");
        UNIT_ASSERT(operation->GetStatistics().GetInputRows().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(operation->GetStatistics().GetInputRows().GetValue(), 0);
    }
}

}   // namespace NKikimr::NOlap::NStats::NScan
