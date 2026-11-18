#include <ydb/core/tx/columnshard/stats/scan/collector.h>
#include <ydb/core/tx/columnshard/stats/scan/observation.h>
#include <ydb/core/tx/columnshard/stats/scan/transport/events.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/base.h>

#include <new>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

#if defined(_unix_)
#include <sys/wait.h>

#include <cerrno>
#include <csignal>
#include <unistd.h>
#endif

namespace NKikimr::NOlap::NStats::NScan {
namespace {

NColumnShardStats::TScanIdentity Identity() {
    NColumnShardStats::TScanIdentity result;
    result.SetTabletId(42);
    result.SetScanAttemptId("attempt");
    return result;
}

NColumnShardStats::TOperation Reader() {
    NColumnShardStats::TOperation result;
    result.SetKey("Reader");
    result.SetName("Reader");
    result.MutableReader();
    return result;
}

NColumnShardStats::TOperation Filter(const TString& key = "Filter") {
    NColumnShardStats::TOperation result;
    result.SetKey(key);
    result.SetName("Filter");
    result.MutableFilter()->SetKind(NColumnShardStats::PREDICATE);
    result.MutableFilter()->SetPredicate("a > 10");
    result.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    result.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    return result;
}

NColumnShardStats::TDataSource Source() {
    NColumnShardStats::TDataSource result;
    result.SetStorageId("default");
    result.SetKind(NColumnShardStats::BLOB_STORAGE);
    result.MutableReadBytes()->SetBoundary(NColumnShardStats::LOGICAL_DELIVERY);
    result.MutableReadBytes()->SetRepresentation(NColumnShardStats::ENCODED_PAYLOAD);
    return result;
}

class TEnvironment {
public:
    TScanStats Collector{ Identity() };
    std::vector<TStatsUpdate> Updates;
    std::unique_ptr<TLocalStats> Root = TLocalStats::Create(Identity(), Reader(), [this](TStatsUpdate&& update) {
        Updates.push_back(std::move(update));
    });

    void Deliver() {
        for (auto& update : Updates) {
            UNIT_ASSERT(Collector.Apply(std::move(update)));
        }
        Updates.clear();
    }

    const NColumnShardStats::TOperation& Operation(const NColumnShardStats::TReport& report) const {
        UNIT_ASSERT_VALUES_EQUAL(report.OperationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(report.GetOperations(0).SubOperationsSize(), 1);
        return report.GetOperations(0).GetSubOperations(0);
    }
};

void Rows(TOperationScope& scope, ui64 input, std::optional<ui64> output = {}) {
    scope.Observe([&](auto& observation) {
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows).SetValue(input);
        if (output) {
            observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows).SetValue(*output);
        }
    });
}

template <class T>
concept CMutableOperationDescription = requires(T& value) { value.SetKey("other"); };

static_assert(!std::is_default_constructible_v<TStatsUpdate>);

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardScanStats) {
    Y_UNIT_TEST(LocalUpdateEventPreservesMeasuredContribution) {
        TEnvironment env;
        auto call = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
        Rows(call, 10, 4);
        call.Finish();
        env.Root->Publish();
        for (auto& update : env.Updates) {
            TEvScanStats::TEvStatsUpdate event(std::move(update));
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(TEvScanStats::TEvStatsUpdate::EventType), static_cast<ui32>(TEvColumnShard::EvStatsUpdate));
            UNIT_ASSERT(env.Collector.Apply(std::move(event.Update)));
        }
        env.Updates.clear();
        const auto report = env.Collector.MakeReport();
        const auto& operation = env.Operation(*report);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetInputRows().GetValue(), 10);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetOutputRows().GetValue(), 4);
    }

    Y_UNIT_TEST(RootExistsBeforeMeasurementAndRegistrationDoesNotCountCalls) {
        TEnvironment env;
        auto& root = env.Root->GetRoot();
        UNIT_ASSERT(root.GetDefinition().HasReader());
        UNIT_ASSERT(!root.GetDefinition().HasCallsCount());
        UNIT_ASSERT(env.Root->AddChild(root, Filter()));
        env.Root->Publish();
        env.Deliver();
        UNIT_ASSERT_VALUES_EQUAL(env.Collector.MakeReport()->OperationsSize(), 0);
        auto scope = env.Root->Measure(&root);
        scope.Finish();
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(report->OperationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(report->GetOperations(0).GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(report->GetOperations(0).SubOperationsSize(), 0);
    }

    Y_UNIT_TEST(RegisteredNodeRemainsUsableAfterAddingSiblings) {
        TEnvironment env;
        auto* filter = env.Root->AddChild(env.Root->GetRoot(), Filter());
        for (ui32 index = 0; index < 128; ++index) {
            UNIT_ASSERT(env.Root->AddChild(env.Root->GetRoot(), Filter(ToString(index))));
        }
        UNIT_ASSERT(env.Root->AddChild(env.Root->GetRoot(), Filter()) == filter);
        auto scope = env.Root->Measure(filter);
        Rows(scope, 10, 4);
        scope.Finish();
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetStatistics().GetOutputRows().GetValue(), 4);
    }

    Y_UNIT_TEST(NodeLookupDistinguishesDirectChildrenFromNestedOperations) {
        TEnvironment env;
        auto& root = env.Root->GetRoot();
        auto* parent = root.AddChild(Filter("parent"));
        auto* nested = parent->AddChild(Filter("nested"));
        root.AddChild(Filter("sibling"));
        UNIT_ASSERT(parent->AddChild(Filter("nested")) == nested);
        UNIT_ASSERT(root.FindChild("parent") == parent);
        UNIT_ASSERT(!root.FindChild("nested"));
        const auto isNested = [](const auto& operation) {
            return operation.GetKey() == "nested";
        };
        UNIT_ASSERT(!root.FindChildIf(isNested));
        UNIT_ASSERT(parent->FindChildIf(isNested) == nested);
        UNIT_ASSERT(root.FindNodeIf(isNested) == nested);
        UNIT_ASSERT(nested->FindNodeIf(isNested) == nested);
        UNIT_ASSERT(!nested->FindChildIf(isNested));
        const auto isMissing = [](const auto& operation) {
            return operation.GetKey() == "missing";
        };
        UNIT_ASSERT(!root.FindChildIf(isMissing));
        UNIT_ASSERT(!root.FindNodeIf(isMissing));
    }

    Y_UNIT_TEST(MeasurementCallbackFailuresPropagateWithoutLosingObservation) {
        TEnvironment env;
        auto scope = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
        const auto fail = [](auto&) {
            throw std::runtime_error("measurement callback failed");
        };
        UNIT_ASSERT_EXCEPTION(scope.Observe(fail), std::runtime_error);
        UNIT_ASSERT_EXCEPTION(scope.GetHandle().Observe(fail), std::runtime_error);
        Rows(scope, 10, 4);
        scope.Finish();
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        UNIT_ASSERT(report->GetFinalizationCause() == NColumnShardStats::FINALIZATION_NONE);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetStatistics().GetOutputRows().GetValue(), 4);
    }

    Y_UNIT_TEST(SenderAllocationFailurePropagatesAndPublicationCanRetry) {
        TScanStats collector{ Identity() };
        bool fail = true;
        auto local = TLocalStats::Create(Identity(), Reader());
        auto scope = local->Measure(local->AddChild(local->GetRoot(), Filter()));
        Rows(scope, 10, 4);
        scope.Finish();
        UNIT_ASSERT_EXCEPTION(local->SetSender([&](TStatsUpdate&& update) {
            if (std::exchange(fail, false)) {
                throw std::bad_alloc();
            }
            UNIT_ASSERT(collector.Apply(std::move(update)));
        }), std::bad_alloc);
        local->Publish();
        const auto report = collector.MakeReport();
        UNIT_ASSERT(report->GetFinalizationCause() == NColumnShardStats::FINALIZATION_NONE);
        const auto& operation = report->GetOperations(0).GetSubOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetOutputRows().GetValue(), 4);
    }

    Y_UNIT_TEST(DuplicateChildReusesCounters) {
        TEnvironment env;
        auto* first = env.Root->AddChild(env.Root->GetRoot(), Filter());
        auto* duplicate = env.Root->AddChild(env.Root->GetRoot(), Filter());
        UNIT_ASSERT(first == duplicate);
        {
            auto call = env.Root->Measure(first);
            Rows(call, 5, 1);
        }
        {
            auto call = env.Root->Measure(duplicate);
            Rows(call, 10, 2);
        }
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetCallsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetStatistics().GetInputRows().GetValue(), 15);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetFilter().GetPredicate(), "a > 10");
    }

    Y_UNIT_TEST(MeasurementsCannotChangeTheFrozenDescription) {
        TEnvironment env;
        auto definition = Filter("price-filter");
        definition.MutableFilter()->SetPredicate("price > 10 AND category = 'books'");
        auto* node = env.Root->AddChild(env.Root->GetRoot(), definition);
        auto scope = env.Root->Measure(node);
        scope.Observe([](auto& observation) {
            static_assert(!CMutableOperationDescription<std::remove_reference_t<decltype(observation)>>);
            observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows).SetValue(20);
            observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows).SetValue(7);
        });
        definition.MutableFilter()->SetPredicate("changed caller copy");
        scope.Finish();
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        const auto& operation = env.Operation(*report);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetFilter().GetPredicate(), "price > 10 AND category = 'books'");
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetInputRows().GetValue(), 20);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetOutputRows().GetValue(), 7);
    }

    Y_UNIT_TEST(ForkHasIndependentNodesAndCounters) {
        TEnvironment env;
        auto worker = env.Root->Fork();
        UNIT_ASSERT(worker);
        UNIT_ASSERT(&worker->GetRoot() != &env.Root->GetRoot());
        auto* foreign = worker->AddChild(worker->GetRoot(), Filter());
        auto scope = worker->Measure(foreign);
        Rows(scope, 3, 1);
        scope.Finish();
        worker->Close();
        env.Root->Close();
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetStatistics().GetInputRows().GetValue(), 3);
        UNIT_ASSERT(report->GetFinalizationCause() != NColumnShardStats::FINALIZATION_OBSERVATION_LOST);
    }

    Y_UNIT_TEST(SameChildAcrossOwnersMergesItsMeasurements) {
        TEnvironment env;
        for (ui64 rows : { 4u, 8u }) {
            auto worker = env.Root->Fork();
            auto scope = worker->Measure(worker->AddChild(worker->GetRoot(), Filter()));
            Rows(scope, rows, rows / 2);
            scope.Finish();
            worker->Close();
        }
        env.Root->Close();
        env.Deliver();
        const auto report = std::move(env.Collector).Finish(NColumnShardStats::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetCallsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetStatistics().GetInputRows().GetValue(), 12);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetStatistics().GetOutputRows().GetValue(), 6);
        UNIT_ASSERT(!env.Operation(*report).HasUnfinished());
        UNIT_ASSERT(report->GetFinality() == NColumnShardStats::FINAL);
    }

    Y_UNIT_TEST(NullNodeMeasurementIsANoop) {
        TEnvironment env;
        UNIT_ASSERT(!env.Root->Measure(nullptr));
        env.Root->Close();
        env.Deliver();
        const auto report = std::move(env.Collector).Finish(NColumnShardStats::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(report->OperationsSize(), 0);
        UNIT_ASSERT(report->GetExecutionOutcome() == NColumnShardStats::SUCCESS);
        UNIT_ASSERT(report->GetFinalizationCause() == NColumnShardStats::OUTPUT_SNAPSHOT);
    }

    Y_UNIT_TEST(ClonedBranchOwnsItsNodesAfterOriginalOwnerIsDestroyed) {
        TEnvironment env;
        auto* original = env.Root->AddChild(env.Root->GetRoot(), Filter());
        auto* nested = env.Root->AddChild(*original, Filter("nested"));
        auto worker = env.Root->Fork();
        auto* clone = worker->CloneBranch(*nested);
        UNIT_ASSERT(clone);
        UNIT_ASSERT(clone != nested);
        UNIT_ASSERT(&clone->GetDefinition() == &nested->GetDefinition());
        env.Root->Close();
        env.Root.reset();
        auto scope = worker->Measure(clone);
        Rows(scope, 10, 3);
        scope.Finish();
        worker->Close();
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        const auto& parent = report->GetOperations(0).GetSubOperations(0);
        UNIT_ASSERT(!parent.HasCallsCount());
        UNIT_ASSERT_VALUES_EQUAL(parent.SubOperationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parent.GetSubOperations(0).GetStatistics().GetOutputRows().GetValue(), 3);
    }

    Y_UNIT_TEST(CompletedAndRegisteredUnfinishedAreSeparate) {
        TEnvironment env;
        auto scope = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
        Rows(scope, 100);
        env.Deliver();
        const auto pending = env.Collector.MakeReport();
        UNIT_ASSERT(!env.Operation(*pending).HasCallsCount());
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*pending).GetUnfinished().GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*pending).GetUnfinished().GetStatistics().GetInputRows().GetValue(), 100);
        UNIT_ASSERT(!env.Operation(*pending).GetUnfinished().GetStatistics().GetOutputRows().HasValue());
        Rows(scope, 100, 40);
        scope.Finish();
        env.Deliver();
        const auto finished = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*finished).GetCallsCount(), 1);
        UNIT_ASSERT(!env.Operation(*finished).HasUnfinished());
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*finished).GetStatistics().GetOutputRows().GetValue(), 40);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*pending).GetUnfinished().GetCallsCount(), 1);
    }

    Y_UNIT_TEST(MixedCallsKeepIndependentNumericCohorts) {
        TEnvironment env;
        auto* filter = env.Root->AddChild(env.Root->GetRoot(), Filter());
        {
            auto first = env.Root->Measure(filter);
            Rows(first, 10, 0);
        }
        auto second = env.Root->Measure(filter);
        Rows(second, 20);
        env.Deliver();
        const auto report = std::move(env.Collector).Finish(NColumnShardStats::SUCCESS);
        const auto& operation = env.Operation(*report);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetInputRows().GetValue(), 10);
        UNIT_ASSERT(operation.GetStatistics().GetOutputRows().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetOutputRows().GetValue(), 0);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetUnfinished().GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetUnfinished().GetStatistics().GetInputRows().GetValue(), 20);
    }

    Y_UNIT_TEST(ReadSurvivesWhileOperationRemainsUnfinished) {
        TEnvironment env;
        auto scope = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
        scope.RecordRead(Source(), 100);
        scope.Suspend();
        env.Deliver();
        const auto pending = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(pending->GetDataSources(0).GetReadBytes().GetValue(), 100);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*pending).GetStatistics().DataSourcesSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*pending).GetUnfinished().GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 100);
        scope.Resume();
        scope.Finish();
        env.Deliver();
        const auto finished = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(finished->GetDataSources(0).GetReadBytes().GetValue(), 100);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*finished).GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 100);
        UNIT_ASSERT(!env.Operation(*finished).HasUnfinished());
    }

    Y_UNIT_TEST(ReadIsChargedOnceAcrossSharedConsumers) {
        TEnvironment env;
        auto* first = env.Root->AddChild(env.Root->GetRoot(), Filter("a"));
        auto* second = env.Root->AddChild(env.Root->GetRoot(), Filter("b"));
        const std::vector<TOperationNode*> consumers{ first, second, first, &env.Root->GetRoot() };
        for (ui64 bytes : { 100u, 50u }) {
            auto scope = env.Root->Measure(first);
            scope.RecordRead(Source(), bytes, consumers);
            scope.Finish();
            env.Deliver();
        }
        const auto report = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(report->GetDataSources(0).GetReadBytes().GetValue(), 150);
        UNIT_ASSERT_VALUES_EQUAL(report->GetDataSources(0).GetReads(), 2);
        const auto& reader = report->GetOperations(0);
        UNIT_ASSERT_VALUES_EQUAL(reader.GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 150);
        UNIT_ASSERT_VALUES_EQUAL(reader.GetSubOperations(0).GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 150);
        UNIT_ASSERT_VALUES_EQUAL(reader.GetSubOperations(1).GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 150);
    }

    Y_UNIT_TEST(LostObservationIsNotACompletedOrKnownPendingCall) {
        TEnvironment env;
        {
            auto scope = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
            scope.RecordRead(Source(), 0);
            scope.Suspend();
        }
        env.Root->Close();
        env.Deliver();
        const auto report = std::move(env.Collector).Finish(NColumnShardStats::CANCELLED);
        UNIT_ASSERT(report->GetFinalizationCause() == NColumnShardStats::FINALIZATION_OBSERVATION_LOST);
        UNIT_ASSERT(!env.Operation(*report).HasCallsCount());
        UNIT_ASSERT(!env.Operation(*report).HasUnfinished());
        UNIT_ASSERT(report->GetDataSources(0).GetReadBytes().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(report->GetDataSources(0).GetReadBytes().GetValue(), 0);
    }

    Y_UNIT_TEST(FrozenOutputDoesNotWaitForOwners) {
        TEnvironment env;
        auto scan = env.Root->Fork();
        auto background = scan->Fork();
        auto scope = background->Measure(background->AddChild(background->GetRoot(), Filter()));
        Rows(scope, 4);
        env.Deliver();
        const std::shared_ptr<const NColumnShardStats::TReport> snapshot = std::move(env.Collector).Finish(NColumnShardStats::SUCCESS);
        const auto frozen = snapshot->SerializeAsString();
        UNIT_ASSERT(snapshot->GetFinality() == NColumnShardStats::FINAL);
        UNIT_ASSERT(snapshot->GetFinalizationCause() == NColumnShardStats::OUTPUT_SNAPSHOT);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*snapshot).GetUnfinished().GetCallsCount(), 1);
        Rows(scope, 4, 1);
        scope.Finish();
        background->Close();
        scan->Close();
        UNIT_ASSERT_VALUES_EQUAL(snapshot->SerializeAsString(), frozen);
    }

    Y_UNIT_TEST(CancelledSnapshotDoesNotImplyUnobservedOperationsCompleted) {
        TEnvironment env;
        auto scope = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
        Rows(scope, 5);
        env.Deliver();
        const auto snapshot = std::move(env.Collector).Finish(NColumnShardStats::CANCELLED);
        UNIT_ASSERT(snapshot->GetExecutionOutcome() == NColumnShardStats::CANCELLED);
        UNIT_ASSERT(!env.Operation(*snapshot).HasCallsCount());
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*snapshot).GetUnfinished().GetCallsCount(), 1);
    }

    Y_UNIT_TEST(PureReportInspectionDoesNotSealTheCollector) {
        TEnvironment env;
        auto* node = env.Root->AddChild(env.Root->GetRoot(), Filter());
        {
            auto scope = env.Root->Measure(node);
            Rows(scope, 4, 1);
        }
        env.Deliver();
        const auto earlier = env.Collector.MakeReport();
        UNIT_ASSERT(earlier->GetFinality() == NColumnShardStats::OPEN);
        {
            auto scope = env.Root->Measure(node);
            Rows(scope, 8, 2);
        }
        env.Deliver();
        const auto later = env.Collector.MakeReport();
        UNIT_ASSERT(later->GetFinality() == NColumnShardStats::OPEN);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*earlier).GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*later).GetCallsCount(), 2);
        const auto final = std::move(env.Collector).Finish(NColumnShardStats::FAILED);
        UNIT_ASSERT(final->GetFinality() == NColumnShardStats::FINAL);
        UNIT_ASSERT(final->GetExecutionOutcome() == NColumnShardStats::FAILED);
    }

    Y_UNIT_TEST(CumulativeNodeUpdatesReplaceAndDeduplicate) {
        TEnvironment env;
        auto scope = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
        Rows(scope, 4, 1);
        scope.Finish();
        auto repeated = env.Updates;
        env.Deliver();
        for (auto& update : repeated) {
            UNIT_ASSERT(env.Collector.Apply(std::move(update)));
        }
        const auto report = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetStatistics().GetInputRows().GetValue(), 4);
    }

    Y_UNIT_TEST(OutOfOrderPartialUpdatesKeepIndependentNodes) {
        TEnvironment env;
        auto* first = env.Root->AddChild(env.Root->GetRoot(), Filter("a"));
        auto* second = env.Root->AddChild(env.Root->GetRoot(), Filter("b"));
        {
            auto scope = env.Root->Measure(first);
            Rows(scope, 4, 1);
        }
        auto earlier = std::move(env.Updates.back());
        env.Updates.clear();
        {
            auto scope = env.Root->Measure(second);
            Rows(scope, 8, 2);
        }
        auto later = std::move(env.Updates.back());
        env.Updates.clear();
        UNIT_ASSERT(env.Collector.Apply(std::move(later)));
        UNIT_ASSERT(env.Collector.Apply(std::move(earlier)));
        const auto report = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(report->GetOperations(0).SubOperationsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(report->GetOperations(0).GetSubOperations(0).GetStatistics().GetInputRows().GetValue(), 4);
        UNIT_ASSERT_VALUES_EQUAL(report->GetOperations(0).GetSubOperations(1).GetStatistics().GetInputRows().GetValue(), 8);
    }

    Y_UNIT_TEST(NumericOrderDoesNotDependOnTheKeyOrArrivalOrder) {
        TEnvironment env;
        auto* later = env.Root->AddChild(env.Root->GetRoot(), Filter("10"), 20);
        auto* earlier = env.Root->AddChild(env.Root->GetRoot(), Filter("2"), 10);
        env.Root->Measure(later).Finish();
        env.Root->Measure(earlier).Finish();
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(report->GetOperations(0).GetSubOperations(0).GetKey(), "2");
        UNIT_ASSERT_VALUES_EQUAL(report->GetOperations(0).GetSubOperations(1).GetKey(), "10");
    }

    Y_UNIT_TEST(ScopeMovesKeepOneRegisteredCall) {
        TEnvironment env;
        auto original = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
        Rows(original, 10, 4);
        auto moved = std::move(original);
        UNIT_ASSERT(!original);
        TOperationScope assigned;
        assigned = std::move(moved);
        UNIT_ASSERT(!moved);
        assigned.Finish();
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetCallsCount(), 1);
        UNIT_ASSERT(!env.Operation(*report).HasUnfinished());
    }

    Y_UNIT_TEST(ClosingStopsNewWorkUntilLastScopeFinishes) {
        TEnvironment env;
        auto* node = env.Root->AddChild(env.Root->GetRoot(), Filter());
        auto scope = env.Root->Measure(node);
        Rows(scope, 10);
        env.Root->Close();
        env.Root->Close();
        UNIT_ASSERT(!env.Root->Measure(node));
        UNIT_ASSERT(!env.Root->AddChild(env.Root->GetRoot(), Filter("late")));
        Rows(scope, 10, 4);
        scope.Finish();
        env.Deliver();
        const auto report = std::move(env.Collector).Finish(NColumnShardStats::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetStatistics().GetOutputRows().GetValue(), 4);
    }

    Y_UNIT_TEST(UnknownReadSizeDoesNotBecomeZero) {
        TEnvironment env;
        auto scope = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
        scope.RecordRead(Source(), std::nullopt);
        scope.Finish();
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        const auto& source = report->GetDataSources(0);
        UNIT_ASSERT(!source.GetReadBytes().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(source.GetVolumeCoverage().GetMeasured(), 0);
        UNIT_ASSERT_VALUES_EQUAL(source.GetVolumeCoverage().GetApplicable(), 1);
    }

    Y_UNIT_TEST(UnknownProvenancePreservesAnObservedDelivery) {
        TEnvironment env;
        auto source = Source();
        source.SetKind(NColumnShardStats::SOURCE_UNKNOWN);
        auto scope = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
        scope.RecordRead(source, 10);
        scope.Finish();
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        UNIT_ASSERT(report->GetDataSources(0).GetKind() == NColumnShardStats::SOURCE_UNKNOWN);
        UNIT_ASSERT_VALUES_EQUAL(report->GetDataSources(0).GetReadBytes().GetValue(), 10);
    }

    Y_UNIT_TEST(OwnerDestructionDetachesLiveScopesWithoutCompletingThem) {
        for (bool closeBeforeDestruction : { false, true }) {
            TEnvironment env;
            auto* node = env.Root->AddChild(env.Root->GetRoot(), Filter());
            {
                auto completed = env.Root->Measure(node);
                Rows(completed, 10, 7);
            }
            auto pending = env.Root->Measure(node);
            pending.RecordRead(Source(), 100);
            pending.Suspend();
            auto anotherPending = env.Root->Measure(node);
            if (closeBeforeDestruction) {
                env.Root->Close();
            }
            env.Root.reset();
            UNIT_ASSERT(!pending);
            UNIT_ASSERT(!anotherPending);
            bool observed = false;
            pending.Observe([&](auto&) {
                observed = true;
            });
            pending.Finish();
            anotherPending.Finish();
            UNIT_ASSERT(!observed);
            env.Deliver();
            const auto report = env.Collector.MakeReport();
            UNIT_ASSERT(report->GetFinalizationCause() == NColumnShardStats::FINALIZATION_OBSERVATION_LOST);
            UNIT_ASSERT_VALUES_EQUAL(env.Operation(*report).GetCallsCount(), 1);
            UNIT_ASSERT(!env.Operation(*report).HasUnfinished());
            UNIT_ASSERT_VALUES_EQUAL(report->GetDataSources(0).GetReadBytes().GetValue(), 100);
        }
    }

    Y_UNIT_TEST(SharedPhaseCallbackAfterOwnerDestructionIsDisabled) {
        TEnvironment env;
        auto phase = std::make_shared<TOperationScope>();
        *phase = env.Root->Measure(&env.Root->GetRoot());
        phase->Suspend();
        env.Root.reset();
        bool observed = false;
        phase->Resume();
        phase->Observe([&](auto&) {
            observed = true;
        });
        phase->Finish();
        UNIT_ASSERT(!*phase);
        UNIT_ASSERT(!observed);
        env.Deliver();
        UNIT_ASSERT(env.Collector.MakeReport()->GetFinalizationCause() == NColumnShardStats::FINALIZATION_OBSERVATION_LOST);
    }

    Y_UNIT_TEST(SameMetastepSupportsStructuralAndMeasuredCallsWithoutInventedZero) {
        TEnvironment env;
        NColumnShardStats::TOperation definition;
        definition.SetKey("BuildResult");
        definition.SetName("BuildResult");
        definition.MutableBuildResult()->MutableBatches()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        auto* node = env.Root->AddChild(env.Root->GetRoot(), definition);
        env.Root->Measure(node).Finish();
        for (bool measured : { true, false }) {
            auto page = env.Root->Measure(node);
            page.Observe([&](auto& observation) {
                auto& batches = observation.Metric(&NColumnShardStats::TBuildResultDetails::MutableBatches);
                auto& input = observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows);
                auto& output = observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows);
                batches.SetApplicability(NColumnShardStats::APPLICABLE);
                input.SetApplicability(NColumnShardStats::APPLICABLE);
                output.SetApplicability(NColumnShardStats::APPLICABLE);
                if (measured) {
                    batches.SetValue(1);
                    input.SetValue(100);
                    output.SetValue(7);
                }
            });
            page.Finish();
        }
        env.Deliver();
        const auto report = env.Collector.MakeReport();
        const auto& operation = env.Operation(*report);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetCallsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetOutputRows().GetValue(), 7);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetOutputRows().GetCoverage().GetMeasured(), 1);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetStatistics().GetOutputRows().GetCoverage().GetApplicable(), 2);
        UNIT_ASSERT_VALUES_EQUAL(operation.GetBuildResult().GetBatches().GetValue(), 1);
    }

    Y_UNIT_TEST(ReadAttributionUsesTheConsumerCallCohortWithoutChargingItTwice) {
        TEnvironment env;
        {
            auto previous = env.Root->Measure(&env.Root->GetRoot());
            previous.RecordReadSelf(Source(), 20);
        }
        auto consumer = env.Root->Measure(&env.Root->GetRoot());
        auto read = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
        read.RecordReadSelf(Source(), 100);
        consumer.AttributeRead(Source(), 100);
        read.Finish();
        env.Deliver();
        const auto pending = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(pending->GetDataSources(0).GetReadBytes().GetValue(), 120);
        UNIT_ASSERT_VALUES_EQUAL(pending->GetOperations(0).GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 20);
        UNIT_ASSERT_VALUES_EQUAL(pending->GetOperations(0).GetUnfinished().GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 100);
        consumer.Finish();
        env.Deliver();
        const auto completed = env.Collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(completed->GetDataSources(0).GetReadBytes().GetValue(), 120);
        UNIT_ASSERT_VALUES_EQUAL(completed->GetOperations(0).GetStatistics().GetDataSources(0).GetReadBytes().GetValue(), 120);
        UNIT_ASSERT(!completed->GetOperations(0).HasUnfinished());
    }

    Y_UNIT_TEST(PhysicalAndLogicalReadBytesStaySeparate) {
        TEnvironment env;
        auto scope = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()));
        scope.RecordRead(Source(), 100);
        auto physical = Source();
        physical.MutableReadBytes()->SetBoundary(NColumnShardStats::PHYSICAL_ATTEMPT);
        scope.RecordRead(physical, 120);
        scope.Finish();
        env.Deliver();
        UNIT_ASSERT_VALUES_EQUAL(env.Collector.MakeReport()->DataSourcesSize(), 2);
    }

    Y_UNIT_TEST(UnattachedSenderAccumulatesMeasurementsUntilPublication) {
        TScanStats collector{ Identity() };
        auto local = TLocalStats::Create(Identity(), Reader(), {});
        UNIT_ASSERT(local);
        auto scope = local->Measure(local->AddChild(local->GetRoot(), Filter()));
        Rows(scope, 10, 4);
        scope.Finish();
        UNIT_ASSERT_VALUES_EQUAL(collector.MakeReport()->OperationsSize(), 0);
        local->SetSender([&](TStatsUpdate&& update) {
            UNIT_ASSERT(collector.Apply(std::move(update)));
        });
        const auto report = collector.MakeReport();
        UNIT_ASSERT_VALUES_EQUAL(report->GetOperations(0).GetSubOperations(0).GetCallsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(report->GetOperations(0).GetSubOperations(0).GetStatistics().GetInputRows().GetValue(), 10);
    }

    Y_UNIT_TEST(CsIdentityDoesNotRequireKqpStage) {
        UNIT_ASSERT(ValidIdentity(Identity()));
        auto invalid = Identity();
        invalid.MutableStage()->SetQueryExecutionId("query");
        UNIT_ASSERT(!ValidIdentity(invalid));
        invalid.MutableStage()->SetPhysicalTxIndex(0);
        invalid.MutableStage()->SetStageIndex(0);
        UNIT_ASSERT(ValidIdentity(invalid));
    }

    Y_UNIT_TEST(ReaderKindBelongsToTheReportHeader) {
        TScanStats collector(Identity(), NColumnShardStats::PLAIN);
        auto local = TLocalStats::Create(Identity(), Reader(), [&](TStatsUpdate&& update) {
            UNIT_ASSERT(collector.Apply(std::move(update)));
        });
        local->Measure(&local->GetRoot()).Finish();
        const auto report = std::move(collector).Finish(NColumnShardStats::SUCCESS);
        UNIT_ASSERT(report->GetReaderKind() == NColumnShardStats::PLAIN);
        UNIT_ASSERT(report->GetOperations(0).HasReader());
    }

    Y_UNIT_TEST(ChildWorkResumesOnlyDeclaredAccountingAncestors) {
        TEnvironment env;
        auto parent = env.Root->Measure(&env.Root->GetRoot());
        auto excluded = env.Root->Measure(env.Root->AddChild(env.Root->GetRoot(), Filter()), parent.GetHandle(), EChildWorkAccounting::Exclude);
        for (auto call = excluded.GetHandle(); call; call = call.GetParent()) {
            call.Suspend();
        }
        const auto flush = [&] {
            env.Root->PublishTo([&](TStatsUpdate&& update) {
                UNIT_ASSERT(env.Collector.Apply(std::move(update)));
            });
            env.Deliver();
            return env.Collector.MakeReport();
        };
        const auto before = flush();
        const auto& initialParent = before->GetOperations(0).GetUnfinished().GetStatistics();
        const auto& initialExcluded = env.Operation(*before).GetUnfinished().GetStatistics();
        for (auto call = excluded.GetHandle(); call; call = call.GetParent()) {
            call.ResumeForChildWork();
        }
        Sleep(TDuration::MilliSeconds(2));
        const auto during = flush();
        const auto& resumedParent = during->GetOperations(0).GetUnfinished().GetStatistics();
        const auto& waitingExcluded = env.Operation(*during).GetUnfinished().GetStatistics();
        UNIT_ASSERT(resumedParent.GetActiveTimeUs().GetValue() > initialParent.GetActiveTimeUs().GetValue());
        UNIT_ASSERT_VALUES_EQUAL(waitingExcluded.GetActiveTimeUs().GetValue(), initialExcluded.GetActiveTimeUs().GetValue());
        UNIT_ASSERT(waitingExcluded.GetWaitTimeUs().GetValue() > initialExcluded.GetWaitTimeUs().GetValue());
        excluded.Resume();
        excluded.Finish();
        parent.Finish();
        const auto after = flush();
        UNIT_ASSERT(!env.Operation(*after).HasUnfinished());
        UNIT_ASSERT_VALUES_EQUAL(env.Operation(*after).GetCallsCount(), 1);
    }
}

}   // namespace NKikimr::NOlap::NStats::NScan
