#include <ydb/core/formats/arrow/reader/stats/stats.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/datetime.h>

namespace NKikimr::NScanStats {

    Y_UNIT_TEST_SUITE(TColumnShardStatsV2) {
        Y_UNIT_TEST(SemanticParentPathDoesNotContainRuntimeIds) {
            auto collector = std::make_shared<TCollector>(42);
            auto assemble = collector->Start(TStep::Assemble, "assemble", "column");
            assemble->Update([](TStep& step) {
                step.SetMetaStep("Filter");
                step.SetCanonicalPath("");
            });
            auto decode = collector->Start(TStep::Decode, "decode", "column", assemble);
            decode->Finish(TStep::completed);
            assemble->Finish(TStep::completed);

            auto report = collector->Drain(true);
            UNIT_ASSERT_VALUES_EQUAL(report->StepsSize(), 2);
            const auto& step = report->GetSteps(0);
            UNIT_ASSERT_VALUES_EQUAL(step.GetStepKind(), TStep::Decode);
            UNIT_ASSERT_VALUES_EQUAL(step.GetMetaStep(), "Filter");
            UNIT_ASSERT_VALUES_EQUAL(step.GetCanonicalPath(), "Assemble");
            UNIT_ASSERT(step.HasParentStepId());
        }

        Y_UNIT_TEST(CanonicalPathNormalizesFetchingAndReserve) {
            auto collector = std::make_shared<TCollector>(42);
            auto fetch = collector->Start(TStep::FetchColumns, "fetch", "columns");
            fetch->Update([](TStep& step) {
                step.SetMetaStep("Aggregation");
                step.SetCanonicalPath("Reserve");
            });
            auto decode = collector->Start(TStep::Decode, "decode", "column", fetch);
            decode->Finish(TStep::completed);
            fetch->Finish(TStep::completed);

            auto report = collector->Drain(true);
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetMetaStep(), "Aggregation");
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetCanonicalPath(), "Reserve/Fetching");
        }

        Y_UNIT_TEST(NewSemanticMetastepStartsANewPath) {
            auto collector = std::make_shared<TCollector>(42);
            auto filter = collector->Start(TStep::Filter, "filter", "predicate");
            auto aggregation = collector->Start(TStep::Aggregation, "aggregate", "sum", filter);
            aggregation->Finish(TStep::completed);
            filter->Finish(TStep::completed);

            auto report = collector->Drain(true);
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetMetaStep(), "Aggregation");
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetCanonicalPath(), "");
        }

        Y_UNIT_TEST(PKMasksRetainTheirOwningMetastep) {
            auto collector = std::make_shared<TCollector>(42);
            auto parent = collector->Start(TStep::Reader, "source", "source");
            parent->Update([](TStep& step) {
                step.SetMetaStep("PKFilter");
                step.SetCanonicalPath("");
            });
            auto mask = collector->Start(TStep::Filter, "deletion", "delete flag", parent);
            mask->Finish(TStep::completed);
            parent->Finish(TStep::completed);

            auto report = collector->Drain(true);
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetMetaStep(), "PKFilter");
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetCanonicalPath(), "");
        }

        Y_UNIT_TEST(ProgramOverviewDoesNotInheritPKFilter) {
            auto collector = std::make_shared<TCollector>(42);
            auto parent = collector->Start(TStep::Reader, "source", "source");
            parent->Update([](TStep& step) { step.SetMetaStep("PKFilter"); });
            auto program = collector->Start(TStep::Reader, "PROGRAM_EXECUTION", "program", parent);
            program->Finish(TStep::completed);
            parent->Finish(TStep::completed);

            auto report = collector->Drain(true);
            UNIT_ASSERT(!report->GetSteps(0).HasMetaStep());
        }

        Y_UNIT_TEST(ReaderKindIsRetainedAcrossDrains) {
            auto collector = std::make_shared<TCollector>(42);
            collector->SetReaderKind("PLAIN");
            UNIT_ASSERT_VALUES_EQUAL(collector->Drain(false)->GetReaderKind(), "PLAIN");
            UNIT_ASSERT_VALUES_EQUAL(collector->Drain(true)->GetReaderKind(), "PLAIN");
        }

        Y_UNIT_TEST(NestedScopesMeasureOwnedWorkWithoutSubtractingDurations) {
            auto collector = std::make_shared<TCollector>(42);
            auto assemble = collector->Start(TStep::Assemble, "assemble", "column");
            std::shared_ptr<TOperation> decode;
            {
                TScope outer(assemble, collector);
                Sleep(TDuration::MilliSeconds(1));
                decode = collector->Start(TStep::Decode, "decode", "column", assemble);
                {
                    TScope inner(decode, collector);
                    Sleep(TDuration::MilliSeconds(4));
                }
                decode->Finish(TStep::completed);
                Sleep(TDuration::MilliSeconds(1));
            }
            assemble->Finish(TStep::completed);

            auto report = collector->Drain(true);
            const auto& child = report->GetSteps(0);
            const auto& parent = report->GetSteps(1);
            UNIT_ASSERT(child.HasOwnedExecutionDuration());
            UNIT_ASSERT(parent.HasOwnedExecutionDuration());
            UNIT_ASSERT(child.GetOwnedExecutionDuration() > 0);
            UNIT_ASSERT(parent.GetOwnedExecutionDuration() < parent.GetIntegralExecutionDuration());
            UNIT_ASSERT(parent.GetIntegralExecutionDuration() >=
                        parent.GetOwnedExecutionDuration() + child.GetOwnedExecutionDuration());
        }

        Y_UNIT_TEST(CollectorOnlyScopePreservesOwnedNesting) {
            auto collector = std::make_shared<TCollector>(42);
            auto parent = collector->Start(TStep::Assemble, "assemble", "column");
            {
                TScope outer(parent, collector);
                TScope contextOnly({}, collector);
                auto child = collector->Start(TStep::Decode, "decode", "column");
                {
                    TScope inner(child, collector);
                    Sleep(TDuration::MilliSeconds(4));
                }
                child->Finish(TStep::completed);
            }
            parent->Finish(TStep::completed);

            auto report = collector->Drain(true);
            UNIT_ASSERT(report->GetSteps(1).GetOwnedExecutionDuration() <
                        report->GetSteps(1).GetIntegralExecutionDuration());
        }

        Y_UNIT_TEST(SkippedUnvisitedOperationDoesNotInventOwnedZero) {
            auto collector = std::make_shared<TCollector>(42);
            auto skipped = collector->Start(TStep::Filter, "filter", "predicate");
            skipped->Finish(TStep::skipped);
            auto report = collector->Drain(true);
            UNIT_ASSERT(!report->GetSteps(0).HasOwnedExecutionDuration());
        }

        Y_UNIT_TEST(ReadyIdentitySurvivesQueueWaitAndDispatch) {
            auto collector = std::make_shared<TCollector>(42);
            auto ready = collector->Start(TStep::Ready, "READY", "Wait for ordered delivery");
            const ui64 id = ready->GetStepId();
            {
                TScope enqueue(ready, collector);
            }
            ready->Suspend();
            Sleep(TDuration::MilliSeconds(2));
            {
                TScope dispatch(ready, collector);
                ready->Finish();
            }
            auto report = collector->Drain(true);
            UNIT_ASSERT_VALUES_EQUAL(report->StepsSize(), 1);
            const auto& step = report->GetSteps(0);
            UNIT_ASSERT_VALUES_EQUAL(step.GetStepId(), id);
            UNIT_ASSERT_VALUES_EQUAL(step.GetMetaStep(), "Output");
            UNIT_ASSERT_VALUES_EQUAL(step.GetOutcome(), TStep::completed);
            UNIT_ASSERT(step.GetIntegralWaitDuration() > 0);
            UNIT_ASSERT(step.HasOwnedExecutionDuration());
        }

        Y_UNIT_TEST(AbandonedReadyClosesItsWaitingInterval) {
            auto collector = std::make_shared<TCollector>(42);
            auto ready = collector->Start(TStep::Ready, "READY", "Wait for ordered delivery");
            {
                TScope enqueue(ready, collector);
            }
            ready->Suspend();
            Sleep(TDuration::MilliSeconds(2));
            ready.reset();

            auto report = collector->Drain(true);
            UNIT_ASSERT(report->GetFinished());
            UNIT_ASSERT_VALUES_EQUAL(report->StepsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetOutcome(), TStep::cancelled);
            UNIT_ASSERT(report->GetSteps(0).GetIntegralWaitDuration() > 0);
        }

        Y_UNIT_TEST(CaptureRetainsSemanticContextAfterParentFinished) {
            auto collector = std::make_shared<TCollector>(42);
            auto parent = collector->Start(TStep::Assemble, "assemble", "column");
            parent->Update([](TStep& step) {
                step.SetMetaStep("Filter");
                step.SetCanonicalPath("");
            });
            TCapture capture;
            {
                TScope scope(parent, collector);
                capture = TCapture();
            }
            parent->Finish();
            {
                TScope restored(capture);
                UNIT_ASSERT(Current() == parent);
                UNIT_ASSERT(CurrentCollector() == collector);
                auto child = collector->Start(TStep::Decode, "decode", "column", Current());
                {
                    TScope decode(child, collector);
                }
                child->Finish();
            }

            auto report = collector->Drain(true);
            UNIT_ASSERT_VALUES_EQUAL(report->StepsSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(1).GetMetaStep(), "Filter");
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(1).GetCanonicalPath(), "Assemble");
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(1).GetParentStepId(), parent->GetStepId());
            UNIT_ASSERT(report->GetFinished());
            UNIT_ASSERT_VALUES_EQUAL(report->GetPendingOperations(), 0);
        }

        Y_UNIT_TEST(FinalSnapshotReportsPendingWorkWithoutWaitingForIt) {
            auto collector = std::make_shared<TCollector>(42);
            auto fetch = collector->Start(TStep::FetchColumns, "FETCH", "Shared cache miss");
            {
                TScope dispatch(fetch, collector);
            }
            fetch->Suspend();

            auto partial = collector->Drain(true);
            UNIT_ASSERT(!partial->GetFinished());
            UNIT_ASSERT(!partial->GetTruncated());
            UNIT_ASSERT_VALUES_EQUAL(partial->GetPendingOperations(), 1);

            fetch->Finish(TStep::cancelled);
            auto completed = collector->Drain(true);
            UNIT_ASSERT(completed->GetFinished());
            UNIT_ASSERT_VALUES_EQUAL(completed->GetPendingOperations(), 0);
        }

        Y_UNIT_TEST(NestedSameOperationKeepsOuterActive) {
            auto collector = std::make_shared<TCollector>(42);
            auto operation = collector->Start(TStep::Calculation, "CALCULATE", "callback");
            {
                TScope outer(operation, collector);
                {
                    TScope nested(operation, collector);
                    Sleep(TDuration::MilliSeconds(2));
                }
                UNIT_ASSERT(Current() == operation);
                operation->Update([](const TStep& step) {
                    UNIT_ASSERT_VALUES_EQUAL(step.GetIntegralExecutionDuration(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(step.GetOwnedExecutionDuration(), 0);
                });
            }
            operation->Finish();
            auto report = collector->Drain(true);
            UNIT_ASSERT_VALUES_EQUAL(report->StepsSize(), 1);
            UNIT_ASSERT(report->GetSteps(0).GetOwnedExecutionDuration() > 0);
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetOwnedExecutionDuration(), report->GetSteps(0).GetIntegralExecutionDuration());
        }

        Y_UNIT_TEST(CaptureDoesNotResumeWaitingParent) {
            auto collector = std::make_shared<TCollector>(42);
            auto parent = collector->Start(TStep::FetchColumns, "FETCH", "Async input");
            {
                TScope dispatch(parent, collector);
            }
            parent->Suspend();
            Sleep(TDuration::MilliSeconds(1));
            {
                TScope context{TCapture(parent, collector)};
                UNIT_ASSERT(Current() == parent);
                Sleep(TDuration::MilliSeconds(2));
                parent->Update([](const TStep& step) {
                    UNIT_ASSERT_VALUES_EQUAL(step.GetIntegralWaitDuration(), 0);
                });
            }
            parent->Finish();
            auto report = collector->Drain(true);
            UNIT_ASSERT(report->GetSteps(0).GetIntegralWaitDuration() > 0);
        }

        Y_UNIT_TEST(ExplicitEmptyAndCollectorOnlyScopesSuppressCapturedOwner) {
            auto collector = std::make_shared<TCollector>(42);
            auto otherCollector = std::make_shared<TCollector>(43);
            auto parent = collector->Start(TStep::Reader, "OWNER", "Captured owner");
            {
                TScope context{TCapture(parent, collector)};
                {
                    TScope disabled(nullptr, nullptr);
                    UNIT_ASSERT(!Current());
                    UNIT_ASSERT(!CurrentCollector());
                }
                UNIT_ASSERT(Current() == parent);
                UNIT_ASSERT(CurrentCollector() == collector);
                {
                    TScope contextOnly(nullptr, otherCollector);
                    UNIT_ASSERT(!Current());
                    UNIT_ASSERT(CurrentCollector() == otherCollector);
                    TCapture captured;
                    UNIT_ASSERT(!captured.Operation);
                    UNIT_ASSERT(captured.Collector == otherCollector);
                }
                UNIT_ASSERT(Current() == parent);
                UNIT_ASSERT(CurrentCollector() == collector);
            }
            parent->Finish();
        }

        Y_UNIT_TEST(ExplicitTimedScopeInsideCapturePerformsTheCallback) {
            auto collector = std::make_shared<TCollector>(42);
            auto parent = collector->Start(TStep::FetchColumns, "FETCH", "Async input");
            parent->Suspend();
            {
                TScope context{TCapture(parent, collector)};
                {
                    TScope callback(parent, collector);
                    Sleep(TDuration::MilliSeconds(2));
                }
                parent->Update([](const TStep& step) {
                    UNIT_ASSERT(step.GetIntegralExecutionDuration() > 0);
                    UNIT_ASSERT(step.GetOwnedExecutionDuration() > 0);
                });
            }
            parent->Finish();
        }

        Y_UNIT_TEST(RetentionCapDoesNotAbortFunctionalScope) {
            auto collector = std::make_shared<TCollector>(42);
            std::shared_ptr<TOperation> operation;
            ui64 recorded = 0;
            while ((operation = collector->Start(TStep::Calculation, "CALCULATE", "literal"))) {
                operation->Finish();
                UNIT_ASSERT(++recorded < 1'000'000);
            }
            bool continued = false;
            {
                TScope scope(operation, collector);
                UNIT_ASSERT(!Current());
                UNIT_ASSERT(CurrentCollector() == collector);
                continued = true;
            }
            UNIT_ASSERT(continued);
            auto report = collector->Drain(true);
            UNIT_ASSERT(report->GetTruncated());
            UNIT_ASSERT_VALUES_EQUAL(report->StepsSize(), recorded);
            UNIT_ASSERT_VALUES_EQUAL(report->GetPendingOperations(), 0);
        }
    } // Y_UNIT_TEST_SUITE(TColumnShardStatsV2)

} // namespace NKikimr::NScanStats
