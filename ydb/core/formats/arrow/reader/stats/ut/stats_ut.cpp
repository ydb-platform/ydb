#include <ydb/core/formats/arrow/reader/stats/stats.h>
#include <library/cpp/testing/unittest/registar.h>

#include <thread>

using namespace NKikimr::NScanStats;

Y_UNIT_TEST_SUITE(ColumnShardRuntimeStats) {
    Y_UNIT_TEST(DisabledScanScopeClearsAndRestoresForeignCollector) {
        auto collector = std::make_shared<TCollector>(1);
        auto parent = collector->Start(TStep::Reader, "reader", "reader");
        {
            TScope outer(parent);
            {
                TScope disabledScan(nullptr, nullptr);
                UNIT_ASSERT(!Current());
                UNIT_ASSERT(!CurrentCollector());
            }
            UNIT_ASSERT(Current() == parent);
            UNIT_ASSERT(CurrentCollector() == collector);
        }
        parent->Finish();
        UNIT_ASSERT(!Current());
        UNIT_ASSERT(!CurrentCollector());
    }

    Y_UNIT_TEST(DroppedChildScopeDoesNotAttributeWorkToParent) {
        auto collector = std::make_shared<TCollector>(1);
        auto parent = collector->Start(TStep::Reader, "reader", "reader");
        {
            TScope outer(parent);
            {
                TScope droppedChild(nullptr);
                UNIT_ASSERT(!Current());
                UNIT_ASSERT(CurrentCollector() == collector);
            }
            UNIT_ASSERT(Current() == parent);
        }
        parent->Finish();
        UNIT_ASSERT(!CurrentCollector());
        {
            TScope callbackWithoutRawRecord(nullptr, collector);
            UNIT_ASSERT(!Current());
            UNIT_ASSERT(CurrentCollector() == collector);
        }
        UNIT_ASSERT(!CurrentCollector());
    }

    Y_UNIT_TEST(LogicalDemandDeduplicatesRangesAcrossReadActions) {
        auto collector = std::make_shared<TCollector>(1);
        auto operation = collector->Start(TStep::FetchColumns, "fetch", "price");
        operation->AddRequestedRange("blob:0:10", 10);
        operation->AddRequestedRange("blob:0:10", 10);
        operation->AddRequestedRange("blob:10:3", 3);
        operation->Finish();
        const auto report = collector->Drain(true);
        UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetDetails().GetFetchColumns().GetRequestedBytes(), 13);
    }

    Y_UNIT_TEST(FinalDrainWithActiveOperationIsIncomplete) {
        auto collector = std::make_shared<TCollector>(1);
        auto operation = collector->Start(TStep::Reader, "reader", "reader");
        UNIT_ASSERT(!collector->Drain(true)->GetFinished());
        operation->Finish();
        UNIT_ASSERT(collector->Drain(true)->GetFinished());
    }

    Y_UNIT_TEST(CacheReadCountersAreCumulativeSnapshots) {
        auto collector = std::make_shared<TCollector>(1);
        UNIT_ASSERT(!collector->Drain(false)->HasCacheStats());
        collector->AddCacheRead(10, true);
        collector->AddCacheRead(20, false);
        const auto first = collector->Drain(false);
        const auto second = collector->Drain(true);
        UNIT_ASSERT_VALUES_EQUAL(first->GetCacheStats().GetCompletedReadRanges(), 2);
        UNIT_ASSERT_VALUES_EQUAL(second->GetCacheStats().GetCompletedReadRanges(), 2);
        UNIT_ASSERT_VALUES_EQUAL(second->GetCacheStats().GetCacheServedReadRanges(), 1);
        UNIT_ASSERT_VALUES_EQUAL(second->GetCacheStats().GetReturnedBytes(), 30);
        UNIT_ASSERT_VALUES_EQUAL(second->GetCacheStats().GetCacheServedBytes(), 10);
    }

    Y_UNIT_TEST(CopyBeforePendingAtStepZeroFinishesOnce) {
        auto collector = std::make_shared<TCollector>(7);
        collector->BindScanId("scan-attempt");
        auto operation = collector->Start(TStep::FetchColumns, "fetch", "price");
        auto callbackCopy = operation;
        {
            TScope scope(operation);
            UNIT_ASSERT(Current() == operation);
        }
        operation->Suspend();
        {
            TScope callback(callbackCopy);
            callbackCopy->AddReadBytes(0);
        }
        std::thread finish([&] { callbackCopy->Finish(); });
        operation->Finish();
        finish.join();
        callbackCopy->Update([](TStep& record) { record.SetReadBytes(123); });
        auto report = collector->Drain(true);
        UNIT_ASSERT_VALUES_EQUAL(report->StepsSize(), 1);
        const auto& step = report->GetSteps(0);
        UNIT_ASSERT_VALUES_EQUAL(step.GetStepId(), 1);
        UNIT_ASSERT(step.GetOutcome() == TStep::completed);
        UNIT_ASSERT(step.HasIntegralExecutionDuration());
        UNIT_ASSERT(step.HasIntegralWaitDuration());
        UNIT_ASSERT(step.HasReadBytes());
        UNIT_ASSERT_VALUES_EQUAL(step.GetReadBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(collector->Drain(true)->StepsSize(), 0);
        UNIT_ASSERT(!Current());
    }

    Y_UNIT_TEST(ParentScopeAndUnknownBytes) {
        auto collector = std::make_shared<TCollector>(1);
        auto parent = collector->Start(TStep::Reader, "reader", "reader");
        {
            TScope outer(parent);
            auto child = collector->Start(TStep::Filter, "filter", "price > 1", Current(), 4);
            {
                TScope inner(child);
                UNIT_ASSERT(Current() == child);
                child->Finish(TStep::skipped);
            }
            UNIT_ASSERT(Current() == parent);
        }
        parent->Finish();
        auto report = collector->Drain(false);
        UNIT_ASSERT_VALUES_EQUAL(report->StepsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetParentStepId(), parent->GetStepId());
        UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetSourceId(), 4);
        UNIT_ASSERT(!report->GetSteps(0).HasReadBytes());
        UNIT_ASSERT(!report->GetFinished());
    }

    Y_UNIT_TEST(CapCoversPreviouslyDrainedRecords) {
        auto collector = std::make_shared<TCollector>(1);
        for (ui32 i = 0; i < 16384; ++i) {
            auto operation = collector->Start(TStep::Reader, "reader", "reader");
            UNIT_ASSERT(operation);
            operation->Finish();
            if (i % 100 == 0) {
                collector->Drain(false);
            }
        }
        UNIT_ASSERT(!collector->Start(TStep::Reader, "reader", "reader"));
        {
            TScope scan(nullptr, collector);
            TScope droppedChild(collector->Start(TStep::Decode, "decode", "price"));
            UNIT_ASSERT(!Current());
            UNIT_ASSERT(CurrentCollector() == collector);
        }
        UNIT_ASSERT(collector->Drain(true)->GetTruncated());
        UNIT_ASSERT(collector->Drain(true)->GetTruncated());
    }
} // Y_UNIT_TEST_SUITE(ColumnShardRuntimeStats)
