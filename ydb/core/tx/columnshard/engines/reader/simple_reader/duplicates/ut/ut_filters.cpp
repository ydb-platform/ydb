#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/filters.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <ydb/core/formats/arrow/reader/batch_iterator.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

namespace {

// Test implementation of IFilterSubscriber that records calls
class TTestFilterSubscriber : public IFilterSubscriber {
public:
    bool FilterReady = false;
    bool Failed = false;
    TString FailureReason;
    NArrow::TColumnFilter ReceivedFilter = NArrow::TColumnFilter::BuildAllowFilter();

    void OnFilterReady(NArrow::TColumnFilter&& filter) override {
        FilterReady = true;
        ReceivedFilter = std::move(filter);
    }

    void OnFailure(const TString& reason) override {
        Failed = true;
        FailureReason = reason;
    }
};

// Helper to create a simple arrow RecordBatch with a single int32 "key" column and a "version" column
std::shared_ptr<arrow::RecordBatch> MakeTestBatch(const std::vector<int32_t>& keys, const std::vector<int32_t>& versions) {
    return NColumnShard::MakeTestBatch<arrow::Int32Type, arrow::Int32Type>({"key", "version"}, keys, versions);
}

std::shared_ptr<arrow::Schema> MakeKeySchema() {
    return arrow::schema({arrow::field("key", arrow::int32())});
}

std::shared_ptr<arrow::Schema> MakeDataSchema() {
    return arrow::schema({arrow::field("key", arrow::int32())});
}

std::vector<std::string> GetVersionColumns() {
    return {"version"};
}

// Helper to create a TBatchIterator from a batch with a given sourceId
NArrow::NMerger::TBatchIterator MakeIterator(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    ui64 sourceId)
{
    auto keySchema = MakeKeySchema();
    auto dataSchema = MakeDataSchema();
    return NArrow::NMerger::TBatchIterator(
        batch, nullptr, *keySchema, *dataSchema, GetVersionColumns(), sourceId);
}

// Helper to create TEvRequestFilter::TPtr for testing
TEvRequestFilter::TPtr MakeTestRequest(
    ui64 portionId,
    ui64 recordsCount,
    const std::shared_ptr<IFilterSubscriber>& subscriber)
{
    // Create a minimal arrow batch for TSimpleRow construction
    auto batch = MakeTestBatch({0}, {0});
    NArrow::TSimpleRow minPK(batch, 0);
    NArrow::TSimpleRow maxPK(batch, 0);

    auto abortionFlag = std::make_shared<TAtomicCounter>(0);

    auto* ev = new TEvRequestFilter(
        minPK, maxPK, portionId, recordsCount,
        TSnapshot(1, 1), subscriber,
        std::shared_ptr<const TAtomicCounter>(abortionFlag));

    auto handle = new NActors::IEventHandle(
        NActors::TActorId(), NActors::TActorId(), ev);

    return TEvRequestFilter::TPtr(static_cast<TEvRequestFilter::THandle*>(handle));
}

std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> MakeTestCounters() {
    return std::make_shared<NColumnShard::TDuplicateFilteringCounters>();
}

// Helper: builds a TFiltersBuilder, adds records/skips for a portion, and delivers the filter
// to a subscriber via AddWaitingPortion mechanism. Returns the subscriber.
std::shared_ptr<TTestFilterSubscriber> BuildAndDeliverFilter(
    ui64 portionId,
    ui64 rowsCount,
    const std::vector<bool>& pattern)  // true = add, false = skip
{
    AFL_VERIFY(pattern.size() == rowsCount);

    auto subscriber = std::make_shared<TTestFilterSubscriber>();
    auto counters = MakeTestCounters();
    auto request = MakeTestRequest(portionId, rowsCount, subscriber);
    auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

    TFiltersBuilder builder;
    builder.AddSource(portionId, rowsCount);
    builder.AddWaitingPortion(portionId, accumulator);

    auto batch = MakeTestBatch(
        std::vector<int32_t>(rowsCount, 1),
        std::vector<int32_t>(rowsCount, 1));

    for (size_t i = 0; i < pattern.size(); ++i) {
        auto iter = MakeIterator(batch, portionId);
        if (pattern[i]) {
            builder.AddRecord(iter);
        } else {
            builder.SkipRecord(iter);
        }
    }

    return subscriber;
}

}  // namespace

Y_UNIT_TEST_SUITE(TFiltersBuilderTests) {

    Y_UNIT_TEST(IsBufferExhaustedAlwaysFalse) {
        TFiltersBuilder builder;
        UNIT_ASSERT(!builder.IsBufferExhausted());

        builder.AddSource(1, 10);
        UNIT_ASSERT(!builder.IsBufferExhausted());
    }

    Y_UNIT_TEST(ValidateDataSchemaDoesNothing) {
        TFiltersBuilder builder;
        auto schema = arrow::schema({arrow::field("key", arrow::int32())});
        UNIT_ASSERT_NO_EXCEPTION(builder.ValidateDataSchema(schema));
    }

    Y_UNIT_TEST(AddRecordIncrementsRowsAdded) {
        TFiltersBuilder builder;
        builder.AddSource(1, 5);

        auto batch = MakeTestBatch({1, 2, 3, 4, 5}, {1, 1, 1, 1, 1});
        auto iter = MakeIterator(batch, /*sourceId=*/1);

        UNIT_ASSERT_VALUES_EQUAL(builder.GetRowsAdded(), 0);
        builder.AddRecord(iter);
        UNIT_ASSERT_VALUES_EQUAL(builder.GetRowsAdded(), 1);
    }

    Y_UNIT_TEST(SkipRecordIncrementsRowsSkipped) {
        TFiltersBuilder builder;
        builder.AddSource(1, 5);

        auto batch = MakeTestBatch({1, 2, 3, 4, 5}, {1, 1, 1, 1, 1});
        auto iter = MakeIterator(batch, /*sourceId=*/1);

        UNIT_ASSERT_VALUES_EQUAL(builder.GetRowsSkipped(), 0);
        builder.SkipRecord(iter);
        UNIT_ASSERT_VALUES_EQUAL(builder.GetRowsSkipped(), 1);
    }

    Y_UNIT_TEST(AddRecordBuildsFilterCorrectly) {
        // Test pattern: add, skip, add -> [true, false, true]
        auto subscriber = BuildAndDeliverFilter(1, 3, {true, false, true});

        UNIT_ASSERT(subscriber->FilterReady);
        auto simpleFilter = subscriber->ReceivedFilter.BuildSimpleFilter();
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter[0], true);
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter[1], false);
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter[2], true);
    }

    Y_UNIT_TEST(MultiplePortionsIndependent) {
        TFiltersBuilder builder;
        builder.AddSource(1, 2);
        builder.AddSource(2, 2);

        auto batch1 = MakeTestBatch({10, 20}, {1, 1});
        auto batch2 = MakeTestBatch({30, 40}, {1, 1});

        // Add records for portion 1: add, skip
        {
            auto iter = MakeIterator(batch1, 1);
            builder.AddRecord(iter);
        }
        {
            auto iter = MakeIterator(batch1, 1);
            builder.SkipRecord(iter);
        }

        // Add records for portion 2: skip, add
        {
            auto iter = MakeIterator(batch2, 2);
            builder.SkipRecord(iter);
        }
        {
            auto iter = MakeIterator(batch2, 2);
            builder.AddRecord(iter);
        }

        UNIT_ASSERT_VALUES_EQUAL(builder.GetRowsAdded(), 2);
        UNIT_ASSERT_VALUES_EQUAL(builder.GetRowsSkipped(), 2);
    }

    Y_UNIT_TEST(AddWaitingPortionAndNotifyReadyFilter) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(/*portionId=*/1, /*recordsCount=*/2, subscriber);

        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        TFiltersBuilder builder;
        builder.AddSource(1, 2);

        // Add all records for the filter to be complete
        auto batch = MakeTestBatch({10, 20}, {1, 1});
        {
            auto iter = MakeIterator(batch, 1);
            builder.AddRecord(iter);
        }
        {
            auto iter = MakeIterator(batch, 1);
            builder.AddRecord(iter);
        }

        // Now notify - filter should be ready since all records are added
        bool result = builder.NotifyReadyFilter(accumulator);
        UNIT_ASSERT(result);
        UNIT_ASSERT(accumulator->IsDone());
        UNIT_ASSERT(subscriber->FilterReady);
    }

    Y_UNIT_TEST(NotifyReadyFilterReturnsFalseWhenIncomplete) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(/*portionId=*/1, /*recordsCount=*/3, subscriber);

        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        TFiltersBuilder builder;
        builder.AddSource(1, 3);

        // Add only 1 of 3 records
        auto batch = MakeTestBatch({10, 20, 30}, {1, 1, 1});
        {
            auto iter = MakeIterator(batch, 1);
            builder.AddRecord(iter);
        }

        // Filter is not complete yet
        bool result = builder.NotifyReadyFilter(accumulator);
        UNIT_ASSERT(!result);
        UNIT_ASSERT(!accumulator->IsDone());
        UNIT_ASSERT(!subscriber->FilterReady);

        // Clean up: abort to satisfy destructor check
        accumulator->Abort("test cleanup");
    }

    Y_UNIT_TEST(NotifyReadyFilterReturnsFalseWhenPortionNotFound) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(/*portionId=*/99, /*recordsCount=*/1, subscriber);

        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        TFiltersBuilder builder;
        // Don't add source for portionId=99

        bool result = builder.NotifyReadyFilter(accumulator);
        UNIT_ASSERT(!result);

        // Clean up
        accumulator->Abort("test cleanup");
    }

    Y_UNIT_TEST(AddWaitingPortionAutoNotifiesOnComplete) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(/*portionId=*/1, /*recordsCount=*/2, subscriber);

        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        TFiltersBuilder builder;
        builder.AddSource(1, 2);

        // Register waiting portion
        builder.AddWaitingPortion(1, accumulator);

        // Add records - when the filter is complete, it should auto-notify
        auto batch = MakeTestBatch({10, 20}, {1, 1});
        {
            auto iter = MakeIterator(batch, 1);
            builder.AddRecord(iter);
        }
        // After first record, filter is not complete yet
        UNIT_ASSERT(!subscriber->FilterReady);

        {
            auto iter = MakeIterator(batch, 1);
            builder.SkipRecord(iter);
        }
        // After second record, filter is complete and waiting portion should be notified
        UNIT_ASSERT(subscriber->FilterReady);
        UNIT_ASSERT(accumulator->IsDone());

        // Verify the filter content
        auto simpleFilter = subscriber->ReceivedFilter.BuildSimpleFilter();
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter[0], true);
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter[1], false);
    }

    Y_UNIT_TEST(RowCountersAccumulate) {
        TFiltersBuilder builder;
        builder.AddSource(1, 100);

        auto batch = MakeTestBatch(std::vector<int32_t>(100, 1), std::vector<int32_t>(100, 1));

        for (int i = 0; i < 5; ++i) {
            auto iter = MakeIterator(batch, 1);
            builder.AddRecord(iter);
        }
        for (int i = 0; i < 3; ++i) {
            auto iter = MakeIterator(batch, 1);
            builder.SkipRecord(iter);
        }

        UNIT_ASSERT_VALUES_EQUAL(builder.GetRowsAdded(), 5);
        UNIT_ASSERT_VALUES_EQUAL(builder.GetRowsSkipped(), 3);
    }

    Y_UNIT_TEST(AllRecordsSkipped) {
        auto subscriber = BuildAndDeliverFilter(1, 3, {false, false, false});

        UNIT_ASSERT(subscriber->FilterReady);
        auto simpleFilter = subscriber->ReceivedFilter.BuildSimpleFilter();
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter.size(), 3);
        for (size_t i = 0; i < simpleFilter.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(simpleFilter[i], false);
        }
    }

    Y_UNIT_TEST(AllRecordsAdded) {
        auto subscriber = BuildAndDeliverFilter(1, 3, {true, true, true});

        UNIT_ASSERT(subscriber->FilterReady);
        UNIT_ASSERT(subscriber->ReceivedFilter.IsTotalAllowFilter());
    }

    Y_UNIT_TEST(AlternatingPattern) {
        auto subscriber = BuildAndDeliverFilter(1, 6, {true, false, true, false, true, false});

        UNIT_ASSERT(subscriber->FilterReady);
        auto simpleFilter = subscriber->ReceivedFilter.BuildSimpleFilter();
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter.size(), 6);
        for (size_t i = 0; i < simpleFilter.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(simpleFilter[i], (i % 2 == 0));
        }
    }

    Y_UNIT_TEST(SingleRecordAdd) {
        auto subscriber = BuildAndDeliverFilter(1, 1, {true});

        UNIT_ASSERT(subscriber->FilterReady);
        UNIT_ASSERT(subscriber->ReceivedFilter.IsTotalAllowFilter());
    }

    Y_UNIT_TEST(SingleRecordSkip) {
        auto subscriber = BuildAndDeliverFilter(1, 1, {false});

        UNIT_ASSERT(subscriber->FilterReady);
        UNIT_ASSERT(subscriber->ReceivedFilter.IsTotalDenyFilter());
    }

    Y_UNIT_TEST(WaitingPortionNotNotifiedUntilComplete) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(1, 5, subscriber);
        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        TFiltersBuilder builder;
        builder.AddSource(1, 5);
        builder.AddWaitingPortion(1, accumulator);

        auto batch = MakeTestBatch(std::vector<int32_t>(5, 1), std::vector<int32_t>(5, 1));

        // Add 4 of 5 records
        for (int i = 0; i < 4; ++i) {
            auto iter = MakeIterator(batch, 1);
            builder.AddRecord(iter);
            UNIT_ASSERT(!subscriber->FilterReady);
        }

        // Add the 5th record - should trigger notification
        {
            auto iter = MakeIterator(batch, 1);
            builder.SkipRecord(iter);
        }
        UNIT_ASSERT(subscriber->FilterReady);
        UNIT_ASSERT(accumulator->IsDone());
    }

    Y_UNIT_TEST(MultiplePortionsWithWaiting) {
        auto subscriber1 = std::make_shared<TTestFilterSubscriber>();
        auto subscriber2 = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();

        auto request1 = MakeTestRequest(1, 2, subscriber1);
        auto request2 = MakeTestRequest(2, 2, subscriber2);

        auto acc1 = std::make_shared<TFilterAccumulator>(request1, counters);
        auto acc2 = std::make_shared<TFilterAccumulator>(request2, counters);

        TFiltersBuilder builder;
        builder.AddSource(1, 2);
        builder.AddSource(2, 2);
        builder.AddWaitingPortion(1, acc1);
        builder.AddWaitingPortion(2, acc2);

        auto batch1 = MakeTestBatch({10, 20}, {1, 1});
        auto batch2 = MakeTestBatch({30, 40}, {1, 1});

        // Complete portion 1
        {
            auto iter = MakeIterator(batch1, 1);
            builder.AddRecord(iter);
        }
        {
            auto iter = MakeIterator(batch1, 1);
            builder.AddRecord(iter);
        }
        UNIT_ASSERT(subscriber1->FilterReady);
        UNIT_ASSERT(!subscriber2->FilterReady);

        // Complete portion 2
        {
            auto iter = MakeIterator(batch2, 2);
            builder.SkipRecord(iter);
        }
        {
            auto iter = MakeIterator(batch2, 2);
            builder.SkipRecord(iter);
        }
        UNIT_ASSERT(subscriber2->FilterReady);

        // Verify filter contents
        UNIT_ASSERT(subscriber1->ReceivedFilter.IsTotalAllowFilter());
        UNIT_ASSERT(subscriber2->ReceivedFilter.IsTotalDenyFilter());
    }
}

Y_UNIT_TEST_SUITE(TFilterAccumulatorTests) {

    Y_UNIT_TEST(ConstructorSetsInitialState) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(42, 10, subscriber);

        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        UNIT_ASSERT(!accumulator->IsDone());
        UNIT_ASSERT(accumulator->GetRequest());
        UNIT_ASSERT_VALUES_EQUAL(accumulator->GetRequest()->Get()->GetPortionId(), 42);
        UNIT_ASSERT_VALUES_EQUAL(accumulator->GetRequest()->Get()->GetRecordsCount(), 10);

        // Clean up
        accumulator->Abort("test cleanup");
    }

    Y_UNIT_TEST(AddFilterSetsDone) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(1, 3, subscriber);

        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        UNIT_ASSERT(!accumulator->IsDone());

        NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, 2);
        filter.Add(false, 1);

        accumulator->AddFilter(std::move(filter));

        UNIT_ASSERT(accumulator->IsDone());
        UNIT_ASSERT(subscriber->FilterReady);
        UNIT_ASSERT_VALUES_EQUAL(subscriber->ReceivedFilter.GetRecordsCountVerified(), 3);
    }

    Y_UNIT_TEST(AbortSetsDone) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(1, 5, subscriber);

        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        UNIT_ASSERT(!accumulator->IsDone());

        accumulator->Abort("test error");

        UNIT_ASSERT(accumulator->IsDone());
        UNIT_ASSERT(subscriber->Failed);
        UNIT_ASSERT_VALUES_EQUAL(subscriber->FailureReason, "test error");
        UNIT_ASSERT(!subscriber->FilterReady);
    }

    Y_UNIT_TEST(DebugStringContainsPortionId) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(42, 10, subscriber);

        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        TString debugStr = accumulator->DebugString();
        UNIT_ASSERT(debugStr.Contains("42"));
        UNIT_ASSERT(debugStr.Contains("Done=0"));

        accumulator->Abort("cleanup");

        debugStr = accumulator->DebugString();
        UNIT_ASSERT(debugStr.Contains("Done=1"));
    }

    Y_UNIT_TEST(GetRequestReturnsOriginal) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(7, 100, subscriber);

        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        const auto& req = accumulator->GetRequest();
        UNIT_ASSERT_VALUES_EQUAL(req->Get()->GetPortionId(), 7);
        UNIT_ASSERT_VALUES_EQUAL(req->Get()->GetRecordsCount(), 100);
        UNIT_ASSERT_VALUES_EQUAL(req->Get()->GetSubscriber().get(), subscriber.get());

        accumulator->Abort("cleanup");
    }

    Y_UNIT_TEST(FilterContentPassedToSubscriber) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(1, 5, subscriber);

        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, 3);
        filter.Add(false, 2);

        accumulator->AddFilter(std::move(filter));

        UNIT_ASSERT(subscriber->FilterReady);
        auto simpleFilter = subscriber->ReceivedFilter.BuildSimpleFilter();
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter[0], true);
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter[1], true);
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter[2], true);
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter[3], false);
        UNIT_ASSERT_VALUES_EQUAL(simpleFilter[4], false);
    }

    Y_UNIT_TEST(AbortDoesNotDeliverFilter) {
        auto subscriber = std::make_shared<TTestFilterSubscriber>();
        auto counters = MakeTestCounters();
        auto request = MakeTestRequest(1, 3, subscriber);

        auto accumulator = std::make_shared<TFilterAccumulator>(request, counters);

        accumulator->Abort("some error");

        UNIT_ASSERT(accumulator->IsDone());
        UNIT_ASSERT(!subscriber->FilterReady);
        UNIT_ASSERT(subscriber->Failed);
        UNIT_ASSERT_VALUES_EQUAL(subscriber->FailureReason, "some error");
    }
}

}  // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
