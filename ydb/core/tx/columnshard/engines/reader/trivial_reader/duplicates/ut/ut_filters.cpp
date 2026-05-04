#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/duplicates/filters.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <ydb/core/formats/arrow/reader/batch_iterator.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

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

// Helper to create a trivial arrow RecordBatch with a single int32 "key" column and a "version" column
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
        auto trivialFilter = subscriber->ReceivedFilter.BuildTrivialFilter();
        UNIT_ASSERT_VALUES_EQUAL(trivialFilter.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(trivialFilter[0], true);
        UNIT_ASSERT_VALUES_EQUAL(trivialFilter[1], true);
        UNIT_ASSERT_VALUES_EQUAL(trivialFilter[2], true);
        UNIT_ASSERT_VALUES_EQUAL(trivialFilter[3], false);
        UNIT_ASSERT_VALUES_EQUAL(trivialFilter[4], false);
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

}  // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
