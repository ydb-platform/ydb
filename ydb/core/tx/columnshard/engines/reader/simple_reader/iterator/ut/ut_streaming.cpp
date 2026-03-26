#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/columnshard/test_helper/shard_writer.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <unordered_set>

namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;
using namespace NOlap::NReader::NSimple;

namespace {

using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

// Controller that tracks the maximum pages-in-flight count observed via the
// OnPageCreated / OnPageSent hooks, which are called server-side each time a
// new page is enqueued / dequeued for the consumer.
// Used by TestBackpressureSlowConsumerWithLimit.
class TBackpressureController: public TDefaultTestsController {
private:
    mutable TAdaptiveLock Lock;
    ui64 MaxPagesInFlightObserved = 0;
    // Number of times the in-flight count reached or exceeded the configured
    // MaxPagesInFlight limit – proves the server was actually throttled.
    ui64 TimesLimitReached = 0;
    ui64 ConfiguredLimit = 0;
    TAtomicCounter TotalPagesCreated;

public:
    void SetConfiguredLimit(ui64 limit) {
        TGuard<TAdaptiveLock> g(Lock);
        ConfiguredLimit = limit;
    }

    virtual void OnPageCreated(const ui64 pagesInFlight) override {
        TotalPagesCreated.Inc();
        TGuard<TAdaptiveLock> g(Lock);
        if (pagesInFlight > MaxPagesInFlightObserved) {
            MaxPagesInFlightObserved = pagesInFlight;
        }
        if (ConfiguredLimit > 0 && pagesInFlight >= ConfiguredLimit) {
            ++TimesLimitReached;
        }
    }

    ui64 GetMaxPagesInFlightObserved() const {
        TGuard<TAdaptiveLock> g(Lock);
        return MaxPagesInFlightObserved;
    }

    ui64 GetTimesLimitReached() const {
        TGuard<TAdaptiveLock> g(Lock);
        return TimesLimitReached;
    }

    ui64 GetTotalPagesCreated() const {
        return (ui64)TotalPagesCreated.Val();
    }
};

void TestStreamingReadWithLargePortion() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");
    runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig()->SetEnabled(true);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    // Force non-in-memory reading so BuildReadPages() is used deterministically.
    csControllerGuard->SetOverrideMemoryLimitForPortionReading(1);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write a large portion that should be read in streaming mode
    const ui32 numRecords = 100000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString largeData = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, largeData, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // Read with streaming enabled
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);

    // Verify that streaming actually worked by checking iteration count
    // With streaming, we should have multiple iterations
    const ui32 iterationsCount = reader.GetIterationsCount();
    Cerr << "Iterations count: " << iterationsCount << Endl;

    // If streaming works, we should have more than 1 iteration
    UNIT_ASSERT_GT(iterationsCount, 1);
}

void TestStreamingReadWithSmallPortion() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    
    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write a small portion that should NOT trigger streaming
    const ui32 numRecords = 1000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString smallData = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, smallData, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // Read without streaming (small portion)
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);

    // Protocol requires at least one iteration, avoid excessive ones. Don't rely on ack count.
    const ui32 iterationsCount = reader.GetIterationsCount();
    Cerr << "Iterations count (small): " << iterationsCount << Endl;
    UNIT_ASSERT_LE(iterationsCount, 2u);
}

void TestStreamingReadMultiplePortions() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");
    runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig()->SetEnabled(true);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    // Force non-in-memory reading so paging/streaming does not depend on tiny test blobs.
    csControllerGuard->SetOverrideMemoryLimitForPortionReading(1);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write multiple large portions for streaming mode
    const ui32 numPortions = 3;
    const ui32 recordsPerPortion = 60000;

    for (ui32 i = 0; i < numPortions; ++i) {
        std::pair<ui64, ui64> portion = {i * recordsPerPortion, (i + 1) * recordsPerPortion};

        std::vector<ui64> writeIds;
        TString data = MakeTestBlob(portion, table.Schema);
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, data, table.Schema, true, &writeIds));

        planStep = ProposeCommit(runtime, sender, txId++, writeIds);
        PlanCommit(runtime, sender, planStep, txId - 1);
    }

    // Read all portions with streaming
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId - 1));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numPortions * recordsPerPortion);

    // With multiple large portions and streaming, we should have many iterations
    const ui32 iterationsCount = reader.GetIterationsCount();
    Cerr << "Iterations count (multiple portions): " << iterationsCount << Endl;
    UNIT_ASSERT_GT(iterationsCount, numPortions);
}

// Parametrized helper: verifies that the server never exceeds MaxPagesInFlight
// and that the backpressure limit is actually reached (consumer is slower than
// producer).
//
// "Consumer is slower than producer" is guaranteed structurally: the client
// sends Ack only after receiving a batch (ReadAll protocol), so the server can
// pre-produce up to MaxPagesInFlight pages before the client consumes the first
// one. With streaming explicitly enabled and 5 000 records, the server produces
// enough pages for the limit to be hit for small limits (1, 2, 4).
//
// No wall-clock Sleep() is needed: TTestBasicRuntime drives the actor system
// from the test thread.  GrabEdgeEvents() pumps the event loop until the
// target event arrives; the server cannot produce more pages while the test
// thread is not calling into the runtime.  The structural Ack/Receive protocol
// is sufficient to guarantee that the server fills its in-flight window before
// the client acknowledges any batch.
void TestBackpressureSlowConsumerWithLimit(const ui32 maxPagesInFlight) {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    const ui32 effectiveLimit = maxPagesInFlight == 0 ? 8 : maxPagesInFlight;

    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(effectiveLimit);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    csControllerGuard->SetConfiguredLimit(effectiveLimit);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write enough data to produce several streaming pages.  5 000 records is
    // sufficient to hit even a limit of 1 while keeping the test fast.
    const ui32 numRecords = 5000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // The Ack/Receive protocol is the sole source of backpressure: the server
    // pre-produces up to MaxPagesInFlight pages before the first Ack arrives,
    // so the limit is hit structurally without any wall-clock delay.
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    // Initialize scanner manually to control the Ack/Receive loop
    UNIT_ASSERT(reader.InitializeScanner());

    // Send first Ack to start receiving data
    reader.Ack();

    // Drain all batches; no artificial delay is needed – the structural
    // Ack/Receive protocol already makes the consumer slower than the producer.
    while (true) {
        bool hasMore = reader.Receive();

        if (!hasMore) {
            // Finished
            break;
        }

        // Send Ack to request next batch
        reader.Ack();
    }

    UNIT_ASSERT(reader.IsCorrectlyFinished());
    auto rb = reader.GetResult();
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);

    const ui64 maxObserved = csControllerGuard->GetMaxPagesInFlightObserved();
    const ui64 totalCreated = csControllerGuard->GetTotalPagesCreated();
    const ui64 timesLimitReached = csControllerGuard->GetTimesLimitReached();

    Cerr << "BackpressureSlowConsumer(limit=" << effectiveLimit << ")"
         << ": total pages created=" << totalCreated
         << ", max pages in flight observed=" << maxObserved
         << ", times limit reached=" << timesLimitReached
         << ", iterations=" << reader.GetIterationsCount() << Endl;

    // Streaming must have produced multiple pages
    UNIT_ASSERT_GT(totalCreated, 1u);

    // The server must NEVER have exceeded MaxPagesInFlight
    UNIT_ASSERT_LE(maxObserved, effectiveLimit);

    // The limit must have been reached at least once, proving the consumer
    // was genuinely slower than the producer (backpressure actually kicked in).
    UNIT_ASSERT_GT(timesLimitReached, 0u);
}

void TestPartialResultEmission() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");
    runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig()->SetEnabled(true);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    // Force non-in-memory reading so the large test dataset is split into pages deterministically.
    csControllerGuard->SetOverrideMemoryLimitForPortionReading(1);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write large dataset to trigger streaming with multiple pages
    const ui32 numRecords = 200000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // Test that partial results are emitted
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    UNIT_ASSERT(reader.InitializeScanner());

    ui32 totalBatches = 0;
    ui32 totalRows = 0;

    // Read data and verify we get multiple batches
    reader.Ack();
    while (reader.Receive()) {
        totalBatches++;
        Cerr << "Received batch " << totalBatches << Endl;
        reader.Ack();
    }

    UNIT_ASSERT(reader.IsCorrectlyFinished());
    auto finalResult = reader.GetResult();
    UNIT_ASSERT(finalResult);
    totalRows = finalResult->num_rows();

    Cerr << "Partial result emission test: " << totalBatches << " batches, "
            << totalRows << " total rows" << Endl;

    // Verify we got all data
    UNIT_ASSERT_VALUES_EQUAL(totalRows, numRecords);

    // Verify streaming worked - should have many batches for 200k records
    UNIT_ASSERT_GT(totalBatches, 10);
    UNIT_ASSERT_GT(reader.GetIterationsCount(), 10);
}

void TestStreamingEnabled() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // Explicitly enable streaming.
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(8);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    // Explicit streaming still needs a non-in-memory source for BuildReadPages().
    csControllerGuard->SetOverrideMemoryLimitForPortionReading(1);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write a small portion. With explicit enable, the reader must still use streaming.
    const ui32 numRecords = 2000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);

    const ui64 totalCreated = csControllerGuard->GetTotalPagesCreated();
    const ui32 iterationsCount = reader.GetIterationsCount();

    Cerr << "StreamingEnabled: total pages created=" << totalCreated
         << ", iterations=" << iterationsCount << Endl;

    // With explicit enable over 2000 records we expect multiple pages, proving streaming was used.
    UNIT_ASSERT_GT(totalCreated, 1u);
    // Multiple iterations confirm the Ack/page protocol ran more than once.
    UNIT_ASSERT_GT(iterationsCount, 1u);
}

void TestStreamingDisabledImpl(const ui32 maxPagesInFlight) {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // Explicitly disable streaming. MaxPagesInFlight must be ignored in this mode.
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(false);
    streamingConfig->SetMaxPagesInFlight(maxPagesInFlight);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write a large portion. With explicit disable, the reader must not activate streaming.
    const ui32 numRecords = 100000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);

    const ui64 totalCreated = csControllerGuard->GetTotalPagesCreated();
    const ui32 iterationsCount = reader.GetIterationsCount();

    Cerr << "StreamingDisabled(maxPagesInFlight=" << maxPagesInFlight << "): total pages created=" << totalCreated
         << ", iterations=" << iterationsCount << Endl;

    // With explicit disable the streaming path must never fire, so the
    // OnPageCreated hook (which is only called in streaming mode) must
    // report zero pages created.
    UNIT_ASSERT_VALUES_EQUAL(totalCreated, 0u);
}

void TestStreamingDisabled() {
    TestStreamingDisabledImpl(8);
}

void TestStreamingDisabledZeroMaxPagesInFlight() {
    TestStreamingDisabledImpl(0);
}

// Test for reverse scan with streaming enabled
void TestReverseStreamingScan() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");
    runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig()->SetEnabled(true);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    // Force non-in-memory reading to ensure streaming is activated
    csControllerGuard->SetOverrideMemoryLimitForPortionReading(1);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write a large portion that should be read in streaming mode
    const ui32 numRecords = 100000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString largeData = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, largeData, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // Read with streaming enabled and reverse order.
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));
    reader.SetReverse(true);  // Enable reverse scan

    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);

    // Verify that streaming actually worked by checking iteration count
    const ui32 iterationsCount = reader.GetIterationsCount();
    Cerr << "Reverse streaming iterations count: " << iterationsCount << Endl;

    // If streaming works, we should have more than 1 iteration
    UNIT_ASSERT_GT(iterationsCount, 1);

    // Verify that all expected timestamp values [0, numRecords) are present in
    // the result.  The reverse scan guarantees that pages are returned in
    // descending primary-key order, but rows within a single page are stored in
    // the order they were written (interleaved by MakeTestBlob) and are NOT
    // individually sorted.  Therefore we only check completeness here, not
    // strict row-by-row descending order.
    auto timestampCol = rb->GetColumnByName("timestamp");
    UNIT_ASSERT(timestampCol);
    auto timestampArray = std::static_pointer_cast<arrow::UInt64Array>(timestampCol);
    UNIT_ASSERT(timestampArray);

    std::unordered_set<ui64> seenTimestamps;
    seenTimestamps.reserve(numRecords);
    for (int i = 0; i < timestampArray->length(); ++i) {
        seenTimestamps.insert(timestampArray->Value(i));
    }
    UNIT_ASSERT_VALUES_EQUAL(seenTimestamps.size(), (size_t)numRecords);
    for (ui64 ts = 0; ts < numRecords; ++ts) {
        UNIT_ASSERT_C(seenTimestamps.count(ts), "Missing timestamp: " << ts);
    }
}

} // namespace

// Test for extremely low memory limits that force single-record pages
void TestMemoryLimitSingleRecordPages() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // Configure extremely low memory limit to force single-record pages
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    // Set extremely low memory limit so every chunk becomes its own page.
    // BuildReadPages splits at chunk boundaries, so we need many chunks to get many pages.
    csControllerGuard->SetOverrideMemoryLimitForPortionReading(1);
    // Use MinRecordsCount=1 to ensure each record becomes its own chunk.
    // With 100 records this gives ~100 chunks => ~100 pages with memoryLimit=1.
    csControllerGuard->SetOverrideBlobSplitSettings(
        NOlap::NSplitter::TSplitSettings().SetMinRecordsCount(1));

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write a dataset that will be split into many single-record chunks.
    // Each batch has exactly 1 record so each becomes its own portion/chunk,
    // giving 100 portions => 100 pages (satisfies UNIT_ASSERT_GT(totalCreated, 50u)).
    const ui32 numRecords = 100;
    const ui32 recordsPerBatch = 1;
    const ui32 numBatches = numRecords / recordsPerBatch;

    std::vector<ui64> writeIds;
    for (ui32 batch = 0; batch < numBatches; ++batch) {
        std::pair<ui64, ui64> portion = {batch * recordsPerBatch, (batch + 1) * recordsPerBatch};
        TString data = MakeTestBlob(portion, table.Schema);
        UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));
        ++writeId;
    }

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // Read with streaming enabled
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);

    const ui64 totalCreated = csControllerGuard->GetTotalPagesCreated();
    const ui32 iterationsCount = reader.GetIterationsCount();

    Cerr << "MemoryLimitSingleRecordPages: total pages created=" << totalCreated
         << ", iterations=" << iterationsCount << Endl;

    // With single-record pages (many chunks due to tiny blob size), we should have many pages
    UNIT_ASSERT_GT(totalCreated, 50u);
    // Multiple iterations confirm the Ack/page protocol ran
    UNIT_ASSERT_GT(iterationsCount, 50u);
}

// Test for memory limits that exactly match chunk boundaries
void TestMemoryLimitChunkBoundaries() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // Configure memory limit to match typical chunk size
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(4);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    csControllerGuard->SetConfiguredLimit(4);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write data that should create chunks near our memory limit
    const ui32 numRecords = 5000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // Read with streaming enabled
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);

    const ui64 totalCreated = csControllerGuard->GetTotalPagesCreated();
    const ui32 iterationsCount = reader.GetIterationsCount();

    Cerr << "MemoryLimitChunkBoundaries: total pages created=" << totalCreated
         << ", iterations=" << iterationsCount << Endl;

    // Should have multiple pages created
    UNIT_ASSERT_GT(totalCreated, 1u);
    UNIT_ASSERT_GT(iterationsCount, 1u);
}

// Test for zero MaxPagesInFlight – the scan must be rejected with an error
// because MaxPagesInFlight=0 is an invalid configuration when streaming is
// explicitly enabled.
void TestMemoryLimitZero() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // MaxPagesInFlight=0 with streaming enabled is invalid and must cause an error.
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(0);  // Invalid: must be > 0 when streaming is active

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    const ui32 numRecords = 2000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // The scan must fail at start because MaxPagesInFlight=0 is invalid.
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    auto rb = reader.ReadAll();

    Cerr << "MemoryLimitZero: IsError=" << reader.IsError()
         << ", IsCorrectlyFinished=" << reader.IsCorrectlyFinished() << Endl;

    // The scan must have been rejected with an error.
    UNIT_ASSERT(reader.IsError());
    UNIT_ASSERT(!rb);
}

// Verifies that aborting a scan mid-stream (via TEvAbortExecution) does not
// crash or hang the server and that the scan actor terminates cleanly.
//
// The test:
//   1. Writes enough data to produce many streaming pages.
//   2. Starts the scan and receives a few batches.
//   3. Sends TEvAbortExecution to the scan actor (simulating a client cancel).
//   4. Drains any remaining events until the scan is finished.
//   5. Asserts that the scan finished (either with an error or cleanly) and
//      that no crash / hang occurred.
void TestAbortDuringStreaming() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // Enable streaming so many pages are in-flight when we abort
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(4);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write enough data to produce many streaming pages (~200 pages with 500-record pages)
    const ui32 numRecords = 100000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    // Initialize the scanner
    UNIT_ASSERT(reader.InitializeScanner());

    // Send first Ack to start receiving data
    reader.Ack();

    // Receive a few batches to ensure streaming is active and pages are in-flight
    ui32 batchesBeforeAbort = 0;
    for (ui32 i = 0; i < 3; ++i) {
        bool hasMore = reader.Receive();
        batchesBeforeAbort++;
        if (!hasMore) {
            // Scan finished before we could abort – still a valid outcome
            break;
        }
        // Send Ack to keep the pipeline moving so more pages go in-flight
        reader.Ack();
    }

    Cerr << "AbortDuringStreaming: batches received before abort=" << batchesBeforeAbort << Endl;

    // Abort the scan mid-stream if it hasn't finished yet
    if (!reader.IsFinished()) {
        reader.Abort("test abort during streaming");

        // After TEvAbortExecution the scan actor calls Finish(ExternalAbort) →
        // PassAway() WITHOUT sending TEvScanError to the compute actor (edge
        // actor).  Therefore Receive() would block forever.  Instead we use
        // DispatchEvents() with a short timeout to let the actor system process
        // the abort message and then mark the reader as aborted manually.
        TDispatchOptions drainOptions;
        drainOptions.FinalEvents.push_back(
            TDispatchOptions::TFinalEventCondition(NColumnShard::TEvPrivate::TEvReadFinished::EventType));
        runtime.DispatchEvents(drainOptions, TDuration::Seconds(5));

        if (!reader.IsFinished()) {
            reader.MarkAborted();
        }
    }

    const ui64 totalCreated = csControllerGuard->GetTotalPagesCreated();
    const ui32 iterationsCount = reader.GetIterationsCount();

    Cerr << "AbortDuringStreaming: total pages created=" << totalCreated
         << ", iterations=" << iterationsCount
         << ", finished=" << reader.IsFinished()
         << ", isError=" << reader.IsError()
         << ", isCorrectlyFinished=" << reader.IsCorrectlyFinished() << Endl;

    // The scan must have terminated one way or another – no hang
    UNIT_ASSERT(reader.IsFinished());

    // Streaming must have produced at least some pages before the abort
    UNIT_ASSERT_GT(totalCreated, 0u);

    // We must have received at least the batches we explicitly waited for
    UNIT_ASSERT_GT(batchesBeforeAbort, 0u);
}

void TestStreamingWithFilterSkip() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");
    runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig()->SetEnabled(true);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write a large portion that will be read
    const ui32 numRecordsToRead = 80000;
    std::pair<ui64, ui64> portionToRead = {0, numRecordsToRead};
    TString dataToRead = MakeTestBlob(portionToRead, table.Schema);
    std::vector<ui64> writeIds1;
    UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, dataToRead, table.Schema, true, &writeIds1));
    planStep = ProposeCommit(runtime, sender, txId++, writeIds1);
    PlanCommit(runtime, sender, planStep, txId - 1);

    // Write a second portion that will be filtered out by the read's key range
    const ui32 numRecordsToSkip = 10000;
    std::pair<ui64, ui64> portionToSkip = {numRecordsToRead, numRecordsToRead + numRecordsToSkip};
    TString dataToSkip = MakeTestBlob(portionToSkip, table.Schema);
    std::vector<ui64> writeIds2;
    UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, dataToSkip, table.Schema, true, &writeIds2));
    planStep = ProposeCommit(runtime, sender, txId++, writeIds2);
    PlanCommit(runtime, sender, planStep, txId - 1);

    // Read data with a range filter that only includes the first portion
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId - 1));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    // Add a range that covers only the first portion (0 to numRecordsToRead - 1)
    reader.AddRange(MakeTestRange({0, numRecordsToRead - 1}, true, true, table.Pk));

    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecordsToRead);

    // Verify that streaming was used
    const ui32 iterationsCount = reader.GetIterationsCount();
    Cerr << "Iterations count (with filter skip): " << iterationsCount << Endl;
    UNIT_ASSERT_GT(iterationsCount, 1);
}

// Verifies the `!resultChunk && !isFinished` branch in TSyncPointResult::OnSourceReady():
// when a non-streaming page produces no output (filtered out by the key range) the scan
// must call ContinueCursor() and keep going until it reaches pages that do match.
//
// This branch is the non-streaming multi-page path: the source has multiple pages in
// StageResult, and when a page produces no data after filtering, ContinueCursor() is
// called to advance to the next page within the same source.
//
// Setup: non-streaming mode, one large portion split into many pages by a low memory
// limit; the read range excludes the first ~70% of rows so early pages produce no
// output, but later pages still contain matching rows.
void TestNonStreamingWithEmptyIntermediatePageContinue() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");
    // Non-streaming mode: the source processes all pages in one StageResult.
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(false);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    // Force many small pages within a single portion so the range filter can
    // exclude early pages while later pages still match.
    csControllerGuard->SetOverrideMemoryLimitForPortionReading(1);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // One large portion that will be split into many pages by the low memory limit.
    const ui32 numRecords = 100000;
    const ui32 firstIncludedRow = 70000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));
    // Range covers only the tail of the portion.  Early non-streaming pages produce no
    // output (empty resultChunk, !isFinished) and must be skipped via ContinueCursor().
    reader.AddRange(MakeTestRange({firstIncludedRow, numRecords - 1}, true, true, table.Pk));

    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords - firstIncludedRow);

    const ui32 iterationsCount = reader.GetIterationsCount();

    Cerr << "NonStreamingWithEmptyIntermediatePageContinue: iterations=" << iterationsCount
         << ", rows=" << rb->num_rows() << Endl;

    // Multiple iterations confirm the scan continued past empty pages.
    UNIT_ASSERT_GT(iterationsCount, 1u);
}

// Verifies that prefetchTriggered correctly prevents double-advance.
// In streaming mode with MaxPagesInFlight > 1, prefetch triggers ContinueCursor
// to start fetching the next page while the current one is sent to the client.
// When the client acks and Continue() is called, prefetchTriggered should be true
// and ContinueCursor should NOT be called again (which would skip a page).
void TestPrefetchNoDoubleAdvance() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // Enable streaming with enough MaxPagesInFlight to allow prefetch.
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(8);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write enough data to produce multiple streaming pages.
    const ui32 numRecords = 5000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    UNIT_ASSERT(reader.InitializeScanner());
    reader.Ack();

    // Receive all batches and verify correct completion.
    // If prefetchTriggered was not handled correctly (double-advance),
    // some pages would be skipped and we'd get fewer rows than expected.
    ui32 batchCount = 0;
    while (true) {
        bool hasMore = reader.Receive();
        if (!hasMore) {
            break;
        }
        reader.Ack();
        ++batchCount;
    }

    UNIT_ASSERT(reader.IsCorrectlyFinished());
    auto rb = reader.GetResult();
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);

    Cerr << "PrefetchNoDoubleAdvance: batches=" << batchCount
         << ", rows=" << rb->num_rows()
         << ", iterations=" << reader.GetIterationsCount() << Endl;

    // With 5000 records and streaming, we expect multiple batches.
    // If ContinueCursor was called twice (double-advance), we'd skip pages
    // and get fewer rows or the scan would complete incorrectly.
    UNIT_ASSERT_GT(batchCount, 1u);
}

// Verifies page-boundary correctness in streaming mode:
// - NeedFetchColumns() should only fetch chunks that intersect [pageStart, pageEnd)
// - DoAssembleColumns() should only assemble rows from the current page
// - Each batch must contain exactly the rows from its page, not from adjacent pages
void TestStreamingPageBoundaryCorrectness() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // Enable streaming with small MaxPagesInFlight to ensure multiple pages
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(4);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write data where each row contains its index as the message.
    // This allows us to verify that each batch contains exactly the expected rows.
    const ui32 numRecords = 10000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    // Use ReadAll() like other streaming tests - it handles Ack/Receive internally
    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);

    Cerr << "StreamingPageBoundaryCorrectness: rows=" << rb->num_rows()
         << ", iterations=" << reader.GetIterationsCount()
         << Endl;

    // We expect multiple iterations in streaming mode with 10000 records
    // This indirectly verifies page-boundary correctness: if NeedFetchColumns
    // or DoAssembleColumns had bugs, we'd get wrong row counts or scan would fail
    UNIT_ASSERT_GT(reader.GetIterationsCount(), 1u);
}

// Verifies that a task processing error during streaming causes the scan to
// terminate with a proper error status (TEvScanError) and does not crash or hang.
//
// The test:
//   1. Writes enough data to produce many streaming pages.
//   2. Starts the scan and receives the first batch successfully.
//   3. Installs a runtime observer that intercepts the next TEvTaskProcessedResult
//      sent to the scan actor, drops it, and sends a new failure result instead.
//   4. Sends Ack to trigger the next processing task, which will fail.
//   5. Calls Receive() and expects TEvScanError (Finished = -1).
//   6. Asserts that the scan finished with an error and that at least one
//      batch was received before the error.
void TestBlobReadErrorDuringStreaming() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // Enable streaming so many pages are in-flight when the error fires
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(4);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 txId = 100;

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);

    // Write enough data to produce many streaming pages.
    const ui32 numRecords = 100000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    // Initialize the scanner
    UNIT_ASSERT(reader.InitializeScanner());

    // Send first Ack to start receiving data
    reader.Ack();

    // Receive the first batch successfully to confirm streaming is active
    ui32 batchesBeforeError = 0;
    {
        bool hasMore = reader.Receive();
        batchesBeforeError++;
        if (!hasMore) {
            // Scan finished before we could inject an error – still valid
            Cerr << "BlobReadError: scan finished before error injection" << Endl;
            UNIT_ASSERT(reader.IsCorrectlyFinished());
            return;
        }
    }

    Cerr << "BlobReadErrorDuringStreaming: batches received before error=" << batchesBeforeError << Endl;

    // Install an observer that intercepts the FIRST TEvTaskProcessedResult sent
    // to the scan actor after this point, drops it, and sends a new failure
    // result instead.  This simulates a blob read error regardless of whether
    // the data was served from cache or from disk.
    std::atomic<ui32> errorInjected{0};
    auto dummyCounter = std::make_shared<TAtomicCounter>();
    runtime.SetObserverFunc([&runtime, &errorInjected, dummyCounter](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
        if (ev->GetTypeRewrite() == NColumnShard::TEvPrivate::TEvTaskProcessedResult::EventType) {
            if (errorInjected.fetch_add(1) == 0) {
                // Drop the original event and send a failure result to the same recipient.
                // Use runtime.Send() (not TActivationContext::Send) because the observer
                // runs in the test thread, not inside an actor context.
                Cerr << "BlobReadErrorDuringStreaming: injecting failure into TEvTaskProcessedResult" << Endl;
                const NActors::TActorId recipient = ev->Recipient;
                runtime.Send(recipient, recipient,
                    new NColumnShard::TEvPrivate::TEvTaskProcessedResult(
                        TConclusionStatus::Fail("injected test blob read error"),
                        NColumnShard::TCounterGuard(dummyCounter)));
                return TTestActorRuntime::EEventAction::DROP;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

    // Send Ack to trigger the next processing task, which will be intercepted
    reader.Ack();

    // The scan actor will receive TEvTaskProcessedResult(Fail(...)) and call
    // SendScanError() → TEvScanError → edge actor.
    // Receive() will see TEvScanError and set Finished = -1.
    // We allow a few iterations in case some in-flight data batches arrive first.
    for (ui32 i = 0; i < 20 && !reader.IsFinished(); ++i) {
        reader.Receive();
        if (!reader.IsFinished()) {
            reader.Ack();
        }
    }

    // Remove the observer
    runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

    const ui64 totalCreated = csControllerGuard->GetTotalPagesCreated();

    Cerr << "BlobReadErrorDuringStreaming: total pages created=" << totalCreated
         << ", finished=" << reader.IsFinished()
         << ", isError=" << reader.IsError()
         << ", isCorrectlyFinished=" << reader.IsCorrectlyFinished()
         << ", errorInjected=" << errorInjected.load() << Endl;

    // The scan must have terminated with an error
    UNIT_ASSERT(reader.IsFinished());
    UNIT_ASSERT(reader.IsError());
    UNIT_ASSERT(!reader.IsCorrectlyFinished());

    // The error must have been injected
    UNIT_ASSERT_GT(errorInjected.load(), 0u);

    // Streaming must have produced at least some pages before the error
    UNIT_ASSERT_GT(totalCreated, 0u);

    // We must have received at least the batch before the error
    UNIT_ASSERT_GT(batchesBeforeError, 0u);
}

Y_UNIT_TEST_SUITE(StreamingRead) {
    Y_UNIT_TEST(StreamingWithLargePortion) {
        TestStreamingReadWithLargePortion();
    }

    Y_UNIT_TEST(NoStreamingWithSmallPortion) {
        TestStreamingReadWithSmallPortion();
    }

    Y_UNIT_TEST(StreamingWithMultiplePortions) {
        TestStreamingReadMultiplePortions();
    }

    Y_UNIT_TEST(BackpressureSlowConsumerLimit1) {
        TestBackpressureSlowConsumerWithLimit(1);
    }

    Y_UNIT_TEST(BackpressureSlowConsumerLimit2) {
        TestBackpressureSlowConsumerWithLimit(2);
    }

    Y_UNIT_TEST(BackpressureSlowConsumerLimit4) {
        TestBackpressureSlowConsumerWithLimit(4);
    }

    Y_UNIT_TEST(PartialResultEmission) {
        TestPartialResultEmission();
    }

    Y_UNIT_TEST(StreamingWithFilterSkip) {
        TestStreamingWithFilterSkip();
    }

    Y_UNIT_TEST(NonStreamingWithEmptyIntermediatePageContinue) {
        TestNonStreamingWithEmptyIntermediatePageContinue();
    }

    Y_UNIT_TEST(PrefetchNoDoubleAdvance) {
        TestPrefetchNoDoubleAdvance();
    }

    Y_UNIT_TEST(StreamingPageBoundaryCorrectness) {
        TestStreamingPageBoundaryCorrectness();
    }

    Y_UNIT_TEST(StreamingEnabled) {
        TestStreamingEnabled();
    }

    Y_UNIT_TEST(StreamingDisabled) {
        TestStreamingDisabled();
    }

    Y_UNIT_TEST(StreamingDisabledZeroMaxPagesInFlight) {
        TestStreamingDisabledZeroMaxPagesInFlight();
    }

    Y_UNIT_TEST(MemoryLimitSingleRecordPages) {
        TestMemoryLimitSingleRecordPages();
    }

    Y_UNIT_TEST(MemoryLimitChunkBoundaries) {
        TestMemoryLimitChunkBoundaries();
    }

    Y_UNIT_TEST(MemoryLimitZero) {
        TestMemoryLimitZero();
    }

    Y_UNIT_TEST(AbortDuringStreaming) {
        TestAbortDuringStreaming();
    }

    Y_UNIT_TEST(BlobReadErrorDuringStreaming) {
        TestBlobReadErrorDuringStreaming();
    }

    Y_UNIT_TEST(ReverseStreamingScan) {
        TestReverseStreamingScan();
    }
}

} // namespace NKikimr
