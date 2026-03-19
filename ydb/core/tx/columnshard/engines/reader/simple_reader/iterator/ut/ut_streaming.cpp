#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/columnshard/test_helper/shard_writer.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

#include <library/cpp/testing/unittest/registar.h>

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

    // Write a large portion that should trigger streaming
    // MinRecordsForPaging default is 50000, so write more than that
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

    // With small portion, we expect minimal iterations
    // The protocol requires at least 2 Acks: one to start, one after receiving data
    const ui32 iterationsCount = reader.GetIterationsCount();
    Cerr << "Iterations count (small): " << iterationsCount << Endl;
    UNIT_ASSERT_VALUES_EQUAL(iterationsCount, 2u);
}

void TestStreamingReadMultiplePortions() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

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

    // Write multiple large portions
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

void TestBackpressure() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

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

    // Write a large portion that will trigger streaming
    const ui32 numRecords = 150000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString largeData = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, largeData, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // Test backpressure by controlling acknowledgments
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    // Initialize scanner
    UNIT_ASSERT(reader.InitializeScanner());

    // Send first Ack to start receiving data
    reader.Ack();

    ui32 receivedBatches = 0;

    // Simulate slow consumer: receive data but delay acknowledgments
    while (true) {
        bool hasMore = reader.Receive();
        receivedBatches++;

        if (!hasMore) {
            // Finished
            break;
        }

        Cerr << "Received batch " << receivedBatches << Endl;

        // Simulate processing delay before sending next Ack
        // This creates backpressure - the reader should not send more data
        // until we acknowledge
        if (receivedBatches % 3 == 0) {
            // Every 3rd batch, add a small delay to simulate slow processing
            Sleep(TDuration::MilliSeconds(10));
        }

        // Send Ack to request next batch
        reader.Ack();
    }

    UNIT_ASSERT(reader.IsCorrectlyFinished());
    auto finalResult = reader.GetResult();
    UNIT_ASSERT(finalResult);
    UNIT_ASSERT_VALUES_EQUAL(finalResult->num_rows(), numRecords);

    // Verify that we received data in multiple iterations (streaming worked)
    const ui32 iterationsCount = reader.GetIterationsCount();
    Cerr << "Total iterations with backpressure: " << iterationsCount << Endl;
    Cerr << "Total batches received: " << receivedBatches << Endl;

    // With backpressure and large data, we should have multiple iterations
    UNIT_ASSERT_GT(iterationsCount, 1);
    UNIT_ASSERT_GT(receivedBatches, 1);
}

// Parametrized helper: verifies that the server never exceeds MaxPagesInFlight
// and that the backpressure limit is actually reached (consumer is slower than
// producer).
//
// "Consumer is slower than producer" is guaranteed structurally: the client
// sends Ack only after receiving a batch (ReadAll protocol), so the server can
// pre-produce up to MaxPagesInFlight pages before the client consumes the first
// one.  With MinRecordsForPaging=500 and 100 000 records the server produces
// ~200 pages, so the limit will be hit many times.
void TestBackpressureSlowConsumerWithLimit(const ui32 maxPagesInFlight) {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    const ui32 effectiveLimit = maxPagesInFlight == 0 ? 8 : maxPagesInFlight;

    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetMaxPagesInFlight(effectiveLimit);
    // Small page size → many pages → limit is easy to hit
    streamingConfig->SetMinRecordsForPaging(500);
    streamingConfig->SetStrategy(NKikimrConfig::TColumnShardConfig::TStreamingConfig::STRATEGY_AUTO);

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

    // Write enough data to produce many streaming pages (~200 pages with 500-record pages)
    const ui32 numRecords = 100000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // ReadAll uses the Ack/Receive protocol: the client sends Ack only after
    // receiving a batch, so the server can pre-produce pages up to the limit
    // before the client consumes any – the consumer is genuinely slower.
    // To ensure backpressure is triggered, we use manual Ack/Receive with guaranteed delays.
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    // Initialize scanner manually to control the Ack/Receive loop
    UNIT_ASSERT(reader.InitializeScanner());

    // Send first Ack to start receiving data
    reader.Ack();

    // Process data with guaranteed delay to ensure consumer is slower than producer
    while (true) {
        bool hasMore = reader.Receive();

        if (!hasMore) {
            // Finished
            break;
        }

        // Add guaranteed delay to make consumer slower than producer
        // This ensures backpressure is actually triggered
        Sleep(TDuration::MilliSeconds(99));
        
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

void TestStrategyAlways() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // STRATEGY_ALWAYS: streaming must be used regardless of portion size.
    // Use a small MinRecordsForPaging so that even a tiny portion gets split
    // into multiple pages.
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetStrategy(NKikimrConfig::TColumnShardConfig::TStreamingConfig::STRATEGY_ALWAYS);
    streamingConfig->SetMinRecordsForPaging(500);
    streamingConfig->SetMaxPagesInFlight(8);

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

    // Write a SMALL portion – well below the default 50 000-record AUTO threshold.
    // With STRATEGY_ALWAYS the reader must still activate streaming.
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

    Cerr << "StrategyAlways: total pages created=" << totalCreated
         << ", iterations=" << iterationsCount << Endl;

    // With STRATEGY_ALWAYS and MinRecordsForPaging=500 over 2000 records we
    // expect at least 2 pages to have been created, proving streaming was used.
    UNIT_ASSERT_GT(totalCreated, 1u);
    // Multiple iterations confirm the Ack/page protocol ran more than once.
    UNIT_ASSERT_GT(iterationsCount, 1u);
}

void TestStrategyNever() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // STRATEGY_NEVER: streaming must be suppressed regardless of portion size.
    // We write a large portion that would normally trigger streaming under AUTO,
    // but with STRATEGY_NEVER no pages should be created via the streaming path.
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetStrategy(NKikimrConfig::TColumnShardConfig::TStreamingConfig::STRATEGY_NEVER);
    streamingConfig->SetMinRecordsForPaging(500);   // irrelevant for NEVER, but set explicitly
    streamingConfig->SetMaxPagesInFlight(8);

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

    // Write a LARGE portion – well above the default 50 000-record AUTO threshold.
    // With STRATEGY_NEVER the reader must NOT activate streaming.
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

    Cerr << "StrategyNever: total pages created=" << totalCreated
         << ", iterations=" << iterationsCount << Endl;

    // With STRATEGY_NEVER the streaming path must never fire, so the
    // OnPageCreated hook (which is only called in streaming mode) must
    // report zero pages created.
    UNIT_ASSERT_VALUES_EQUAL(totalCreated, 0u);
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
    streamingConfig->SetMinRecordsForPaging(1);  // Trigger streaming for any data
    streamingConfig->SetStrategy(NKikimrConfig::TColumnShardConfig::TStreamingConfig::STRATEGY_AUTO);

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
    streamingConfig->SetMinRecordsForPaging(1000);  // Trigger streaming
    streamingConfig->SetMaxPagesInFlight(4);
    streamingConfig->SetStrategy(NKikimrConfig::TColumnShardConfig::TStreamingConfig::STRATEGY_AUTO);

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
// enabled (strategy != Never).
void TestMemoryLimitZero() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // MaxPagesInFlight=0 with STRATEGY_AUTO is invalid and must cause an error.
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetMinRecordsForPaging(1000000);
    streamingConfig->SetMaxPagesInFlight(0);  // Invalid: must be > 0 when streaming is active
    streamingConfig->SetStrategy(NKikimrConfig::TColumnShardConfig::TStreamingConfig::STRATEGY_AUTO);

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

    // Use small pages so many are in-flight when we abort
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetMinRecordsForPaging(500);
    streamingConfig->SetMaxPagesInFlight(4);
    streamingConfig->SetStrategy(NKikimrConfig::TColumnShardConfig::TStreamingConfig::STRATEGY_AUTO);

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

    // Use small pages so many are in-flight when the error fires
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetMinRecordsForPaging(500);
    streamingConfig->SetMaxPagesInFlight(4);
    streamingConfig->SetStrategy(NKikimrConfig::TColumnShardConfig::TStreamingConfig::STRATEGY_AUTO);

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

    Y_UNIT_TEST(Backpressure) {
        TestBackpressure();
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

    Y_UNIT_TEST(StrategyAlways) {
        TestStrategyAlways();
    }

    Y_UNIT_TEST(StrategyNever) {
        TestStrategyNever();
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
}

} // namespace NKikimr
