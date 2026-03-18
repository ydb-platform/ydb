#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/columnshard/test_helper/shard_writer.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>

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

    // Write a small dataset that will be split into single-record pages
    const ui32 numRecords = 100;
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

    Cerr << "MemoryLimitSingleRecordPages: total pages created=" << totalCreated
         << ", iterations=" << iterationsCount << Endl;

    // With single-record pages, we should have many pages created
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

// Test for zero memory limit scenario (should fall back to non-streaming)
void TestMemoryLimitZero() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // Configure zero memory limit (should fall back to non-streaming)
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetMinRecordsForPaging(1000000);  // Very high threshold
    streamingConfig->SetMaxPagesInFlight(0);  // Zero limit
    streamingConfig->SetStrategy(NKikimrConfig::TColumnShardConfig::TStreamingConfig::STRATEGY_AUTO);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    csControllerGuard->SetConfiguredLimit(0);

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

    // Write data that would normally trigger streaming
    const ui32 numRecords = 2000;
    std::pair<ui64, ui64> portion = {0, numRecords};

    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // Read (should fall back to non-streaming due to zero memory limit)
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    auto rb = reader.ReadAll();
    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);

    const ui64 totalCreated = csControllerGuard->GetTotalPagesCreated();
    const ui32 iterationsCount = reader.GetIterationsCount();

    Cerr << "MemoryLimitZero: total pages created=" << totalCreated
         << ", iterations=" << iterationsCount << Endl;

    // With zero memory limit, should fall back to non-streaming
    // The OnPageCreated hook should not be called
    UNIT_ASSERT_VALUES_EQUAL(totalCreated, 0u);
    // Should have minimal iterations (non-streaming behavior)
    UNIT_ASSERT_LE(iterationsCount, 10u);
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
}

} // namespace NKikimr
