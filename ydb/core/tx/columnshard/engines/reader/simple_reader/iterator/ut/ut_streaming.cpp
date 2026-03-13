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
    {
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
    {
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
    {
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
}

void TestBackpressureSlowConsumer() {
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

    // Write data that will trigger streaming
    const ui32 numRecords = 100000;
    std::pair<ui64, ui64> portion = {0, numRecords};
    
    std::vector<ui64> writeIds;
    TString data = MakeTestBlob(portion, table.Schema);
    UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, table.Schema, true, &writeIds));

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    // Test slow consumer with controlled acknowledgments
    {
        TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
        reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));
        
        // Initialize scanner
        UNIT_ASSERT(reader.InitializeScanner());
        
        ui32 batchCount = 0;
        
        // Manually control the read loop with delays between batches
        reader.Ack();
        while (reader.Receive()) {
            batchCount++;
            Cerr << "Received batch " << batchCount << Endl;
            
            // Simulate slow processing with delay every 2 batches
            if (batchCount % 2 == 0) {
                Sleep(TDuration::MilliSeconds(20));
            }
            
            // Send next Ack
            reader.Ack();
        }
        
        UNIT_ASSERT(reader.IsCorrectlyFinished());
        auto finalResult = reader.GetResult();
        UNIT_ASSERT(finalResult);
        UNIT_ASSERT_VALUES_EQUAL(finalResult->num_rows(), numRecords);
        
        Cerr << "Slow consumer test: received " << batchCount << " batches, "
             << "total iterations: " << reader.GetIterationsCount() << Endl;
        
        // Verify streaming worked with multiple batches
        UNIT_ASSERT_GT(batchCount, 1);
        UNIT_ASSERT_GT(reader.GetIterationsCount(), 1);
    }
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
    {
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
}

} // namespace

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
    {
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

    Y_UNIT_TEST(BackpressureSlowConsumer) {
        TestBackpressureSlowConsumer();
    }

    Y_UNIT_TEST(PartialResultEmission) {
        TestPartialResultEmission();
    }

    Y_UNIT_TEST(StreamingWithFilterSkip) {
        TestStreamingWithFilterSkip();
    }
}

} // namespace NKikimr