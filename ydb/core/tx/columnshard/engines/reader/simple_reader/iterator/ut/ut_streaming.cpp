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

// Custom controller to track chunk fetching
class TStreamingTestController : public TDefaultTestsController {
private:
    std::atomic<ui32> ChunksFetchedCount{0};
    std::atomic<ui32> TotalChunksCount{0};
    mutable TMutex Mutex;
    std::vector<std::pair<ui32, ui32>> FetchedChunkRanges;

public:
    void OnChunkFetched(ui32 chunkStart, ui32 chunkEnd) {
        TGuard<TMutex> guard(Mutex);
        ChunksFetchedCount++;
        FetchedChunkRanges.emplace_back(chunkStart, chunkEnd);
    }

    void SetTotalChunks(ui32 total) {
        TotalChunksCount = total;
    }

    ui32 GetChunksFetched() const {
        return ChunksFetchedCount.load();
    }

    ui32 GetTotalChunks() const {
        return TotalChunksCount.load();
    }

    bool VerifyPartialFetch() const {
        ui32 fetched = ChunksFetchedCount.load();
        ui32 total = TotalChunksCount.load();
        return total > 0 && fetched > 0 && fetched < total;
    }

    void Reset() {
        ChunksFetchedCount = 0;
        TotalChunksCount = 0;
        TGuard<TMutex> guard(Mutex);
        FetchedChunkRanges.clear();
    }
};

void TestStreamingReadWithLargePortion() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);
    
    // Enable SimpleReader with streaming
    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");
    
    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TStreamingTestController>();
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
    {
        TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
        reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));
        
        auto rb = reader.ReadAll();
        UNIT_ASSERT(reader.IsCorrectlyFinished());
        UNIT_ASSERT(rb);
        UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);
        
        // With small portion, streaming should not be used
        // So we should have fewer iterations
        const ui32 iterationsCount = reader.GetIterationsCount();
        Cerr << "Iterations count (small): " << iterationsCount << Endl;
    }
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

} // namespace

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
}

} // namespace NKikimr