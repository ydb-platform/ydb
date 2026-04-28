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
// OnPageCreated hook, which is invoked server-side each time a new streaming
// page is enqueued for the consumer.
// Used by TestBackpressureSlowConsumerWithLimit and several other streaming tests.
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

// Controller that tracks resource guard counts per streaming page.
// Used by TestResourceGuardsAccumulation to detect the bug where
// ResourceGuards are not cleared between streaming pages, causing
// monotonically growing guard counts.
class TResourceGuardTrackingController: public TDefaultTestsController {
private:
    mutable TAdaptiveLock Lock;
    std::vector<ui64> GuardCountsPerPage;
    std::vector<ui64> GuardMemoryPerPage;
    ui64 MaxGuardCount = 0;
    TAtomicCounter TotalPagesCreated;

public:
    virtual void OnPageCreated(const ui64 /*pagesInFlight*/) override {
        TotalPagesCreated.Inc();
    }

    virtual void OnStreamingPageResult(const ui64 resourceGuardsCount, const ui64 resourceGuardsMemory) override {
        TGuard<TAdaptiveLock> g(Lock);
        GuardCountsPerPage.push_back(resourceGuardsCount);
        GuardMemoryPerPage.push_back(resourceGuardsMemory);
        if (resourceGuardsCount > MaxGuardCount) {
            MaxGuardCount = resourceGuardsCount;
        }
    }

    ui64 GetTotalPagesCreated() const {
        return (ui64)TotalPagesCreated.Val();
    }

    std::vector<ui64> GetGuardCountsPerPage() const {
        TGuard<TAdaptiveLock> g(Lock);
        return GuardCountsPerPage;
    }

    std::vector<ui64> GetGuardMemoryPerPage() const {
        TGuard<TAdaptiveLock> g(Lock);
        return GuardMemoryPerPage;
    }

    ui64 GetMaxGuardCount() const {
        TGuard<TAdaptiveLock> g(Lock);
        return MaxGuardCount;
    }

    // Returns true if guard counts are monotonically increasing (indicating the bug).
    // A correct implementation should have roughly constant guard counts per page.
    bool IsGuardCountMonotonicallyIncreasing() const {
        TGuard<TAdaptiveLock> g(Lock);
        if (GuardCountsPerPage.size() < 3) {
            return false;
        }
        // Check if at least 3 consecutive pages show strictly increasing guard counts.
        // A run of 3 strictly increasing values requires 2 consecutive increasing transitions.
        ui32 currentRun = 1;
        for (size_t i = 1; i < GuardCountsPerPage.size(); ++i) {
            if (GuardCountsPerPage[i] > GuardCountsPerPage[i - 1]) {
                if (++currentRun >= 3) {
                    return true;
                }
            } else {
                currentRun = 1;
            }
        }
        return false;
    }
};

// Tracks per-page memory reservation sizes from DoStartReserveMemory (NSSA path).
// Without the page-range filter, each page would reserve memory for the entire
// portion (N × page_size), so reservations would grow linearly with page number.
// This controller is used to regression-test that the per-page filter is in place.
class TMemoryReservationTrackingController: public TDefaultTestsController {
private:
    mutable TAdaptiveLock Lock;
    std::vector<ui64> ReservationsPerPage;
    TAtomicCounter TotalPagesCreated;

public:
    virtual void OnPageCreated(const ui64 /*pagesInFlight*/) override {
        TotalPagesCreated.Inc();
    }

    virtual void OnStreamingMemoryReserved(const ui64 sizeToReserve) override {
        TGuard<TAdaptiveLock> g(Lock);
        ReservationsPerPage.push_back(sizeToReserve);
    }

    ui64 GetTotalPagesCreated() const {
        return (ui64)TotalPagesCreated.Val();
    }

    std::vector<ui64> GetReservationsPerPage() const {
        TGuard<TAdaptiveLock> g(Lock);
        return ReservationsPerPage;
    }

    ui64 GetMaxReservation() const {
        TGuard<TAdaptiveLock> g(Lock);
        if (ReservationsPerPage.empty()) {
            return 0;
        }
        return *std::max_element(ReservationsPerPage.begin(), ReservationsPerPage.end());
    }

    // Returns true if reservations are monotonically non-decreasing across all
    // pages and strictly increase at least once. This matches the regression
    // signature: without the page-range filter, every subsequent page reserves
    // memory for itself plus all remaining pages, so the per-page reservation
    // sequence grows linearly (k × size for the k-th page) and never decreases.
    bool IsReservationMonotonicallyIncreasing() const {
        TGuard<TAdaptiveLock> g(Lock);
        if (ReservationsPerPage.size() < 3) {
            return false;
        }
        bool sawStrictIncrease = false;
        for (size_t i = 1; i < ReservationsPerPage.size(); ++i) {
            if (ReservationsPerPage[i] < ReservationsPerPage[i - 1]) {
                return false;
            }
            if (ReservationsPerPage[i] > ReservationsPerPage[i - 1]) {
                sawStrictIncrease = true;
            }
        }
        return sawStrictIncrease;
    }
};

void TestMemoryReservationPerPage() {
    // This test verifies that DoStartReserveMemory (NSSA path) only reserves memory
    // for the current streaming page's chunks, not for the entire portion.
    //
    // Regression check: without the page-range filter in DoStartReserveMemory,
    // each page's reservation would cover all N pages worth of chunks, so the
    // reservation would grow linearly: page 1 → 1×size, page 2 → 2×size, ...,
    // page N → N×size.
    //
    // With the filter in place, each page should reserve approximately the same
    // amount (1 page worth of data), bounded by a small constant factor.
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(2);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TMemoryReservationTrackingController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    // Force non-in-memory reading so streaming pages are created.
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

    // Write enough data to produce many streaming pages.
    const ui32 numRecords = 10000;
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

    const ui64 totalPagesCreated = csControllerGuard->GetTotalPagesCreated();
    const auto reservations = csControllerGuard->GetReservationsPerPage();
    const ui64 maxReservation = csControllerGuard->GetMaxReservation();
    const bool isMonotonicallyIncreasing = csControllerGuard->IsReservationMonotonicallyIncreasing();

    Cerr << "MemoryReservationPerPage: totalPagesCreated=" << totalPagesCreated
         << ", reservationsCount=" << reservations.size()
         << ", maxReservation=" << maxReservation
         << ", isMonotonicallyIncreasing=" << isMonotonicallyIncreasing
         << ", reservationsPerPage=[";
    for (size_t i = 0; i < reservations.size(); ++i) {
        if (i > 0) Cerr << ",";
        Cerr << reservations[i];
        if (i >= 20) {
            Cerr << "...(" << reservations.size() - i - 1 << " more)";
            break;
        }
    }
    Cerr << "]" << Endl;

    // The NSSA path (DoStartReserveMemory) is only triggered when the program uses
    // the NSSA graph. If no reservations were recorded, the legacy path was used
    // instead and the regression assertions below would not run. Fail loudly in
    // that case so missing coverage is visible in CI rather than being silently
    // skipped (which would let real regressions in DoStartReserveMemory go
    // undetected).
    UNIT_ASSERT_GE_C(reservations.size(), 2u,
        "TestMemoryReservationPerPage: NSSA path (DoStartReserveMemory) was not exercised "
        "(reservations.size()=" << reservations.size() << "). The regression guard for the "
        "page-range filter cannot run under the current configuration. Adjust the test setup "
        "to force the NSSA graph path, otherwise this test is a no-op and hides regressions.");

    // Streaming must have produced multiple pages for this test to be meaningful.
    UNIT_ASSERT_GT(totalPagesCreated, 3u);

    // Regression check: if reservations are monotonically increasing, the
    // page-range filter in DoStartReserveMemory has regressed.
    // Without the filter: page K reserves K × page_size bytes (entire portion up to page K).
    // With the filter: each page reserves approximately 1 × page_size bytes.
    //
    // We use a soft check: the max reservation should not exceed 3× the first page's
    // reservation. With the bug, it would be N × firstPageReservation.
    if (reservations[0] > 0) {
        const ui64 firstPageReservation = reservations[0];
        const ui64 threshold = firstPageReservation * 3;
        Cerr << "MemoryReservationPerPage: firstPageReservation=" << firstPageReservation
             << ", maxReservation=" << maxReservation
             << ", threshold=" << threshold
             << ", BUG_DETECTED=" << (maxReservation > threshold ? "YES" : "NO") << Endl;

        UNIT_ASSERT_C(maxReservation <= threshold,
            "DoStartReserveMemory is reserving memory for the entire portion instead of the current page! "
            "maxReservation=" << maxReservation << " > threshold=" << threshold <<
            " (firstPageReservation=" << firstPageReservation << "). "
            "This indicates the page-range filter is missing in DoStartReserveMemory().");
    }

    // Additional check: reservations should NOT be monotonically increasing.
    UNIT_ASSERT_C(!isMonotonicallyIncreasing,
        "Memory reservations are monotonically increasing across streaming pages, "
        "confirming the whole-portion over-accounting bug in DoStartReserveMemory().");
}

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

// Reverse streaming smoke test with a small, bounded page count.
//
// Keeps the dataset small enough to finish quickly while still forcing multiple
// reverse-streaming pages (via memoryLimit=1 and MinRecordsCount=1). Validates
// that reverse streaming terminates correctly and returns all rows.
void TestReverseStreamingSmallBounded() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(2);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    csControllerGuard->SetConfiguredLimit(2);
    csControllerGuard->SetOverrideMemoryLimitForPortionReading(1);
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

    const ui32 numRecords = 32;
    std::vector<ui64> writeIds;
    for (ui32 i = 0; i < numRecords; ++i) {
        TString data = MakeTestBlob({i, i + 1}, table.Schema);
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, data, table.Schema, true, &writeIds));
    }

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));
    reader.SetReverse(true);

    auto rb = reader.ReadAll();

    Cerr << "ReverseStreamingSmallBounded: finished=" << reader.IsFinished()
         << ", isError=" << reader.IsError()
         << ", ackIterations=" << reader.GetIterationsCount()
         << ", totalPagesCreated=" << csControllerGuard->GetTotalPagesCreated()
         << ", maxPagesInFlightObserved=" << csControllerGuard->GetMaxPagesInFlightObserved()
         << Endl;

    UNIT_ASSERT(reader.IsCorrectlyFinished());
    UNIT_ASSERT(rb);
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);
    UNIT_ASSERT_GT(csControllerGuard->GetTotalPagesCreated(), 1u);
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
    // Force non-in-memory reading so streaming pages are created deterministically.
    // Without this, the default MemoryLimitScanPortion (~100MB) keeps the small test
    // blob in-memory, BuildReadPages() is bypassed, and the page/iteration assertions
    // below become vacuous.
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

    // Force non-in-memory reading so streaming pages are actually created and the
    // abort-mid-stream path is exercised deterministically. Without this, the
    // portion may be read in-memory (under the default ~100MB threshold) and
    // TotalPagesCreated stays at 0, making the assertion below vacuous/flaky.
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

    // Abort the scan mid-stream if it hasn't finished yet. Abort() transitions
    // the reader into the finished/error state internally, so we just need to
    // drain pending events so the abort is actually processed by the actor
    // system before the test inspects controller counters below.
    if (!reader.IsFinished()) {
        reader.Abort("test abort during streaming");

        TDispatchOptions drainOptions;
        drainOptions.FinalEvents.push_back(
            TDispatchOptions::TFinalEventCondition(NColumnShard::TEvPrivate::TEvReadFinished::EventType));
        runtime.DispatchEvents(drainOptions, TDuration::Seconds(5));
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

    // Force non-in-memory reading so streaming pages are created deterministically.
    // Without this, small per-row payloads can fit under the in-memory threshold and
    // the read completes in a single page, making the iterationsCount > 1 assertion flaky.
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

// Regression test for the dual-path streaming activation in IDataSource::Finalize().
// Streaming pages are computed early by TDecideStreamingModeStep. Finalize() must
// not rebuild a different page set and independently flip streaming mode later.
//
// This scenario stays on the streaming-safe projection path: one large portion,
// no predicate/range filtering, deterministic paging, and a bounded in-flight
// window. If Finalize() regresses to the late activation path, page accounting
// and iteration behavior become inconsistent.
void TestStreamingEarlyPagesNoLateRebuildConflict() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(2);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    csControllerGuard->SetConfiguredLimit(2);
    // Force non-in-memory reading so early pages are computed deterministically.
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

    const ui32 iterationsCount = reader.GetIterationsCount();
    const ui64 totalPagesCreated = csControllerGuard->GetTotalPagesCreated();
    const ui64 maxObserved = csControllerGuard->GetMaxPagesInFlightObserved();
    const ui64 timesLimitReached = csControllerGuard->GetTimesLimitReached();

    Cerr << "StreamingEarlyPagesNoLateRebuildConflict: iterations=" << iterationsCount
         << ", rows=" << rb->num_rows()
         << ", totalPagesCreated=" << totalPagesCreated
         << ", maxPagesInFlightObserved=" << maxObserved
         << ", timesLimitReached=" << timesLimitReached << Endl;

    // Multiple pages prove the early-page streaming path was actually used.
    UNIT_ASSERT_GT(totalPagesCreated, 1u);
    UNIT_ASSERT_GT(iterationsCount, 1u);
    // The bounded in-flight window must still hold; a late rebuild path would
    // bypass the intended per-page streaming behavior.
    UNIT_ASSERT_LE(maxObserved, 2u);
    UNIT_ASSERT_GT(timesLimitReached, 0u);
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
    // Force non-in-memory reading so streaming actually pages and the prefetch / ContinueCursor
    // logic is exercised. Without this, each portion can be served as a single in-memory
    // page, making batchCount==1 and turning the UNIT_ASSERT_GT(batchCount, 1u) check into a
    // false negative for the double-advance bug this test guards against.
    csControllerGuard->SetOverrideMemoryLimitForPortionReading(1);
    // Force the portion to split into many small chunks so BuildReadPages() yields multiple
    // pages deterministically, regardless of the natural chunk boundaries of the test data.
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

    // Write the data as multiple separate portions so that the scan produces multiple
    // client-visible batches (one batch per portion at minimum). A single large write
    // creates only one portion which is delivered as a single batch even when paged
    // internally — that would make UNIT_ASSERT_GT(batchCount, 1u) a false negative.
    const ui32 numPortions = 50;
    const ui32 recordsPerPortion = 100;
    const ui32 numRecords = numPortions * recordsPerPortion;

    std::vector<ui64> writeIds;
    for (ui32 i = 0; i < numPortions; ++i) {
        std::pair<ui64, ui64> portion = {i * recordsPerPortion, (i + 1) * recordsPerPortion};
        TString data = MakeTestBlob(portion, table.Schema);
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, data, table.Schema, true, &writeIds));
    }

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

    // With 50 portions of 100 records each and streaming, we expect multiple batches.
    // If ContinueCursor was called twice (double-advance), we'd skip pages
    // and get fewer rows or the scan would complete incorrectly.
    UNIT_ASSERT_GT(batchCount, 1u);
}

// Controller that records every streaming page advance performed inside
// IDataSource::ContinueCursor(). Used by TestPrefetchTriggeredRaceCondition to
// detect the actual signature of the prefetch / Continue() race: a double-advance
// of the same (sourceIdx, pageBefore) transition, or a non-contiguous page sequence.
class TPageAdvanceTrackingController: public TDefaultTestsController {
private:
    mutable TAdaptiveLock Lock;
    // (sourceIdx, pageBefore) -> count of times this transition occurred.
    THashMap<std::pair<ui64, ui32>, ui32> TransitionCounts;
    // Per-source ordered list of (pageBefore, pageAfter) transitions.
    THashMap<ui64, std::vector<std::pair<ui32, ui32>>> TransitionsBySource;
    THashMap<ui64, bool> ReverseBySource;
    TAtomicCounter TotalPagesCreated;

public:
    virtual void OnPageCreated(const ui64 /*pagesInFlight*/) override {
        TotalPagesCreated.Inc();
    }

    virtual void OnStreamingPageAdvance(
        const ui64 sourceIdx, const ui32 pageBefore, const ui32 pageAfter, const bool reverse) override {
        TGuard<TAdaptiveLock> g(Lock);
        TransitionCounts[std::make_pair(sourceIdx, pageBefore)] += 1;
        TransitionsBySource[sourceIdx].emplace_back(pageBefore, pageAfter);
        ReverseBySource[sourceIdx] = reverse;
    }

    ui64 GetTotalPagesCreated() const {
        return (ui64)TotalPagesCreated.Val();
    }

    // Returns the highest occurrence count for any (sourceIdx, pageBefore)
    // transition. In a correct implementation this must be exactly 1; values
    // greater than 1 prove a page was advanced more than once (the double-
    // advance signature of the prefetch/Continue race).
    ui32 GetMaxTransitionCount() const {
        TGuard<TAdaptiveLock> g(Lock);
        ui32 maxCount = 0;
        for (const auto& [_, count] : TransitionCounts) {
            maxCount = std::max(maxCount, count);
        }
        return maxCount;
    }

    ui64 GetTotalAdvances() const {
        TGuard<TAdaptiveLock> g(Lock);
        ui64 total = 0;
        for (const auto& [_, list] : TransitionsBySource) {
            total += list.size();
        }
        return total;
    }

    // Returns the first (sourceIdx, pageBefore -> pageAfter) transition that is
    // not strictly contiguous (pageAfter != pageBefore +/- 1 according to scan
    // direction), if any. Format: "src=X, before=Y, after=Z".
    std::optional<TString> FindNonContiguousTransition() const {
        TGuard<TAdaptiveLock> g(Lock);
        for (const auto& [src, list] : TransitionsBySource) {
            const bool reverse = ReverseBySource.at(src);
            for (const auto& [before, after] : list) {
                const ui32 expected = reverse ? before - 1 : before + 1;
                if (after != expected) {
                    return TStringBuilder() << "src=" << src << ", before=" << before
                                            << ", after=" << after
                                            << ", reverse=" << reverse;
                }
            }
        }
        return std::nullopt;
    }

    // Returns the (sourceIdx, pageBefore) of the first transition that occurred
    // more than once, if any.
    std::optional<TString> FindDuplicateTransition() const {
        TGuard<TAdaptiveLock> g(Lock);
        for (const auto& [key, count] : TransitionCounts) {
            if (count > 1) {
                return TStringBuilder() << "src=" << key.first
                                        << ", pageBefore=" << key.second
                                        << ", occurrences=" << count;
            }
        }
        return std::nullopt;
    }

    THashMap<ui64, std::vector<std::pair<ui32, ui32>>> GetTransitionsBySource() const {
        TGuard<TAdaptiveLock> g(Lock);
        return TransitionsBySource;
    }
};

// Race-condition regression test for TSyncPointResult::OnSourceReady() ordering
// of SetPrefetchTriggered(true) vs ContinueCursor():
//
//     simpleSource->SetPrefetchTriggered(true);   // must run BEFORE
//     simpleSource->ContinueCursor(source);       // potential synchronous re-entry
//
// ContinueCursor() schedules an async fetch task for the next streaming page.
// If that task can complete synchronously (e.g. cached data, single-threaded
// test runtime) and re-enter OnSourceReady before SetPrefetchTriggered(true)
// has executed, then a later ack-driven Continue() would observe
// PrefetchTriggered == false and call ContinueCursor() a second time on the
// same source — advancing the early-page index twice and skipping a page.
//
// Black-box "did all rows arrive?" assertions cannot reliably catch this:
// the symptom (a missing page worth of rows) only manifests when the race
// actually triggers, which is non-deterministic under TTestBasicRuntime.
//
// This test instead asserts the structural invariant directly via a controller
// hook (OnStreamingPageAdvance) wired into IDataSource::ContinueCursor():
//
//   * Each (sourceIdx, pageBefore) transition must occur EXACTLY ONCE.
//   * The page-index sequence per source must be strictly contiguous
//     (pageAfter == pageBefore + 1 for forward scans).
//   * The total advance count must equal totalPages - 1 per source.
//   * Every timestamp 0..numRecords-1 must appear EXACTLY ONCE in the result
//     (catches both skipped pages and duplicated pages).
//
// Configuration: streaming enabled with MaxPagesInFlight > 1 so the prefetch
// path in OnSourceReady is actually exercised, and memoryLimit=1 +
// MinRecordsCount=1 to force many small pages within a single source. The
// scan is driven via the explicit Ack/Receive loop (not ReadAll) so that
// any double-advance triggered between an ack and the next batch will be
// recorded by the controller.
void TestPrefetchTriggeredRaceCondition() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    // MaxPagesInFlight > 1 is REQUIRED: with limit=1 the prefetch branch in
    // OnSourceReady (lines 88-102 of result.cpp) is bypassed entirely and the
    // race window does not exist — making the test vacuous. Use 4 to actively
    // drive the prefetch path.
    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(4);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TPageAdvanceTrackingController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    // Force non-in-memory reading and small chunk boundaries so a single
    // portion is split into multiple streaming pages, exercising the
    // intra-source prefetch / advance loop. Combined with multi-portion writes
    // below this maximizes the chance that ContinueCursor() actually advances
    // an early-page index, which is the only path where the
    // SetPrefetchTriggered(true) / ContinueCursor() race can manifest.
    csControllerGuard->SetOverrideMemoryLimitForPortionReading(1);
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

    // Multiple portions => multiple sources, so OnPageCreated fires many times
    // (sanity that streaming is active) and the cross-source prefetch loop in
    // OnSourceReady is exercised even though each individual source produces
    // only one page (the intra-source advance path is rarely hit under
    // TTestBasicRuntime; see comment near the totalAdvances check below).
    const ui32 numPortions = 50;
    const ui32 recordsPerPortion = 100;
    const ui32 numRecords = numPortions * recordsPerPortion;
    std::vector<ui64> writeIds;
    for (ui32 i = 0; i < numPortions; ++i) {
        std::pair<ui64, ui64> portion = {i * recordsPerPortion, (i + 1) * recordsPerPortion};
        TString data = MakeTestBlob(portion, table.Schema);
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, data, table.Schema, true, &writeIds));
    }

    planStep = ProposeCommit(runtime, sender, txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));
    reader.SetReplyColumnIds(table.GetColumnIds({"timestamp", "message"}));

    // Drive the scan via the explicit Ack/Receive loop so the test thread yields
    // back to the runtime between every page. This maximizes the chance that
    // any racy interleaving of ContinueCursor() / SetPrefetchTriggered(true)
    // occurs while we are observing.
    UNIT_ASSERT(reader.InitializeScanner());
    reader.Ack();

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

    const ui64 totalPagesCreated = csControllerGuard->GetTotalPagesCreated();
    const ui64 totalAdvances = csControllerGuard->GetTotalAdvances();
    const ui32 maxTransitionCount = csControllerGuard->GetMaxTransitionCount();
    const auto duplicate = csControllerGuard->FindDuplicateTransition();
    const auto nonContiguous = csControllerGuard->FindNonContiguousTransition();
    const auto transitionsBySource = csControllerGuard->GetTransitionsBySource();

    Cerr << "PrefetchTriggeredRaceCondition: batches=" << batchCount
         << ", rows=" << rb->num_rows()
         << ", totalPagesCreated=" << totalPagesCreated
         << ", totalAdvances=" << totalAdvances
         << ", maxTransitionCount=" << maxTransitionCount
         << ", iterations=" << reader.GetIterationsCount() << Endl;

    // -------- Sanity: streaming was active --------
    // Streaming pages must have been emitted (sanity: streaming was active).
    UNIT_ASSERT_GT(totalPagesCreated, 1u);

    // The intra-source advance path (ContinueCursor advancing the early-page
    // index) is the ONLY place where the SetPrefetchTriggered/ContinueCursor
    // race can manifest.  If it didn't fire under this configuration we still
    // run the result-correctness check (4) below so the test stays useful as
    // a black-box smoke test, but we log a warning so that nobody assumes the
    // structural race-condition checks (1)-(3) executed.
    if (totalAdvances == 0) {
        Cerr << "PrefetchTriggeredRaceCondition: WARNING — intra-source page "
             << "advance path was not triggered (totalAdvances=0). Race-condition "
             << "structural checks are vacuous; relying on result-completeness "
             << "check only." << Endl;
    }

    // -------- The actual race-condition assertions --------
    // These run only when the intra-source page-advance path actually fired
    // (totalAdvances > 0). When it didn't, totalAdvances == 0 and the per-
    // transition map is empty; check (4) below still catches the bug's
    // observable symptom (skipped or duplicated rows) as a fallback.

    if (totalAdvances > 0) {
        // (1) No (sourceIdx, pageBefore) transition may occur more than once.
        //     A second occurrence is the exact signature of the prefetch /
        //     Continue() double-advance bug.
        UNIT_ASSERT_C(maxTransitionCount == 1,
            "Double-advance detected in IDataSource::ContinueCursor(): "
            << (duplicate ? *duplicate : TString("(unknown)"))
            << ". This is the prefetch / Continue() race: SetPrefetchTriggered(true) "
            "did not execute before ContinueCursor() re-entered OnSourceReady, so a "
            "subsequent ack-driven Continue() advanced the page a second time, "
            "skipping the page in between.");

        // (2) Each per-source advance sequence must be strictly contiguous.
        //     A gap (pageAfter != pageBefore + 1) would imply pages were silently
        //     skipped without going through ContinueCursor — a different breakage
        //     of the same invariant.
        UNIT_ASSERT_C(!nonContiguous.has_value(),
            "Non-contiguous page advance detected: " << nonContiguous.value_or("?"));

        // (3) Per source: the page-index sequence must be strictly 0,1,2,...,K-1.
        //     A double-advance would skip a page (e.g. 0->1 then 1->3 instead of
        //     1->2), which is also caught by (2), but we additionally pin the
        //     starting page to 0 to detect a missed first advance.
        for (const auto& [src, list] : transitionsBySource) {
            if (list.empty()) {
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(list.front().first, 0u,
                "First advance of source " << src << " did not start at page 0");
            const ui32 lastAfter = list.back().second;
            UNIT_ASSERT_VALUES_EQUAL_C(lastAfter, list.size(),
                "Source " << src << ": last pageAfter=" << lastAfter
                << " does not match advance count=" << list.size()
                << " (page sequence not strictly 0,1,2,...)");
        }
    }

    // (4) Result completeness: every timestamp must appear EXACTLY ONCE.
    //     A double-advance manifests as a missing range of timestamps; a
    //     duplicate emission would manifest as a timestamp count > 1. Both
    //     are caught here even if the controller-level checks somehow missed
    //     the race.
    UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), numRecords);
    auto timestampCol = rb->GetColumnByName("timestamp");
    UNIT_ASSERT(timestampCol);
    auto timestampArray = std::static_pointer_cast<arrow::UInt64Array>(timestampCol);
    UNIT_ASSERT(timestampArray);
    std::vector<ui32> seenCounts(numRecords, 0);
    for (int i = 0; i < timestampArray->length(); ++i) {
        const ui64 ts = timestampArray->Value(i);
        UNIT_ASSERT_LT_C(ts, numRecords, "Out-of-range timestamp: " << ts);
        ++seenCounts[ts];
    }
    for (ui32 ts = 0; ts < numRecords; ++ts) {
        UNIT_ASSERT_VALUES_EQUAL_C(seenCounts[ts], 1u,
            "Timestamp " << ts << " appears " << seenCounts[ts]
            << " times (expected exactly 1). Skipped or duplicated streaming page.");
    }

    // (5) Streaming pages must produce client-visible batches; if batches==0
    //     while advances > 0, something silently dropped page results.
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

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TBackpressureController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

    // Force out-of-memory reading so the portion is actually split into early pages.
    // Without this, 10 000 records fit under the default MemoryLimitScanPortion,
    // the source is marked in-memory, no early pages are produced, and the
    // pageStartRow/pageEndRow branches in NeedFetchColumns()/DoAssembleColumns()
    // are never executed — making the page-boundary assertions vacuous.
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

    // We expect multiple iterations in streaming mode with 10000 records.
    // This indirectly verifies page-boundary correctness: if NeedFetchColumns
    // or DoAssembleColumns had bugs, we'd get wrong row counts or scan would fail.
    UNIT_ASSERT_GT(reader.GetIterationsCount(), 1u);

    // Stronger guarantee: with the memory limit override above, the portion
    // must actually be split into multiple early pages. This ensures the
    // page-boundary code paths (pageStartRow/pageEndRow filtering) were
    // exercised, rather than the source being assembled as a single in-memory
    // batch.
    UNIT_ASSERT_GT(csControllerGuard->GetTotalPagesCreated(), 1u);
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
    // Force non-in-memory reading so the streaming page path is entered deterministically,
    // independent of the testing controller's default memory limit. Without this override,
    // small per-row payloads could fit under the in-memory threshold, the BuildReadPages()
    // path would be bypassed, totalPagesCreated would stay at 0, and the
    // UNIT_ASSERT_GT(totalCreated, 0u) assertion below would become vacuous (the test would
    // not actually cover the intended "error during streaming" behavior).
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

// Regression test: verifies that ResourceGuards do NOT accumulate across streaming pages.
//
// Historically, ContinueCursor() reset stage data for the next page but did not clear
// ResourceGuards. Each page's TAllocateMemoryStep appends new guards via
// RegisterAllocationGuard(); without clearing, guards would accumulate across pages:
//   - Page 0: N guards
//   - Page 1: 2*N guards (N from page 0 + N from page 1)
//   - Page 2: 3*N guards
//   - ...
//   - Page K: (K+1)*N guards
//
// This test tracks guard counts per page via the OnStreamingPageResult hook and
// asserts that counts stay roughly constant (i.e. guards are cleared between pages).
//
// EXPECTED BEHAVIOR (current, fixed): guard counts are roughly constant per page.
// REGRESSION SIGNATURE: guard counts grow monotonically with each page.
void TestResourceGuardsAccumulation() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    runtime.GetAppData(0).ColumnShardConfig.SetReaderClassName("SIMPLE");

    auto* streamingConfig = runtime.GetAppData(0).ColumnShardConfig.MutableStreamingConfig();
    streamingConfig->SetEnabled(true);
    streamingConfig->SetMaxPagesInFlight(2);

    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TResourceGuardTrackingController>();
    csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    // Force non-in-memory reading so streaming pages are created.
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

    // Write enough data to produce many streaming pages.
    // With memoryLimit=1, each chunk boundary becomes a page boundary.
    const ui32 numRecords = 10000;
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

    const ui64 totalPagesCreated = csControllerGuard->GetTotalPagesCreated();
    const auto guardCounts = csControllerGuard->GetGuardCountsPerPage();
    const auto guardMemory = csControllerGuard->GetGuardMemoryPerPage();
    const ui64 maxGuardCount = csControllerGuard->GetMaxGuardCount();
    const bool isMonotonicallyIncreasing = csControllerGuard->IsGuardCountMonotonicallyIncreasing();

    Cerr << "ResourceGuardsAccumulation: totalPagesCreated=" << totalPagesCreated
         << ", maxGuardCount=" << maxGuardCount
         << ", isMonotonicallyIncreasing=" << isMonotonicallyIncreasing
         << ", guardCountsPerPage=[";
    for (size_t i = 0; i < guardCounts.size(); ++i) {
        if (i > 0) Cerr << ",";
        Cerr << guardCounts[i];
        if (i >= 20) {
            Cerr << "...(" << guardCounts.size() - i - 1 << " more)";
            break;
        }
    }
    Cerr << "]" << Endl;

    Cerr << "ResourceGuardsAccumulation: guardMemoryPerPage=[";
    for (size_t i = 0; i < guardMemory.size(); ++i) {
        if (i > 0) Cerr << ",";
        Cerr << guardMemory[i];
        if (i >= 20) {
            Cerr << "...(" << guardMemory.size() - i - 1 << " more)";
            break;
        }
    }
    Cerr << "]" << Endl;

    // Streaming must have produced multiple pages for this test to be meaningful.
    UNIT_ASSERT_GT(totalPagesCreated, 3u);

    // Regression check: guard counts must not accumulate across streaming pages
    // with the current implementation, which clears ResourceGuards in ContinueCursor().
    // If this assertion starts failing, it likely means that fix regressed.
    //
    // We use a soft check: the max guard count across all pages should not exceed
    // 3x the guard count of the first page. With the bug, it would be N*firstPageGuards
    // where N is the number of pages.
    if (guardCounts.size() >= 2 && guardCounts[0] > 0) {
        const ui64 firstPageGuards = guardCounts[0];
        const ui64 threshold = firstPageGuards * 3;  // Allow some variance
        Cerr << "ResourceGuardsAccumulation: firstPageGuards=" << firstPageGuards
             << ", maxGuardCount=" << maxGuardCount
             << ", threshold=" << threshold
             << ", BUG_DETECTED=" << (maxGuardCount > threshold ? "YES" : "NO") << Endl;

        UNIT_ASSERT_C(maxGuardCount <= threshold,
            "ResourceGuards are accumulating across streaming pages! "
            "maxGuardCount=" << maxGuardCount << " > threshold=" << threshold <<
            " (firstPageGuards=" << firstPageGuards << "). "
            "This indicates ResourceGuards.clear() is missing in ContinueCursor().");
    }

    // Additional check: guard counts should NOT be monotonically increasing.
    UNIT_ASSERT_C(!isMonotonicallyIncreasing,
        "ResourceGuard counts are monotonically increasing across streaming pages, "
        "confirming the accumulation bug in ContinueCursor().");
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

    Y_UNIT_TEST(StreamingEarlyPagesNoLateRebuildConflict) {
        TestStreamingEarlyPagesNoLateRebuildConflict();
    }

    Y_UNIT_TEST(PrefetchNoDoubleAdvance) {
        TestPrefetchNoDoubleAdvance();
    }

    Y_UNIT_TEST(PrefetchTriggeredRaceCondition) {
        TestPrefetchTriggeredRaceCondition();
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

    Y_UNIT_TEST(ReverseStreamingSmallBounded) {
        TestReverseStreamingSmallBounded();
    }

    Y_UNIT_TEST(ResourceGuardsAccumulation) {
        TestResourceGuardsAccumulation();
    }

    Y_UNIT_TEST(MemoryReservationPerPage) {
        TestMemoryReservationPerPage();
    }
}

} // namespace NKikimr
