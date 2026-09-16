#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tablet/tablet_setup.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/hash_set.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

namespace {

constexpr ui32 ChannelsCount = 5;
constexpr ui64 TableId = 1;

// First-boot history: every channel has a single entry starting at generation 0.
TIntrusivePtr<TTabletStorageInfo> MakeOneEntryInfo(ui64 tabletId) {
    auto info = MakeIntrusive<TTabletStorageInfo>();
    info->TabletID = tabletId;
    info->TabletType = TTabletTypes::ColumnShard;
    info->Channels.resize(ChannelsCount);
    for (ui32 ch = 0; ch < ChannelsCount; ++ch) {
        info->Channels[ch].Channel = ch;
        info->Channels[ch].Type = TBlobStorageGroupType(TBlobStorageGroupType::ErasureNone);
        info->Channels[ch].History.emplace_back(0u, 0u);
    }
    return info;
}

// Reboot history: [0, secondFromGeneration) becomes a past entry and the second entry is active.
TIntrusivePtr<TTabletStorageInfo> MakeTwoEntryInfo(ui64 tabletId, ui32 secondFromGeneration) {
    auto info = MakeOneEntryInfo(tabletId);
    for (ui32 ch = 0; ch < ChannelsCount; ++ch) {
        info->Channels[ch].History.emplace_back(secondFromGeneration, 0u);
    }
    return info;
}

// Boots a ColumnShard without a bootstrapper; the launcher receives the tablet's TEvCutTabletHistory.
TActorId BootTablet(TTestBasicRuntime& runtime, TIntrusivePtr<TTabletStorageInfo> info, const TActorId& launcher) {
    auto setup = MakeIntrusive<TTabletSetupInfo>(&CreateColumnShard, TMailboxType::Simple, 0u, TMailboxType::Simple, 0u);
    const TActorId actorId = runtime.Register(CreateTablet(launcher, info.Get(), setup.Get(), 0), 0);
    TDispatchOptions opts;
    opts.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
    runtime.DispatchEvents(opts);
    runtime.SimulateSleep(TDuration::MilliSeconds(200));
    return actorId;
}

// Poisons the tablet and reboots it so that everything written up to firstGeneration sits in a past history entry.
void RebootWithOldEntry(TTestBasicRuntime& runtime, const TActorId& tabletActorId, const ui32 firstGeneration, const TActorId& launcher) {
    runtime.Send(new IEventHandle(tabletActorId, tabletActorId, new TEvents::TEvPoison()), 0);
    runtime.SimulateSleep(TDuration::Seconds(1));
    BootTablet(runtime, MakeTwoEntryInfo(TTestTxConfig::TxTablet0, firstGeneration + 1), launcher);
}

void DriveWakeups(TTestBasicRuntime& runtime, TActorId sender, const int count, const bool& stop) {
    for (int i = 0; i < count && !stop; ++i) {
        runtime.AdvanceCurrentTime(TDuration::Seconds(2));
        Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
        runtime.SimulateSleep(TDuration::MilliSeconds(200));
    }
}

class TCutHistoryTabletController: public NYDBTest::NColumnShard::TController {
public:
    bool IsCSCutHistoryEnabled() const override {
        return true;
    }
};

// The generation the tablet writes in during its first boot, and the data channels (>= 2) it puts blobs into.
struct TWrittenBlobs {
    ui32 Generation = 0;
    THashSet<ui32> DataChannels;
};

void ConfigureCutter(TTestBasicRuntime& runtime) {
    runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
    runtime.GetAppData().ColumnShardConfig.SetCutHistoryNominateCadenceSeconds(1);
}

}   // anonymous namespace

Y_UNIT_TEST_SUITE(TCutHistoryTablet) {
    // An old entry that holds live portion blobs must not be cut.
    Y_UNIT_TEST(LiveDataInOldGroupIsNotCut) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryTabletController>();
        const TActorId launcher = runtime.AllocateEdgeActor();
        ConfigureCutter(runtime);

        TWrittenBlobs written;
        TActorId tabletActorId;
        {
            auto putObserver = runtime.AddObserver<TEvBlobStorage::TEvPut>([&](TEvBlobStorage::TEvPut::TPtr& ev) {
                const TLogoBlobID& id = ev->Get()->Id;
                if (id.TabletID() == TTestTxConfig::TxTablet0) {
                    written.Generation = Max(written.Generation, id.Generation());
                    if (id.Channel() >= 2) {
                        written.DataChannels.insert(id.Channel());
                    }
                }
            });
            tabletActorId = BootTablet(runtime, MakeOneEntryInfo(TTestTxConfig::TxTablet0), launcher);
            TActorId sender = runtime.AllocateEdgeActor();
            Y_UNUSED(SetupSchema(runtime, sender, TableId));
            TestTableDescription table;
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, TTestTxConfig::TxTablet0, 1, TableId, MakeTestBlob({ 0, 1000 }, table.Schema), table.Schema, &writeIds));
            const auto planStep = ProposeCommit(runtime, sender, TTestTxConfig::TxTablet0, 1, writeIds);
            PlanCommit(runtime, sender, TTestTxConfig::TxTablet0, planStep, TSet<ui64>{ 1 });
            for (int i = 0; i < 10; ++i) {
                runtime.SimulateSleep(TDuration::Seconds(2));
            }
        }
        UNIT_ASSERT_C(!written.DataChannels.empty(), "the write must put portion blobs into data channels");

        RebootWithOldEntry(runtime, tabletActorId, written.Generation, launcher);

        THashSet<ui32> cutChannels;
        auto cutObserver = runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            if (ev->Get()->Record.GetFromGeneration() == 0) {
                cutChannels.insert(ev->Get()->Record.GetChannel());
            }
        });
        const bool neverStop = false;
        DriveWakeups(runtime, runtime.AllocateEdgeActor(), 25, neverStop);
        for (const ui32 channel : written.DataChannels) {
            UNIT_ASSERT_C(!cutChannels.contains(channel), "an old entry holding live portion blobs was cut, channel " << channel);
        }
    }

    // Hooks track portions through compaction and cleanup: an old entry is cut only once it holds no data.
    Y_UNIT_TEST(HooksTrackPortionsAcrossCompactionAndCleanup) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryTabletController>();
        const TActorId launcher = runtime.AllocateEdgeActor();
        ConfigureCutter(runtime);

        TWrittenBlobs written;
        TActorId tabletActorId;
        {
            auto putObserver = runtime.AddObserver<TEvBlobStorage::TEvPut>([&](TEvBlobStorage::TEvPut::TPtr& ev) {
                const TLogoBlobID& id = ev->Get()->Id;
                if (id.TabletID() == TTestTxConfig::TxTablet0) {
                    written.Generation = Max(written.Generation, id.Generation());
                    if (id.Channel() >= 2) {
                        written.DataChannels.insert(id.Channel());
                    }
                }
            });
            tabletActorId = BootTablet(runtime, MakeOneEntryInfo(TTestTxConfig::TxTablet0), launcher);
            TActorId sender = runtime.AllocateEdgeActor();
            Y_UNUSED(SetupSchema(runtime, sender, TableId));
            TestTableDescription table;
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, TTestTxConfig::TxTablet0, 1, TableId, MakeTestBlob({ 0, 1000 }, table.Schema), table.Schema, &writeIds));
            const auto planStep = ProposeCommit(runtime, sender, TTestTxConfig::TxTablet0, 1, writeIds);
            PlanCommit(runtime, sender, TTestTxConfig::TxTablet0, planStep, TSet<ui64>{ 1 });
            for (int i = 0; i < 10; ++i) {
                runtime.SimulateSleep(TDuration::Seconds(2));
            }
        }
        UNIT_ASSERT_C(!written.DataChannels.empty(), "write must put blobs into data channels");

        RebootWithOldEntry(runtime, tabletActorId, written.Generation, launcher);

        // After reboot with live data, the old entry must NOT be cut (hooks protect it).
        THashSet<ui32> cutChannels;
        auto cutObserver = runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            if (ev->Get()->Record.GetFromGeneration() == 0) {
                cutChannels.insert(ev->Get()->Record.GetChannel());
            }
        });
        const bool neverStop = false;
        DriveWakeups(runtime, runtime.AllocateEdgeActor(), 30, neverStop);
        for (const ui32 channel : written.DataChannels) {
            UNIT_ASSERT_C(
                !cutChannels.contains(channel), "a data channel with live compacted portions was incorrectly cut, channel " << channel);
        }
    }

    // After a reboot the seeded portionKeyCount equals the number of committed portions in memory.
    Y_UNIT_TEST(SeededCountersMatchInMemoryPortionCount) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        class TCountingController: public TCutHistoryTabletController {
        public:
            std::atomic<size_t> InMemoryPortionCount{ 0 };
            std::atomic<size_t> SeedingPortionKeyCount{ static_cast<size_t>(-1) };

            void DoOnTabletInitCompleted(const TColumnShard& shard) override {
                size_t count = 0;
                if (shard.HasIndex()) {
                    const auto& idx = shard.GetIndexAs<NOlap::TColumnEngineForLogs>();
                    for (const auto& [pathId, granule] : idx.GetTables()) {
                        count += granule->GetPortions().size();
                    }
                }
                InMemoryPortionCount.store(count);
            }

            void OnCutHistorySeedingCompleted(const size_t portionKeyCount) override {
                SeedingPortionKeyCount.store(portionKeyCount);
            }
        };

        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCountingController>();
        const TActorId launcher = runtime.AllocateEdgeActor();
        ConfigureCutter(runtime);

        TWrittenBlobs written;
        TActorId tabletActorId;
        {
            auto putObserver = runtime.AddObserver<TEvBlobStorage::TEvPut>([&](TEvBlobStorage::TEvPut::TPtr& ev) {
                const TLogoBlobID& id = ev->Get()->Id;
                if (id.TabletID() == TTestTxConfig::TxTablet0) {
                    written.Generation = Max(written.Generation, id.Generation());
                    if (id.Channel() >= 2) {
                        written.DataChannels.insert(id.Channel());
                    }
                }
            });
            tabletActorId = BootTablet(runtime, MakeOneEntryInfo(TTestTxConfig::TxTablet0), launcher);
            TActorId sender = runtime.AllocateEdgeActor();
            Y_UNUSED(SetupSchema(runtime, sender, TableId));
            TestTableDescription table;
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, TTestTxConfig::TxTablet0, 1, TableId, MakeTestBlob({ 0, 1000 }, table.Schema), table.Schema, &writeIds));
            const auto planStep = ProposeCommit(runtime, sender, TTestTxConfig::TxTablet0, 1, writeIds);
            PlanCommit(runtime, sender, TTestTxConfig::TxTablet0, planStep, TSet<ui64>{ 1 });
            for (int i = 0; i < 10; ++i) {
                runtime.SimulateSleep(TDuration::Seconds(2));
            }
        }
        UNIT_ASSERT_C(!written.DataChannels.empty(), "the write must put portion blobs into data channels");

        RebootWithOldEntry(runtime, tabletActorId, written.Generation, launcher);

        const size_t inMemoryCount = guard->InMemoryPortionCount.load();
        UNIT_ASSERT_C(inMemoryCount > 0, "in-memory portion count must be > 0 after writing rows");

        // Register cut observer before any wakeups so no cut slips by unnoticed.
        THashSet<ui32> cutChannels;
        auto cutObserver = runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            if (ev->Get()->Record.GetFromGeneration() == 0) {
                cutChannels.insert(ev->Get()->Record.GetChannel());
            }
        });

        // Drive until seeding completes, then a few more rounds to check for spurious cuts.
        const bool neverStop = false;
        DriveWakeups(runtime, runtime.AllocateEdgeActor(), 30, neverStop);
        const size_t seedingCount = guard->SeedingPortionKeyCount.load();
        UNIT_ASSERT_C(seedingCount != static_cast<size_t>(-1), "seeding must have completed");
        UNIT_ASSERT_VALUES_EQUAL_C(seedingCount, inMemoryCount, "seeded portionKeyCount must equal the in-memory portion count");

        DriveWakeups(runtime, runtime.AllocateEdgeActor(), 20, neverStop);
        for (const ui32 channel : written.DataChannels) {
            UNIT_ASSERT_C(!cutChannels.contains(channel), "an old entry holding live portion blobs must not be cut, channel " << channel);
        }
    }

    // With a tiny SeedBatchPortions override the seeding splits into many batches but still completes.
    Y_UNIT_TEST(SeedingCompletesWithSmallBatchSize) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        class TSmallBatchController: public TCutHistoryTabletController {
        public:
            std::atomic<size_t> SeedingPortionKeyCount{ static_cast<size_t>(-1) };

            ui64 GetSeedBatchPortions(const ui64 /*defaultValue*/) const override {
                return 1;
            }

            void OnCutHistorySeedingCompleted(const size_t portionKeyCount) override {
                SeedingPortionKeyCount.store(portionKeyCount);
            }
        };

        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TSmallBatchController>();
        const TActorId launcher = runtime.AllocateEdgeActor();
        ConfigureCutter(runtime);

        TWrittenBlobs written;
        TActorId tabletActorId;
        {
            auto putObserver = runtime.AddObserver<TEvBlobStorage::TEvPut>([&](TEvBlobStorage::TEvPut::TPtr& ev) {
                const TLogoBlobID& id = ev->Get()->Id;
                if (id.TabletID() == TTestTxConfig::TxTablet0) {
                    written.Generation = Max(written.Generation, id.Generation());
                    if (id.Channel() >= 2) {
                        written.DataChannels.insert(id.Channel());
                    }
                }
            });
            tabletActorId = BootTablet(runtime, MakeOneEntryInfo(TTestTxConfig::TxTablet0), launcher);
            TActorId sender = runtime.AllocateEdgeActor();
            Y_UNUSED(SetupSchema(runtime, sender, TableId));
            TestTableDescription table;
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, TTestTxConfig::TxTablet0, 1, TableId, MakeTestBlob({ 0, 1000 }, table.Schema), table.Schema, &writeIds));
            const auto planStep = ProposeCommit(runtime, sender, TTestTxConfig::TxTablet0, 1, writeIds);
            PlanCommit(runtime, sender, TTestTxConfig::TxTablet0, planStep, TSet<ui64>{ 1 });
            for (int i = 0; i < 10; ++i) {
                runtime.SimulateSleep(TDuration::Seconds(2));
            }
        }
        UNIT_ASSERT_C(!written.DataChannels.empty(), "the write must put portion blobs into data channels");

        RebootWithOldEntry(runtime, tabletActorId, written.Generation, launcher);

        // Register cut observer before any wakeups so no cut slips by unnoticed.
        THashSet<ui32> cutChannels;
        auto cutObserver = runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            if (ev->Get()->Record.GetFromGeneration() == 0) {
                cutChannels.insert(ev->Get()->Record.GetChannel());
            }
        });

        // Drive until seeding completes (many small batches → more iterations needed).
        const bool neverStop = false;
        DriveWakeups(runtime, runtime.AllocateEdgeActor(), 60, neverStop);
        const size_t seedingCount = guard->SeedingPortionKeyCount.load();
        UNIT_ASSERT_C(seedingCount != static_cast<size_t>(-1), "seeding must complete even with SeedBatchPortions=1");
        UNIT_ASSERT_C(seedingCount > 0, "seeded portionKeyCount must be > 0 after writing rows");

        // Old entry must not be cut (seeding found all portions across the many small batches).
        DriveWakeups(runtime, runtime.AllocateEdgeActor(), 20, neverStop);
        for (const ui32 channel : written.DataChannels) {
            UNIT_ASSERT_C(!cutChannels.contains(channel), "an old entry with seeded live portions must not be cut, channel " << channel);
        }
    }

    // A seeding error injected via the test hook transitions the cutter to Failed and raises OnCutHistorySeedingFailed.
    Y_UNIT_TEST(SeedingTxInjectedErrorTransitionsToFailed) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        class TInjectErrorController: public TCutHistoryTabletController {
        public:
            std::atomic<bool> FailedCalled{ false };
            std::atomic<int> CompletedCount{ 0 };

            TString GetSeedingInjectedErrorForTest() const override {
                return "test-injected-error";
            }

            void OnCutHistorySeedingFailed(const TString& /*reason*/) override {
                FailedCalled.store(true);
            }

            void OnCutHistorySeedingCompleted(const size_t /*portionKeyCount*/) override {
                CompletedCount.fetch_add(1);
            }
        };

        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TInjectErrorController>();
        const TActorId launcher = runtime.AllocateEdgeActor();
        ConfigureCutter(runtime);

        TWrittenBlobs written;
        TActorId tabletActorId;
        {
            auto putObserver = runtime.AddObserver<TEvBlobStorage::TEvPut>([&](TEvBlobStorage::TEvPut::TPtr& ev) {
                const TLogoBlobID& id = ev->Get()->Id;
                if (id.TabletID() == TTestTxConfig::TxTablet0) {
                    written.Generation = Max(written.Generation, id.Generation());
                    if (id.Channel() >= 2) {
                        written.DataChannels.insert(id.Channel());
                    }
                }
            });
            tabletActorId = BootTablet(runtime, MakeOneEntryInfo(TTestTxConfig::TxTablet0), launcher);
            TActorId sender = runtime.AllocateEdgeActor();
            Y_UNUSED(SetupSchema(runtime, sender, TableId));
            TestTableDescription table;
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, TTestTxConfig::TxTablet0, 1, TableId, MakeTestBlob({ 0, 1000 }, table.Schema), table.Schema, &writeIds));
            const auto planStep = ProposeCommit(runtime, sender, TTestTxConfig::TxTablet0, 1, writeIds);
            PlanCommit(runtime, sender, TTestTxConfig::TxTablet0, planStep, TSet<ui64>{ 1 });
            for (int i = 0; i < 10; ++i) {
                runtime.SimulateSleep(TDuration::Seconds(2));
            }
        }
        UNIT_ASSERT_C(!written.DataChannels.empty(), "the write must put portion blobs into data channels");

        RebootWithOldEntry(runtime, tabletActorId, written.Generation, launcher);
        // The first boot seeds an empty database and completes; only the run after the reboot reads portions.
        const int completedBeforeWakeups = guard->CompletedCount.load();

        const bool neverStop = false;
        DriveWakeups(runtime, runtime.AllocateEdgeActor(), 30, neverStop);

        UNIT_ASSERT_C(guard->FailedCalled.load(), "OnCutHistorySeedingFailed must be called when a seeding error is injected");
        UNIT_ASSERT_VALUES_EQUAL_C(guard->CompletedCount.load(), completedBeforeWakeups, "seeding must not complete after it failed");
    }

    // Seeding walks several batches when a batch holds one portion, and still completes.
    Y_UNIT_TEST(SeedingCompletesAcrossMultipleBatches) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        class TBatchCountController: public TCutHistoryTabletController {
        public:
            std::atomic<int> BatchCompletedCount{ 0 };
            std::atomic<bool> SeedingCompleted{ false };

            ui64 GetSeedBatchPortions(const ui64 /*defaultValue*/) const override {
                return 1;
            }

            void OnSeedingBatchCompleted(const size_t /*portionCount*/, const ui64 /*bytesCharged*/, const ui64 /*nextN*/) override {
                BatchCompletedCount.fetch_add(1);
            }

            void OnCutHistorySeedingCompleted(const size_t /*portionKeyCount*/) override {
                SeedingCompleted.store(true);
            }
        };

        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TBatchCountController>();
        const TActorId launcher = runtime.AllocateEdgeActor();
        ConfigureCutter(runtime);

        TWrittenBlobs written;
        TActorId tabletActorId;
        {
            auto putObserver = runtime.AddObserver<TEvBlobStorage::TEvPut>([&](TEvBlobStorage::TEvPut::TPtr& ev) {
                const TLogoBlobID& id = ev->Get()->Id;
                if (id.TabletID() == TTestTxConfig::TxTablet0) {
                    written.Generation = Max(written.Generation, id.Generation());
                    if (id.Channel() >= 2) {
                        written.DataChannels.insert(id.Channel());
                    }
                }
            });
            tabletActorId = BootTablet(runtime, MakeOneEntryInfo(TTestTxConfig::TxTablet0), launcher);
            TActorId sender = runtime.AllocateEdgeActor();
            Y_UNUSED(SetupSchema(runtime, sender, TableId));
            TestTableDescription table;
            // Write two separate batches to create two separately indexed portions.
            std::vector<ui64> writeIds1;
            UNIT_ASSERT(WriteData(
                runtime, sender, TTestTxConfig::TxTablet0, 1, TableId, MakeTestBlob({ 0, 500 }, table.Schema), table.Schema, &writeIds1));
            const auto planStep1 = ProposeCommit(runtime, sender, TTestTxConfig::TxTablet0, 1, writeIds1);
            PlanCommit(runtime, sender, TTestTxConfig::TxTablet0, planStep1, TSet<ui64>{ 1 });
            std::vector<ui64> writeIds2;
            UNIT_ASSERT(WriteData(
                runtime, sender, TTestTxConfig::TxTablet0, 2, TableId, MakeTestBlob({ 500, 1000 }, table.Schema), table.Schema, &writeIds2));
            const auto planStep2 = ProposeCommit(runtime, sender, TTestTxConfig::TxTablet0, 2, writeIds2);
            PlanCommit(runtime, sender, TTestTxConfig::TxTablet0, planStep2, TSet<ui64>{ 2 });
            for (int i = 0; i < 10; ++i) {
                runtime.SimulateSleep(TDuration::Seconds(2));
            }
        }
        UNIT_ASSERT_C(!written.DataChannels.empty(), "the writes must put portion blobs into data channels");

        RebootWithOldEntry(runtime, tabletActorId, written.Generation, launcher);

        const bool neverStop = false;
        DriveWakeups(runtime, runtime.AllocateEdgeActor(), 120, neverStop);

        UNIT_ASSERT_C(guard->SeedingCompleted.load(), "seeding must complete across several batches");
        UNIT_ASSERT_C(guard->BatchCompletedCount.load() > 0, "a non-final batch must be reported");
    }

    // An old entry that holds no data is cut once the first GC round of the new incarnation commits.
    Y_UNIT_TEST(EmptyOldEntryIsCutAfterFirstGcRound) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryTabletController>();
        const TActorId launcher = runtime.AllocateEdgeActor();
        ConfigureCutter(runtime);

        TWrittenBlobs written;
        TActorId tabletActorId;
        {
            auto putObserver = runtime.AddObserver<TEvBlobStorage::TEvPut>([&](TEvBlobStorage::TEvPut::TPtr& ev) {
                const TLogoBlobID& id = ev->Get()->Id;
                if (id.TabletID() == TTestTxConfig::TxTablet0) {
                    written.Generation = Max(written.Generation, id.Generation());
                }
            });
            tabletActorId = BootTablet(runtime, MakeOneEntryInfo(TTestTxConfig::TxTablet0), launcher);
            // The schema creates the index that background activities, and so the cutter, need; no data is written.
            TActorId sender = runtime.AllocateEdgeActor();
            Y_UNUSED(SetupSchema(runtime, sender, TableId));
        }
        UNIT_ASSERT_C(written.Generation > 0, "the first boot must write its log");

        RebootWithOldEntry(runtime, tabletActorId, written.Generation, launcher);

        bool sawOldEntryCut = false;
        auto cutObserver = runtime.AddObserver<TEvTablet::TEvCutTabletHistory>([&](TEvTablet::TEvCutTabletHistory::TPtr& ev) {
            if (ev->Get()->Record.GetFromGeneration() == 0 && ev->Get()->Record.GetChannel() >= 2) {
                sawOldEntryCut = true;
            }
        });
        DriveWakeups(runtime, runtime.AllocateEdgeActor(), 60, sawOldEntryCut);
        UNIT_ASSERT_C(sawOldEntryCut, "an empty old entry must be cut after the first GC round commits");
    }
}

}   // namespace NKikimr
