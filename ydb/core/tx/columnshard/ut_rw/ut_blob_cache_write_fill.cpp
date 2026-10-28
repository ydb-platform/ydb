#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;

namespace {

using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

// End-to-end write-fill checks against a real ColumnShard tablet and the node-local BLOB_CACHE actor
// registered by testlib (see ydb/core/testlib/basics/services.cpp). Assertions go through the actor's
// public counters (Adds / Forgets / Hits / StickyBlobs), the same ones an operator sees in monitoring.
class TWriteFillTester {
public:
    static constexpr ui64 TableId = 1;

    TWriteFillTester()
        : ControllerGuard(NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>())
    {
        TTester::Setup(Runtime);
        ControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

        Sender = Runtime.AllocateEdgeActor();
        CreateTestBootstrapper(Runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime.DispatchEvents(options);
    }

    void CreateTable(const bool cacheBlobsAfterWrite) {
        NKikimrTxColumnShard::TSchemaTxBody schemaTx;
        UNIT_ASSERT(schemaTx.ParseFromString(TTestSchema::CreateStandaloneTableTxBody(TableId, Table.Schema, Table.Pk)));
        schemaTx.MutableInitShard()->MutableTables(0)->MutableSchema()->MutableOptions()->SetCacheBlobsAfterWrite(cacheBlobsAfterWrite);
        PlanStep = SetupSchema(Runtime, Sender, schemaTx.SerializeAsString(), ++TxId);
    }

    void AlterTable(const bool cacheBlobsAfterWrite) {
        NKikimrTxColumnShard::TSchemaTxBody schemaTx;
        UNIT_ASSERT(schemaTx.ParseFromString(TTestSchema::AlterTableTxBody(TableId, true, ++SchemaVersion, Table.Schema, Table.Pk, {})));
        auto* schema = schemaTx.MutableAlterTable()->MutableSchema();
        // Without a bumped version the shard treats the ALTER as a duplicate of the current schema and ignores it.
        schema->SetVersion(SchemaVersion);
        schema->MutableOptions()->SetCacheBlobsAfterWrite(cacheBlobsAfterWrite);
        PlanStep = SetupSchema(Runtime, Sender, schemaTx.SerializeAsString(), ++TxId);
    }

    void Write(const std::pair<ui64, ui64>& keyRange) {
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(Runtime, Sender, ++WriteId, TableId, MakeTestBlob(keyRange, Table.Schema), Table.Schema, true, &writeIds));
        PlanStep = ProposeCommit(Runtime, Sender, ++TxId, writeIds);
        PlanCommit(Runtime, Sender, PlanStep, TxId);
        // Blob write results (and therefore TEvCacheBlobRange) are delivered asynchronously.
        Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    }

    void SetIcbCacheDataAfterIndexing(const bool enabled) {
        TControlBoard::SetValue(enabled ? 1 : 0, Runtime.GetAppData(0).Icb->ColumnShardControls.CacheDataAfterIndexing);
    }

    void Compact() {
        ControllerGuard->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, new TEvDataShard::TEvCompactTable(1, TableId));
        TEvDataShard::TEvCompactTableResult::TPtr compacted;
        for (ui32 attempt = 0; attempt < 30 && !compacted; ++attempt) {
            ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, new TEvPrivate::TEvPeriodicWakeup(true));
            Runtime.SimulateSleep(TDuration::Seconds(1));
            compacted = Runtime.GrabEdgeEvent<TEvDataShard::TEvCompactTableResult>(Sender, TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT(compacted);
        UNIT_ASSERT_VALUES_EQUAL(compacted->Get()->Record.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
        ControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        // Let the write-index tx complete: this is where the compaction output is write-filled.
        Runtime.SimulateSleep(TDuration::Seconds(1));
    }

    // Compaction only marks the source portions as removed; their blobs are declared for deletion (and forgotten
    // by the cache) later, by the portion-cleanup task, once no snapshot can still read them. Drive that here:
    // drop snapshot staleness limits and advance simulated time / plan steps until the cache reports a Forget.
    void WaitForgets() {
        ControllerGuard->SetOverrideMaxReadStaleness(TDuration::Zero());
        ControllerGuard->SetOverrideStalenessLivetimePing(TDuration::Zero());
        ControllerGuard->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, new TEvPrivate::TEvPingSnapshotsUsage());
        for (ui32 attempt = 0; attempt < 60 && Forgets() == 0; ++attempt) {
            Runtime.SimulateSleep(TDuration::Seconds(1));
            // A committed write advances the plan step, which is what the cleanup staleness checks look at.
            Write({ 1000 + attempt, 1001 + attempt });
            ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, new TEvPrivate::TEvPeriodicWakeup(true));
        }
        UNIT_ASSERT_C(Forgets() > 0,
            "cleanup of compacted portions did not forget any blob; cleanups started: " << ControllerGuard->GetCleaningStartedCounter().Val());
    }

    ui64 ReadAllRows() {
        TShardReader reader(Runtime, TTestTxConfig::TxTablet0, TableId, NOlap::TSnapshot(PlanStep, Max<ui64>()));
        reader.SetReplyColumnIds(Table.GetColumnIds({ "timestamp" }));
        auto batch = reader.ReadAll();
        UNIT_ASSERT(reader.IsCorrectlyFinished());
        return batch ? batch->num_rows() : 0;
    }

    i64 BlobCacheCounter(const char* name, const bool derivative) {
        return Runtime.GetDynamicCounters(0)->GetSubgroup("type", "BLOB_CACHE")->GetCounter(name, derivative)->Val();
    }

    i64 Adds() {
        return BlobCacheCounter("Adds", true);
    }

    i64 Forgets() {
        return BlobCacheCounter("Forgets", true);
    }

    i64 Hits() {
        return BlobCacheCounter("Hits", true);
    }

    i64 StickyBlobs() {
        return BlobCacheCounter("StickyBlobs", false);
    }

    i64 SizeBlobs() {
        return BlobCacheCounter("SizeBlobs", false);
    }

    TTestBasicRuntime Runtime;
    TActorId Sender;
    NKikimr::NYDBTest::TControllers::TGuard<TDefaultTestsController> ControllerGuard;
    TestTableDescription Table;
    TPlanStep PlanStep;
    ui64 TxId = 10;
    ui64 WriteId = 0;
    ui32 SchemaVersion = 1;
};

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardBlobCacheWriteFill) {
    Y_UNIT_TEST(WriteFillsCacheWhenOptedIn) {
        TWriteFillTester tester;
        tester.CreateTable(true);
        UNIT_ASSERT_VALUES_EQUAL(tester.Adds(), 0);

        tester.Write({ 0, 100 });
        UNIT_ASSERT_C(tester.Adds() > 0, "write must fill the blob cache when the table opted in");
        UNIT_ASSERT_C(tester.StickyBlobs() > 0, "write-filled blobs must be sticky");
        UNIT_ASSERT_VALUES_EQUAL(tester.SizeBlobs(), tester.StickyBlobs());

        const i64 hitsBefore = tester.Hits();
        UNIT_ASSERT_VALUES_EQUAL(tester.ReadAllRows(), 100);
        UNIT_ASSERT_C(tester.Hits() > hitsBefore, "the first scan after a write-fill must be served from the cache");
    }

    Y_UNIT_TEST(WriteDoesNotFillCacheWithoutOptIn) {
        TWriteFillTester tester;
        tester.CreateTable(false);
        tester.Write({ 0, 100 });
        UNIT_ASSERT_VALUES_EQUAL(tester.Adds(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tester.StickyBlobs(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tester.ReadAllRows(), 100);
    }

    Y_UNIT_TEST(IcbOffDisablesWriteFill) {
        TWriteFillTester tester;
        tester.CreateTable(true);

        tester.SetIcbCacheDataAfterIndexing(false);
        tester.Write({ 0, 100 });
        UNIT_ASSERT_VALUES_EQUAL_C(tester.Adds(), 0, "ICB CacheDataAfterIndexing=0 must win over the schema opt-in");

        tester.SetIcbCacheDataAfterIndexing(true);
        tester.Write({ 100, 200 });
        UNIT_ASSERT_C(tester.Adds() > 0, "re-enabling the ICB switch must resume write-fill without a restart");
    }

    Y_UNIT_TEST(CompactionForgetsOldBlobsAndFillsNew) {
        TWriteFillTester tester;
        tester.CreateTable(true);
        tester.Write({ 0, 100 });
        tester.Write({ 0, 100 });
        const i64 addsAfterWrites = tester.Adds();
        UNIT_ASSERT_C(addsAfterWrites > 0, "writes must fill the cache");
        UNIT_ASSERT_VALUES_EQUAL(tester.Forgets(), 0);

        tester.Compact();
        UNIT_ASSERT_C(tester.Adds() > addsAfterWrites, "compaction output must be write-filled");
        UNIT_ASSERT_VALUES_EQUAL(tester.ReadAllRows(), 100);

        // Source blobs are forgotten when the removed portions are cleaned up, not at compaction commit.
        tester.WaitForgets();
    }

    Y_UNIT_TEST(CompactionFillsAfterAlterOptIn) {
        // Data written before ALTER carries a schema without the opt-in; compaction writes its result with the
        // newest schema, so the gate must consult the last schema (not the source portions') to fill the cache.
        TWriteFillTester tester;
        tester.CreateTable(false);
        tester.Write({ 0, 100 });
        tester.Write({ 0, 100 });
        UNIT_ASSERT_VALUES_EQUAL(tester.Adds(), 0);

        tester.AlterTable(true);
        tester.Compact();
        UNIT_ASSERT_C(tester.Adds() > 0, "compaction after ALTER ... CACHE_BLOBS_AFTER_WRITE must fill the cache");
        UNIT_ASSERT_VALUES_EQUAL(tester.ReadAllRows(), 100);
    }
}

}   // namespace NKikimr
