#include <ydb/core/base/tablet.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

using namespace NColumnShard;
using namespace NTxUT;
using namespace Tests;
using TController = NYDBTest::NColumnShard::TController;

struct TMetadataCounters {
    ui64 Total;
    ui64 Transactions;
    ui64 Scans;
    ui64 WaitingScans;
    ui64 Tables;
};

class TMetadataMemoryEnv {
public:
    TTestBasicRuntime Runtime;
    TActorId Sender;

    TMetadataMemoryEnv() {
        TTester::Setup(Runtime);
        Sender = Runtime.AllocateEdgeActor();
        CreateTestBootstrapper(Runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);
        TDispatchOptions options;
        options.FinalEvents.emplace_back([](IEventHandle& ev) {
            if (ev.GetTypeRewrite() != NNodeWhiteboard::TEvWhiteboard::EvTabletStateUpdate) {
                return false;
            }
            const auto& record = ev.Get<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>()->Record;
            return record.GetTabletId() == TTestTxConfig::TxTablet0 && record.GetState() == NKikimrWhiteboard::TTabletStateInfo::Active;
        }, 1);
        Runtime.DispatchEvents(options);
    }

    TMetadataCounters ReadCounters() {
        // Use the normal periodic sampling path, without advancing the scan deadline.
        ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, new TEvPrivate::TEvPeriodicWakeup(true));
        ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, new TEvTablet::TEvGetCounters());
        TAutoPtr<IEventHandle> handle;
        const auto* response = Runtime.GrabEdgeEvent<TEvTablet::TEvGetCountersResponse>(handle);
        const auto& counters = response->Record.GetTabletCounters().GetAppCounters();
        auto get = [&](TStringBuf name) -> ui64 {
            const TString fullName = TString("ColumnShard/MetadataMemory/") + name;
            for (const auto& counter : counters.GetSimpleCounters()) {
                if (counter.GetName() == fullName) {
                    return counter.GetValue();
                }
            }
            UNIT_FAIL("Missing metadata counter " << fullName);
            return 0;
        };
        TMetadataCounters result{
            get("EstimatedBytes"),
            get("TransactionsEstimatedBytes"),
            get("ScanRecordsEstimatedBytes"),
            get("WaitingScanRecordsEstimatedBytes"),
            get("TablesEstimatedBytes"),
        };
        UNIT_ASSERT_VALUES_EQUAL(result.Total, result.Transactions + result.Scans + result.WaitingScans + result.Tables);
        return result;
    }

    TMetadataCounters WaitForScans(ui64 scans, ui64 waitingScans) {
        for (size_t attempt = 0; attempt < 50; ++attempt) {
            const auto counters = ReadCounters();
            if (counters.Scans == scans && counters.WaitingScans == waitingScans) {
                return counters;
            }
            Runtime.SimulateSleep(TDuration::MilliSeconds(10));
        }
        const auto counters = ReadCounters();
        UNIT_ASSERT_VALUES_EQUAL(counters.Scans, scans);
        UNIT_ASSERT_VALUES_EQUAL(counters.WaitingScans, waitingScans);
        return counters;
    }

    void QueueScan(ui64 step, ui64 txId) {
        auto request = std::make_unique<TEvDataShard::TEvKqpScan>();
        request->Record.SetLocalPathId(1);
        request->Record.SetTxId(txId);
        request->Record.MutableSnapshot()->SetStep(step);
        request->Record.MutableSnapshot()->SetTxId(txId);
        ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, request.release());
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardMetadataMemory) {
    Y_UNIT_TEST(TtlVersionsCountOnlyNewEntries) {
        TTtlVersions versions;
        NKikimrSchemeOp::TColumnDataLifeCycle ttl;
        ttl.MutableDisabled();
        UNIT_ASSERT_VALUES_EQUAL(versions.GetMemoryUsage(), 0);
        const auto firstPath = TInternalPathId::FromRawValue(1);
        const auto secondPath = TInternalPathId::FromRawValue(2);
        versions.AddVersionFromProto(firstPath, NOlap::TSnapshot(10, 1), ttl);
        versions.AddVersionFromProto(firstPath, NOlap::TSnapshot(10, 1), ttl);
        UNIT_ASSERT_VALUES_EQUAL(versions.GetMemoryUsage(), sizeof(NOlap::TTiering));
        versions.AddVersionFromProto(firstPath, NOlap::TSnapshot(20, 2), ttl);
        versions.AddVersionFromProto(secondPath, NOlap::TSnapshot(10, 1), ttl);
        UNIT_ASSERT_VALUES_EQUAL(versions.GetMemoryUsage(), 3 * sizeof(NOlap::TTiering));
    }

    Y_UNIT_TEST(TablesAndRestart) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<TController>();
        controller->DisableBackground(NYDBTest::ICSController::EBackground::Cleanup);
        TMetadataMemoryEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.ReadCounters().Total, 0);

        Y_UNUSED(SetupSchema(env.Runtime, env.Sender, 1));
        const auto created = env.ReadCounters();
        UNIT_ASSERT_VALUES_EQUAL(created.Tables, sizeof(TTableInfo) + sizeof(NOlap::TTiering));
        UNIT_ASSERT_VALUES_EQUAL(created.Scans, 0);
        UNIT_ASSERT_VALUES_EQUAL(created.WaitingScans, 0);

        Y_UNUSED(SetupSchema(env.Runtime, env.Sender, TTestSchema::DropTableTxBody(1, 2), 20));
        const auto dropped = env.ReadCounters();
        // Drop retains the table until cleanup, and adds its PathsToDrop record.
        UNIT_ASSERT_VALUES_EQUAL(dropped.Tables, created.Tables + sizeof(ui64));
        RebootTablet(env.Runtime, TTestTxConfig::TxTablet0, env.Sender);
        const auto restarted = env.ReadCounters();
        UNIT_ASSERT_VALUES_EQUAL(restarted.Tables, dropped.Tables);
        UNIT_ASSERT_VALUES_EQUAL(restarted.Scans, 0);
        UNIT_ASSERT_VALUES_EQUAL(restarted.WaitingScans, 0);
    }

    Y_UNIT_TEST(ScanRecordsAndTeardown) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<TController>();
        TMetadataMemoryEnv env;
        Y_UNUSED(SetupSchema(env.Runtime, env.Sender, 1));
        const auto schema = TTestSchema::YdbSchema();
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(env.Runtime, env.Sender, 1, 1, MakeTestBlob({ 0, 10 }, schema), schema, true, &writeIds));
        const auto step = ProposeCommit(env.Runtime, env.Sender, 100, writeIds);
        PlanCommit(env.Runtime, env.Sender, step, 100);

        TShardReader reader(env.Runtime, TTestTxConfig::TxTablet0, 1, NOlap::TSnapshot(step, 200));
        reader.SetReplyColumnIds(TTestSchema::GetColumnIds(schema, { "timestamp", "message" }));
        UNIT_ASSERT(reader.InitializeScanner());
        const auto active = env.WaitForScans(sizeof(ui64) + sizeof(TInstant), 0);
        reader.Ack();
        reader.ContinueReadAll();
        UNIT_ASSERT(!reader.IsError());
        const auto finished = env.WaitForScans(0, 0);
        UNIT_ASSERT_VALUES_EQUAL(finished.Tables, active.Tables);

        constexpr ui64 waitingRecordBytes = sizeof(NOlap::TSnapshot) + sizeof(void*);
        env.QueueScan(step.Val() + 1000000, 300);
        env.WaitForScans(0, waitingRecordBytes);
        env.QueueScan(step.Val() + 1000000, 301);
        env.WaitForScans(0, 2 * waitingRecordBytes);
        RebootTablet(env.Runtime, TTestTxConfig::TxTablet0, env.Sender);
        const auto restarted = env.WaitForScans(0, 0);
        UNIT_ASSERT_VALUES_EQUAL(restarted.Tables, finished.Tables);
    }
}

}   // namespace NKikimr
