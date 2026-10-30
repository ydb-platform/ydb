#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tx_helpers.h>

#include <initializer_list>

namespace NKikimr::NStat {
namespace {

struct TStatsEntry {
    ui64 LocalPathId;
    ui64 RowCount;
    ui64 BytesSize;
    bool Full = true;
};

struct TTotals {
    ui64 RowCount = 0;
    ui64 BytesSize = 0;
};

class TCountersEnv {
public:
    TCountersEnv()
        : Env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.FeatureFlags.SetEnableColumnStatistics(false);
        })
        , Runtime(*Env.GetServer().GetRuntime())
        , AutomaticReports(Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetSchemeShardId() > 2;
        })
        , Sender(Runtime.AllocateEdgeActor())
    {
        CreateDatabase(Env, "Database");
        ResolvePathId(Runtime, "/Root/Database", nullptr, &TabletId);
        UNIT_ASSERT(TabletId);
        Connect();
        CheckCounters(0, 0);
    }

    void SendStats(ui64 schemeShardId, std::initializer_list<TStatsEntry> entries, bool full = true) {
        NKikimrStat::TSchemeShardStats stats;
        stats.SetAreAllStatsFull(full);
        for (const auto& entry : entries) {
            auto* proto = stats.AddEntries();
            proto->MutablePathId()->SetOwnerId(schemeShardId);
            proto->MutablePathId()->SetLocalId(entry.LocalPathId);
            proto->SetRowCount(entry.RowCount);
            proto->SetBytesSize(entry.BytesSize);
            proto->SetAreStatsFull(entry.Full);
        }
        auto event = std::make_unique<TEvStatistics::TEvSchemeShardStats>();
        event->Record.SetSchemeShardId(schemeShardId);
        UNIT_ASSERT(stats.SerializeToString(event->Record.MutableStats()));
        Runtime.SendToPipe(Pipe, Sender, event.release());
    }

    void UpdateStats(ui64 schemeShardId, std::initializer_list<TStatsEntry> entries, bool full = true) {
        SendStats(schemeShardId, entries, full);
        Sync();
    }

    TTotals ReadCounters() {
        Runtime.SendToPipe(Pipe, Sender, new TEvTablet::TEvGetCounters());
        auto response = Runtime.GrabEdgeEventRethrow<TEvTablet::TEvGetCountersResponse>(Sender);
        TTotals totals;
        bool foundRows = false;
        bool foundBytes = false;
        for (const auto& counter : response->Get()->Record.GetTabletCounters().GetAppCounters().GetSimpleCounters()) {
            if (counter.GetName() == "StatisticsAggregator/BaseStatisticsTotalRowCount") {
                totals.RowCount = counter.GetValue();
                foundRows = true;
            } else if (counter.GetName() == "StatisticsAggregator/BaseStatisticsTotalBytesSize") {
                totals.BytesSize = counter.GetValue();
                foundBytes = true;
            }
        }
        UNIT_ASSERT(foundRows && foundBytes);
        return totals;
    }

    void CheckCounters(ui64 rows, ui64 bytes) {
        const auto totals = ReadCounters();
        UNIT_ASSERT_VALUES_EQUAL(totals.RowCount, rows);
        UNIT_ASSERT_VALUES_EQUAL(totals.BytesSize, bytes);
    }

    void Sync() {
        // The response on the same pipe ensures the preceding stats reports
        // have reached the tablet before waiting for its transactions.
        ReadCounters();
        NTabletFlatScheme::TSchemeChanges scheme;
        TString error;
        UNIT_ASSERT_VALUES_EQUAL_C(
            LocalSchemeTx(Runtime, TabletId, "", true, scheme, error),
            NKikimrProto::OK, error);
    }

    void Reboot() {
        RebootTablet(Runtime, TabletId, Sender);
        Connect();
    }

    TTestEnv Env;
    TTestActorRuntime& Runtime;
    TBlockEvents<TEvStatistics::TEvSchemeShardStats> AutomaticReports;
    ui64 TabletId = 0;

private:
    void Connect() {
        Pipe = Runtime.ConnectToPipe(TabletId, Sender, 0, GetPipeConfigWithRetries());
    }

    TActorId Sender;
    TActorId Pipe;
};

} // namespace

Y_UNIT_TEST_SUITE(BaseStatisticsCounters) {
    Y_UNIT_TEST(ReplacementsAndPartialReports) {
        TCountersEnv env;

        // An incomplete first snapshot must not bootstrap the totals.
        env.UpdateStats(1, {{1, 77, 770, false}, {2, 9, 90}}, false);
        env.CheckCounters(0, 0);

        env.UpdateStats(1, {{1, 10, 100}, {2, 20, 200}});
        env.CheckCounters(30, 300);
        env.UpdateStats(2, {{1, 5, 500}});
        env.CheckCounters(35, 800);

        // Retain the old value for an incomplete existing path, take the full
        // new value, and use zero for an incomplete path not reported before.
        env.UpdateStats(1, {{1, 0, 0, false}, {2, 7, 70}, {3, 0, 0, false}}, false);
        env.CheckCounters(22, 670);
        env.UpdateStats(1, {{1, 0, 0, false}, {2, 7, 70}, {3, 0, 0, false}}, false);
        env.CheckCounters(22, 670);

        // A full replacement removes absent tables and may reduce both sums.
        env.UpdateStats(1, {{3, 2, 12}});
        env.CheckCounters(7, 512);
        env.UpdateStats(1, {});
        env.CheckCounters(5, 500);
        env.UpdateStats(2, {});
        env.CheckCounters(0, 0);
    }

    Y_UNIT_TEST(QueuedUpdatesUseCommittedTotals) {
        TCountersEnv env;
        env.UpdateStats(1, {{1, 10, 100}});
        env.UpdateStats(2, {{1, 20, 200}});
        env.CheckCounters(30, 300);

        TBlockEvents<TEvBlobStorage::TEvPut> writes(env.Runtime, [&](const auto& ev) {
            return ev->Get()->Id.TabletID() == env.TabletId;
        });
        env.SendStats(1, {{1, 40, 400}});
        env.SendStats(2, {{1, 5, 50}});
        env.SendStats(1, {{1, 3, 30}});
        env.Runtime.WaitFor("blocked statistics write", [&] { return !writes.empty(); });
        env.CheckCounters(30, 300);

        writes.Unblock().Stop();
        env.Sync();
        env.CheckCounters(8, 80);
    }

    Y_UNIT_TEST(RebuildTotalsAfterReboot) {
        TCountersEnv env;
        env.UpdateStats(1, {{1, 10, 100}, {2, 20, 200}});
        env.UpdateStats(2, {{1, 7, 700}});
        env.CheckCounters(37, 1000);

        env.Reboot();
        env.CheckCounters(37, 1000);
        env.UpdateStats(1, {{2, 4, 40}});
        env.CheckCounters(11, 740);
        env.UpdateStats(2, {});
        env.CheckCounters(4, 40);

        env.Reboot();
        env.CheckCounters(4, 40);
    }
}

} // namespace NKikimr::NStat
