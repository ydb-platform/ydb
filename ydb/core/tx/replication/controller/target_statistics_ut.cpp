#include "replication.h"
#include "target_table.h"
#include "target_statistics.h"

#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/ydb_convert/replication_description.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {
namespace {
TInstant At(ui64 seconds) {
    return TInstant::Seconds(seconds);
}
}

Y_UNIT_TEST_SUITE(ReplicationStatistics) {
    Y_UNIT_TEST(WriteStartupWindowIdleAndRestart) {
        TTargetStatistics stats;
        UNIT_ASSERT(!stats.GetWriteSpeed(At(100)));
        stats.AddWriteBytes(At(100), 0);
        UNIT_ASSERT(!stats.GetWriteSpeed(At(100)));
        stats.AddWriteBytes(At(102), 200);
        UNIT_ASSERT_DOUBLES_EQUAL(*stats.GetWriteSpeed(At(102)), 100, 1e-9);
        stats.AddWriteBytes(At(110), 800);
        UNIT_ASSERT_DOUBLES_EQUAL(*stats.GetWriteSpeed(At(110)), 100, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(*stats.GetWriteSpeed(At(160)), 1000.0 / 60, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(*stats.GetWriteSpeed(At(162)), 800.0 / 60, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(*stats.GetWriteSpeed(At(170)), 0, 1e-9);
        TTargetStatistics restarted;
        UNIT_ASSERT(!restarted.GetWriteSpeed(At(170)));
    }

    Y_UNIT_TEST(ScanWarmupStallResetAndOrdering) {
        TTargetStatistics stats;
        UNIT_ASSERT(!stats.ObserveScan(At(100), 100, 0, false));
        UNIT_ASSERT_VALUES_EQUAL(*stats.ObserveScan(At(110), 100, 10, false), TDuration::Seconds(90));
        // An older response cannot regress progress or change the partition set.
        UNIT_ASSERT(!stats.ObserveScan(At(105), 20, 1, false));
        UNIT_ASSERT_VALUES_EQUAL(*stats.ObserveScan(At(120), 100, 20, false), TDuration::Seconds(80));
        UNIT_ASSERT_VALUES_EQUAL(*stats.ObserveScan(At(130), 100, 20, false), TDuration::Seconds(120));
        UNIT_ASSERT(!stats.ObserveScan(At(431), 100, 20, false));
        UNIT_ASSERT_VALUES_EQUAL(*stats.ObserveScan(At(441), 100, 30, false), TDuration::Seconds(70));
        UNIT_ASSERT(!stats.ObserveScan(At(451), 200, 40, false));
        UNIT_ASSERT(!stats.ObserveScan(At(461), 200, 1, false));
        UNIT_ASSERT_VALUES_EQUAL(*stats.ObserveScan(At(471), 200, 11, false), TDuration::Seconds(189));
        stats.ResetScan();
        UNIT_ASSERT(!stats.ObserveScan(At(481), 200, 21, false));
        UNIT_ASSERT_VALUES_EQUAL(*stats.ObserveScan(At(491), 200, 200, false), TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(*stats.ObserveScan(At(501), 0, 0, true), TDuration::Zero());
        UNIT_ASSERT(!stats.ObserveScan(At(511), 0, 0, false));
    }

    Y_UNIT_TEST(SparseProgressDurationLimit) {
        TTargetStatistics stats;
        stats.ObserveScan(At(100), Max<ui32>(), 0, false);
        UNIT_ASSERT(!stats.ObserveScan(At(400), Max<ui32>(), 1, false));
        stats.ResetScan();
        stats.ObserveScan(At(100), 1000000, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(*stats.ObserveScan(At(400), 1000000, 1, false),
            TDuration::Seconds(299999700));
    }

    Y_UNIT_TEST(ConcurrentUnequalRatesAndQueuedIndex) {
        TTargetStatistics base;
        TTargetStatistics index;
        TTargetStatistics queued;
        base.ObserveScan(At(100), 100, 0, false);
        index.ObserveScan(At(100), 40, 0, false);
        auto baseEta = base.ObserveScan(At(110), 100, 50, false);
        auto indexEta = index.ObserveScan(At(110), 40, 5, false);
        UNIT_ASSERT_VALUES_EQUAL(*baseEta, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(*indexEta, TDuration::Seconds(70));
        TReplicationStatistics parallel(true);
        parallel.Add(100, baseEta);
        parallel.Add(25, indexEta);
        UNIT_ASSERT_VALUES_EQUAL(*parallel.ScanEta, TDuration::Seconds(70));
        UNIT_ASSERT_DOUBLES_EQUAL(*parallel.WriteSpeed, 125, 1e-9);
        parallel.Add(std::nullopt, queued.ObserveScan(At(110), 20, 0, false));
        UNIT_ASSERT(!parallel.ScanEta);
        UNIT_ASSERT(!parallel.WriteSpeed);

        TReplicationStatistics baseFinished(true);
        baseFinished.Add(0, base.ObserveScan(At(120), 100, 100, false));
        baseFinished.Add(25, index.ObserveScan(At(120), 40, 10, false));
        UNIT_ASSERT_VALUES_EQUAL(*baseFinished.ScanEta, TDuration::Seconds(60));
        TReplicationStatistics allFinished(true);
        allFinished.Add(0, base.ObserveScan(At(130), 0, 0, true));
        allFinished.Add(0, index.ObserveScan(At(130), 40, 40, false));
        UNIT_ASSERT_VALUES_EQUAL(*allFinished.ScanEta, TDuration::Zero());
        TReplicationStatistics incomplete(false);
        incomplete.Add(0, TDuration::Zero());
        UNIT_ASSERT(!incomplete.ScanEta);
    }

    Y_UNIT_TEST(DeferredSourceChangeRetiresDescriberAndHistory) {
        NKikimrReplication::TReplicationConfig config;
        config.MutableSrcConnectionParams()->SetEndpoint("A");
        auto replication = MakeIntrusive<TReplication>(ui64(1), TPathId(1, 2), config, "/Root");
        const auto tid = replication->AddTarget(TReplication::ETargetKind::Table,
            std::make_shared<TTargetTable::TTableConfig>("/Root/table", "/Root/copy"));
        auto& stats = replication->FindTarget(tid)->GetReplicationStats();
        stats.ObserveScan(At(100), 100, 0, false);
        UNIT_ASSERT(stats.ObserveScan(At(110), 100, 10, false));
        const TActorId oldDescriber(1, 100);
        replication->AddTargetDescriber(oldDescriber);
        config.MutableSrcConnectionParams()->SetEndpoint("B");
        replication->SetConfig(NKikimrReplication::TReplicationConfig(config));
        config.MutableSrcConnectionParams()->SetEndpoint("A");
        replication->SetConfig(std::move(config));
        UNIT_ASSERT(!replication->RemoveTargetDescriber(oldDescriber));
        UNIT_ASSERT(!stats.ObserveScan(At(120), 100, 20, false));
        const TActorId currentDescriber(1, 101);
        replication->AddTargetDescriber(currentDescriber);
        // No replacement proxy exists yet: a new describer also belongs to A's
        // old proxy epoch during the deferred ALTER gap.
        UNIT_ASSERT(!replication->CanDescribeSource());
        UNIT_ASSERT(!replication->RemoveTargetDescriber(currentDescriber));
        UNIT_ASSERT(!replication->RemoveTargetDescriber(currentDescriber));
    }

    Y_UNIT_TEST(DiscoveryAndPublicOptionalMapping) {
        auto replication = MakeIntrusive<TReplication>(ui64(1), TPathId(1, 2),
            NKikimrReplication::TReplicationConfig(), "/Root");
        UNIT_ASSERT(!replication->IsTargetDiscoveryComplete());
        replication->FinishTargetDiscovery(false);
        UNIT_ASSERT(!replication->IsTargetDiscoveryComplete());
        replication->FinishTargetDiscovery();
        UNIT_ASSERT(replication->IsTargetDiscoveryComplete());

        NKikimrReplication::TEvDescribeReplicationResult internal;
        internal.MutableState()->MutableStandBy();
        auto* target = internal.AddTargets();
        target->SetId(1);
        Ydb::Replication::DescribeReplicationResult oldServer;
        FillReplicationDescription(oldServer, internal);
        UNIT_ASSERT(!oldServer.running().stats().has_write_speed_bytes_per_second());
        UNIT_ASSERT(!oldServer.items(0).stats().has_initial_scan_eta());
        internal.MutableState()->MutableStandBy()->SetWriteSpeedBytesPerSecond(125.5);
        internal.MutableState()->MutableStandBy()->MutableInitialScanEta()->set_seconds(42);
        target->SetWriteSpeedBytesPerSecond(0);
        target->MutableInitialScanEta()->set_seconds(0);
        Ydb::Replication::DescribeReplicationResult current;
        FillReplicationDescription(current, internal);
        UNIT_ASSERT_DOUBLES_EQUAL(current.running().stats().write_speed_bytes_per_second(), 125.5, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(current.running().stats().initial_scan_eta().seconds(), 42);
        UNIT_ASSERT(current.items(0).stats().has_write_speed_bytes_per_second());
        UNIT_ASSERT(current.items(0).stats().has_initial_scan_eta());
        UNIT_ASSERT_VALUES_EQUAL(current.items(0).stats().initial_scan_eta().seconds(), 0);
    }
}
}
