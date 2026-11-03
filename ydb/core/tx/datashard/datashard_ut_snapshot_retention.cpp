#include "ut_common/datashard_ut_common.h"

#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <util/string/printf.h>

#include <iterator>
#include <memory>
#include <optional>

namespace NKikimr::NDataShard {
namespace {

class TSnapshotRetentionTest {
public:
    TSnapshotRetentionTest() {
        NKikimrConfig::TAppConfig app;
        app.MutableFeatureFlags()->SetEnableSnapshotsLocking(false);
        app.MutableDataShardConfig()->SetCleanupSnapshotPeriod(100);
        auto* config = app.MutableLongTxServiceConfig();
        config->SetLocalSnapshotPromotionTimeSeconds(1);
        config->SetMaxClockSkewMs(200);
        // Publish registry states explicitly. Keep service maintenance outside
        // the simulated duration of these tests so it cannot overwrite them.
        config->SetSnapshotsRegistryUpdateIntervalSeconds(3600);

        Tests::TServerSettings settings(PortManager_.GetPort(2134));
        settings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(100)
            .SetKeepSnapshotTimeout(TDuration::Seconds(2))
            .SetAppConfig(app);
        Server = new Tests::TServer(settings);
        auto& runtime = GetRuntime();
        runtime.GetAppData(0).LongTxServiceConfig = app.GetLongTxServiceConfig();
        Sender = runtime.AllocateEdgeActor();
        NKqp::InitRoot(Server, Sender);
        runtime.SimulateSleep(TDuration::Seconds(1));

        runtime.GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(true);
        runtime.GetAppData(0).SnapshotRegistryHolder->Set(nullptr);
        CreateShardedTable(Server, Sender, "/Root", "table-a", 1);
        CreateShardedTable(Server, Sender, "/Root", "table-b", 1);
        TableA = ResolveTableId(Server, Sender, "/Root/table-a");
        TableB = ResolveTableId(Server, Sender, "/Root/table-b");
        ShardA = NKqp::GetTableShards(Server, Sender, "/Root/table-a").at(0);
        ShardB = NKqp::GetTableShards(Server, Sender, "/Root/table-b").at(0);
    }

    TTestActorRuntime& GetRuntime() {
        return *Server->GetRuntime();
    }

    TRowVersion WriteVersion(ui32 value) {
        ExecSQL(Server, Sender, Sprintf(
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, %u);"
            "UPSERT INTO `/Root/table-b` (key, value) VALUES (1, %u);", value, value));
        return AcquireReadSnapshot(GetRuntime(), "/Root");
    }

    void PublishRegistry(const TVector<TRowVersion>& snapshots = {}, const TVector<TTableId>& tables = {},
        const TRowVersion& border = TRowVersion::Max(), std::optional<TInstant> collectionTime = std::nullopt)
    {
        auto builder = CreateImmutableSnapshotRegistryBuilder();
        builder->SetOldestCollectionTime(collectionTime.value_or(GetRuntime().GetCurrentTime()));
        builder->SetSnapshotBorder(border);
        for (const auto& snapshot : snapshots) {
            builder->AddSnapshot(tables, snapshot);
        }
        auto& holder = GetRuntime().GetAppData(0).SnapshotRegistryHolder;
        if (!holder) {
            holder = CreateImmutableSnapshotRegistryHolder();
        }
        holder->Set(std::move(*builder).Build());
    }

    void RunCleanup() {
        GetRuntime().SimulateSleep(TDuration::Seconds(3));
    }

    bool IsRemoved(ui64 shard, const TRowVersion& snapshot) {
        return GetRemovedRowVersions(Server, shard).Contains(snapshot, snapshot.Next());
    }

    TRowVersion GetCleanupBoundary(ui64 shard) {
        const auto ranges = GetRemovedRowVersions(Server, shard);
        UNIT_ASSERT(!ranges.empty());
        return std::prev(ranges.end())->Upper;
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> ReadSnapshot(ui64 shard, const TTableId& tableId,
        const TRowVersion& snapshot, ui64 readId = 1, ui64 maxRows = Max<ui64>())
    {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;
        record.SetReadId(readId);
        record.MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(tableId.PathId.LocalPathId);
        record.AddColumns(1);
        record.AddColumns(2);
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        record.SetMaxRows(maxRows);
        snapshot.ToProto(record.MutableSnapshot());
        AddFullRangeQuery(*request);
        return SendRead(Server, shard, request.release(), Sender);
    }

    void AckRead(ui64 shard, const NKikimrTxDataShard::TEvReadResult& result) {
        auto request = std::make_unique<TEvDataShard::TEvReadAck>();
        request->Record.SetReadId(result.GetReadId());
        request->Record.SetSeqNo(result.GetSeqNo());
        request->Record.SetMaxRows(100);
        request->Record.SetMaxBytes(Max<ui64>());
        GetRuntime().SendToPipe(shard, Sender, request.release());
    }

    void CancelRead(ui64 shard, ui64 readId) {
        auto request = std::make_unique<TEvDataShard::TEvReadCancel>();
        request->Record.SetReadId(readId);
        GetRuntime().SendToPipe(shard, Sender, request.release());
    }

public:
    Tests::TServer::TPtr Server;
    TActorId Sender;
    TTableId TableA;
    TTableId TableB;
    ui64 ShardA = 0;
    ui64 ShardB = 0;

private:
    TPortManager PortManager_;
};

void TestPersistentAndRegistryRetention(bool registryReleasedFirst) {
    TSnapshotRetentionTest test;
    test.WriteVersion(1);
    const auto snapshot = CreateVolatileSnapshot(test.Server, {"/Root/table-a"}, TDuration::Hours(1));
    test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
    test.WriteVersion(2);
    test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
    test.PublishRegistry({snapshot}, {test.TableA});
    test.RunCleanup();
    UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
    UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));

    if (registryReleasedFirst) {
        test.PublishRegistry();
    } else {
        UNIT_ASSERT(DiscardVolatileSnapshot(test.Server, {"/Root/table-a"}, snapshot));
    }
    test.RunCleanup();
    UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

    if (registryReleasedFirst) {
        UNIT_ASSERT(DiscardVolatileSnapshot(test.Server, {"/Root/table-a"}, snapshot));
    } else {
        test.PublishRegistry();
    }
    test.RunCleanup();
    UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
}

} // namespace

Y_UNIT_TEST_SUITE(DataShardSnapshotRetentionCleanup) {
    Y_UNIT_TEST(ScopedPointsAreReleasedWithoutAdvancingBoundary) {
        TSnapshotRetentionTest test;
        const auto first = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        const auto middle = test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        const auto last = test.WriteVersion(3);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(4);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({first, last}, {test.TableA});
        test.RunCleanup();

        UNIT_ASSERT(!test.IsRemoved(test.ShardA, first));
        UNIT_ASSERT(test.IsRemoved(test.ShardA, middle));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, last));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, first));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, last));
        const auto boundary = test.GetCleanupBoundary(test.ShardA);
        UNIT_ASSERT(last < boundary);

        // No writes and no newer registry collection: only the explicit points change.
        const auto collectionTime = test.GetRuntime().GetAppData(0).SnapshotRegistryHolder->Get()->GetOldestCollectionTime();
        test.PublishRegistry({last}, {test.TableA}, TRowVersion::Max(), collectionTime);
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, first));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, last));
        UNIT_ASSERT_VALUES_EQUAL(test.GetCleanupBoundary(test.ShardA), boundary);

        test.PublishRegistry({}, {}, TRowVersion::Max(), collectionTime);
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, last));
        UNIT_ASSERT_VALUES_EQUAL(test.GetCleanupBoundary(test.ShardA), boundary);
    }

    Y_UNIT_TEST(AllTablesRegistrationProtectsBothShards) {
        TSnapshotRetentionTest test;
        const auto snapshot = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot});
        test.RunCleanup();

        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));

        test.GetRuntime().GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(false);
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
    }

    Y_UNIT_TEST(PersistentSnapshotOutlivesRegistryRegistration) {
        TestPersistentAndRegistryRetention(true);
    }

    Y_UNIT_TEST(RegistryRegistrationOutlivesPersistentSnapshot) {
        TestPersistentAndRegistryRetention(false);
    }

    Y_UNIT_TEST(RetainedPointSurvivesCompactionAndRestart) {
        TSnapshotRetentionTest test;
        const auto snapshot = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        const auto compaction = CompactTable(test.GetRuntime(), test.ShardA, test.TableA, false);
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
        RebootTablet(test.GetRuntime(), test.ShardA, test.Sender);
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
    }

    Y_UNIT_TEST(MissingRegistryPreservesRetainedPointDuringRestart) {
        TSnapshotRetentionTest test;
        const auto snapshot = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        const auto before = GetRemovedRowVersions(test.Server, test.ShardA).ToString();

        test.GetRuntime().GetAppData(0).SnapshotRegistryHolder->Set(nullptr);
        RebootTablet(test.GetRuntime(), test.ShardA, test.Sender);
        test.RunCleanup();
        UNIT_ASSERT_VALUES_EQUAL(GetRemovedRowVersions(test.Server, test.ShardA).ToString(), before);

        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
    }

    Y_UNIT_TEST(MissingHolderPreventsCleanupUntilRegistryAppears) {
        TSnapshotRetentionTest test;
        const auto snapshot = test.WriteVersion(1);
        test.GetRuntime().GetAppData(0).SnapshotRegistryHolder = nullptr;
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
    }

    Y_UNIT_TEST(BorderProtectsContinuousRange) {
        TSnapshotRetentionTest test;
        const auto oldSnapshot = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        const auto border = test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        const auto newSnapshot = test.WriteVersion(3);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(4);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({}, {}, border);
        test.RunCleanup();

        UNIT_ASSERT(test.IsRemoved(test.ShardA, oldSnapshot));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, border));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, newSnapshot));
        UNIT_ASSERT_VALUES_EQUAL(test.GetCleanupBoundary(test.ShardA), border);

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, border));
        UNIT_ASSERT(test.IsRemoved(test.ShardA, newSnapshot));
    }

    Y_UNIT_TEST(StaleRegistryStopsCleanupBoundary) {
        TSnapshotRetentionTest test;
        test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        const auto collectionTime = test.GetRuntime().GetCurrentTime();
        const TRowVersion boundary((collectionTime - TDuration::MilliSeconds(1200)).MilliSeconds(), 0);
        test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        const auto snapshot = test.WriteVersion(3);
        test.PublishRegistry({}, {}, TRowVersion::Max(), collectionTime);
        test.RunCleanup();
        UNIT_ASSERT_VALUES_EQUAL(test.GetCleanupBoundary(test.ShardA), boundary);
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        test.GetRuntime().SimulateSleep(TDuration::Seconds(10));
        test.WriteVersion(4);
        test.RunCleanup();
        UNIT_ASSERT_VALUES_EQUAL(test.GetCleanupBoundary(test.ShardA), boundary);

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
    }
}

Y_UNIT_TEST_SUITE(DataShardSnapshotRetentionRead) {
    Y_UNIT_TEST(DelayedFirstReadSurvivesCompactionAndRestart) {
        TSnapshotRetentionTest test;
        const auto snapshot = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));

        const auto compaction = CompactTable(test.GetRuntime(), test.ShardA, test.TableA, false);
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
        RebootTablet(test.GetRuntime(), test.ShardA, test.Sender);

        const auto retained = test.ReadSnapshot(test.ShardA, test.TableA, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(retained.get()), "1, 1\n");
        UNIT_ASSERT(retained->Record.GetFinished());

        // The same version was not retained for the other table.
        const auto removed = test.ReadSnapshot(test.ShardB, test.TableB, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(removed->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        const auto released = test.ReadSnapshot(test.ShardA, test.TableA, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(released->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(RegistrationCannotResurrectRemovedHistory) {
        TSnapshotRetentionTest test;
        const auto snapshot = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));

        test.PublishRegistry({snapshot}, {test.TableA});
        const auto result = test.ReadSnapshot(test.ShardA, test.TableA, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(MissingRegistryDoesNotResurrectRemovedHistory) {
        TSnapshotRetentionTest test;
        const auto snapshot = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();

        test.GetRuntime().GetAppData(0).SnapshotRegistryHolder->Set(nullptr);
        const auto retained = test.ReadSnapshot(test.ShardA, test.TableA, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(retained.get()), "1, 1\n");
        const auto removed = test.ReadSnapshot(test.ShardB, test.TableB, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(removed->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(DisabledRetentionRejectsOldSnapshot) {
        TSnapshotRetentionTest test;
        const auto snapshot = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        test.GetRuntime().GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(false);
        const auto result = test.ReadSnapshot(test.ShardA, test.TableA, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST_TWIN(IteratorKeepsSnapshotAfterRegistryRelease, DisableRetention) {
        TSnapshotRetentionTest test;
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        const auto snapshot = AcquireReadSnapshot(test.GetRuntime(), "/Root");
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (2, 20);"
            "DELETE FROM `/Root/table-a` WHERE key = 3;");
        test.WriteVersion(10);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));

        const auto first = test.ReadSnapshot(test.ShardA, test.TableA, snapshot, 1, 1);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(first.get()), "1, 1\n");
        UNIT_ASSERT(!first->Record.GetFinished());

        test.PublishRegistry();
        if (DisableRetention) {
            test.GetRuntime().GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(false);
        }
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        const auto compaction = CompactTable(test.GetRuntime(), test.ShardA, test.TableA, false);
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);

        test.AckRead(test.ShardA, first->Record);
        const auto rest = WaitReadResult(test.Server);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(rest.get()), "2, 2\n3, 3\n");
        UNIT_ASSERT(rest->Record.GetFinished());

        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
    }

    Y_UNIT_TEST(LastCancelledIteratorReleasesSnapshot) {
        TSnapshotRetentionTest test;
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 1), (2, 2);");
        const auto snapshot = AcquireReadSnapshot(test.GetRuntime(), "/Root");
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(10);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();

        for (ui64 readId : {1, 2}) {
            const auto result = test.ReadSnapshot(test.ShardA, test.TableA, snapshot, readId, 1);
            UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(result.get()), "1, 1\n");
            UNIT_ASSERT(!result->Record.GetFinished());
        }

        test.PublishRegistry();
        test.CancelRead(test.ShardA, 1);
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        test.CancelRead(test.ShardA, 2);
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
    }

    Y_UNIT_TEST(TruncateInvalidatesRetainedPointBelowWatermark) {
        TSnapshotRetentionTest test;
        const auto snapshot = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        const auto txId = AsyncTruncateTable(test.Server, test.Sender, "/Root", "table-a");
        WaitTxNotification(test.Server, test.Sender, txId);
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));

        const auto result = test.ReadSnapshot(test.ShardA, test.TableA, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }
}

} // namespace NKikimr::NDataShard
