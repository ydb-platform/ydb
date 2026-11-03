#include "ut_common/datashard_ut_common.h"
#include "datashard_ut_common_kqp.h"

#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <util/string/printf.h>

#include <iterator>
#include <memory>
#include <optional>

namespace NKikimr::NDataShard {
namespace {

class TSnapshotRetentionTest {
public:
    explicit TSnapshotRetentionTest(bool serviceRegistry = false, ui32 nodeCount = 1) {
        NKikimrConfig::TAppConfig app;
        app.MutableFeatureFlags()->SetEnableSnapshotsLocking(false);
        app.MutableDataShardConfig()->SetCleanupSnapshotPeriod(100);
        auto* config = app.MutableLongTxServiceConfig();
        config->SetLocalSnapshotPromotionTimeSeconds(1);
        config->SetMaxClockSkewMs(200);
        // Publish registry states explicitly. Keep service maintenance outside
        // the simulated duration of these tests so it cannot overwrite them.
        config->SetSnapshotsRegistryUpdateIntervalSeconds(3600);
        if (serviceRegistry) {
            config->SetSnapshotsRegistryUpdateIntervalSeconds(1);
            config->SetSnapshotsExchangeIntervalSeconds(1);
            config->SetPrefillTimeoutSeconds(1);
        }

        Tests::TServerSettings settings(PortManager_.GetPort(2134));
        settings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(nodeCount)
            .SetDomainPlanResolution(100)
            .SetKeepSnapshotTimeout(TDuration::Seconds(2))
            .SetAppConfig(app);
        Server = new Tests::TServer(settings);
        auto& runtime = GetRuntime();
        for (ui32 node = 0; node < nodeCount; ++node) {
            runtime.GetAppData(node).LongTxServiceConfig = app.GetLongTxServiceConfig();
        }
        Sender = runtime.AllocateEdgeActor();
        NKqp::InitRoot(Server, Sender);
        runtime.SimulateSleep(TDuration::Seconds(1));

        for (ui32 node = 0; node < nodeCount; ++node) {
            runtime.GetAppData(node).FeatureFlags.SetEnableSnapshotsLocking(true);
            runtime.GetAppData(node).SnapshotRegistryHolder->Set(nullptr);
        }
        ui32 tableNode = 0;
        auto pinTables = runtime.AddObserver<TEvHive::TEvCreateTablet>([&](auto& ev) {
            ev->Get()->Record.ClearAllowedNodeIDs();
            ev->Get()->Record.AddAllowedNodeIDs(runtime.GetNodeId(tableNode));
        });
        CreateShardedTable(Server, Sender, "/Root", "table-a", 1);
        tableNode = nodeCount - 1;
        CreateShardedTable(Server, Sender, "/Root", "table-b", 1);
        pinTables.Remove();
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
        const TRowVersion& border = TRowVersion::Max(), std::optional<TInstant> collectionTime = std::nullopt,
        ui32 nodeIndex = 0)
    {
        auto builder = CreateImmutableSnapshotRegistryBuilder();
        builder->SetOldestCollectionTime(collectionTime.value_or(GetRuntime().GetCurrentTime()));
        builder->SetSnapshotBorder(border);
        for (const auto& snapshot : snapshots) {
            builder->AddSnapshot(tables, snapshot);
        }
        auto& holder = GetRuntime().GetAppData(nodeIndex).SnapshotRegistryHolder;
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
        request->Record.SetMaxBytes(Max<ui32>());
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

Y_UNIT_TEST_SUITE(DataShardSnapshotRetentionTransfer) {
    Y_UNIT_TEST(CleanupWhilePreparingSplitSnapshotIsTransferred) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto snapshot = test.WriteVersion(1);
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();

        bool splitting = false;
        auto observeSplit = runtime.AddObserver<TEvDataShard::TEvSplit>([&](auto&) {
            splitting = true;
        });
        TBlockEvents<TEvResourceBroker::TEvResourceAllocated> compactions(runtime, [&](const auto&) {
            return splitting;
        });
        SetSplitMergePartCountLimit(test.Server->GetRuntime(), -1);
        const auto txId = AsyncSplitTable(test.Server, test.Sender, "/Root/table-a", test.ShardA, 4);
        runtime.WaitFor("split snapshot compaction is waiting", [&] { return !compactions.empty(); });

        // Cleanup still runs while the source is making its physical snapshot.
        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        test.PublishRegistry({snapshot}, {test.TableA});
        compactions.Stop().Unblock();
        observeSplit.Remove();
        WaitTxNotification(test.Server, test.Sender, txId);
        for (ui64 shard : NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-a")) {
            UNIT_ASSERT(test.IsRemoved(shard, snapshot));
            const auto result = test.ReadSnapshot(shard, test.TableA, snapshot);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
        }
    }

    Y_UNIT_TEST_TWIN(SnapshotSurvivesSplitMergeAndRestart, MissingRegistry) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 10), (3, 30), (5, 50);");
        const auto retained = AcquireReadSnapshot(runtime, "/Root");
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 11), (3, 31), (5, 51);");
        const auto removed = AcquireReadSnapshot(runtime, "/Root");
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 12), (5, 52);"
            "DELETE FROM `/Root/table-a` WHERE key = 3;");
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({retained}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, removed));
        UNIT_ASSERT(retained < test.GetCleanupBoundary(test.ShardA));

        auto verifyShards = [&](const TVector<ui64>& shards) {
            TString rows;
            for (ui64 shard : shards) {
                UNIT_ASSERT(!test.IsRemoved(shard, retained));
                UNIT_ASSERT(test.IsRemoved(shard, removed));
                const auto compaction = CompactTable(runtime, shard, test.TableA, false);
                UNIT_ASSERT_VALUES_EQUAL(compaction.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
                RebootTablet(runtime, shard, test.Sender);
                rows += FormatIntReadResult(test.ReadSnapshot(shard, test.TableA, retained).get());
                const auto result = test.ReadSnapshot(shard, test.TableA, removed);
                UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
            }
            UNIT_ASSERT_VALUES_EQUAL(rows, "1, 10\n3, 30\n5, 50\n");
        };

        SetSplitMergePartCountLimit(test.Server->GetRuntime(), -1);
        if (MissingRegistry) {
            runtime.GetAppData(0).SnapshotRegistryHolder->Set(nullptr);
        }
        const auto splitTx = AsyncSplitTable(test.Server, test.Sender, "/Root/table-a", test.ShardA, 4);
        WaitTxNotification(test.Server, test.Sender, splitTx);
        auto shards = NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-a");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        verifyShards(shards);

        test.PublishRegistry({retained, removed}, {test.TableA});
        if (MissingRegistry) {
            runtime.GetAppData(0).SnapshotRegistryHolder->Set(nullptr);
        }
        const auto mergeTx = AsyncMergeTable(test.Server, test.Sender, "/Root/table-a", shards);
        WaitTxNotification(test.Server, test.Sender, mergeTx);
        shards = NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-a");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
        verifyShards(shards);

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(shards.at(0), retained));
    }

    Y_UNIT_TEST(MergeRejectsSnapshotMissingFromOneSource) {
        TSnapshotRetentionTest test(false, 2);
        auto& runtime = test.GetRuntime();
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 10), (5, 50);");
        const auto snapshot = AcquireReadSnapshot(runtime, "/Root");
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 11), (5, 51);");
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.PublishRegistry({snapshot}, {test.TableA}, TRowVersion::Max(), std::nullopt, 1);
        test.RunCleanup();

        ui32 destinationNode = 0;
        auto pinDestinations = runtime.AddObserver<TEvHive::TEvCreateTablet>([&](auto& ev) {
            ev->Get()->Record.ClearAllowedNodeIDs();
            ev->Get()->Record.AddAllowedNodeIDs(runtime.GetNodeId(destinationNode++ % 2));
        });
        SetSplitMergePartCountLimit(test.Server->GetRuntime(), -1);
        const auto splitTx = AsyncSplitTable(test.Server, test.Sender, "/Root/table-a", test.ShardA, 4);
        WaitTxNotification(test.Server, test.Sender, splitTx);
        pinDestinations.Remove();
        const auto sources = NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-a");
        UNIT_ASSERT_VALUES_EQUAL(sources.size(), 2u);

        // The second node has collected newer information without this snapshot.
        test.PublishRegistry({}, {}, TRowVersion::Max(), std::nullopt, 1);
        test.RunCleanup();
        UNIT_ASSERT_VALUES_UNEQUAL(test.IsRemoved(sources[0], snapshot), test.IsRemoved(sources[1], snapshot));

        // A late registration cannot repair the missing half of the table.
        test.PublishRegistry({snapshot}, {test.TableA}, TRowVersion::Max(), std::nullopt, 1);
        const auto mergeTx = AsyncMergeTable(test.Server, test.Sender, "/Root/table-a", sources);
        WaitTxNotification(test.Server, test.Sender, mergeTx);
        const auto merged = NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-a").at(0);
        UNIT_ASSERT(test.IsRemoved(merged, snapshot));
        const auto result = test.ReadSnapshot(merged, test.TableA, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(LegacySplitDoesNotTrustNewRegistryRegistrations) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto snapshot = test.WriteVersion(1);
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();

        auto legacySender = runtime.AddObserver<TEvDataShard::TEvSplitTransferSnapshot>([](auto& ev) {
            for (auto& table : *ev->Get()->Record.MutableTableSnapshot()) {
                table.ClearRemovedRowVersions();
            }
        });
        SetSplitMergePartCountLimit(test.Server->GetRuntime(), -1);
        const auto txId = AsyncSplitTable(test.Server, test.Sender, "/Root/table-a", test.ShardA, 4);
        WaitTxNotification(test.Server, test.Sender, txId);
        legacySender.Remove();
        for (ui64 shard : NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-a")) {
            UNIT_ASSERT(test.IsRemoved(shard, snapshot));
            const auto result = test.ReadSnapshot(shard, test.TableA, snapshot);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
        }
    }

    Y_UNIT_TEST_TWIN(CopyTableUsesDestinationIdentity, AllTables) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto retained = test.WriteVersion(1);
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto removed = test.WriteVersion(2);
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(3);
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({retained}, AllTables ? TVector<TTableId>{} : TVector<TTableId>{test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, removed));

        const auto txId = AsyncCreateCopyTable(test.Server, test.Sender, "/Root", "table-copy", "/Root/table-a");
        WaitTxNotification(test.Server, test.Sender, txId);
        const auto copyId = ResolveTableId(test.Server, test.Sender, "/Root/table-copy");
        const auto copyShard = NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-copy").at(0);
        test.RunCleanup();
        UNIT_ASSERT_VALUES_EQUAL(test.IsRemoved(copyShard, retained), !AllTables);
        UNIT_ASSERT(test.IsRemoved(copyShard, removed));

        const auto compaction = CompactTable(runtime, copyShard, copyId, false);
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
        RebootTablet(runtime, copyShard, test.Sender);
        const auto result = test.ReadSnapshot(copyShard, copyId, retained);
        if (AllTables) {
            UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(result.get()), "1, 1\n");
        } else {
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
        }
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(test.ReadSnapshot(test.ShardA, test.TableA, retained).get()), "1, 1\n");

        // Changing identity or registering a previously removed version cannot
        // resurrect history in the copy.
        test.PublishRegistry({retained, removed}, {copyId});
        const auto lost = test.ReadSnapshot(copyShard, copyId, removed);
        UNIT_ASSERT_VALUES_EQUAL(lost->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }
}

Y_UNIT_TEST_SUITE(DataShardSnapshotRetentionIntegration) {
    Y_UNIT_TEST(KqpFirstRemoteReadAfterCleanup) {
        TSnapshotRetentionTest test(true, 2);
        auto& runtime = test.GetRuntime();
        test.WriteVersion(1);

        TRowVersion snapshot = TRowVersion::Min();
        auto captureSnapshot = runtime.AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult>([&](auto& ev) {
            if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
                snapshot = ev->Get()->Snapshot;
            }
        });
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(NKqpHelpers::KqpSimpleBegin(runtime, sessionId, txId,
            "SELECT key, value FROM `/Root/table-a` ORDER BY key;"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
        captureSnapshot.Remove();
        UNIT_ASSERT(snapshot);

        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-b` (key, value) VALUES (1, 2);");
        runtime.WaitFor("snapshot propagated to the remote node", [&] {
            const auto& registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            return registry && registry->HasSnapshot(test.TableB, snapshot);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardB));
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));

        // This transaction has never visited table-b before its ordinary MVCC
        // window expired. Its KQP handle, propagated by LongTxService, protects it.
        UNIT_ASSERT_VALUES_EQUAL(NKqpHelpers::KqpSimpleContinue(runtime, sessionId, txId,
            "SELECT key, value FROM `/Root/table-b` ORDER BY key;"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
        NKqpHelpers::CloseSession(runtime, sessionId);
        runtime.WaitFor("released snapshot removed from the remote registry", [&] {
            const auto& registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            return registry && !registry->HasSnapshot(test.TableB, snapshot);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
    }

    Y_UNIT_TEST(LastHandleReleasesSharedSnapshotAcrossNodes) {
        TSnapshotRetentionTest test(true, 2);
        auto& runtime = test.GetRuntime();
        test.WriteVersion(1);
        using TService = NLongTxService::TEvLongTxService;
        TVector<TActorId> senders;
        for (ui32 i = 0; i < 3; ++i) {
            const auto sender = runtime.AllocateEdgeActor();
            senders.push_back(sender);
            runtime.Send(NLongTxService::MakeLongTxServiceID(runtime.GetNodeId(0)), sender,
                new TService::TEvAcquireReadSnapshot("/Root"), 0, true);
        }
        TVector<NKqp::TSnapshotHandle> handles;
        TRowVersion snapshot = TRowVersion::Min();
        for (const auto& sender : senders) {
            auto ev = runtime.GrabEdgeEventRethrow<TService::TEvAcquireReadSnapshotResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, Ydb::StatusIds::SUCCESS);
            if (snapshot) {
                UNIT_ASSERT_VALUES_EQUAL(snapshot, ev->Get()->Snapshot);
            }
            snapshot = ev->Get()->Snapshot;
            handles.push_back(std::move(ev->Get()->SnapshotHandle));
        }

        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        runtime.WaitFor("shared snapshot propagated", [&] {
            const auto& registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            return registry && registry->HasSnapshot(test.TableB, snapshot);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardB));

        for (ui32 i = 0; i < 2; ++i) {
            handles[i].Reset();
            runtime.SimulateSleep(TDuration::Seconds(5));
            UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));
            UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(test.ReadSnapshot(test.ShardB, test.TableB, snapshot).get()), "1, 1\n");
        }
        handles.back().Reset();
        runtime.WaitFor("last shared snapshot handle released", [&] {
            const auto& registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            return registry && !registry->HasSnapshot(test.TableB, snapshot);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
    }
}

} // namespace NKikimr::NDataShard
