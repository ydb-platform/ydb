#include "ut_common/datashard_ut_common.h"
#include "datashard_failpoints.h"
#include "datashard_ut_common_kqp.h"

#include <ydb/core/base/statestorage.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/long_tx_service/long_tx_service.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>
#include <ydb/core/tx/long_tx_service/snapshots_storage.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/interconnect.h>

#include <util/generic/hash_set.h>
#include <util/string/printf.h>

#include <iterator>
#include <memory>
#include <optional>

namespace NKikimr::NDataShard {
namespace {

class TSnapshotRetentionTest {
public:
    explicit TSnapshotRetentionTest(bool serviceRegistry = false, ui32 nodeCount = 1,
        bool enableDataShardLocking = true, TDuration promotionTime = TDuration::Seconds(1),
        std::optional<bool> inMemoryStateMigration = std::nullopt)
    {
        NKikimrConfig::TAppConfig app;
        app.MutableFeatureFlags()->SetEnableSnapshotsLocking(false);
        app.MutableTableServiceConfig()->SetEnableSnapshotIsolationRW(true);
        if (enableDataShardLocking) {
            app.MutableFeatureFlags()->SetEnableDataShardSnapshotLocking(true);
        }
        app.MutableDataShardConfig()->SetCleanupSnapshotPeriod(100);
        auto* config = app.MutableLongTxServiceConfig();
        config->SetLocalSnapshotPromotionTimeSeconds(promotionTime.Seconds());
        config->SetMaxClockSkewMs(200);
        // Tests that inject registry states keep maintenance outside their
        // simulated duration. Integration tests restore real maintenance below.
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
        if (inMemoryStateMigration) {
            settings.FeatureFlags.SetEnableDataShardInMemoryStateMigration(*inMemoryStateMigration);
        }
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
        if (serviceRegistry) {
            // Establish exchange membership before using a two-second MVCC
            // window for the snapshots acquired by the test.
            runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);
            runtime.SimulateSleep(TDuration::Seconds(10));
            for (ui32 node = 0; node < nodeCount; ++node) {
                UNIT_ASSERT(runtime.GetAppData(node).SnapshotRegistryHolder->Get());
            }
        }
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
        const auto collectedAt = collectionTime.value_or(GetRuntime().GetCurrentTime());
        builder->SetOldestCollectionTime(collectedAt);
        builder->SetMinPromotionCutoffStep(NLongTxService::PromotionCutoffStep(collectedAt,
            TDuration::Seconds(GetRuntime().GetAppData(nodeIndex).LongTxServiceConfig.GetLocalSnapshotPromotionTimeSeconds())));
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

    ui64 GetSimpleCounter(ui64 shard, const TString& name) {
        auto& runtime = GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(shard, sender, new TEvTablet::TEvGetCounters(),
            0, GetPipeConfigWithRetries());
        const auto response = runtime.GrabEdgeEventRethrow<TEvTablet::TEvGetCountersResponse>(sender);
        for (const auto& counter : response->Get()->Record.GetTabletCounters().GetAppCounters().GetSimpleCounters()) {
            if (counter.GetName() == name) {
                return counter.GetValue();
            }
        }
        UNIT_ASSERT_C(false, "Counter not found: " << name);
        return 0;
    }

    NTable::TRowVersionRanges GetRemovedRanges(ui64 shard) {
        auto& runtime = GetRuntime();
        const auto tablet = ResolveTablet(runtime, shard);
        const ui32 nodeIndex = tablet.NodeId() - runtime.GetFirstNodeId();
        const auto sender = runtime.AllocateEdgeActor(nodeIndex);
        // This diagnostic event is local-only. Send it from the tablet's node
        // rather than forwarding it across interconnect from node zero.
        runtime.Send(tablet, sender, new TEvDataShard::TEvGetRemovedRowVersions(TPathId{}), nodeIndex, true);
        const auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetRemovedRowVersionsResult>(sender);
        return ev->Get()->RemovedRowVersions;
    }

    bool IsRemoved(ui64 shard, const TRowVersion& snapshot) {
        return GetRemovedRanges(shard).Contains(snapshot, snapshot.Next());
    }

    TRowVersion GetCleanupBoundary(ui64 shard) {
        const auto ranges = GetRemovedRanges(shard);
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

std::unique_ptr<NEvents::TDataEvents::TEvWrite> MakeSnapshotUpdate(const TTableId& tableId,
    const TRowVersion& snapshot, ui64 lockTxId, ui64 lockNodeId, ui32 value,
    NKikimrDataEvents::ELockMode lockMode = NKikimrDataEvents::OPTIMISTIC)
{
    auto request = MakeWriteRequestOneKeyValue(std::nullopt,
        NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
        NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE,
        tableId, TShardedTableOptions::DefaultColumns(), 1, value);
    request->SetLockId(lockTxId, lockNodeId);
    request->Record.SetLockMode(lockMode);
    request->Record.MutableMvccSnapshot()->SetStep(snapshot.Step);
    request->Record.MutableMvccSnapshot()->SetTxId(snapshot.TxId);
    return request;
}

std::unique_ptr<NEvents::TDataEvents::TEvWrite> MakeFinalSnapshotUpdate(const TTableId& tableId,
    const TRowVersion& snapshot, std::optional<ui64> txId = std::nullopt,
    NKikimrDataEvents::TEvWrite::ETxMode mode = NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE)
{
    // Final SnapshotRW writes carry their original read snapshot without a
    // LockTxId, and may still have buffered operations that read that history.
    const std::vector<ui32> columns{1, 2};
    const std::vector<TCell> cells{
        TCell::Make(ui32(1)), TCell::Make(ui32(3)),
        TCell::Make(ui32(2)), TCell::Make(ui32(200)),
    };
    auto request = MakeWriteRequest(txId, mode,
        NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE, tableId, columns, cells);
    request->Record.SetLockMode(NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
    request->Record.MutableMvccSnapshot()->SetStep(snapshot.Step);
    request->Record.MutableMvccSnapshot()->SetTxId(snapshot.TxId);
    return request;
}

void ForceFullCompaction(TTestActorRuntime& runtime, ui64 shard, const TTableId& tableId, bool compactBorrowed = false) {
    auto request = std::make_unique<TEvDataShard::TEvCompactTable>(tableId.PathId);
    // These tests compact the same data again after acquiring a history pin.
    // Exercise compaction even when the first pass left one clean part.
    request->Record.SetCompactSinglePartedShards(true);
    request->Record.SetCompactBorrowed(compactBorrowed);
    const auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(shard, sender, request.release(), 0, GetPipeConfigWithRetries());
    const auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactTableResult>(sender, TDuration::Seconds(30));
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Record.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK,
        result->Get()->Record.ShortDebugString());
}

TRowVersion PrepareFinalSnapshotHistory(TSnapshotRetentionTest& test, bool retained) {
    auto& runtime = test.GetRuntime();
    const auto snapshot = test.WriteVersion(1);
    runtime.SimulateSleep(TDuration::Seconds(1));
    ExecSQL(test.Server, test.Sender,
        "UPSERT INTO `/Root/table-a` (key, value) VALUES (2, 20);"
        "UPSERT INTO `/Root/table-b` (key, value) VALUES (2, 20);");
    const auto inserted = AcquireReadSnapshot(runtime, "/Root");
    // Advance the write horizon beyond the insertion too, so compaction can
    // remove every version that would distinguish its absence at the snapshot.
    runtime.SimulateSleep(TDuration::Seconds(1));
    ExecSQL(test.Server, test.Sender,
        "UPSERT INTO `/Root/table-a` (key, value) VALUES (2, 20);"
        "UPSERT INTO `/Root/table-b` (key, value) VALUES (2, 20);");
    runtime.SimulateSleep(TDuration::Seconds(3));
    test.PublishRegistry(retained ? TVector<TRowVersion>{snapshot} : TVector<TRowVersion>{},
        {test.TableA, test.TableB});
    test.RunCleanup();
    for (const auto& [shard, tableId] : {std::make_pair(test.ShardA, test.TableA),
                                       std::make_pair(test.ShardB, test.TableB)}) {
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(shard));
        UNIT_ASSERT(inserted < test.GetCleanupBoundary(shard));
        UNIT_ASSERT_VALUES_EQUAL(test.IsRemoved(shard, snapshot), !retained);
        const auto compaction = CompactTable(runtime, shard, tableId, false);
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
    }
    return snapshot;
}

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
    Y_UNIT_TEST_TWIN(DisabledDataShardRetentionIgnoresRegistry, RegistryReady) {
        TSnapshotRetentionTest test(false, 1, false);
        auto& app = test.GetRuntime().GetAppData(0);
        UNIT_ASSERT(app.FeatureFlags.GetEnableSnapshotsLocking());
        UNIT_ASSERT(!app.FeatureFlags.GetEnableDataShardSnapshotLocking());

        const auto snapshot = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        if (RegistryReady) {
            test.PublishRegistry({snapshot}, {test.TableA});
            UNIT_ASSERT(app.SnapshotRegistryHolder->Get()->HasSnapshot(test.TableA, snapshot));
        } else {
            UNIT_ASSERT(!app.SnapshotRegistryHolder->Get());
        }
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
        UNIT_ASSERT_VALUES_EQUAL(test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryWaiting"), 0u);
        UNIT_ASSERT_VALUES_EQUAL(test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryCleanupLagMs"), 0u);
        const auto result = test.ReadSnapshot(test.ShardA, test.TableA, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

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
        const auto before = test.GetRemovedRanges(test.ShardA).ToString();

        test.GetRuntime().GetAppData(0).SnapshotRegistryHolder->Set(nullptr);
        RebootTablet(test.GetRuntime(), test.ShardA, test.Sender);
        test.RunCleanup();
        UNIT_ASSERT_VALUES_EQUAL(test.GetRemovedRanges(test.ShardA).ToString(), before);

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
        UNIT_ASSERT_VALUES_EQUAL(test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryWaiting"), 1u);
        UNIT_ASSERT_VALUES_EQUAL(test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryCleanupLagMs"), 0u);

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        UNIT_ASSERT_VALUES_EQUAL(test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryWaiting"), 0u);
        UNIT_ASSERT_VALUES_EQUAL(test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryCleanupLagMs"), 0u);
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
        UNIT_ASSERT_VALUES_EQUAL(test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryWaiting"), 0u);
        const auto initialLag = test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryCleanupLagMs");
        UNIT_ASSERT(initialLag > 0);

        test.GetRuntime().SimulateSleep(TDuration::Seconds(10));
        test.WriteVersion(4);
        test.RunCleanup();
        UNIT_ASSERT_VALUES_EQUAL(test.GetCleanupBoundary(test.ShardA), boundary);
        UNIT_ASSERT_VALUES_EQUAL(test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryWaiting"), 0u);
        UNIT_ASSERT(test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryCleanupLagMs") > initialLag);

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        UNIT_ASSERT_VALUES_EQUAL(test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryWaiting"), 0u);
        UNIT_ASSERT_VALUES_EQUAL(test.GetSimpleCounter(test.ShardA, "DataShard/MvccSnapshotRegistryCleanupLagMs"), 0u);
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

    Y_UNIT_TEST_TWIN(DisabledRetentionRejectsOldSnapshot, DisableSnapshotsLocking) {
        TSnapshotRetentionTest test;
        const auto snapshot = test.WriteVersion(1);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.GetRuntime().SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        if (DisableSnapshotsLocking) {
            test.GetRuntime().GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(false);
        } else {
            test.GetRuntime().GetAppData(0).FeatureFlags.SetEnableDataShardSnapshotLocking(false);
        }
        const auto result = test.ReadSnapshot(test.ShardA, test.TableA, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST_QUAD(IteratorKeepsSnapshotAfterRegistryRelease, DisableSnapshotsLocking, DisableDataShardLocking) {
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
        if (DisableSnapshotsLocking) {
            test.GetRuntime().GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(false);
        }
        if (DisableDataShardLocking) {
            test.GetRuntime().GetAppData(0).FeatureFlags.SetEnableDataShardSnapshotLocking(false);
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

    Y_UNIT_TEST_TWIN(IteratorDoesNotBlockCleanupOfNewerVersions, DisableRetention) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        const auto snapshot = AcquireReadSnapshot(runtime, "/Root");
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (2, 20);"
            "DELETE FROM `/Root/table-a` WHERE key = 3;");
        const auto newer = test.WriteVersion(10);
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(20);
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot, newer}, {test.TableA});
        test.RunCleanup();
        const auto boundary = test.GetCleanupBoundary(test.ShardA);
        UNIT_ASSERT(newer < boundary);
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, newer));

        const auto first = test.ReadSnapshot(test.ShardA, test.TableA, snapshot, 1, 1);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(first.get()), "1, 1\n");
        UNIT_ASSERT(!first->Record.GetFinished());

        test.PublishRegistry();
        if (DisableRetention) {
            runtime.GetAppData(0).FeatureFlags.SetEnableDataShardSnapshotLocking(false);
        }
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        // The released point below the existing watermark must be collected
        // while the older iterator is still waiting for an ACK.
        UNIT_ASSERT(test.IsRemoved(test.ShardA, newer));

        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto recent = test.WriteVersion(30);
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(40);
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(boundary < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(test.IsRemoved(test.ShardA, recent));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, recent));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        const auto compaction = CompactTable(runtime, test.ShardA, test.TableA, false);
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
        test.AckRead(test.ShardA, first->Record);
        const auto rest = WaitReadResult(test.Server);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(rest.get()), "2, 2\n3, 3\n");
        UNIT_ASSERT(rest->Record.GetFinished());
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
    }

    Y_UNIT_TEST(PersistentSnapshotRemovalKeepsIteratorVersion) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 1), (2, 2);");
        const auto snapshot = CreateVolatileSnapshot(test.Server, {"/Root/table-a"}, TDuration::Hours(1));
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(10);
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));

        const auto first = test.ReadSnapshot(test.ShardA, test.TableA, snapshot, 1, 1);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(first.get()), "1, 1\n");
        UNIT_ASSERT(!first->Record.GetFinished());

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardSnapshotLocking(false);
        UNIT_ASSERT(DiscardVolatileSnapshot(test.Server, {"/Root/table-a"}, snapshot));
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        const auto compaction = CompactTable(runtime, test.ShardA, test.TableA, false);
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
        test.AckRead(test.ShardA, first->Record);
        const auto rest = WaitReadResult(test.Server);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(rest.get()), "2, 2\n");
        UNIT_ASSERT(rest->Record.GetFinished());
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
    }

    Y_UNIT_TEST(CleanupMergesRegistryPersistentAndIteratorPoints) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        const auto persistent = CreateVolatileSnapshot(test.Server, {"/Root/table-a"}, TDuration::Hours(1));
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto registryOnly = test.WriteVersion(10);
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto iteratorVersion = test.WriteVersion(20);
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (2, 200);"
            "DELETE FROM `/Root/table-a` WHERE key = 3;");
        const auto removed = test.WriteVersion(30);
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(40);
        runtime.SimulateSleep(TDuration::Seconds(3));

        auto builder = CreateImmutableSnapshotRegistryBuilder();
        builder->SetOldestCollectionTime(runtime.GetCurrentTime());
        builder->SetMinPromotionCutoffStep(NLongTxService::PromotionCutoffStep(runtime.GetCurrentTime(),
            TDuration::Seconds(runtime.GetAppData(0).LongTxServiceConfig.GetLocalSnapshotPromotionTimeSeconds())));
        builder->AddSnapshot({}, registryOnly);
        for (const auto& version : {persistent, registryOnly, iteratorVersion}) {
            builder->AddSnapshot({test.TableA}, version);
        }
        runtime.GetAppData(0).SnapshotRegistryHolder->Set(std::move(*builder).Build());
        test.RunCleanup();
        UNIT_ASSERT(iteratorVersion < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, persistent));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, registryOnly));
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, registryOnly));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, iteratorVersion));
        UNIT_ASSERT(test.IsRemoved(test.ShardA, removed));

        const auto first = test.ReadSnapshot(test.ShardA, test.TableA, iteratorVersion, 1, 1);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(first.get()), "1, 20\n");
        UNIT_ASSERT(!first->Record.GetFinished());
        // Exercise duplicates between table/all-table registrations, the
        // persistent map and the admitted iterator in the same cleanup.
        test.RunCleanup();
        test.PublishRegistry({registryOnly});
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, persistent));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, registryOnly));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, iteratorVersion));

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, registryOnly));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, registryOnly));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, persistent));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, iteratorVersion));
        UNIT_ASSERT(DiscardVolatileSnapshot(test.Server, {"/Root/table-a"}, persistent));
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, persistent));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, iteratorVersion));

        const auto compaction = CompactTable(runtime, test.ShardA, test.TableA, false);
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
        test.AckRead(test.ShardA, first->Record);
        const auto rest = WaitReadResult(test.Server);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(rest.get()), "2, 2\n3, 3\n");
        UNIT_ASSERT(rest->Record.GetFinished());
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, iteratorVersion));
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

    Y_UNIT_TEST_TWIN(TruncateInvalidatesRetainedPointBelowWatermark, Persistent) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto initial = test.WriteVersion(1);
        const auto snapshot = Persistent
            ? CreateVolatileSnapshot(test.Server, {"/Root/table-a"}, TDuration::Hours(1))
            : initial;
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        const auto readSnapshot = [&](ui64 readId) {
            // Use the current schema on each new request, so schema validation
            // cannot mask admission of physically invalidated snapshot history.
            test.TableA = ResolveTableId(test.Server, test.Sender, "/Root/table-a");
            UNIT_ASSERT(runtime.GetAppData(0).SnapshotRegistryHolder->Get()->HasSnapshot(test.TableA, snapshot));
            const auto tables = GetTablesByPathId(test.Server, test.ShardA).first;
            auto request = GetBaseReadRequest(test.TableA, tables.at(test.TableA.PathId).GetDescription(),
                readId, NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
            UNIT_ASSERT_VALUES_EQUAL(request->Record.GetTableId().GetSchemaVersion(), test.TableA.SchemaVersion);
            AddFullRangeQuery(*request);
            return SendRead(test.Server, test.ShardA, request.release(), test.Sender);
        };
        const auto before = readSnapshot(1);
        UNIT_ASSERT_VALUES_EQUAL(before->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(before.get()), "1, 1\n");
        UNIT_ASSERT(before->Record.GetFinished());

        const auto txId = AsyncTruncateTable(test.Server, test.Sender, "/Root", "table-a");
        WaitTxNotification(test.Server, test.Sender, txId);
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));

        // The registry registration survives TRUNCATE, including when a
        // persistent snapshot record used to cover the same version.
        const auto result = readSnapshot(2);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
        RebootTablet(runtime, test.ShardA, test.Sender);
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        const auto restarted = readSnapshot(3);
        UNIT_ASSERT_VALUES_EQUAL(restarted->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }
}

Y_UNIT_TEST_SUITE(DataShardSnapshotRetentionWrite) {
    Y_UNIT_TEST_TWIN(FinalSnapshotUpdateAfterCompaction, Retained) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto snapshot = PrepareFinalSnapshotHistory(test, Retained);

        auto request = MakeFinalSnapshotUpdate(test.TableA, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(request->Record.GetLockTxId(), 0u);
        const auto result = Write(runtime, test.Sender, test.ShardA, std::move(request),
            Retained ? NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED
                     : NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED);
        UNIT_ASSERT(result.GetTxLocks().empty());
        if constexpr (!Retained) {
            UNIT_ASSERT(!result.GetIssues().empty());
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("snapshot"), result.ShortDebugString());
        }
        const TVector<ui64> shards{test.ShardA};
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, test.TableA),
            Retained ? "key = 1, value = 3\nkey = 2, value = 20\n"
                     : "key = 1, value = 1\nkey = 2, value = 20\n");
        if constexpr (Retained) {
            // The post-snapshot insert must stay invisible to UPDATE even after
            // compaction, while the commit itself uses a new write version.
            UNIT_ASSERT_VALUES_EQUAL(
                FormatIntReadResult(test.ReadSnapshot(test.ShardA, test.TableA, snapshot).get()), "1, 1\n");
        }
    }

    Y_UNIT_TEST(FinalSnapshotUpdatePinsHistoryWhileWaiting) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto snapshot = PrepareFinalSnapshotHistory(test, true);
        const ui64 txId = 100;
        TBlockOperationsFailPoint::TGuard blocked([&](ui64 tabletId, ui64 operationTxId) {
            return tabletId == test.ShardA && operationTxId == txId;
        });
        auto request = MakeFinalSnapshotUpdate(test.TableA, snapshot, txId);
        runtime.SendToPipe(test.ShardA, test.Sender, request.release(), 0, GetPipeConfigWithRetries());
        runtime.WaitFor("final snapshot UPDATE before execution", [&] { return !blocked.empty(); });

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
        ForceFullCompaction(runtime, test.ShardA, test.TableA);
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        blocked.Disable();
        blocked.Unblock();
        // Immediate admission happens at execution, after duplicate detection.
        // The pin preserves history while waiting but cannot replace the
        // registry registration required when this fresh batch is admitted.
        const auto result = WaitForWriteCompleted(runtime, test.Sender,
            NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED);
        UNIT_ASSERT_VALUES_EQUAL(result.GetTxId(), txId);
        UNIT_ASSERT(!result.GetIssues().empty());
        UNIT_ASSERT_C(result.GetIssues(0).message().Contains("snapshot"), result.ShortDebugString());
        const TVector<ui64> shards{test.ShardA};
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, test.TableA),
            "key = 1, value = 1\nkey = 2, value = 20\n");
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
    }

    Y_UNIT_TEST_TWIN(RemovedFinalSnapshotUpdateIsRejectedAtPrepare, Volatile) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto snapshot = PrepareFinalSnapshotHistory(test, false);
        const auto mode = Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE
                                   : NKikimrDataEvents::TEvWrite::MODE_PREPARE;
        for (const auto& [shard, tableId] : {std::make_pair(test.ShardA, test.TableA),
                                           std::make_pair(test.ShardB, test.TableB)}) {
            auto request = MakeFinalSnapshotUpdate(tableId, snapshot, 100, mode);
            auto* locks = request->Record.MutableLocks();
            locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            locks->AddSendingShards(test.ShardA);
            locks->AddSendingShards(test.ShardB);
            locks->AddReceivingShards(test.ShardA);
            locks->AddReceivingShards(test.ShardB);
            const auto rejected = Write(runtime, test.Sender, shard, std::move(request),
                NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED);
            UNIT_ASSERT(!rejected.GetIssues().empty());
            UNIT_ASSERT_C(rejected.GetIssues(0).message().Contains("snapshot"), rejected.ShortDebugString());
            UNIT_ASSERT(rejected.GetTxLocks().empty());
            const TVector<ui64> shards{shard};
            UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, tableId),
                "key = 1, value = 1\nkey = 2, value = 20\n");
        }
    }

    Y_UNIT_TEST_TWIN(RetainedFinalSnapshotUpdateCommitsOnTwoShards, Volatile) {
        TSnapshotRetentionTest test(false, 1, true, TDuration::Seconds(1), Volatile);
        auto& runtime = test.GetRuntime();
        const auto snapshot = PrepareFinalSnapshotHistory(test, true);
        const auto mode = Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE
                                   : NKikimrDataEvents::TEvWrite::MODE_PREPARE;
        const ui64 txId = 100;
        ui64 minStep = 0;
        ui64 maxStep = Max<ui64>();
        ui64 coordinator = 0;
        for (const auto& [shard, tableId] : {std::make_pair(test.ShardA, test.TableA),
                                           std::make_pair(test.ShardB, test.TableB)}) {
            auto request = MakeFinalSnapshotUpdate(tableId, snapshot, txId, mode);
            auto* locks = request->Record.MutableLocks();
            locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            locks->AddSendingShards(test.ShardA);
            locks->AddSendingShards(test.ShardB);
            locks->AddReceivingShards(test.ShardA);
            locks->AddReceivingShards(test.ShardB);
            const auto prepared = Write(runtime, test.Sender, shard, std::move(request));
            minStep = Max(minStep, prepared.GetMinStep());
            maxStep = Min(maxStep, prepared.GetMaxStep());
            coordinator = prepared.GetDomainCoordinators(0);
        }

        // Admission at prepare owns the history until the transaction finishes;
        // registry release must not invalidate either participant's decision.
        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));
        if constexpr (Volatile) {
            // Volatile prepares have no persisted body. Their admitted history
            // must already be protected when in-memory state is restored.
            RebootTablet(runtime, test.ShardA, test.Sender);
            test.RunCleanup();
            UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        }
        SendProposeToCoordinator(runtime, test.Sender, {test.ShardA, test.ShardB}, {
            .TxId = txId,
            .Coordinator = coordinator,
            .MinStep = minStep,
            .MaxStep = maxStep,
            .Volatile = Volatile,
        });
        THashSet<ui64> completed;
        for (ui32 i = 0; i < 2; ++i) {
            const auto result = WaitForWriteCompleted(runtime, test.Sender);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxId(), txId);
            completed.insert(result.GetOrigin());
        }
        UNIT_ASSERT(completed.contains(test.ShardA));
        UNIT_ASSERT(completed.contains(test.ShardB));
        for (const auto& [shard, tableId] : {std::make_pair(test.ShardA, test.TableA),
                                           std::make_pair(test.ShardB, test.TableB)}) {
            const TVector<ui64> shards{shard};
            UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, tableId),
                "key = 1, value = 3\nkey = 2, value = 20\n");
        }
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
    }

    Y_UNIT_TEST_TWIN(PreparedFinalSnapshotSurvivesRegistryReleaseAndRestart, Cancel) {
        TSnapshotRetentionTest test(false, 1, true, TDuration::Seconds(1), false);
        auto& runtime = test.GetRuntime();
        const auto snapshot = PrepareFinalSnapshotHistory(test, true);
        const ui64 txId = 100;
        const auto prepared = Write(runtime, test.Sender, test.ShardA,
            MakeFinalSnapshotUpdate(test.TableA, snapshot, txId, NKikimrDataEvents::TEvWrite::MODE_PREPARE));

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
        RebootTablet(runtime, test.ShardA, test.Sender);
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        ForceFullCompaction(runtime, test.ShardA, test.TableA);
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        if constexpr (Cancel) {
            runtime.SendToPipe(test.ShardA, test.Sender, new TEvDataShard::TEvCancelTransactionProposal(txId),
                0, GetPipeConfigWithRetries());
        } else {
            SendProposeToCoordinator(runtime, test.Sender, {test.ShardA}, {
                .TxId = txId,
                .Coordinator = prepared.GetDomainCoordinators(0),
                .MinStep = prepared.GetMinStep(),
                .MaxStep = prepared.GetMaxStep(),
            });
            const auto result = WaitForWriteCompleted(runtime, test.Sender);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxId(), txId);
        }
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        const TVector<ui64> shards{test.ShardA};
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, test.TableA),
            Cancel ? "key = 1, value = 1\nkey = 2, value = 20\n"
                   : "key = 1, value = 3\nkey = 2, value = 20\n");
    }

    Y_UNIT_TEST(RetainedSnapshotReadLockIsBrokenByNewerWrite) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto snapshot = test.WriteVersion(1);
        runtime.SimulateSleep(TDuration::Seconds(1));
        // Advance the cleanup horizon without changing the key we will lock.
        ExecSQL(test.Server, test.Sender, "UPSERT INTO `/Root/table-a` (key, value) VALUES (2, 20);");
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        const ui64 lockTxId = 1234567890001;
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));
        const auto tables = GetTablesByPathId(test.Server, test.ShardA).first;
        auto request = GetBaseReadRequest(test.TableA, tables.at(test.TableA.PathId).GetDescription(),
            1, NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
        request->Record.SetLockTxId(lockTxId);
        request->Record.SetLockNodeId(runtime.GetNodeId(0));
        AddKeyQuery(*request, {1});
        const auto read = SendRead(test.Server, test.ShardA, request.release(), test.Sender);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(read.get()), "1, 1\n");
        UNIT_ASSERT_VALUES_EQUAL(read->Record.GetTxLocks().size(), 1u);
        const auto lock = read->Record.GetTxLocks(0);
        UNIT_ASSERT_C(lock.GetCounter() < TSysTables::TLocksTable::TLock::ErrorMin, lock.ShortDebugString());
        UNIT_ASSERT(!lock.GetHasWrites());

        // The concurrent commit happens after the old-version read acquired its lock.
        ExecSQL(test.Server, test.Sender, "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 2);");
        CommitLock(runtime, test.Sender, test.ShardA, lock,
            NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
        const TVector<ui64> shards{test.ShardA};
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, test.TableA),
            "key = 1, value = 2\nkey = 2, value = 20\n");
    }

    Y_UNIT_TEST_TWIN(RetainedSnapshotWriteCommits, SnapshotIsolation) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto snapshot = test.WriteVersion(1);
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender, "UPSERT INTO `/Root/table-a` (key, value) VALUES (2, 20);");
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        const ui64 lockTxId = 1234567890001;
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));
        auto request = MakeSnapshotUpdate(test.TableA, snapshot, lockTxId, runtime.GetNodeId(0), 3,
            SnapshotIsolation ? NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION : NKikimrDataEvents::OPTIMISTIC);
        const auto written = Write(runtime, test.Sender, test.ShardA, std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(written.GetTxLocks().size(), 1u);
        const auto lock = written.GetTxLocks(0);
        UNIT_ASSERT_C(lock.GetCounter() < TSysTables::TLocksTable::TLock::ErrorMin, lock.ShortDebugString());
        UNIT_ASSERT(lock.GetHasWrites());

        // Keep the original snapshot and lock mode on the final commit too,
        // as KQP does for SnapshotRW transactions.
        auto commit = std::make_unique<NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        commit->Record.SetLockMode(
            SnapshotIsolation ? NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION : NKikimrDataEvents::OPTIMISTIC);
        commit->Record.MutableMvccSnapshot()->SetStep(snapshot.Step);
        commit->Record.MutableMvccSnapshot()->SetTxId(snapshot.TxId);
        auto* locks = commit->Record.MutableLocks();
        locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
        locks->AddSendingShards(test.ShardA);
        locks->AddReceivingShards(test.ShardA);
        *locks->AddLocks() = lock;
        Write(runtime, test.Sender, test.ShardA, std::move(commit));

        const TVector<ui64> shards{test.ShardA};
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, test.TableA),
            "key = 1, value = 3\nkey = 2, value = 20\n");
        UNIT_ASSERT_VALUES_EQUAL(
            FormatIntReadResult(test.ReadSnapshot(test.ShardA, test.TableA, snapshot).get()), "1, 1\n");
    }

    Y_UNIT_TEST_TWIN(RemovedSnapshotWriteSeqNumDuplicateIsReplayed, MixedBatch) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardUncommittedWriteSeqNum(true);
        ExecSQL(test.Server, test.Sender, "UPSERT INTO `/Root/table-a` (key, value) VALUES (2, 20);");
        const auto snapshot = test.WriteVersion(1);

        const ui64 lockTxId = 1234567890001;
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));
        const auto makeRequest = [&]() {
            auto request = MakeSnapshotUpdate(test.TableA, snapshot, lockTxId, runtime.GetNodeId(0), 3);
            auto* seqNum = request->Record.MutableOperations(0)->MutableWriteSeqNum();
            seqNum->SetWriterIndex(0);
            seqNum->SetWriteSeqNum(1);
            return request;
        };
        const auto written = Write(runtime, test.Sender, test.ShardA, makeRequest());
        UNIT_ASSERT_VALUES_EQUAL(written.GetTxLocks().size(), 1u);
        const auto lock = written.GetTxLocks(0);
        UNIT_ASSERT_C(lock.GetCounter() < TSysTables::TLocksTable::TLock::ErrorMin, lock.ShortDebugString());
        UNIT_ASSERT(lock.GetHasWrites());
        UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(lock).GetWriteSeqNum(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(written.GetTxStats().TableAccessStatsSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(written.GetTxStats().GetTableAccessStats(0).GetUpdateRow().GetRows(), 1u);

        // Advance cleanup without changing either the locked key or the fresh
        // operation's key, so a conflict cannot mask stale-snapshot admission.
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender, "UPSERT INTO `/Root/table-a` (key, value) VALUES (3, 30);");
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));

        if constexpr (MixedBatch) {
            auto request = makeRequest();
            auto fresh = MakeWriteRequestOneKeyValue(std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE,
                test.TableA, TShardedTableOptions::DefaultColumns(), 2, 200);
            auto* operation = request->Record.AddOperations();
            *operation = fresh->Record.GetOperations(0);
            operation->SetPayloadIndex(request->AddPayload(TRope(fresh->GetPayload(operation->GetPayloadIndex()))));
            operation->MutableWriteSeqNum()->SetWriterIndex(0);
            operation->MutableWriteSeqNum()->SetWriteSeqNum(2);
            const auto rejected = Write(runtime, test.Sender, test.ShardA, std::move(request),
                NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED);
            UNIT_ASSERT(!rejected.GetIssues().empty());
            UNIT_ASSERT_C(rejected.GetIssues(0).message().Contains("snapshot"), rejected.ShortDebugString());
            UNIT_ASSERT(rejected.GetTxLocks().empty());
        }

        // An exact redelivery needs no history. A rejected fresh tail must also
        // leave the old chain position and its stored successful result intact.
        const auto replayed = Write(runtime, test.Sender, test.ShardA, makeRequest());
        UNIT_ASSERT_VALUES_EQUAL(replayed.GetTxLocks().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(replayed.GetTxLocks(0).DebugString(), lock.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(replayed.GetTxStats().TableAccessStatsSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(replayed.GetTxStats().GetTableAccessStats(0).DebugString(),
            written.GetTxStats().GetTableAccessStats(0).DebugString());
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));

        CommitLock(runtime, test.Sender, test.ShardA, lock);
        const TVector<ui64> shards{test.ShardA};
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, test.TableA),
            "key = 1, value = 3\nkey = 2, value = 20\nkey = 3, value = 30\n");
    }

    Y_UNIT_TEST_TWIN(RemovedSnapshotWriteIsRejected, Reregister) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto snapshot = test.WriteVersion(1);
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));

        if (Reregister) {
            // Membership cannot make already removed history readable again.
            test.PublishRegistry({snapshot}, {test.TableA});
        }
        const ui64 lockTxId = 1234567890001;
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));
        auto request = MakeSnapshotUpdate(test.TableA, snapshot, lockTxId, runtime.GetNodeId(0), 3);
        const auto rejected = Write(runtime, test.Sender, test.ShardA, std::move(request),
            NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED);
        UNIT_ASSERT(!rejected.GetIssues().empty());
        UNIT_ASSERT_C(rejected.GetIssues(0).message().Contains("snapshot"), rejected.ShortDebugString());
        UNIT_ASSERT(rejected.GetTxLocks().empty());
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        const TVector<ui64> shards{test.ShardA};
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, test.TableA), "key = 1, value = 2\n");
    }

    Y_UNIT_TEST(SnapshotWriteForcedOnlineIsRejected) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto snapshot = test.WriteVersion(1);
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender, "UPSERT INTO `/Root/table-a` (key, value) VALUES (2, 20);");
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        auto alter = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto& transaction = *alter->Record.MutableTransaction()->MutableModifyScheme();
        transaction.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
        transaction.SetWorkingDir("/Root");
        auto* description = transaction.MutableAlterTable();
        description->SetName("table-a");
        description->MutablePartitionConfig()->MutablePipelineConfig()->SetDisableImmediate(true);
        runtime.Send(new IEventHandle(MakeTxProxyID(), test.Sender, alter.Release()));
        const auto altered = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(test.Sender);
        UNIT_ASSERT_VALUES_EQUAL(altered->Get()->Record.GetStatus(),
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress);
        WaitTxNotification(test.Server, test.Sender, altered->Get()->Record.GetTxId());
        test.TableA = ResolveTableId(test.Server, test.Sender, "/Root/table-a");
        test.PublishRegistry({snapshot}, {test.TableA});
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));

        const ui64 lockTxId = 1234567890001;
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));
        auto request = MakeSnapshotUpdate(test.TableA, snapshot, lockTxId, runtime.GetNodeId(0), 3);
        const auto rejected = Write(runtime, test.Sender, test.ShardA, std::move(request),
            NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        UNIT_ASSERT(!rejected.GetIssues().empty());
        UNIT_ASSERT_C(rejected.GetIssues(0).message().Contains("planned transaction"), rejected.ShortDebugString());
        UNIT_ASSERT(rejected.GetTxLocks().empty());
        const TVector<ui64> shards{test.ShardA};
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, test.TableA),
            "key = 1, value = 1\nkey = 2, value = 20\n");
    }
}

Y_UNIT_TEST_SUITE(DataShardSnapshotRetentionTransfer) {
    Y_UNIT_TEST(CleanupWhilePreparingCopySnapshotIsTransferred) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto snapshot = test.WriteVersion(1);
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();

        const auto localTableId = GetTablesByPathId(test.Server, test.ShardA).first.at(test.TableA.PathId).GetLocalId();
        const auto taskName = Sprintf("gen0-table-%u-tablet-%" PRIu64, localTableId, test.ShardA);
        TActorId executor;
        ui64 taskId = 0;
        auto observeCompactionTask = runtime.AddObserver<NResourceBroker::TEvResourceBroker::TEvSubmitTask>([&](auto& ev) {
            if (ev->Get()->Task.Name == taskName) {
                executor = ev->Sender;
                taskId = ev->Get()->Task.TaskId;
            }
        });
        using TEvAllocated = NResourceBroker::TEvResourceBroker::TEvResourceAllocated;
        std::vector<TEvAllocated::TPtr> compactions;
        auto blockCompactions = runtime.AddObserver<TEvAllocated>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == executor && ev->Get()->TaskId == taskId) {
                compactions.emplace_back(std::move(ev));
            }
        });
        const auto txId = AsyncCreateCopyTable(test.Server, test.Sender, "/Root", "table-copy", "/Root/table-a");
        runtime.WaitFor("copy snapshot compaction is waiting", [&] { return !compactions.empty(); });

        // CopyTable leaves the source active while its snapshot is being flushed.
        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        test.PublishRegistry({snapshot});
        blockCompactions.Remove();
        observeCompactionTask.Remove();
        for (auto& ev : compactions) {
            const ui32 nodeIndex = ev->GetRecipientRewrite().NodeId() - runtime.GetFirstNodeId();
            runtime.Send(ev.Release(), nodeIndex, true);
        }
        WaitTxNotification(test.Server, test.Sender, txId);
        const auto copyId = ResolveTableId(test.Server, test.Sender, "/Root/table-copy");
        const auto copyShard = NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-copy").at(0);
        UNIT_ASSERT(test.IsRemoved(copyShard, snapshot));
        const auto result = test.ReadSnapshot(copyShard, copyId, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
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
                const auto compaction = CompactTable(runtime, shard, test.TableA, true);
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
        for (ui64 shard : sources) {
            const auto compaction = CompactTable(runtime, shard, test.TableA, true);
            UNIT_ASSERT_VALUES_EQUAL(compaction.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
        }

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

    Y_UNIT_TEST_TWIN(MixedVersionMergePreservesEachSourcesReadableSnapshots, ModernFirst) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 10), (5, 50);");
        const auto snapshot = CreateVolatileSnapshot(test.Server, {"/Root/table-a"}, TDuration::Hours(1));
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, 11), (5, 51);");
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        SetSplitMergePartCountLimit(test.Server->GetRuntime(), -1);
        const auto splitTx = AsyncSplitTable(test.Server, test.Sender, "/Root/table-a", test.ShardA, 4);
        WaitTxNotification(test.Server, test.Sender, splitTx);
        const auto sources = NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-a");
        UNIT_ASSERT_VALUES_EQUAL(sources.size(), 2u);
        const ui64 legacy = sources.at(0);
        const ui64 modern = sources.at(1);

        // Keep a real persistent pin only on the legacy source. The modern
        // source now preserves the same history solely through the registry.
        auto discard = MakeHolder<TEvDataShard::TEvDiscardVolatileSnapshotRequest>();
        discard->Record.SetOwnerId(test.TableA.PathId.OwnerId);
        discard->Record.SetPathId(test.TableA.PathId.LocalPathId);
        discard->Record.SetStep(snapshot.Step);
        discard->Record.SetTxId(snapshot.TxId);
        runtime.SendToPipe(modern, test.Sender, discard.Release());
        const auto discarded = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvDiscardVolatileSnapshotResponse>(test.Sender);
        UNIT_ASSERT_VALUES_EQUAL(ui32(discarded->Get()->Record.GetStatus()),
            ui32(NKikimrTxDataShard::TEvDiscardVolatileSnapshotResponse::DISCARDED));
        const auto refresh = [&](ui64 shard) {
            auto request = MakeHolder<TEvDataShard::TEvRefreshVolatileSnapshotRequest>();
            request->Record.SetOwnerId(test.TableA.PathId.OwnerId);
            request->Record.SetPathId(test.TableA.PathId.LocalPathId);
            request->Record.SetStep(snapshot.Step);
            request->Record.SetTxId(snapshot.TxId);
            runtime.SendToPipe(shard, test.Sender, request.Release());
            const auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvRefreshVolatileSnapshotResponse>(test.Sender);
            return response->Get()->Record.GetStatus();
        };
        using TRefresh = NKikimrTxDataShard::TEvRefreshVolatileSnapshotResponse;
        UNIT_ASSERT_VALUES_EQUAL(ui32(refresh(legacy)), ui32(TRefresh::REFRESHED));
        UNIT_ASSERT_VALUES_EQUAL(ui32(refresh(modern)), ui32(TRefresh::SNAPSHOT_NOT_FOUND));
        test.RunCleanup();
        for (ui64 shard : sources) {
            UNIT_ASSERT(snapshot < test.GetCleanupBoundary(shard));
            UNIT_ASSERT(!test.IsRemoved(shard, snapshot));
            ForceFullCompaction(runtime, shard, test.TableA, true);
        }

        TBlockEvents<TEvDataShard::TEvSplitTransferSnapshot> transfers(runtime);
        TBlockEvents<TEvDataShard::TEvSplitTransferSnapshotAck> acknowledgements(runtime);
        const auto mergeTx = AsyncMergeTable(test.Server, test.Sender, "/Root/table-a", sources);
        runtime.WaitFor("both merge source snapshots are captured", [&] { return transfers.size() == 2; },
            TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_UNEQUAL(transfers.front()->Get()->Record.GetSrcTabletId(),
            transfers.back()->Get()->Record.GetSrcTabletId());
        for (auto& ev : transfers) {
            auto& record = ev->Get()->Record;
            UNIT_ASSERT(record.TableSnapshotSize());
            if (record.GetSrcTabletId() == legacy) {
                UNIT_ASSERT_VALUES_EQUAL(record.PersistentSnapshotsSize(), 1u);
                UNIT_ASSERT_VALUES_EQUAL(record.GetPersistentSnapshots(0).GetStep(), snapshot.Step);
                UNIT_ASSERT_VALUES_EQUAL(record.GetPersistentSnapshots(0).GetTxId(), snapshot.TxId);
                for (auto& table : *record.MutableTableSnapshot()) {
                    table.ClearRemovedRowVersions();
                }
            } else {
                UNIT_ASSERT_VALUES_EQUAL(record.GetSrcTabletId(), modern);
                UNIT_ASSERT_VALUES_EQUAL(record.PersistentSnapshotsSize(), 0u);
                for (const auto& table : record.GetTableSnapshot()) {
                    UNIT_ASSERT(table.HasRemovedRowVersions());
                }
            }
        }
        const ui64 firstSource = ModernFirst ? modern : legacy;
        if (transfers.front()->Get()->Record.GetSrcTabletId() != firstSource) {
            std::swap(transfers.front(), transfers.back());
        }
        const auto firstSender = transfers.front()->Sender;
        transfers.Unblock(1);
        runtime.WaitFor("the first source is committed before the second arrives", [&] {
            return !acknowledgements.empty();
        }, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(acknowledgements.front()->GetRecipientRewrite(), firstSender);
        acknowledgements.Unblock(1);
        transfers.Stop().Unblock();
        acknowledgements.Stop().Unblock();
        WaitTxNotification(test.Server, test.Sender, mergeTx);
        const auto merged = NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-a").at(0);

        // Persistent snapshots intersect across sources, but each source's
        // physically readable history is combined independently of that set.
        UNIT_ASSERT_VALUES_EQUAL(ui32(refresh(merged)), ui32(TRefresh::SNAPSHOT_NOT_FOUND));
        const auto verify = [&](ui64 readId) {
            UNIT_ASSERT(snapshot < test.GetCleanupBoundary(merged));
            UNIT_ASSERT(!test.IsRemoved(merged, snapshot));
            const auto result = test.ReadSnapshot(merged, test.TableA, snapshot, readId);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(result.get()), "1, 10\n5, 50\n");
            UNIT_ASSERT(result->Record.GetFinished());
        };
        verify(1);
        ForceFullCompaction(runtime, merged, test.TableA, true);
        RebootTablet(runtime, merged, test.Sender);
        UNIT_ASSERT_VALUES_EQUAL(ui32(refresh(merged)), ui32(TRefresh::SNAPSHOT_NOT_FOUND));
        verify(2);

        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(merged, snapshot));
        const auto removed = test.ReadSnapshot(merged, test.TableA, snapshot, 3);
        UNIT_ASSERT_VALUES_EQUAL(removed->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST_TWIN(LegacySplitDoesNotTrustNewRegistryRegistrations, MissingRegistry) {
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
        if (MissingRegistry) {
            runtime.GetAppData(0).SnapshotRegistryHolder->Set(nullptr);
        }
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

    Y_UNIT_TEST(CopyTableWaitsForRegistryBeforeAdditionalCleanup) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        const auto retained = test.WriteVersion(1);
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto removed = test.WriteVersion(2);
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(3);
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({retained}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(retained < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, retained));
        UNIT_ASSERT(test.IsRemoved(test.ShardA, removed));

        runtime.GetAppData(0).SnapshotRegistryHolder->Set(nullptr);
        const auto txId = AsyncCreateCopyTable(test.Server, test.Sender, "/Root", "table-copy", "/Root/table-a");
        WaitTxNotification(test.Server, test.Sender, txId);
        const auto copyId = ResolveTableId(test.Server, test.Sender, "/Root/table-copy");
        const auto copyShard = NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-copy").at(0);
        test.RunCleanup();

        // Source deletions must be restored even without a ready registry, but
        // its watermark is insufficient to delete the surviving retained point.
        UNIT_ASSERT(test.IsRemoved(copyShard, removed));
        UNIT_ASSERT(!test.IsRemoved(copyShard, retained));
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(test.ReadSnapshot(copyShard, copyId, retained).get()), "1, 1\n");
        const auto lost = test.ReadSnapshot(copyShard, copyId, removed);
        UNIT_ASSERT_VALUES_EQUAL(lost->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);

        // Once ready, the registry allows collection using the copy's identity.
        // The source-only registration keeps the original table's point alive.
        test.PublishRegistry({retained}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(copyShard, retained));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, retained));
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

        const auto compaction = CompactTable(runtime, copyShard, copyId, true);
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
    Y_UNIT_TEST(KqpExecutorRetainsDirectlyAcquiredSnapshot) {
        TSnapshotRetentionTest test(true, 2);
        auto& runtime = test.GetRuntime();
        using TService = NLongTxService::TEvLongTxService;
        auto pinTarget = runtime.AddObserver<TEvHive::TEvCreateTablet>([&](auto& ev) {
            ev->Get()->Record.ClearAllowedNodeIDs();
            ev->Get()->Record.AddAllowedNodeIDs(runtime.GetNodeId(1));
        });
        UNIT_ASSERT_VALUES_EQUAL(NKqpHelpers::KqpSchemeExec(runtime, R"(
            CREATE TABLE `/Root/table-c` (key Uint32, value Uint32, PRIMARY KEY (key))
            WITH (PARTITION_AT_KEYS = (2));
        )", "/Root"), "SUCCESS");
        pinTarget.Remove();
        const auto targetId = ResolveTableId(test.Server, test.Sender, "/Root/table-c");
        const auto targetShards = NKqp::GetTableShards(test.Server, test.Sender, "/Root/table-c");
        UNIT_ASSERT_VALUES_EQUAL(targetShards.size(), 2u);
        for (ui64 shard : targetShards) {
            UNIT_ASSERT_VALUES_EQUAL(ResolveTablet(runtime, shard).NodeId(), runtime.GetNodeId(1));
        }
        const auto delayedShard = ResolveTablet(runtime, targetShards.back());
        const auto writeGeneration = [&](ui32 generation) {
            ExecSQL(test.Server, test.Sender, Sprintf(
                "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, %u);"
                "UPSERT INTO `/Root/table-b` (key, value) VALUES (1, %u);"
                "UPSERT INTO `/Root/table-c` (key, value) VALUES (1, %u), (2, %u);",
                generation, generation, generation, generation));
        };
        const TString sessionId = NKqpHelpers::CreateSessionRPC(runtime, "/Root");
        const auto makeRequest = [&] {
            // One SERIALIZABLE read phase spanning two shards makes the data
            // executor acquire its own snapshot, without a session manager.
            auto request = NKqpHelpers::MakeSimpleRequestRPC(
                "SELECT key, value FROM `/Root/table-c` ORDER BY key;", sessionId, {}, true);
            request.mutable_query_cache_policy()->set_keep_in_cache(true);
            return request;
        };
        const TString originalRows = "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }";
        writeGeneration(1);
        const auto warmed = NKqpHelpers::AwaitResponse(runtime,
            NKqpHelpers::SendRequest(runtime, makeRequest(), "/Root"));
        UNIT_ASSERT_VALUES_EQUAL_C(warmed.operation().status(), Ydb::StatusIds::SUCCESS, warmed.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(NKqpHelpers::FormatResult(warmed), originalRows);
        test.RunCleanup();
        writeGeneration(2);
        writeGeneration(1);

        TActorId executor;
        TRowVersion snapshot = TRowVersion::Min();
        ui32 acquisitions = 0;
        bool acquiredWithHandle = false;
        auto captureRequest = runtime.AddObserver<TService::TEvAcquireReadSnapshot>([&](auto& ev) {
            for (const auto& tableId : ev->Get()->TableIds) {
                if (tableId.PathId == targetId.PathId) {
                    UNIT_ASSERT_VALUES_EQUAL(runtime.FindActorName(ev->Sender), "KQP_DATA_EXECUTER_ACTOR");
                    UNIT_ASSERT_VALUES_EQUAL(ev->Sender.NodeId(), runtime.GetNodeId(0));
                    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TableIds.size(), 1u);
                    executor = ev->Sender;
                    ++acquisitions;
                }
            }
        });
        auto captureSnapshot = runtime.AddObserver<TService::TEvAcquireReadSnapshotResult>([&](auto& ev) {
            if (executor && ev->GetRecipientRewrite() == executor) {
                UNIT_ASSERT_VALUES_EQUAL(runtime.FindActorName(ev->GetRecipientRewrite()), "KQP_DATA_EXECUTER_ACTOR");
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, Ydb::StatusIds::SUCCESS);
                snapshot = ev->Get()->Snapshot;
                // Observe ownership without copying the handle or retaining the event.
                acquiredWithHandle = bool(ev->Get()->SnapshotHandle);
            }
        });
        TBlockEvents<TEvDataShard::TEvRead> reads(runtime, [&](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return snapshot && ev->GetRecipientRewrite() == delayedShard
                && record.HasSnapshot() && TRowVersion::FromProto(record.GetSnapshot()) == snapshot
                && TPathId(record.GetTableId().GetOwnerId(), record.GetTableId().GetTableId()) == targetId.PathId;
        });
        auto pending = NKqpHelpers::SendRequest(runtime, makeRequest(), "/Root");
        runtime.WaitFor("the executor's first read on one remote shard is blocked", [&] {
            return !reads.empty() || pending.HasValue() || pending.HasException();
        }, TDuration::Seconds(30));
        UNIT_ASSERT(!reads.empty());
        UNIT_ASSERT_VALUES_EQUAL(acquisitions, 1u);
        UNIT_ASSERT(acquiredWithHandle);
        captureRequest.Remove();
        captureSnapshot.Remove();
        runtime.WaitFor("the executor's scoped snapshot is published remotely", [&] {
            for (ui32 node = 0; node < 2; ++node) {
                const auto registry = runtime.GetAppData(node).SnapshotRegistryHolder->Get();
                if (!registry || !registry->GetActiveSnapshots(targetId).contains(snapshot)
                    || registry->HasSnapshot(TTableId{}, snapshot)
                    || registry->HasSnapshot(test.TableA, snapshot) || registry->HasSnapshot(test.TableB, snapshot))
                {
                    return false;
                }
            }
            return true;
        }, TDuration::Seconds(30));
        for (ui32 i = 0; i < 3; ++i) {
            writeGeneration(2);
            test.RunCleanup();
        }
        UNIT_ASSERT(runtime.GetAppData(1).SnapshotRegistryHolder->Get()->GetActiveSnapshots(targetId).contains(snapshot));
        for (ui64 shard : targetShards) {
            UNIT_ASSERT(snapshot < test.GetCleanupBoundary(shard));
            UNIT_ASSERT(!test.IsRemoved(shard, snapshot));
            ForceFullCompaction(runtime, shard, targetId);
        }
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
        UNIT_ASSERT(!pending.HasValue() && !pending.HasException());
        reads.Stop().Unblock();
        const auto result = NKqpHelpers::AwaitResponse(runtime, pending);
        UNIT_ASSERT_VALUES_EQUAL_C(result.operation().status(), Ydb::StatusIds::SUCCESS, result.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(NKqpHelpers::FormatResult(result), originalRows);
        runtime.WaitFor("the completed executor releases its scoped snapshot", [&] {
            for (ui32 node = 0; node < 2; ++node) {
                const auto registry = runtime.GetAppData(node).SnapshotRegistryHolder->Get();
                if (!registry || registry->HasSnapshot(targetId, snapshot)) {
                    return false;
                }
            }
            return true;
        }, TDuration::Seconds(30));
        test.RunCleanup();
        for (ui64 shard : targetShards) {
            UNIT_ASSERT(test.IsRemoved(shard, snapshot));
        }
        NKqpHelpers::CloseSession(runtime, sessionId);
    }

    Y_UNIT_TEST_QUAD(KqpRetainedSnapshotInsertEraseConflictSurvivesCompaction, Compact, PreviouslyDeleted) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        if constexpr (PreviouslyDeleted) {
            ExecSQL(test.Server, test.Sender, "INSERT INTO `/Root/table-a` (key, value) VALUES (2, 10);");
            runtime.SimulateSleep(TDuration::Seconds(1));
            ExecSQL(test.Server, test.Sender, "DELETE FROM `/Root/table-a` WHERE key = 2;");
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        test.WriteVersion(1);
        const TString sessionId = NKqpHelpers::CreateSessionRPC(runtime, "/Root");
        TRowVersion snapshot = TRowVersion::Min();
        auto captureSnapshot = runtime.AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.HasSnapshot() && record.GetTableId().GetOwnerId() == test.TableA.PathId.OwnerId
                && record.GetTableId().GetTableId() == test.TableA.PathId.LocalPathId)
            {
                snapshot = TRowVersion::FromProto(record.GetSnapshot());
            }
        });
        auto begin = NKqpHelpers::MakeSimpleRequestRPC(
            "SELECT key, value FROM `/Root/table-a` WHERE key = 1;", sessionId, {}, false);
        begin.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_write();
        const auto started = NKqpHelpers::AwaitResponse(runtime,
            NKqpHelpers::SendRequest(runtime, std::move(begin), "/Root"));
        captureSnapshot.Remove();
        UNIT_ASSERT_VALUES_EQUAL_C(started.operation().status(), Ydb::StatusIds::SUCCESS, started.ShortDebugString());
        UNIT_ASSERT(snapshot);
        Ydb::Table::ExecuteQueryResult startedResult;
        UNIT_ASSERT(started.operation().result().UnpackTo(&startedResult));
        const TString txId = startedResult.tx_meta().id();
        UNIT_ASSERT(!txId.empty());
        test.PublishRegistry({snapshot}, {test.TableA});

        // This transaction has neither read nor written key 2. Separate commits
        // insert and erase it after its snapshot, leaving only conflict evidence.
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender, "INSERT INTO `/Root/table-a` (key, value) VALUES (2, 20);");
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender, "DELETE FROM `/Root/table-a` WHERE key = 2;");
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender, "UPSERT INTO `/Root/table-a` (key, value) VALUES (3, 30);");
        runtime.SimulateSleep(TDuration::Seconds(3));
        test.PublishRegistry({snapshot}, {test.TableA});
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        if constexpr (Compact) {
            ForceFullCompaction(runtime, test.ShardA, test.TableA);
        }
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        UNIT_ASSERT_VALUES_EQUAL(
            FormatIntReadResult(test.ReadSnapshot(test.ShardA, test.TableA, snapshot).get()), "1, 1\n");

        TActorId writer;
        auto captureWrite = runtime.AddObserver<NEvents::TDataEvents::TEvWrite>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetLockMode() != NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION) {
                return;
            }
            for (const auto& operation : record.GetOperations()) {
                if (operation.GetTableId().GetOwnerId() == test.TableA.PathId.OwnerId
                    && operation.GetTableId().GetTableId() == test.TableA.PathId.LocalPathId)
                {
                    UNIT_ASSERT_VALUES_EQUAL(operation.GetType(), NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT);
                    UNIT_ASSERT_VALUES_EQUAL(record.GetLockTxId(), 0u);
                    UNIT_ASSERT_VALUES_EQUAL(record.GetTxMode(), NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
                    UNIT_ASSERT(record.HasMvccSnapshot());
                    UNIT_ASSERT_VALUES_EQUAL(record.GetMvccSnapshot().GetStep(), snapshot.Step);
                    UNIT_ASSERT_VALUES_EQUAL(record.GetMvccSnapshot().GetTxId(), snapshot.TxId);
                    writer = ev->Sender;
                }
            }
        });
        bool writeConflict = false;
        auto captureConflict = runtime.AddObserver<NEvents::TDataEvents::TEvWriteResult>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (!writer || ev->GetRecipientRewrite() != writer || record.GetOrigin() != test.ShardA) {
                return;
            }
            for (const auto& issue : record.GetIssues()) {
                if (issue.message().Contains("Write conflict with concurrent transaction.")) {
                    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
                    UNIT_ASSERT_VALUES_EQUAL(record.GetTxStats().GetLocksBrokenAsVictim(), 1u);
                    writeConflict = true;
                }
            }
        });
        const auto failed = NKqpHelpers::AwaitResponse(runtime, NKqpHelpers::SendRequest(runtime,
            NKqpHelpers::MakeSimpleRequestRPC(
                "UPSERT INTO `/Root/table-a` (key, value) VALUES (2u, 200u);", sessionId, txId, true), "/Root"));
        captureWrite.Remove();
        captureConflict.Remove();
        UNIT_ASSERT(writer);
        UNIT_ASSERT_C(writeConflict, failed.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(failed.operation().status(), Ydb::StatusIds::ABORTED, failed.ShortDebugString());
        const TVector<ui64> shards{test.ShardA};
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, test.TableA),
            "key = 1, value = 1\nkey = 3, value = 30\n");

        // Releasing the registration lets cleanup collect the old snapshot;
        // a fresh transaction can then insert the key successfully.
        test.PublishRegistry();
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        ForceFullCompaction(runtime, test.ShardA, test.TableA);
        auto retry = NKqpHelpers::MakeSimpleRequestRPC(
            "SELECT key FROM `/Root/table-a` WHERE key = 1;"
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (2u, 200u);", sessionId, {}, true);
        retry.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_write();
        const auto retried = NKqpHelpers::AwaitResponse(runtime,
            NKqpHelpers::SendRequest(runtime, std::move(retry), "/Root"));
        UNIT_ASSERT_VALUES_EQUAL_C(retried.operation().status(), Ydb::StatusIds::SUCCESS, retried.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, test.TableA),
            "key = 1, value = 1\nkey = 2, value = 200\nkey = 3, value = 30\n");
        NKqpHelpers::CloseSession(runtime, sessionId);
    }

    Y_UNIT_TEST_TWIN(KqpFinalSnapshotUpdateWithRemovedHistoryIsAborted, Planned) {
        TSnapshotRetentionTest test;
        auto& runtime = test.GetRuntime();
        test.WriteVersion(1);
        const TString sessionId = NKqpHelpers::CreateSessionRPC(runtime, "/Root");
        TString query = "SELECT key, value FROM `/Root/table-a` WHERE key = 1;";
        if constexpr (Planned) {
            query += "SELECT key, value FROM `/Root/table-b` WHERE key = 1;";
        }
        query += "UPDATE `/Root/table-a` ON (key, value) VALUES (1u, 3u), (2u, 200u);";
        if constexpr (Planned) {
            query += "UPDATE `/Root/table-b` ON (key, value) VALUES (1u, 3u), (2u, 200u);";
        }
        const auto makeRequest = [&] {
            auto request = NKqpHelpers::MakeSimpleRequestRPC(query, sessionId, {}, true);
            request.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_write();
            return request;
        };

        TRowVersion snapshot = TRowVersion::Min();
        auto captureReads = runtime.AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.HasSnapshot() && record.GetTableId().GetOwnerId() == test.TableA.PathId.OwnerId
                && (record.GetTableId().GetTableId() == test.TableA.PathId.LocalPathId
                    || record.GetTableId().GetTableId() == test.TableB.PathId.LocalPathId))
            {
                const auto readSnapshot = TRowVersion::FromProto(record.GetSnapshot());
                if (snapshot) {
                    UNIT_ASSERT_VALUES_EQUAL(readSnapshot, snapshot);
                }
                snapshot = readSnapshot;
            }
        });
        TBlockEvents<NEvents::TDataEvents::TEvWrite> writes(runtime, [&](const auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetLockMode() != NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION
                || record.GetLockTxId())
            {
                return false;
            }
            for (const auto& operation : record.GetOperations()) {
                const auto& tableId = operation.GetTableId();
                if (operation.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE
                    && tableId.GetOwnerId() == test.TableA.PathId.OwnerId
                    && (tableId.GetTableId() == test.TableA.PathId.LocalPathId
                        || tableId.GetTableId() == test.TableB.PathId.LocalPathId))
                {
                    return true;
                }
            }
            return false;
        });
        auto pending = NKqpHelpers::SendRequest(runtime, makeRequest(), "/Root");
        const size_t participantCount = Planned ? 2 : 1;
        runtime.WaitFor("KQP final snapshot UPDATE batches before shard admission", [&] {
            return writes.size() >= participantCount || pending.HasValue() || pending.HasException();
        });
        UNIT_ASSERT_VALUES_EQUAL(writes.size(), participantCount);
        UNIT_ASSERT(snapshot);
        THashSet<TActorId> writers;
        THashSet<ui64> tables;
        for (const auto& ev : writes) {
            const auto& record = ev->Get()->Record;
            UNIT_ASSERT(record.HasMvccSnapshot());
            UNIT_ASSERT_VALUES_EQUAL(record.GetMvccSnapshot().GetStep(), snapshot.Step);
            UNIT_ASSERT_VALUES_EQUAL(record.GetMvccSnapshot().GetTxId(), snapshot.TxId);
            UNIT_ASSERT_VALUES_EQUAL(record.GetLockTxId(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(record.GetTxMode() != NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, Planned);
            UNIT_ASSERT(!record.GetOperations().empty());
            for (const auto& operation : record.GetOperations()) {
                UNIT_ASSERT_VALUES_EQUAL(operation.GetType(), NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE);
                tables.insert(operation.GetTableId().GetTableId());
            }
            writers.insert(ev->Sender);
        }
        UNIT_ASSERT(tables.contains(test.TableA.PathId.LocalPathId));
        UNIT_ASSERT_VALUES_EQUAL(tables.contains(test.TableB.PathId.LocalPathId), Planned);
        captureReads.Remove();

        // The exact-key reads and final batches are already complete in KQP.
        // Change only a key they did not read, so a broken read lock cannot
        // mask the stale-history admission failure on either shard.
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (2, 20);"
            "UPSERT INTO `/Root/table-b` (key, value) VALUES (2, 20);");
        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(test.Server, test.Sender,
            "UPSERT INTO `/Root/table-a` (key, value) VALUES (2, 20);"
            "UPSERT INTO `/Root/table-b` (key, value) VALUES (2, 20);");
        test.PublishRegistry();
        test.RunCleanup();
        for (const auto& [shard, tableId] : {std::make_pair(test.ShardA, test.TableA),
                                           std::make_pair(test.ShardB, test.TableB)}) {
            UNIT_ASSERT(test.IsRemoved(shard, snapshot));
            const auto compacted = CompactTable(runtime, shard, tableId, false);
            UNIT_ASSERT_VALUES_EQUAL(compacted.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
        }
        UNIT_ASSERT(!pending.HasValue() && !pending.HasException());
        bool staleWriteRejected = false;
        auto captureRejectedWrite = runtime.AddObserver<NEvents::TDataEvents::TEvWriteResult>([&](auto& ev) {
            if (!writers.contains(ev->GetRecipientRewrite())) {
                return;
            }
            const auto& record = ev->Get()->Record;
            for (const auto& issue : record.GetIssues()) {
                if (issue.message().Contains("writes at stale snapshot")) {
                    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED);
                    staleWriteRejected = true;
                }
            }
        });
        writes.Stop().Unblock();
        const auto failed = NKqpHelpers::AwaitResponse(runtime, pending);
        captureRejectedWrite.Remove();
        UNIT_ASSERT(staleWriteRejected);
        UNIT_ASSERT_VALUES_EQUAL_C(failed.operation().status(), Ydb::StatusIds::ABORTED, failed.ShortDebugString());
        UNIT_ASSERT_C(failed.operation().ShortDebugString().Contains("stale snapshot"), failed.ShortDebugString());
        for (const auto& [shard, tableId] : {std::make_pair(test.ShardA, test.TableA),
                                           std::make_pair(test.ShardB, test.TableB)}) {
            const TVector<ui64> shards{shard};
            UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shards, tableId),
                "key = 1, value = 1\nkey = 2, value = 20\n");
        }

        const auto retried = NKqpHelpers::AwaitResponse(runtime,
            NKqpHelpers::SendRequest(runtime, makeRequest(), "/Root"));
        UNIT_ASSERT_VALUES_EQUAL_C(retried.operation().status(), Ydb::StatusIds::SUCCESS, retried.ShortDebugString());
        const TVector<ui64> shardsA{test.ShardA};
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shardsA, test.TableA),
            "key = 1, value = 3\nkey = 2, value = 200\n");
        if constexpr (Planned) {
            const TVector<ui64> shardsB{test.ShardB};
            UNIT_ASSERT_VALUES_EQUAL(ReadTable(test.Server, shardsB, test.TableB),
                "key = 1, value = 3\nkey = 2, value = 200\n");
        }
        NKqpHelpers::CloseSession(runtime, sessionId);
    }

    Y_UNIT_TEST(DelayedCoordinatorReplyReacquiresSnapshotBeforeRegistration) {
        TSnapshotRetentionTest test(true, 2);
        auto& runtime = test.GetRuntime();
        using TService = NLongTxService::TEvLongTxService;
        test.WriteVersion(1);
        const auto sender = runtime.AllocateEdgeActor(0);
        bool requested = false;
        auto requestObserver = runtime.AddObserver<TService::TEvAcquireReadSnapshot>([&](auto& ev) {
            if (ev->Sender == sender) {
                UNIT_ASSERT(!ev->Get()->Snapshot);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TableIds.size(), 1u);
                UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite().NodeId(), runtime.GetNodeId(0));
                requested = true;
            }
        });
        bool clientResultSeen = false;
        auto resultObserver = runtime.AddObserver<TService::TEvAcquireReadSnapshotResult>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == sender) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 123u);
                clientResultSeen = true;
            }
        });
        TActorId firstAcquireActor;
        TBlockEvents<TEvTxProxy::TEvAcquireReadStepResult> delayed(runtime, [&](const auto& ev) {
            if (!firstAcquireActor && requested
                && ev->GetRecipientRewrite().NodeId() == runtime.GetNodeId(0)
                && runtime.FindActorName(ev->GetRecipientRewrite()) == "LONG_TX_SERVICE_ACQUIRE_SNAPSHOT")
            {
                firstAcquireActor = ev->GetRecipientRewrite();
            }
            return firstAcquireActor && ev->GetRecipientRewrite() == firstAcquireActor;
        });
        runtime.Send(new IEventHandle(NLongTxService::MakeLongTxServiceID(runtime.GetNodeId(0)), sender,
            new TService::TEvAcquireReadSnapshot("/Root", {test.TableB}), 0, 123), 0, true);
        runtime.WaitFor("the ordinary acquisition's coordinator reply is delayed", [&] {
            return !delayed.empty();
        }, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(delayed.size(), 1u);
        UNIT_ASSERT(delayed.front()->Get()->Record.GetCoordinatorID());
        const TRowVersion original(delayed.front()->Get()->Record.GetStep(), Max<ui64>());
        UNIT_ASSERT(original > TRowVersion::Min());

        // Only this acquisition child is blocked. Other writers and snapshot
        // acquisitions advance the target while no handle protects original.
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.RunCleanup();
        test.RunCleanup();
        UNIT_ASSERT(original < test.GetCleanupBoundary(test.ShardB));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, original));
        UNIT_ASSERT(!clientResultSeen);
        for (ui32 node : {0u, 1u}) {
            const auto registry = runtime.GetAppData(node).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry);
            UNIT_ASSERT(!registry->HasSnapshot(test.TableB, original));
        }

        // Deliver the exact old coordinator result. The service must reacquire
        // rather than register history that a remote shard has already removed.
        delayed.Stop().Unblock();
        auto acquired = runtime.GrabEdgeEventRethrow<TService::TEvAcquireReadSnapshotResult>(sender);
        UNIT_ASSERT(clientResultSeen);
        UNIT_ASSERT_VALUES_EQUAL_C(acquired->Get()->Status, Ydb::StatusIds::SUCCESS,
            acquired->Get()->Issues.ToString());
        UNIT_ASSERT(acquired->Get()->SnapshotHandle);
        const auto replacement = acquired->Get()->Snapshot;
        UNIT_ASSERT(original < replacement);
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, replacement));
        const auto firstRead = test.ReadSnapshot(test.ShardB, test.TableB, replacement);
        UNIT_ASSERT_VALUES_EQUAL(firstRead->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(firstRead.get()), "1, 2\n");
        UNIT_ASSERT(firstRead->Record.GetFinished());

        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(3);
        runtime.WaitFor("the replacement snapshot is published to the remote node", [&] {
            const auto registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            return registry && registry->GetActiveSnapshots(test.TableB).contains(replacement);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(replacement < test.GetCleanupBoundary(test.ShardB));
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, replacement));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, original));
        for (ui32 node : {0u, 1u}) {
            const auto registry = runtime.GetAppData(node).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry);
            UNIT_ASSERT(!registry->GetActiveSnapshots(test.TableB).contains(original));
        }
        const auto retainedRead = test.ReadSnapshot(test.ShardB, test.TableB, replacement, 2);
        UNIT_ASSERT_VALUES_EQUAL(retainedRead->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(retainedRead.get()), "1, 2\n");
        UNIT_ASSERT(retainedRead->Record.GetFinished());

        acquired->Get()->SnapshotHandle.Reset();
        runtime.WaitFor("the replacement snapshot is released on the remote node", [&] {
            const auto registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            return registry && !registry->HasSnapshot(test.TableB, replacement);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardB, replacement));
        const auto removed = test.ReadSnapshot(test.ShardB, test.TableB, replacement, 3);
        UNIT_ASSERT_VALUES_EQUAL(removed->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(ExplicitSnapshotSurvivesDelayedPublication) {
        TSnapshotRetentionTest test(true, 2);
        auto& runtime = test.GetRuntime();
        using TService = NLongTxService::TEvLongTxService;
        TBlockEvents<TService::TEvPropagateSnapshots> publication(runtime);
        // Let maintenance publish any collection already received before the
        // block, so the baseline also reflects the producer's buffered state.
        runtime.SimulateSleep(TDuration::Seconds(2));
        const auto initialRegistry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
        UNIT_ASSERT(initialRegistry);
        const auto collectionTime = initialRegistry->GetOldestCollectionTime();
        const ui64 promotionCutoff = initialRegistry->GetMinPromotionCutoffStep();
        const auto& config = runtime.GetAppData(1).LongTxServiceConfig;
        const auto margin = TDuration::Seconds(config.GetLocalSnapshotPromotionTimeSeconds())
            + TDuration::MilliSeconds(config.GetMaxClockSkewMs());
        const ui64 skewMs = config.GetMaxClockSkewMs();
        const TRowVersion cleanupFloor(promotionCutoff > skewMs ? promotionCutoff - skewMs : 0, 0);
        const auto initialRemoved = test.GetRemovedRanges(test.ShardB);
        const auto cleanupLimit = Max(cleanupFloor,
            initialRemoved.empty() ? TRowVersion::Min() : std::prev(initialRemoved.end())->Upper);
        const auto snapshot = test.WriteVersion(1);
        UNIT_ASSERT(cleanupLimit < snapshot);
        const auto sender = runtime.AllocateEdgeActor();
        runtime.Send(NLongTxService::MakeLongTxServiceID(runtime.GetNodeId(0)), sender,
            new TService::TEvAcquireReadSnapshot("/Root", { test.TableA, test.TableB }, snapshot), 0, true);
        auto registered = runtime.GrabEdgeEventRethrow<TService::TEvAcquireReadSnapshotResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(
            registered->Get()->Status, Ydb::StatusIds::SUCCESS, registered->Get()->Issues.ToString());
        UNIT_ASSERT(registered->Get()->SnapshotHandle);
        UNIT_ASSERT_VALUES_EQUAL(registered->Get()->Snapshot, snapshot);

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        // Keep cleanup running for several promotion/skew windows while the
        // real service cannot publish either its snapshots or a newer floor.
        const auto cleanupStarted = runtime.GetCurrentTime();
        for (ui32 value = 2; value <= 6; ++value) {
            test.WriteVersion(value);
            test.RunCleanup();
            UNIT_ASSERT(!publication.empty());
            const auto registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry);
            UNIT_ASSERT_VALUES_EQUAL(registry->GetOldestCollectionTime(), collectionTime);
            UNIT_ASSERT_VALUES_EQUAL(registry->GetMinPromotionCutoffStep(), promotionCutoff);
            UNIT_ASSERT(!registry->HasSnapshot(test.TableB, snapshot));
            UNIT_ASSERT(test.GetCleanupBoundary(test.ShardB) <= cleanupLimit);
            UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
            UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));
            UNIT_ASSERT_VALUES_EQUAL(
                FormatIntReadResult(test.ReadSnapshot(test.ShardB, test.TableB, snapshot).get()), "1, 1\n");
        }
        UNIT_ASSERT(margin < runtime.GetCurrentTime() - cleanupStarted);

        publication.Stop().Unblock();
        runtime.WaitFor("explicit snapshot published to the remote node", [&] {
            const auto& current = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            return current && current->GetActiveSnapshots(test.TableB).contains(snapshot);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardB));
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));
        UNIT_ASSERT_VALUES_EQUAL(
            FormatIntReadResult(test.ReadSnapshot(test.ShardB, test.TableB, snapshot).get()), "1, 1\n");

        registered->Get()->SnapshotHandle.Reset();
        runtime.WaitFor("explicit snapshot released on the remote node", [&] {
            const auto& current = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            return current && !current->HasSnapshot(test.TableB, snapshot);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
        const auto removed = test.ReadSnapshot(test.ShardB, test.TableB, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(removed->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(UninitializedPrefillDonorCannotAuthorizeCleanupOnRestart) {
        TSnapshotRetentionTest test(true, 3);
        auto& runtime = test.GetRuntime();
        using TService = NLongTxService::TEvLongTxService;
        const auto snapshot = test.WriteVersion(1);
        const auto sender = runtime.AllocateEdgeActor(0);
        runtime.Send(NLongTxService::MakeLongTxServiceID(runtime.GetNodeId(0)), sender,
            new TService::TEvAcquireReadSnapshot("/Root", {test.TableB}, snapshot), 0, true);
        auto acquired = runtime.GrabEdgeEventRethrow<TService::TEvAcquireReadSnapshotResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(acquired->Get()->Status, Ydb::StatusIds::SUCCESS,
            acquired->Get()->Issues.ToString());
        UNIT_ASSERT(acquired->Get()->SnapshotHandle);
        runtime.WaitFor("live snapshot published before the registry restart", [&] {
            const auto registry = runtime.GetAppData(2).SnapshotRegistryHolder->Get();
            return registry && registry->GetActiveSnapshots(test.TableB).contains(snapshot);
        }, TDuration::Seconds(30));
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.WriteVersion(2);
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardB));
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));

        // Restart the two remote registries while the owner on node 0 keeps
        // its real retention handle. Delay full exchange throughout recovery.
        TBlockEvents<TService::TEvPropagateSnapshots> propagation(runtime);
        TBlockEvents<TService::TEvRemoteSnapshotsPrefill> ownerPrefill(runtime, [&](const auto& ev) {
            return ev->GetRecipientRewrite().NodeId() == runtime.GetNodeId(0);
        });
        for (ui32 node : {1u, 2u}) {
            const auto service = NLongTxService::MakeLongTxServiceID(runtime.GetNodeId(node));
            const auto actor = runtime.GetLocalServiceId(service, node);
            runtime.Send(new IEventHandle(actor, {}, new TEvents::TEvPoison()), node, true);
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(10));
        for (ui32 node : {1u, 2u}) {
            runtime.GetAppData(node).SnapshotRegistryHolder->Set(nullptr);
        }
        const auto removedBefore = test.GetRemovedRanges(test.ShardB).ToString();
        const auto restartService = [&](ui32 node) {
            const auto actor = runtime.Register(NLongTxService::CreateLongTxService(), node);
            runtime.RegisterService(NLongTxService::MakeLongTxServiceID(runtime.GetNodeId(node)), actor, node);
            runtime.EnableScheduleForActor(actor);
        };

        restartService(1);
        runtime.WaitFor("donor is waiting for its own prefill", [&] {
            return !ownerPrefill.empty();
        }, TDuration::Seconds(10));
        const auto donor = ownerPrefill.front()->Sender;
        UNIT_ASSERT_VALUES_EQUAL(donor.NodeId(), runtime.GetNodeId(1));
        runtime.SimulateSleep(TDuration::Seconds(2));
        UNIT_ASSERT(!runtime.GetAppData(1).SnapshotRegistryHolder->Get());

        // Restrict the joining node's discovery to that donor, rather than
        // depending on the random choice between the owner and the donor.
        const auto restrictPeers = [&](const auto& ev, auto& entries) {
            if (ev->GetRecipientRewrite().NodeId() != runtime.GetNodeId(2)
                || !ev->Get()->Path.StartsWith("snapshotexch+"))
            {
                return;
            }
            for (auto it = entries.begin(); it != entries.end();) {
                if (it->first.NodeId() != runtime.GetNodeId(1)) {
                    it = entries.erase(it);
                } else {
                    ++it;
                }
            }
        };
        TActorId joiningSubscriber;
        TActorId joiningExchanger;
        std::unique_ptr<TEvStateStorage::TEvBoardInfo> completeBoardInfo;
        auto boardInfo = runtime.AddObserver<TEvStateStorage::TEvBoardInfo>([&](auto& ev) {
            if (ev->GetRecipientRewrite().NodeId() == runtime.GetNodeId(2)
                && ev->Get()->Path.StartsWith("snapshotexch+"))
            {
                joiningSubscriber = ev->Sender;
                joiningExchanger = ev->GetRecipientRewrite();
                completeBoardInfo = std::make_unique<TEvStateStorage::TEvBoardInfo>(*ev->Get());
            }
            restrictPeers(ev, ev->Get()->InfoEntries);
        });
        auto boardUpdates = runtime.AddObserver<TEvStateStorage::TEvBoardInfoUpdate>([&](auto& ev) {
            restrictPeers(ev, ev->Get()->Updates);
        });
        bool requestedDonor = false;
        auto prefillRequest = runtime.AddObserver<TService::TEvRemoteSnapshotsPrefill>([&](auto& ev) {
            if (ev->Sender.NodeId() == runtime.GetNodeId(2)) {
                UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite(), donor);
                requestedDonor = true;
            }
        });
        size_t prefillReplies = 0;
        auto prefillResult = runtime.AddObserver<TService::TEvRemoteSnapshotsPrefillResult>([&](auto& ev) {
            if (ev->Sender == donor && ev->GetRecipientRewrite().NodeId() == runtime.GetNodeId(2)) {
                ++prefillReplies;
            }
        });
        restartService(2);
        runtime.WaitFor("joining node requests the uninitialized donor", [&] {
            return requestedDonor;
        }, TDuration::Seconds(10));
        boardInfo.Remove();
        boardUpdates.Remove();
        UNIT_ASSERT(completeBoardInfo);
        // The prefill target stays B. Restore full membership before C's
        // timeout, so its eventual collection includes the live owner too.
        // Its propagation remains blocked throughout the restart below.
        runtime.Send(new IEventHandle(joiningExchanger, joiningSubscriber, completeBoardInfo.release()), 2, true);
        runtime.SimulateSleep(TDuration::Seconds(2));

        // The durable watermark is already past the retained point. Neither
        // the initialization sweep nor periodic cleanup may fill its gap while
        // the joining node has received no authoritative registry state.
        RebootTablet(runtime, test.ShardB, test.Sender);
        test.RunCleanup();
        UNIT_ASSERT_VALUES_EQUAL(prefillReplies, 0u);
        UNIT_ASSERT(!runtime.GetAppData(2).SnapshotRegistryHolder->Get());
        UNIT_ASSERT_VALUES_EQUAL(test.GetRemovedRanges(test.ShardB).ToString(), removedBefore);
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));
        UNIT_ASSERT(!propagation.empty());

        prefillRequest.Remove();
        prefillResult.Remove();
        ownerPrefill.Stop().Unblock();
        propagation.Stop().Unblock();
        runtime.WaitFor("joining registry recovers the retained point", [&] {
            const auto registry = runtime.GetAppData(2).SnapshotRegistryHolder->Get();
            return registry && registry->GetActiveSnapshots(test.TableB).contains(snapshot);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));
        ForceFullCompaction(runtime, test.ShardB, test.TableB);
        UNIT_ASSERT_VALUES_EQUAL(
            FormatIntReadResult(test.ReadSnapshot(test.ShardB, test.TableB, snapshot).get()), "1, 1\n");

        acquired->Get()->SnapshotHandle.Reset();
        runtime.WaitFor("released point leaves the recovered registry", [&] {
            const auto registry = runtime.GetAppData(2).SnapshotRegistryHolder->Get();
            return registry && !registry->HasSnapshot(test.TableB, snapshot);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
    }

    Y_UNIT_TEST(YoungRemoteSnapshotUsesProducerPromotionCutoff) {
        const auto producerPromotion = TDuration::Seconds(10);
        const auto receiverPromotion = TDuration::Seconds(1);
        TSnapshotRetentionTest test(true, 2, true, producerPromotion);
        auto& runtime = test.GetRuntime();
        auto& receiverConfig = runtime.GetAppData(1).LongTxServiceConfig;
        receiverConfig.SetLocalSnapshotPromotionTimeSeconds(receiverPromotion.Seconds());
        using TService = NLongTxService::TEvLongTxService;
        test.WriteVersion(1);
        const auto initialRegistry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
        UNIT_ASSERT(initialRegistry);
        const auto initialCollection = initialRegistry->GetOldestCollectionTime();
        const auto sender = runtime.AllocateEdgeActor(0);
        runtime.Send(NLongTxService::MakeLongTxServiceID(runtime.GetNodeId(0)), sender,
            new TService::TEvAcquireReadSnapshot("/Root", {test.TableB}), 0, true);
        auto acquired = runtime.GrabEdgeEventRethrow<TService::TEvAcquireReadSnapshotResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(acquired->Get()->Status, Ydb::StatusIds::SUCCESS,
            acquired->Get()->Issues.ToString());
        UNIT_ASSERT(acquired->Get()->SnapshotHandle);
        const auto snapshot = acquired->Get()->Snapshot;

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        for (ui32 value = 2; value <= 3; ++value) {
            test.WriteVersion(value);
            test.RunCleanup();
            const auto age = runtime.GetCurrentTime() - TInstant::MilliSeconds(snapshot.Step);
            UNIT_ASSERT(age > TDuration::Seconds(2));
            UNIT_ASSERT(age < producerPromotion);
            const auto registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry);
            UNIT_ASSERT(initialCollection < registry->GetOldestCollectionTime());
            UNIT_ASSERT(!registry->GetActiveSnapshots(test.TableB).contains(snapshot));
            UNIT_ASSERT(!registry->HasSnapshot(test.TableB, snapshot));
            UNIT_ASSERT(registry->GetMinPromotionCutoffStep() <= snapshot.Step);
            UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));
            const auto read = test.ReadSnapshot(test.ShardB, test.TableB, snapshot, 10 + value);
            UNIT_ASSERT_VALUES_EQUAL(read->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(read.get()), "1, 1\n");
            UNIT_ASSERT(read->Record.GetFinished());
        }
        // Exchange is fresh, and the receiver's shorter local margin would
        // already authorize collection. Only the owner's cutoff protects S.
        const auto youngRegistry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
        const auto receiverMargin = receiverPromotion + TDuration::MilliSeconds(receiverConfig.GetMaxClockSkewMs());
        UNIT_ASSERT(TInstant::MilliSeconds(snapshot.Step) < youngRegistry->GetOldestCollectionTime() - receiverMargin);
        runtime.WaitFor("young remote snapshot promoted by its owner", [&] {
            const auto registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            return registry && registry->GetActiveSnapshots(test.TableB).contains(snapshot);
        }, TDuration::Seconds(30));
        test.WriteVersion(4);
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardB));
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));
        ForceFullCompaction(runtime, test.ShardB, test.TableB);
        UNIT_ASSERT_VALUES_EQUAL(
            FormatIntReadResult(test.ReadSnapshot(test.ShardB, test.TableB, snapshot, 20).get()), "1, 1\n");

        acquired->Get()->SnapshotHandle.Reset();
        runtime.WaitFor("released heterogeneous-promotion snapshot leaves the remote registry", [&] {
            const auto registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            return registry && !registry->HasSnapshot(test.TableB, snapshot);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
        const auto removed = test.ReadSnapshot(test.ShardB, test.TableB, snapshot, 21);
        UNIT_ASSERT_VALUES_EQUAL(removed->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(YoungLocalSnapshotSurvivesCleanupBeforePromotion) {
        const auto promotionTime = TDuration::Seconds(10);
        TSnapshotRetentionTest test(true, 1, true, promotionTime);
        auto& runtime = test.GetRuntime();
        using TService = NLongTxService::TEvLongTxService;
        test.WriteVersion(1);
        const auto initialRegistry = runtime.GetAppData(0).SnapshotRegistryHolder->Get();
        UNIT_ASSERT(initialRegistry);
        const auto collectionTime = initialRegistry->GetOldestCollectionTime();

        const auto sender = runtime.AllocateEdgeActor();
        runtime.Send(NLongTxService::MakeLongTxServiceID(runtime.GetNodeId(0)), sender,
            new TService::TEvAcquireReadSnapshot("/Root", { test.TableA }), 0, true);
        auto acquired = runtime.GrabEdgeEventRethrow<TService::TEvAcquireReadSnapshotResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(
            acquired->Get()->Status, Ydb::StatusIds::SUCCESS, acquired->Get()->Issues.ToString());
        UNIT_ASSERT(acquired->Get()->SnapshotHandle);
        const auto snapshot = acquired->Get()->Snapshot;

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        for (ui32 value = 2; value <= 3; ++value) {
            test.WriteVersion(value);
            test.RunCleanup();
            const auto age = runtime.GetCurrentTime() - TInstant::MilliSeconds(snapshot.Step);
            UNIT_ASSERT(TDuration::Seconds(2) < age);
            UNIT_ASSERT(age < promotionTime);
            const auto registry = runtime.GetAppData(0).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry);
            UNIT_ASSERT(collectionTime < registry->GetOldestCollectionTime());
            UNIT_ASSERT(!registry->HasSnapshot(test.TableA, snapshot));
            UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
            UNIT_ASSERT_VALUES_EQUAL(
                FormatIntReadResult(test.ReadSnapshot(test.ShardA, test.TableA, snapshot).get()), "1, 1\n");
        }

        runtime.WaitFor("young snapshot promoted into the local registry", [&] {
            const auto registry = runtime.GetAppData(0).SnapshotRegistryHolder->Get();
            return registry && registry->HasSnapshot(test.TableA, snapshot);
        }, TDuration::Seconds(30));
        test.WriteVersion(4);
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardA));
        UNIT_ASSERT(!test.IsRemoved(test.ShardA, snapshot));
        UNIT_ASSERT_VALUES_EQUAL(
            FormatIntReadResult(test.ReadSnapshot(test.ShardA, test.TableA, snapshot).get()), "1, 1\n");

        acquired->Get()->SnapshotHandle.Reset();
        runtime.WaitFor("released snapshot removed from the local registry", [&] {
            const auto registry = runtime.GetAppData(0).SnapshotRegistryHolder->Get();
            return registry && !registry->HasSnapshot(test.TableA, snapshot);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardA, snapshot));
        const auto removed = test.ReadSnapshot(test.ShardA, test.TableA, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(removed->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(LiveSnapshotOwnerExpiresAndCannotResurrectRemovedHistory) {
        TSnapshotRetentionTest test(true, 3);
        auto& runtime = test.GetRuntime();
        using TService = NLongTxService::TEvLongTxService;
        for (ui32 node = 0; node < 3; ++node) {
            runtime.GetAppData(node).LongTxServiceConfig.SetUnavailableNodeSnapshotsExpirationTimeSeconds(5);
        }
        test.WriteVersion(1);

        // Isolate a live session owner, while the exchange leader and the
        // target's node remain connected and continue collecting snapshots.
        std::optional<ui32> leaderIndex;
        auto captureLeader = runtime.AddObserver<TService::TEvCollectSnapshots>([&](auto& ev) {
            if (ev->Sender == ev->GetRecipientRewrite()) {
                leaderIndex = ev->Sender.NodeId() - runtime.GetFirstNodeId();
            }
        });
        runtime.WaitFor("snapshot exchange leader", [&] { return leaderIndex.has_value(); }, TDuration::Seconds(30));
        captureLeader.Remove();
        UNIT_ASSERT(*leaderIndex < 3);
        const ui32 ownerIndex = *leaderIndex == 0 ? 1 : 0;
        const ui32 ownerNodeId = runtime.GetNodeId(ownerIndex);

        Ydb::Table::CreateSessionRequest createSession;
        auto created = NKqpHelpers::AwaitResponse(runtime,
            NRpcService::DoLocalRpc<NKqpHelpers::TEvCreateSessionRequest>(
                std::move(createSession), "/Root", "", runtime.GetActorSystem(ownerIndex)));
        UNIT_ASSERT_VALUES_EQUAL_C(created.operation().status(), Ydb::StatusIds::SUCCESS, created.ShortDebugString());
        Ydb::Table::CreateSessionResult session;
        UNIT_ASSERT(created.operation().result().UnpackTo(&session));
        const TString sessionId = session.session_id();
        UNIT_ASSERT(!sessionId.empty());
        auto execute = [&](Ydb::Table::ExecuteDataQueryRequest&& request) {
            return NKqpHelpers::AwaitResponse(runtime,
                NRpcService::DoLocalRpc<NKqpHelpers::TEvExecuteDataQueryRequest>(
                    std::move(request), "/Root", "", runtime.GetActorSystem(ownerIndex)));
        };

        TActorId snapshotManager;
        TRowVersion snapshot = TRowVersion::Min();
        bool registered = false;
        auto captureRequest = runtime.AddObserver<TService::TEvAcquireReadSnapshot>([&](auto& ev) {
            if (!snapshotManager && ev->Sender.NodeId() == ownerNodeId) {
                snapshotManager = ev->Sender;
            }
        });
        auto captureSnapshot = runtime.AddObserver<TService::TEvAcquireReadSnapshotResult>([&](auto& ev) {
            if (snapshotManager && ev->GetRecipientRewrite() == snapshotManager
                && ev->Get()->Status == Ydb::StatusIds::SUCCESS)
            {
                snapshot = ev->Get()->Snapshot;
                registered = bool(ev->Get()->SnapshotHandle);
            }
        });
        auto begin = NKqpHelpers::MakeSimpleRequestRPC(
            "SELECT key, value FROM `/Root/table-a` ORDER BY key;", sessionId, {}, false);
        begin.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_only();
        auto started = execute(std::move(begin));
        captureRequest.Remove();
        captureSnapshot.Remove();
        UNIT_ASSERT_VALUES_EQUAL_C(started.operation().status(), Ydb::StatusIds::SUCCESS, started.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(NKqpHelpers::FormatResult(started),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
        UNIT_ASSERT_VALUES_EQUAL(snapshotManager.NodeId(), ownerNodeId);
        UNIT_ASSERT(snapshot);
        UNIT_ASSERT(registered);
        Ydb::Table::ExecuteQueryResult startedResult;
        UNIT_ASSERT(started.operation().result().UnpackTo(&startedResult));
        const TString txId = startedResult.tx_meta().id();
        UNIT_ASSERT(!txId.empty());
        runtime.WaitFor("live owner's snapshot published on every node", [&] {
            for (ui32 node = 0; node < 3; ++node) {
                const auto registry = runtime.GetAppData(node).SnapshotRegistryHolder->Get();
                if (!registry || !registry->HasSnapshot(test.TableB, snapshot)) {
                    return false;
                }
            }
            return true;
        }, TDuration::Seconds(30));

        size_t dropped = 0;
        auto isolateOwner = [&](auto& ev) {
            const auto sender = ev->Sender;
            const auto recipient = ev->GetRecipientRewrite();
            if ((sender.NodeId() == ownerNodeId) != (recipient.NodeId() == ownerNodeId)) {
                ++dropped;
                // Let the real exchange round finish without the isolated
                // child, just as an interconnect failure would do.
                runtime.Send(new IEventHandle(sender, TActorId(),
                    new TEvInterconnect::TEvNodeDisconnected(recipient.NodeId())),
                    sender.NodeId() - runtime.GetFirstNodeId(), true);
                ev.Reset();
            }
        };
        auto collect = runtime.AddObserver<TService::TEvCollectSnapshots>(isolateOwner);
        auto propagate = runtime.AddObserver<TService::TEvPropagateSnapshots>(isolateOwner);
        for (ui32 value = 2; value <= 7; ++value) {
            test.WriteVersion(value);
            test.RunCleanup();
        }
        UNIT_ASSERT(dropped);
        runtime.WaitFor("isolated live owner's remote snapshot expires", [&] {
            const auto registry = runtime.GetAppData(2).SnapshotRegistryHolder->Get();
            return registry && !registry->HasSnapshot(test.TableB, snapshot);
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
        UNIT_ASSERT(runtime.GetAppData(ownerIndex).SnapshotRegistryHolder->Get()->HasSnapshot(test.TableB, snapshot));

        // Data and RPC traffic still work, and the original session and
        // transaction are alive even though healthy peers expired its state.
        const auto alive = execute(NKqpHelpers::MakeSimpleRequestRPC("SELECT 1;", sessionId, txId, false));
        UNIT_ASSERT_VALUES_EQUAL_C(alive.operation().status(), Ydb::StatusIds::SUCCESS, alive.ShortDebugString());
        collect.Remove();
        propagate.Remove();
        runtime.WaitFor("live owner's old snapshot republished after exchange recovery", [&] {
            const auto registry = runtime.GetAppData(2).SnapshotRegistryHolder->Get();
            return registry && registry->HasSnapshot(test.TableB, snapshot);
        }, TDuration::Seconds(30));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));

        // Registry membership cannot restore history removed during expiry.
        // The transaction's first target read must fail instead of returning
        // empty or incomplete data.
        const auto stale = execute(NKqpHelpers::MakeSimpleRequestRPC(
            "SELECT key, value FROM `/Root/table-b` ORDER BY key;", sessionId, txId, false));
        UNIT_ASSERT_C(stale.operation().status() == Ydb::StatusIds::ABORTED
            || stale.operation().status() == Ydb::StatusIds::PRECONDITION_FAILED, stale.ShortDebugString());
        const TString staleIssues = stale.operation().ShortDebugString();
        UNIT_ASSERT_C(staleIssues.Contains("snapshot"), stale.ShortDebugString());
        const auto fresh = execute(NKqpHelpers::MakeSimpleRequestRPC(
            "SELECT key, value FROM `/Root/table-b` ORDER BY key;", sessionId, {}, true));
        UNIT_ASSERT_VALUES_EQUAL_C(fresh.operation().status(), Ydb::StatusIds::SUCCESS, fresh.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(NKqpHelpers::FormatResult(fresh),
            "{ items { uint32_value: 1 } items { uint32_value: 7 } }");

        Ydb::Table::DeleteSessionRequest deleteSession;
        deleteSession.set_session_id(sessionId);
        auto closed = NKqpHelpers::AwaitResponse(runtime,
            NRpcService::DoLocalRpc<NKqpHelpers::TEvDeleteSessionRequest>(
                std::move(deleteSession), "/Root", "", runtime.GetActorSystem(ownerIndex)));
        UNIT_ASSERT_VALUES_EQUAL_C(closed.operation().status(), Ydb::StatusIds::SUCCESS, closed.ShortDebugString());
    }

    Y_UNIT_TEST_TWIN(KqpCommittingQueryRetainsScopedSnapshots, UseIndex) {
        TSnapshotRetentionTest test(true, 2);
        auto& runtime = test.GetRuntime();
        using TService = NLongTxService::TEvLongTxService;
        auto pinTarget = runtime.AddObserver<TEvHive::TEvCreateTablet>([&](auto& ev) {
            ev->Get()->Record.ClearAllowedNodeIDs();
            ev->Get()->Record.AddAllowedNodeIDs(runtime.GetNodeId(1));
        });
        const auto options = TShardedTableOptions().Columns({
            {"key", "Uint32", true, false},
            {"indexed", "Uint32", false, false},
            {"value", "Uint32", false, false},
        }).Indexes({{"by_index", {"indexed"}, {}, NKikimrSchemeOp::EIndexTypeGlobal}});
        const auto [targetShards, targetId] = CreateShardedTable(test.Server, test.Sender, "/Root", "table-c", options);
        pinTarget.Remove();
        const ui64 targetShard = targetShards.at(0);
        const TString indexPath = "/Root/table-c/by_index/indexImplTable";
        const auto indexId = ResolveTableId(test.Server, test.Sender, indexPath);
        const ui64 indexShard = NKqp::GetTableShards(test.Server, test.Sender, indexPath).at(0);
        UNIT_ASSERT_VALUES_EQUAL(ResolveTablet(runtime, targetShard).NodeId(), runtime.GetNodeId(1));
        UNIT_ASSERT_VALUES_EQUAL(ResolveTablet(runtime, indexShard).NodeId(), runtime.GetNodeId(1));

        const auto writeGeneration = [&](ui32 generation) {
            ExecSQL(test.Server, test.Sender, Sprintf(
                "UPSERT INTO `/Root/table-a` (key, value) VALUES (1, %u);"
                "UPSERT INTO `/Root/table-b` (key, value) VALUES (1, %u);"
                "UPSERT INTO `/Root/table-c` (key, indexed, value) VALUES (2, %u, %u);",
                generation, generation, generation == 1 ? 100u : 200u, generation));
        };
        const auto makeQuery = [&](ui32 indexed) -> TString {
            const TString filter = UseIndex ? Sprintf(" VIEW by_index WHERE indexed = %uu", indexed) : TString();
            return TStringBuilder() << "SELECT key, value FROM ("
                << "SELECT key, value FROM `/Root/table-a` UNION ALL "
                << "SELECT key, value FROM `/Root/table-c`" << filter << ") ORDER BY key;";
        };
        const TString sessionId = NKqpHelpers::CreateSessionRPC(runtime, "/Root");
        const auto makeRequest = [&](const TString& query) {
            auto request = NKqpHelpers::MakeSimpleRequestRPC(query, sessionId, {}, true);
            request.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_only();
            request.mutable_query_cache_policy()->set_keep_in_cache(true);
            return request;
        };
        const TString originalRows = "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }";
        writeGeneration(1);
        const TString query = makeQuery(100);
        const auto warmed = NKqpHelpers::AwaitResponse(runtime, NKqpHelpers::SendRequest(runtime, makeRequest(query), "/Root"));
        UNIT_ASSERT_VALUES_EQUAL_C(warmed.operation().status(), Ydb::StatusIds::SUCCESS, warmed.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(NKqpHelpers::FormatResult(warmed), originalRows);
        // Reuse the cached plan in the same session, after its wildcard handle
        // is gone and real writes have advanced the coordinator's read step.
        test.RunCleanup();
        writeGeneration(2);
        writeGeneration(1);

        TActorId snapshotManager;
        TRowVersion snapshot = TRowVersion::Min();
        THashSet<TPathId> requestedTables, readTables;
        bool acquiredWithHandle = false;
        auto captureRequest = runtime.AddObserver<TService::TEvAcquireReadSnapshot>([&](auto& ev) {
            for (const auto& tableId : ev->Get()->TableIds) {
                if (tableId.PathId == targetId.PathId && !snapshotManager) {
                    snapshotManager = ev->Sender;
                    for (const auto& requested : ev->Get()->TableIds) {
                        requestedTables.insert(requested.PathId);
                    }
                    break;
                }
            }
        });
        auto captureSnapshot = runtime.AddObserver<TService::TEvAcquireReadSnapshotResult>([&](auto& ev) {
            if (snapshotManager && ev->GetRecipientRewrite() == snapshotManager
                && ev->Get()->Status == Ydb::StatusIds::SUCCESS)
            {
                snapshot = ev->Get()->Snapshot;
                acquiredWithHandle = bool(ev->Get()->SnapshotHandle);
            }
        });
        auto captureReads = runtime.AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (snapshot && record.HasSnapshot() && TRowVersion::FromProto(record.GetSnapshot()) == snapshot) {
                readTables.insert(TPathId(record.GetTableId().GetOwnerId(), record.GetTableId().GetTableId()));
            }
        });
        const auto delayedPath = UseIndex ? indexId.PathId : targetId.PathId;
        TBlockEvents<TEvDataShard::TEvRead> reads(runtime, [&](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return snapshot && record.HasSnapshot() && TRowVersion::FromProto(record.GetSnapshot()) == snapshot
                && TPathId(record.GetTableId().GetOwnerId(), record.GetTableId().GetTableId()) == delayedPath;
        });
        auto pending = NKqpHelpers::SendRequest(runtime, makeRequest(query), "/Root");
        runtime.WaitFor("the committing query's first target read is blocked", [&] {
            return !reads.empty() || pending.HasValue() || pending.HasException();
        }, TDuration::Seconds(30));
        UNIT_ASSERT_C(!reads.empty(), "The query must acquire a scoped snapshot before its first target read");
        UNIT_ASSERT(acquiredWithHandle);
        UNIT_ASSERT(!requestedTables.contains(TPathId{}));
        UNIT_ASSERT(requestedTables.contains(test.TableA.PathId));
        UNIT_ASSERT(requestedTables.contains(targetId.PathId));
        UNIT_ASSERT(!requestedTables.contains(test.TableB.PathId));
        if constexpr (UseIndex) {
            UNIT_ASSERT(requestedTables.contains(indexId.PathId));
        }
        captureRequest.Remove();
        captureSnapshot.Remove();

        // The writer uses a newer snapshot, so it cannot hit the query's read blocker.
        runtime.SimulateSleep(TDuration::Seconds(2));
        for (ui32 i = 0; i < 3; ++i) {
            writeGeneration(2);
            test.RunCleanup();
        }
        runtime.WaitFor("the scoped snapshot is published without a wildcard", [&] {
            for (ui32 node = 0; node < 2; ++node) {
                const auto registry = runtime.GetAppData(node).SnapshotRegistryHolder->Get();
                if (!registry || !registry->HasSnapshot(test.TableA, snapshot)
                    || !registry->HasSnapshot(targetId, snapshot)
                    || (UseIndex && !registry->HasSnapshot(indexId, snapshot))
                    || registry->HasSnapshot(TTableId{}, snapshot) || registry->HasSnapshot(test.TableB, snapshot))
                {
                    return false;
                }
            }
            return true;
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(targetShard));
        UNIT_ASSERT(!test.IsRemoved(targetShard, snapshot));
        if constexpr (UseIndex) {
            UNIT_ASSERT(snapshot < test.GetCleanupBoundary(indexShard));
            UNIT_ASSERT(!test.IsRemoved(indexShard, snapshot));
        }
        UNIT_ASSERT(snapshot < test.GetCleanupBoundary(test.ShardB));
        UNIT_ASSERT(test.IsRemoved(test.ShardB, snapshot));
        UNIT_ASSERT(!pending.HasValue() && !pending.HasException());

        reads.Stop().Unblock();
        const auto result = NKqpHelpers::AwaitResponse(runtime, pending);
        UNIT_ASSERT_VALUES_EQUAL_C(result.operation().status(), Ydb::StatusIds::SUCCESS, result.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(NKqpHelpers::FormatResult(result), originalRows);
        UNIT_ASSERT(readTables.contains(targetId.PathId));
        if constexpr (UseIndex) {
            UNIT_ASSERT(readTables.contains(indexId.PathId));
        }
        runtime.WaitFor("the committing query releases its scoped snapshot", [&] {
            const auto registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            return registry && !registry->HasSnapshot(targetId, snapshot)
                && (!UseIndex || !registry->HasSnapshot(indexId, snapshot));
        }, TDuration::Seconds(30));
        test.RunCleanup();
        UNIT_ASSERT(test.IsRemoved(targetShard, snapshot));
        if constexpr (UseIndex) {
            UNIT_ASSERT(test.IsRemoved(indexShard, snapshot));
        }
        const auto fresh = NKqpHelpers::AwaitResponse(runtime,
            NKqpHelpers::SendRequest(runtime, makeRequest(makeQuery(200)), "/Root"));
        UNIT_ASSERT_VALUES_EQUAL_C(fresh.operation().status(), Ydb::StatusIds::SUCCESS, fresh.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(NKqpHelpers::FormatResult(fresh),
            "{ items { uint32_value: 1 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
        NKqpHelpers::CloseSession(runtime, sessionId);
    }

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
        UNIT_ASSERT(!test.IsRemoved(test.ShardB, snapshot));

        for (ui32 i = 0; i < 2; ++i) {
            handles[i].Reset();
            runtime.SimulateSleep(TDuration::Seconds(5));
            UNIT_ASSERT(runtime.GetAppData(1).SnapshotRegistryHolder->Get()->HasSnapshot(test.TableB, snapshot));
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
