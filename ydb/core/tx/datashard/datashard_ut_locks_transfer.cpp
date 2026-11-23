#include "datashard_ut_common_kqp.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common_tx.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/protos/query_stats.pb.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NKikimr::NDataShard::NTxHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardLocksTransfer) {

struct TTestEnv {
    TPortManager PortManager;
    TServer::TPtr Server;
    TActorId Sender;

    TDisableDataShardLogBatching DisableDataShardLogBatching;

    TTableId TableId;
    TVector<ui64> Shards;

    TTestActorRuntime& GetRuntime() { return *Server->GetRuntime(); }

    TTestEnv(const TVector<ui32>& splitPoints, bool transferEnabled = true) {
        NKikimrConfig::TAppConfig app;
        TServerSettings serverSettings(PortManager.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);
        serverSettings.FeatureFlags.SetEnableDataShardLocksTransferOnSplit(transferEnabled);
        serverSettings.FeatureFlags.SetEnableDataShardUncommittedWriteSeqNum(true);

        Server = new TServer(serverSettings);
        auto& runtime = GetRuntime();
        Sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(Server, Sender);

        SetSplitMergePartCountLimit(&GetRuntime(), -1);

        TStringBuilder partitionAtKeys;
        if (!splitPoints.empty()) {
            partitionAtKeys << " WITH (PARTITION_AT_KEYS = (" << JoinSeq(", ", splitPoints) << "))";
        }

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key UInt32, value int, PRIMARY KEY (key))
            )" + partitionAtKeys),
            "SUCCESS"
        );

        TableId = ResolveTableId(Server, Sender, "/Root/table");
        UNIT_ASSERT(TableId);
        Shards = GetTableShards(Server, Sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(Shards.size(), splitPoints.size() + 1);
    }

    auto GetAll() {
        const auto& self = *this;
        return std::tie(Server, GetRuntime(), self.Sender, self.TableId, self.Shards);
    }

    void Split(size_t shardIndex, ui32 splitKey) {
        ui64 txId = AsyncSplitTable(Server, Sender, "/Root/table", Shards.at(shardIndex), splitKey);
        WaitTxNotification(Server, Sender, txId);
        Shards = GetTableShards(Server, Sender, "/Root/table");
    }

    void CompactBorrowedParts() {
        for (ui64 shard : Shards) {
            CompactBorrowed(GetRuntime(), shard, TableId);
        }
    }

    void Merge(size_t fromIdx, size_t toIdx) {
        CompactBorrowedParts();
        TVector<ui64> tablets(Shards.begin() + fromIdx, Shards.begin() + toIdx + 1);
        ui64 txId = AsyncMergeTable(Server, Sender, "/Root/table", tablets);
        WaitTxNotification(Server, Sender, txId);
        Shards = GetTableShards(Server, Sender, "/Root/table");
    }
};

using TTransferSnapshot = TEvDataShard::TEvSplitTransferSnapshot;

NKikimrDataEvents::TLock WriteAt(TTestEnv& env, TTransactionState& tx, ui64 shard,
        TVector<ui64> ancestors, ui64 wsn, TWriteOperation op) {
    auto req = tx.CreateWrite(env.TableId, ancestors, shard, op);
    req->Record.MutableOperations(0)->MutableWriteSeqNum()->SetWriteSeqNum(wsn);
    auto& runtime = env.GetRuntime();
    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(shard, sender, req.release());
    TTransactionState::TWritePromise promise{tx, sender};
    auto result = promise.NextResult();
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED,
        result->Record.ShortDebugString());
    UNIT_ASSERT_VALUES_EQUAL(result->Record.TxLocksSize(), 1);
    return result->Record.GetTxLocks(0);
}

TWriteOperation Increment(i32 key, i32 delta) {
    return {NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INCREMENT, {{key, delta}}};
}

void CheckNoOpenTxs(TTestEnv& env, ui64 shard) {
    auto& runtime = env.GetRuntime();
    runtime.SendToPipe(shard, env.Sender, new TEvDataShard::TEvGetOpenTxs(env.TableId.PathId));
    auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(env.Sender);
    UNIT_ASSERT(result->Get()->OpenTxs.empty());
}

void ReplaySnapshot(TTestActorRuntime& runtime, ui64 destination, const TTransferSnapshot::TPtr& event) {
    auto snapshot = std::make_unique<TTransferSnapshot>();
    snapshot->Record.CopyFrom(event->Get()->Record);
    runtime.SendToPipe(destination, event->Sender, snapshot.release());
}

void DeliverFirstSnapshotAndReboot(TTestEnv& env, TBlockEvents<TTransferSnapshot>& snapshots,
        ui64 firstSource, ui64 destination) {
    auto& runtime = env.GetRuntime();
    UNIT_ASSERT_VALUES_EQUAL(snapshots.size(), 2);
    if (snapshots[0]->Get()->Record.GetSrcTabletId() != firstSource) {
        std::swap(snapshots[0], snapshots[1]);
    }
    bool acknowledged = false;
    auto observer = runtime.AddObserver<TEvDataShard::TEvSplitTransferSnapshotAck>(
        [&](auto& ev) { acknowledged |= ev->Get()->Record.GetTabletId() == destination; });
    snapshots.Unblock(1);
    runtime.WaitFor("first source persisted", [&] { return acknowledged; });
    RebootTablet(runtime, destination, env.Sender);
    snapshots.Stop();
    for (const auto& snapshot : snapshots) {
        ReplaySnapshot(runtime, destination, snapshot);
    }
    snapshots.clear();
}

Y_UNIT_TEST_TWIN(RemovedTransactionInvalidatesAllRestoredLocks, AbortFirst) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    env.Split(0, 10);
    const auto sources = env.Shards;
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    WriteAt(env, tx, sources[0], {}, 1, TWriteOperation::Upsert(1, 100));
    WriteAt(env, tx, sources[1], {}, 1, TWriteOperation::Upsert(15, 1500));
    // Keep B's undecided rows in a part, then persist its abort separately.
    // B sends RemoveTx in the data snapshot without any lock metadata.
    UNIT_ASSERT_VALUES_EQUAL(CompactTable(runtime, sources[1], env.TableId, true).GetStatus(),
        NKikimrTxDataShard::TEvCompactTableResult::OK);
    UNIT_ASSERT_VALUES_EQUAL(tx.Rollback(sources[1]), "OK");
    ui64 destination = 0;
    auto initObserver = runtime.AddObserver<TEvDataShard::TEvInitSplitMergeDestination>(
        [&](auto& ev) { destination = ev->Get()->Record.GetSplitDescription().GetDestinationRanges(0).GetTabletID(); });
    TBlockEvents<TTransferSnapshot> snapshots(runtime);
    const ui64 mergeTx = AsyncMergeTable(env.Server, env.Sender, "/Root/table", sources);
    runtime.WaitFor("data and abort snapshots", [&] { return snapshots.size() >= 2; });
    for (const auto& snapshot : snapshots) {
        if (snapshot->Get()->Record.GetSrcTabletId() == sources[1]) {
            UNIT_ASSERT_VALUES_EQUAL(snapshot->Get()->Record.LocksSize(), 0);
        }
    }
    DeliverFirstSnapshotAndReboot(env, snapshots, sources[AbortFirst ? 1 : 0], destination);
    WaitTxNotification(env.Server, env.Sender, mergeTx);
    env.Shards = GetTableShards(env.Server, env.Sender, "/Root/table");
    // Check cleanup before a failed client commit can remove the lock itself.
    CheckNoOpenTxs(env, destination);
    tx.MapAncestorShard(destination, sources[0]);
    UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(env.TableId, destination), "ERROR: STATUS_LOCKS_BROKEN");
    CheckNoOpenTxs(env, destination);
    RebootTablet(runtime, destination, env.Sender);
    UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(env.TableId, destination), "ERROR: STATUS_LOCKS_BROKEN");
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT * FROM `/Root/table`;"), "");
}

Y_UNIT_TEST_TWIN(OrphanedWritesUseOrdinaryCleanup, Restart) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    WriteAt(env, tx, env.Shards[0], {}, 1, TWriteOperation::Upsert({{1, 100}, {15, 1500}}));
    // Model loss of the source's entire lock record. The imported open id must
    // stay untracked so ordinary cleanup can abort it after Ready.
    auto snapshots = runtime.AddObserver<TTransferSnapshot>([](auto& ev) {
        ev->Get()->Record.ClearLocks();
    });
    size_t subscriptions = 0;
    auto subscriptionObserver = runtime.AddObserver<NLongTxService::TEvLongTxService::TEvSubscribeLock>([&](auto& ev) {
        subscriptions += ev->Get()->Record.GetLockId() == tx.LockTxId;
    });
    env.Split(0, 10);
    runtime.SimulateSleep(TDuration::Seconds(1));
    for (ui64 shard : env.Shards) {
        if (Restart) {
            RebootTablet(runtime, shard, env.Sender);
        }
        CheckNoOpenTxs(env, shard);
    }
    UNIT_ASSERT_VALUES_EQUAL(subscriptions, 0);
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT * FROM `/Root/table`;"), "");
}

Y_UNIT_TEST_TWIN(WriteWithoutWsnIsBrokenByTransfer, Merge) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    if (Merge) {
        env.Split(0, 10);
    }
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    const ui64 current = env.Shards[0];
    UNIT_ASSERT_VALUES_EQUAL(tx.Write(env.TableId, current, TWriteOperation::Upsert(1, 200)), "OK");
    const auto* currentLock = tx.FindLastLock(current);
    UNIT_ASSERT(currentLock);
    UNIT_ASSERT_VALUES_EQUAL(currentLock->GetCreatedOn(), current);
    UNIT_ASSERT_VALUES_EQUAL(currentLock->WriteSeqNumsSize(), 0);
    RebootTablet(runtime, current, env.Sender);
    if (Merge) {
        env.Merge(0, 1);
    } else {
        env.Split(0, 5);
    }
    const ui64 destination = env.Shards[0];
    tx.MapAncestorShard(destination, current);
    CheckNoOpenTxs(env, destination);
    UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(env.TableId, destination), "ERROR: STATUS_LOCKS_BROKEN");
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT key, value FROM `/Root/table` ORDER BY key;"), "");
}

Y_UNIT_TEST_QUAD(BorrowedTransactionOutsideSliceAllowsCurrentLock, Resplit, LargeSlice) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    // Keep in-memory state available across both consecutive tablet reboots.
    // The default retention window is only one second and may expire at boot.
    runtime.GetAppData().DataShardConfig.SetInMemoryStateMigrationTimeoutMs(60'000);
    env.Split(0, 10);
    const ui64 createdOn = env.Shards[0];
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime,
        "UPSERT INTO `/Root/table` (key, value) VALUES (15, 1500), (25, 2500);"), "<empty>");
    if (LargeSlice) {
        TStringBuilder query;
        query << "UPSERT INTO `/Root/table` (key, value) VALUES ";
        for (ui32 i = 0; i < 2048; ++i) {
            if (i) {
                query << ", ";
            }
            query << "(" << 100 + i << ", " << i << ")";
        }
        query << ";";
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, query), "<empty>");
    }
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    WriteAt(env, tx, createdOn, {}, 1, TWriteOperation::Upsert(1, 100));
    env.Merge(0, 1);
    // One part spans both creation ranges, but only its left side contains L.
    UNIT_ASSERT_VALUES_EQUAL(CompactTable(runtime, env.Shards[0], env.TableId, true).GetStatus(),
        NKikimrTxDataShard::TEvCompactTableResult::OK);
    size_t trackingSnapshots = 0;
    auto observer = runtime.AddObserver<TTransferSnapshot>([&](auto& ev) {
        const auto& snapshot = ev->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(snapshot.LocksSize(), 1);
        const auto& lock = snapshot.GetLocks(0);
        if (lock.GetFlags() == ui64(NDataShard::ELockFlags::TrackingOnly)) {
            UNIT_ASSERT(!lock.AncestorLocksSize());
            UNIT_ASSERT(!lock.WriteTablesSize());
            UNIT_ASSERT(lock.GetLockNodeId());
            ++trackingSnapshots;
        }
    });
    env.Split(0, 10);
    const ui64 left = env.Shards[0];
    auto checkBorrowedReference = [&](ui64 shard) {
        runtime.SendToPipe(shard, env.Sender, new TEvDataShard::TEvGetOpenTxs(env.TableId.PathId));
        auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(env.Sender);
        UNIT_ASSERT(result->Get()->OpenTxs.contains(tx.LockTxId));
    };
    checkBorrowedReference(env.Shards[1]);
    RebootTablet(runtime, env.Shards[1], env.Sender);
    if (Resplit) {
        // The next source must not advertise the out-of-slice reference as
        // transaction data with missing metadata to either destination.
        env.Split(1, 20);
    }
    UNIT_ASSERT_VALUES_EQUAL(trackingSnapshots, Resplit ? 3 : 1);
    for (size_t i = 1; i < env.Shards.size(); ++i) {
        const ui64 shard = env.Shards[i];
        checkBorrowedReference(shard);
        RebootTablet(runtime, shard, env.Sender);
        const i32 key = i == 1 ? 15 : 25;
        const auto lock = WriteAt(env, tx, shard, {}, 1, TWriteOperation::Upsert(key, key * 100 + 1));
        UNIT_ASSERT_VALUES_EQUAL(lock.GetDataShard(), shard);
        UNIT_ASSERT_VALUES_EQUAL(lock.GetCreatedOn(), shard);
    }
    tx.MapAncestorShard(left, createdOn);
    tx.InitCommit(std::vector<ui64>(env.Shards.begin(), env.Shards.end()));
    TVector<TTransactionState::TWritePromise> pending;
    for (ui64 shard : env.Shards) {
        pending.push_back(tx.PrepareCommit(env.TableId, shard));
    }
    tx.SendPlan();
    for (auto& result : pending) {
        UNIT_ASSERT_VALUES_EQUAL(result.NextString(), "OK");
    }
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT key, value FROM `/Root/table` WHERE key < 30 ORDER BY key;"),
        TString("{ items { uint32_value: 1 } items { int32_value: 100 } }, ") +
        "{ items { uint32_value: 15 } items { int32_value: 1501 } }, " +
        (Resplit ? "{ items { uint32_value: 25 } items { int32_value: 2501 } }"
                 : "{ items { uint32_value: 25 } items { int32_value: 2500 } }"));
}

// Produce a logical lock on the left and a borrowed reference with a tracker
// on the right. Both are later transferred to the same merge destination.
ui64 PrepareTrackingSplit(TTestEnv& env, TTransactionState& tx) {
    env.Split(0, 10);
    const ui64 createdOn = env.Shards[0];
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(env.GetRuntime(),
        "UPSERT INTO `/Root/table` (key, value) VALUES (15, 1500);"), "<empty>");
    WriteAt(env, tx, createdOn, {}, 1, TWriteOperation::Upsert(1, 100));
    env.Merge(0, 1);
    UNIT_ASSERT_VALUES_EQUAL(CompactTable(env.GetRuntime(), env.Shards[0], env.TableId, true).GetStatus(),
        NKikimrTxDataShard::TEvCompactTableResult::OK);
    env.Split(0, 10);
    return createdOn;
}

Y_UNIT_TEST_TWIN(TrackingAndLogicalMergeAcrossRestart, TrackingFirst) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    const ui64 createdOn = PrepareTrackingSplit(env, tx);
    const auto sources = env.Shards;
    env.CompactBorrowedParts();
    ui64 destination = 0;
    auto observer = runtime.AddObserver<TEvDataShard::TEvInitSplitMergeDestination>(
        [&](auto& ev) { destination = ev->Get()->Record.GetSplitDescription().GetDestinationRanges(0).GetTabletID(); });
    TBlockEvents<TTransferSnapshot> snapshots(runtime);
    const ui64 mergeTx = AsyncMergeTable(env.Server, env.Sender, "/Root/table", sources);
    runtime.WaitFor("logical and tracking snapshots", [&] { return snapshots.size() >= 2; });
    for (const auto& snapshot : snapshots) {
        const auto& record = snapshot->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.LocksSize(), 1);
        const bool tracking = record.GetSrcTabletId() == sources[1];
        UNIT_ASSERT_VALUES_EQUAL(record.GetLocks(0).AncestorLocksSize(), tracking ? 0 : 1);
    }
    DeliverFirstSnapshotAndReboot(env, snapshots, sources[TrackingFirst ? 1 : 0], destination);
    WaitTxNotification(env.Server, env.Sender, mergeTx);
    RebootTablet(runtime, destination, env.Sender);
    tx.MapAncestorShard(destination, createdOn);
    UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(env.TableId, destination), "OK");
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT key, value FROM `/Root/table` ORDER BY key;"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 15 } items { int32_value: 1500 } }");
}

Y_UNIT_TEST(TrackingSnapshotRejectsInvalidOwnerAndFlags) {
    for (ui32 invalid = 0; invalid != 2; ++invalid) {
        TTestEnv env({});
        auto& runtime = env.GetRuntime();
        TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
        tx.WriterIndex = 123;
        PrepareTrackingSplit(env, tx);
        const ui64 source = env.Shards[1];
        TBlockEvents<TTransferSnapshot> snapshots(runtime);
        const ui64 splitTx = AsyncSplitTable(env.Server, env.Sender, "/Root/table", source, 20);
        runtime.WaitFor("two tracking snapshots", [&] { return snapshots.size() >= 2; });
        for (auto& snapshot : snapshots) {
            auto& record = snapshot->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.LocksSize(), 1);
            switch (invalid) {
                case 0: record.MutableLocks(0)->SetLockNodeId(0); break;
                case 1: record.MutableLocks(0)->SetFlags(ui64(NDataShard::ELockFlags::TrackingOnly | NDataShard::ELockFlags::Frozen)); break;
            }
        }
        snapshots.Stop().Unblock();
        WaitTxNotification(env.Server, env.Sender, splitTx);
        env.Shards = GetTableShards(env.Server, env.Sender, "/Root/table");
        for (size_t i = 1; i < env.Shards.size(); ++i) {
            // Rejection leaves a persistent broken lock with borrowed references.
            // The first local write must fail, including after a destination reboot.
            const ui32 key = i == 1 ? 17 : 25;
            UNIT_ASSERT_VALUES_EQUAL(tx.Write(env.TableId, env.Shards[i], TWriteOperation::Upsert(key, 200)),
                "ERROR: STATUS_LOCKS_BROKEN");
            RebootTablet(runtime, env.Shards[i], env.Sender);
            UNIT_ASSERT_VALUES_EQUAL(tx.Write(env.TableId, env.Shards[i], TWriteOperation::Upsert(key, 200)),
                "ERROR: STATUS_LOCKS_BROKEN");
        }
    }
}

Y_UNIT_TEST_TWIN(TrackingOnlyFollowsOwnerLifetime, Unavailable) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    PrepareTrackingSplit(env, tx);
    const ui64 trackingShard = env.Shards[1];
    RebootTablet(runtime, trackingShard, env.Sender);
    if (Unavailable) {
        runtime.SendToPipe(trackingShard, env.Sender, new NLongTxService::TEvLongTxService::TEvLockStatus(
            tx.LockTxId, tx.LockNodeId, NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE));
    } else {
        tx.LockHandle.Reset();
    }
    runtime.SimulateSleep(TDuration::Seconds(1));
    size_t subscriptions = 0;
    auto observer = runtime.AddObserver<NLongTxService::TEvLongTxService::TEvSubscribeLock>([&](auto& ev) {
        subscriptions += ev->Get()->Record.GetLockId() == tx.LockTxId;
    });
    RebootTablet(runtime, trackingShard, env.Sender);
    // Removing an in-memory tracker leaves out-of-slice part references intact.
    // On reboot they are untracked and startup cleanup records the abort.
    CheckNoOpenTxs(env, trackingShard);
    UNIT_ASSERT_VALUES_EQUAL(subscriptions, 0);
    // The abort decision also covers a later first write under this id.
    auto req = tx.CreateWrite(env.TableId, TVector<ui64>{}, trackingShard, TWriteOperation::Upsert(15, 1501));
    req->Record.MutableOperations(0)->MutableWriteSeqNum()->SetWriteSeqNum(1);
    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(trackingShard, sender, req.release());
    TTransactionState::TWritePromise retry{tx, sender};
    UNIT_ASSERT_VALUES_EQUAL(retry.NextString(), "ERROR: STATUS_LOCKS_BROKEN");
}

Y_UNIT_TEST(EmptyCopiesFollowCreatedOnRange) {
    TTestEnv env({});
    env.Split(0, 10);
    auto& runtime = env.GetRuntime();
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    const ui64 owner = env.Shards[0];
    WriteAt(env, tx, owner, {}, 1, TWriteOperation::Upsert(1, 100));

    env.Merge(0, 1);
    TVector<NKikimrTxDataShard::TEvSplitTransferSnapshot> snapshots;
    auto observer = runtime.AddObserver<TTransferSnapshot>([&](TTransferSnapshot::TPtr& ev) {
        snapshots.push_back(ev->Get()->Record);
    });
    env.Split(0, 5);
    UNIT_ASSERT_VALUES_EQUAL(snapshots.size(), 2);
    TString createdRange;
    for (const auto& snapshot : snapshots) {
        UNIT_ASSERT_VALUES_EQUAL(snapshot.LocksSize(), 1);
        const auto& lock = snapshot.GetLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(lock.AncestorLocksSize(), 1);
        const auto& ancestor = lock.GetAncestorLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(ancestor.GetTabletId(), owner);
        UNIT_ASSERT_VALUES_EQUAL(ancestor.GetCreatedOn(), owner);
        UNIT_ASSERT(ancestor.HasCreatedOnRange());
        if (createdRange.empty()) {
            createdRange = ancestor.GetCreatedOnRange().SerializeAsString();
        }
        UNIT_ASSERT_VALUES_EQUAL(ancestor.GetCreatedOnRange().SerializeAsString(), createdRange);
    }
    snapshots.clear();
    env.Split(1, 10);
    UNIT_ASSERT_VALUES_EQUAL(snapshots.size(), 2);
    ui32 copies = 0;
    ui32 trackers = 0;
    for (const auto& snapshot : snapshots) {
        for (const auto& lock : snapshot.GetLocks()) {
            if (lock.GetFlags() == ui64(NDataShard::ELockFlags::TrackingOnly)) {
                ++trackers;
                UNIT_ASSERT(!lock.AncestorLocksSize());
                continue;
            }
            ++copies;
            UNIT_ASSERT_VALUES_EQUAL(lock.AncestorLocksSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(lock.GetAncestorLocks(0).GetCreatedOnRange().SerializeAsString(), createdRange);
        }
    }
    // [5,10) needs an empty copy; [10,+inf) merely touches the boundary.
    UNIT_ASSERT_VALUES_EQUAL(copies, 1);
    UNIT_ASSERT_VALUES_EQUAL(trackers, 1);
    RebootTablet(runtime, env.Shards[1], env.Sender);
    auto lock = WriteAt(env, tx, env.Shards[1], {owner}, 1, TWriteOperation::Upsert({}));
    UNIT_ASSERT_VALUES_EQUAL(lock.GetCreatedOn(), owner);
    UNIT_ASSERT_VALUES_EQUAL(lock.GetWriteSeqNums(0).GetWriteSeqNum(), 1);
    UNIT_ASSERT_VALUES_EQUAL(tx.ReadKey(env.TableId, env.Shards[0], 1), "1, 100\n");
}

Y_UNIT_TEST(MergeRetainsSiblingBranchesAcrossAnotherSplit) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    const ui64 owner = env.Shards[0];
    WriteAt(env, tx, owner, {}, 1, TWriteOperation::Upsert({{1, 1}, {15, 1}}));
    env.Split(0, 10);
    const auto sources = env.Shards;
    WriteAt(env, tx, sources[0], {owner}, 2, Increment(1, 10));
    WriteAt(env, tx, sources[1], {owner}, 2, Increment(15, 20));
    env.Merge(0, 1);
    const ui64 merged = env.Shards[0];
    RebootTablet(runtime, merged, env.Sender);
    WriteAt(env, tx, merged, {owner, sources[0]}, 2, Increment(1, 999));
    WriteAt(env, tx, merged, {owner, sources[1]}, 2, Increment(15, 999));

    TVector<NKikimrTxDataShard::TLockWriteSeqNumBranch> branches;
    auto observer = runtime.AddObserver<TTransferSnapshot>([&](TTransferSnapshot::TPtr& ev) {
        const auto& locks = ev->Get()->Record.GetLocks();
        UNIT_ASSERT_VALUES_EQUAL(locks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(locks[0].AncestorLocksSize(), 1);
        branches.assign(locks[0].GetAncestorLocks(0).GetWriteSeqNumStates(0).GetBranches().begin(),
            locks[0].GetAncestorLocks(0).GetWriteSeqNumStates(0).GetBranches().end());
    });
    env.Split(0, 10);
    observer.Remove();
    UNIT_ASSERT_VALUES_EQUAL(branches.size(), 3);
    THashSet<TString> paths;
    for (const auto& branch : branches) {
        paths.insert(NDataShard::SerializeLockShardsPath({branch.GetShardsPath().data(), size_t(branch.ShardsPathSize())}));
        UNIT_ASSERT(!branch.GetSerializedResult().empty());
    }
    UNIT_ASSERT(paths.contains(NDataShard::SerializeLockShardsPath(TVector<ui64>{owner})));
    UNIT_ASSERT(paths.contains(NDataShard::SerializeLockShardsPath(TVector<ui64>{owner, sources[0]})));
    UNIT_ASSERT(paths.contains(NDataShard::SerializeLockShardsPath(TVector<ui64>{owner, sources[1]})));
    for (ui32 i = 0; i != 2; ++i) {
        WriteAt(env, tx, env.Shards[i], {owner, sources[i], merged}, 2, Increment(i ? 15 : 1, 999));
        tx.MapAncestorShard(env.Shards[i], owner);
    }
    tx.InitCommit({env.Shards[0], env.Shards[1]});
    auto left = tx.PrepareCommit(env.TableId, env.Shards[0]);
    auto right = tx.PrepareCommit(env.TableId, env.Shards[1]);
    tx.SendPlan();
    UNIT_ASSERT_VALUES_EQUAL(left.NextString(), "OK");
    UNIT_ASSERT_VALUES_EQUAL(right.NextString(), "OK");
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT key, value FROM `/Root/table` ORDER BY key;"),
        "{ items { uint32_value: 1 } items { int32_value: 11 } }, "
        "{ items { uint32_value: 15 } items { int32_value: 21 } }");
}

Y_UNIT_TEST(IndependentCreatedOnLocksRemainSeparate) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    const ui64 owner = env.Shards[0];
    env.Split(0, 10);
    const auto sources = env.Shards;
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    for (ui32 i = 0; i != 2; ++i) {
        auto lock = WriteAt(env, tx, sources[i], {owner}, 1, TWriteOperation::Upsert(i ? 15 : 1, i ? 20 : 10));
        UNIT_ASSERT_VALUES_EQUAL(lock.GetCreatedOn(), sources[i]);
    }
    env.Merge(0, 1);
    RebootTablet(runtime, env.Shards[0], env.Sender);
    for (ui32 i = 0; i != 2; ++i) {
        auto lock = WriteAt(env, tx, env.Shards[0], {owner, sources[i]}, 1, Increment(i ? 15 : 1, 999));
        UNIT_ASSERT_VALUES_EQUAL(lock.GetCreatedOn(), sources[i]);
    }
    tx.MapAncestorShard(env.Shards[0], owner);
    UNIT_ASSERT_VALUES_EQUAL(tx.GetLocksForShard(env.Shards[0]).size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(env.TableId, env.Shards[0]), "OK");
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT key, value FROM `/Root/table` ORDER BY key;"),
        "{ items { uint32_value: 1 } items { int32_value: 10 } }, "
        "{ items { uint32_value: 15 } items { int32_value: 20 } }");
}

Y_UNIT_TEST_TWIN(MissingCopyBreaksWholeTransactionAfterRestart, MissingFirst) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    const ui64 owner = env.Shards[0];
    WriteAt(env, tx, owner, {}, 1, TWriteOperation::Upsert({{1, 100}, {15, 1500}}));
    env.Split(0, 10);
    const auto sources = env.Shards;
    // A second, independent logical lock must also be broken with this LockTxId.
    WriteAt(env, tx, sources[1], {}, 1, TWriteOperation::Upsert(16, 1600));

    env.CompactBorrowedParts();
    ui64 destination = 0;
    auto initObserver = runtime.AddObserver<TEvDataShard::TEvInitSplitMergeDestination>(
        [&](auto& ev) { destination = ev->Get()->Record.GetSplitDescription().GetDestinationRanges(0).GetTabletID(); });
    const ui64 missingSource = sources[MissingFirst ? 0 : 1];
    ui64 deliveredSource = 0;
    TBlockEvents<TTransferSnapshot> snapshots(runtime, [&](const auto& ev) {
        if (ev->Get()->Record.GetSrcTabletId() == missingSource) {
            auto* locks = ev->Get()->Record.MutableLocks();
            for (auto& lock : *locks) {
                auto* ancestors = lock.MutableAncestorLocks();
                for (int i = ancestors->size(); i-- > 0;) {
                    if (ancestors->Get(i).GetTabletId() == owner) {
                        ancestors->DeleteSubrange(i, 1);
                    }
                }
            }
            for (int i = locks->size(); i-- > 0;) {
                if (!locks->Get(i).AncestorLocksSize()) {
                    locks->DeleteSubrange(i, 1);
                }
            }
        }
        return ev->Get()->Record.GetSrcTabletId() != deliveredSource;
    });
    const ui64 mergeTx = AsyncMergeTable(env.Server, env.Sender, "/Root/table", sources);
    runtime.WaitFor("two snapshots", [&] { return snapshots.size() >= 2; });
    UNIT_ASSERT(destination);
    // Always deliver the left source first; vary which source lacks the copy.
    if (snapshots[0]->Get()->Record.GetSrcTabletId() != sources[0]) {
        std::swap(snapshots[0], snapshots[1]);
    }
    ui32 acknowledgements = 0;
    auto ackObserver = runtime.AddObserver<TEvDataShard::TEvSplitTransferSnapshotAck>(
        [&](auto&) { ++acknowledgements; });
    auto duplicate = std::make_unique<TTransferSnapshot>();
    duplicate->Record.CopyFrom(snapshots.front()->Get()->Record);
    const auto firstSender = snapshots.front()->Sender;
    deliveredSource = sources[0];
    snapshots.Unblock(1);
    runtime.WaitFor("first snapshot committed", [&] { return acknowledgements > 0; });
    runtime.SendToPipe(destination, firstSender, duplicate.release());
    runtime.WaitFor("duplicate ignored", [&] { return acknowledgements > 1; });
    RebootTablet(runtime, destination, env.Sender);
    snapshots.Stop();
    for (const auto& snapshot : snapshots) {
        ReplaySnapshot(runtime, destination, snapshot);
    }
    snapshots.clear();
    WaitTxNotification(env.Server, env.Sender, mergeTx);
    env.Shards = GetTableShards(env.Server, env.Sender, "/Root/table");
    UNIT_ASSERT_VALUES_EQUAL(env.Shards[0], destination);
    RebootTablet(runtime, destination, env.Sender);
    tx.MapAncestorShard(destination, owner);
    tx.MapAncestorShard(destination, sources[1]);
    UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(env.TableId, destination), "ERROR: STATUS_LOCKS_BROKEN");
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT * FROM `/Root/table`;"), "");
}

Y_UNIT_TEST_TWIN(InconsistentCopiesBreakWholeTransaction, CorruptFirst) {
    for (ui32 mismatch = 0; mismatch != 4; ++mismatch) {
        TTestEnv env({});
        auto& runtime = env.GetRuntime();
        TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
        tx.WriterIndex = 123;
        const ui64 owner = env.Shards[0];
        WriteAt(env, tx, owner, {}, 1, TWriteOperation::Upsert({{1, 100}, {15, 1500}}));
        env.Split(0, 10);
        const auto sources = env.Shards;
        env.CompactBorrowedParts();
        TBlockEvents<TTransferSnapshot> snapshots(runtime);
        const ui64 mergeTx = AsyncMergeTable(env.Server, env.Sender, "/Root/table", sources);
        runtime.WaitFor("two snapshots", [&] { return snapshots.size() >= 2; });
        // Corrupt either the first or the second copy, including an identical
        // full path that claims a different applied WSN.
        auto* ancestor = snapshots[CorruptFirst ? 0 : 1]->Get()->Record.MutableLocks(0)->MutableAncestorLocks(0);
        switch (mismatch) {
            case 0: ancestor->SetGeneration(ancestor->GetGeneration() + 1); break;
            case 1: ancestor->SetCounter(ancestor->GetCounter() + 1); break;
            case 2: ancestor->MutableCreatedOnRange()->SetToInclusive(true); break;
            case 3:
                ancestor->MutableWriteSeqNumStates(0)->SetWriteSeqNum(2);
                ancestor->MutableWriteSeqNumStates(0)->MutableBranches(0)->SetWriteSeqNum(2);
                break;
        }
        snapshots.Stop().Unblock();
        WaitTxNotification(env.Server, env.Sender, mergeTx);
        env.Shards = GetTableShards(env.Server, env.Sender, "/Root/table");
        tx.MapAncestorShard(env.Shards[0], owner);
        UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(env.TableId, env.Shards[0]), "ERROR: STATUS_LOCKS_BROKEN");
    }
}

Y_UNIT_TEST(LargeRejectedLockFollowsOwnerLifetime) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    const ui64 owner = env.Shards[0];
    WriteAt(env, tx, owner, {}, 1, TWriteOperation::Upsert({{1, 100}, {15, 1500}}));
    env.Split(0, 10);
    env.CompactBorrowedParts();
    TBlockEvents<TTransferSnapshot> snapshots(runtime);
    const ui64 mergeTx = AsyncMergeTable(env.Server, env.Sender, "/Root/table", env.Shards);
    runtime.WaitFor("two snapshots", [&] { return snapshots.size() >= 2; });
    for (auto& snapshot : snapshots) {
        auto* state = snapshot->Get()->Record.MutableLocks(0)->MutableAncestorLocks(0)->MutableWriteSeqNumStates(0);
        // Rejection keeps a large valid subtree until ordinary owner cleanup.
        for (ui64 i = 0; i != 2000; ++i) {
            auto* branch = state->AddBranches();
            branch->AddShardsPath(owner);
            branch->AddShardsPath(1000000 + i);
            branch->SetWriteSeqNum(1);
        }
    }
    auto* conflicting = snapshots[1]->Get()->Record.MutableLocks(0)->MutableAncestorLocks(0);
    conflicting->SetCounter(conflicting->GetCounter() + 1);
    snapshots.Stop().Unblock();
    WaitTxNotification(env.Server, env.Sender, mergeTx);
    env.Shards = GetTableShards(env.Server, env.Sender, "/Root/table");
    RebootTablet(runtime, env.Shards[0], env.Sender);
    runtime.SendToPipe(env.Shards[0], env.Sender, new TEvDataShard::TEvGetOpenTxs(env.TableId.PathId));
    auto open = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(env.Sender);
    UNIT_ASSERT(open->Get()->OpenTxs.contains(tx.LockTxId));
    tx.MapAncestorShard(env.Shards[0], owner);
    UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(env.TableId, env.Shards[0]), "ERROR: STATUS_LOCKS_BROKEN");
    tx.LockHandle.Reset();
    runtime.SimulateSleep(TDuration::Seconds(1));
    CheckNoOpenTxs(env, env.Shards[0]);
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT * FROM `/Root/table`;"), "");
}

Y_UNIT_TEST(InconsistentWsnBranchesBreakLock) {
    for (ui32 mismatch = 0; mismatch != 7; ++mismatch) {
        TTestEnv env({});
        auto& runtime = env.GetRuntime();
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime,
            "UPSERT INTO `/Root/table` (key, value) VALUES (1, 100);"), "<empty>");
        TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
        tx.WriterIndex = 123;
        const ui64 owner = env.Shards[0];
        WriteAt(env, tx, owner, {}, 1, Increment(1, 10));
        TBlockEvents<TTransferSnapshot> snapshots(runtime);
        const ui64 splitTx = AsyncSplitTable(env.Server, env.Sender, "/Root/table", owner, 10);
        runtime.WaitFor("two snapshots", [&] { return snapshots.size() >= 2; });
        for (auto& snapshot : snapshots) {
            auto* state = snapshot->Get()->Record.MutableLocks(0)->MutableAncestorLocks(0)->MutableWriteSeqNumStates(0);
            UNIT_ASSERT_VALUES_EQUAL(state->GetWriteSeqNum(), 1);
            UNIT_ASSERT_VALUES_EQUAL(state->BranchesSize(), 1);
            switch (mismatch) {
                case 0: state->ClearBranches(); break;
                case 1: state->SetWriteSeqNum(2); break;
                case 2: state->SetWriteSeqNum(0); break;
                case 3: {
                    const auto branch = state->GetBranches(0);
                    state->AddBranches()->CopyFrom(branch);
                    break;
                }
                case 4: state->MutableBranches(0)->AddShardsPath(0); break;
                case 5: state->MutableBranches(0)->AddShardsPath(owner); break;
                case 6:
                    state->SetWriteSeqNum(0);
                    state->ClearBranches();
                    break;
            }
        }
        snapshots.Stop().Unblock();
        WaitTxNotification(env.Server, env.Sender, splitTx);
        env.Shards = GetTableShards(env.Server, env.Sender, "/Root/table");
        for (ui64 shard : env.Shards) {
            RebootTablet(runtime, shard, env.Sender);
        }
        // A retry must fail instead of applying INCREMENT again under WSN=1.
        auto req = tx.CreateWrite(env.TableId, TVector<ui64>{owner}, env.Shards[0], Increment(1, 10));
        req->Record.MutableOperations(0)->MutableWriteSeqNum()->SetWriteSeqNum(1);
        const auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(env.Shards[0], sender, req.release());
        TTransactionState::TWritePromise retry{tx, sender};
        UNIT_ASSERT_VALUES_EQUAL(retry.NextString(), "ERROR: STATUS_LOCKS_BROKEN");
        tx.MapAncestorShard(env.Shards[0], owner);
        UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(env.TableId, env.Shards[0]), "ERROR: STATUS_LOCKS_BROKEN");
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT key, value FROM `/Root/table`;"),
            "{ items { uint32_value: 1 } items { int32_value: 100 } }");
    }
}

Y_UNIT_TEST_TWIN(RejectedNoOpWriteCanRestartAfterSourceRemoval, AncestorRetry) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    const ui64 owner = env.Shards[0];
    // KQP has no acknowledged lock to validate when the first result is lost.
    TBlockEvents<NEvents::TDataEvents::TEvWriteResult> results(runtime);
    auto firstWrite = tx.SendWrite(env.TableId, owner, Increment(1, 10));
    UNIT_ASSERT_VALUES_EQUAL(firstWrite.NextString(TDuration::Seconds(1)), "<timeout>");
    UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
    const auto& result = results.front()->Get()->Record;
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks(0).GetWriteSeqNums(0).GetWriteSeqNum(), 1);
    results.Stop();
    results.clear();
    // INCREMENT of an absent row consumes WSN without creating an open UpdateTx.
    CheckNoOpenTxs(env, owner);
    TBlockEvents<TTransferSnapshot> snapshots(runtime);
    const ui64 splitTx = AsyncSplitTable(env.Server, env.Sender, "/Root/table", owner, 10);
    runtime.WaitFor("two snapshots", [&] { return snapshots.size() >= 2; });
    for (auto& snapshot : snapshots) {
        auto& record = snapshot->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.LocksSize(), 1);
        record.MutableLocks(0)->ClearAncestorLocks();
    }
    snapshots.Stop().Unblock();
    WaitTxNotification(env.Server, env.Sender, splitTx);
    env.Shards = GetTableShards(env.Server, env.Sender, "/Root/table");
    size_t subscriptions = 0;
    auto subscriptionObserver = runtime.AddObserver<NLongTxService::TEvLongTxService::TEvSubscribeLock>([&](auto& ev) {
        subscriptions += ev->Get()->Record.GetLockId() == tx.LockTxId;
    });
    for (ui64 shard : env.Shards) {
        RebootTablet(runtime, shard, env.Sender);
    }
    UNIT_ASSERT(subscriptions >= env.Shards.size());
    size_t rejectedCopies = 0;
    auto snapshotObserver = runtime.AddObserver<TTransferSnapshot>([&](auto& ev) {
        for (const auto& lock : ev->Get()->Record.GetLocks()) {
            rejectedCopies += lock.GetLockId() == tx.LockTxId;
        }
    });
    env.Merge(0, 1);
    env.Split(0, 10);
    UNIT_ASSERT_VALUES_EQUAL(rejectedCopies, 0);

    // Source-side removal discarded the broken no-op state. WSN=1 starts a fresh
    // chain, and its new result is the only lock expectation held by the client.
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime,
        "UPSERT INTO `/Root/table` (key, value) VALUES (1, 100);"), "<empty>");
    const TVector<ui64> ancestors = AncestorRetry ? TVector<ui64>{owner} : TVector<ui64>{};
    WriteAt(env, tx, env.Shards[0], ancestors, 1, Increment(1, 10));
    if (AncestorRetry) {
        tx.MapAncestorShard(env.Shards[0], owner);
    }
    UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(env.TableId, env.Shards[0]), "OK");
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT key, value FROM `/Root/table`;"),
        "{ items { uint32_value: 1 } items { int32_value: 110 } }");
}

Y_UNIT_TEST_TWIN(LegacyDestinationInitializesTableBeforeLocks, TransferEnabled) {
    TTestEnv env({}, TransferEnabled);
    auto& runtime = env.GetRuntime();
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime,
        "UPSERT INTO `/Root/table` (key, value) VALUES (1, 100);"), "<empty>");
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    const ui64 owner = env.Shards[0];
    if (TransferEnabled) {
        WriteAt(env, tx, owner, {}, 1, Increment(1, 10));
    }
    auto observer = runtime.AddObserver<TEvDataShard::TEvInitSplitMergeDestination>(
        [&](auto& ev) { ev->Get()->Record.ClearCreateTable(); });
    env.Split(0, 10);
    for (ui64 shard : env.Shards) {
        RebootTablet(runtime, shard, env.Sender);
    }
    if (TransferEnabled) {
        // Retry uses the transferred lock, including the restored table range.
        WriteAt(env, tx, env.Shards[0], {owner}, 1, Increment(1, 999));
        UNIT_ASSERT_VALUES_EQUAL(tx.ReadKey(env.TableId, env.Shards[0], 1), "1, 110\n");
    }
    UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT key, value FROM `/Root/table`;"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }");
}

Y_UNIT_TEST_TWIN(LegacySnapshotBreaksLock, CurrentFormat) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    const ui64 owner = env.Shards[0];
    WriteAt(env, tx, owner, {}, 1, TWriteOperation::Upsert(1, 100));
    TBlockEvents<TTransferSnapshot> snapshots(runtime);
    const ui64 splitTx = AsyncSplitTable(env.Server, env.Sender, "/Root/table", owner, 10);
    runtime.WaitFor("two snapshots", [&] { return snapshots.size() >= 2; });
    for (auto& snapshot : snapshots) {
        auto* lock = snapshot->Get()->Record.MutableLocks(0);
        if (CurrentFormat) {
            // A new-format ancestor entry with the required identity missing.
            lock->MutableAncestorLocks(0)->ClearCreatedOn();
        } else {
            // The old current-lock encoding, including persisted wire replay.
            lock->MutableWriteSeqNumStates()->CopyFrom(lock->GetAncestorLocks(0).GetWriteSeqNumStates());
            lock->ClearAncestorLocks();
        }
        const TString serialized = snapshot->Get()->Record.SerializeAsString();
        UNIT_ASSERT(snapshot->Get()->Record.ParseFromString(serialized));
    }
    snapshots.Stop().Unblock();
    WaitTxNotification(env.Server, env.Sender, splitTx);
    env.Shards = GetTableShards(env.Server, env.Sender, "/Root/table");
    for (ui64 shard : env.Shards) {
        RebootTablet(runtime, shard, env.Sender);
        tx.MapAncestorShard(shard, owner);
        UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(env.TableId, shard), "ERROR: STATUS_LOCKS_BROKEN");
    }
}

Y_UNIT_TEST(SourceRestartResendsCompleteLockMetadata) {
    TTestEnv env({});
    auto& runtime = env.GetRuntime();
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;
    const ui64 owner = env.Shards[0];
    WriteAt(env, tx, owner, {}, 1, TWriteOperation::Upsert(1, 100));
    env.Split(0, 10);
    const ui64 source = env.Shards[0];
    WriteAt(env, tx, source, {owner}, 2, Increment(1, 10));
    TBlockEvents<TTransferSnapshot> snapshots(runtime);
    const ui64 splitTx = AsyncSplitTable(env.Server, env.Sender, "/Root/table", source, 5);
    runtime.WaitFor("initial snapshots", [&] { return snapshots.size() >= 2; });
    const TString metadata = snapshots.front()->Get()->Record.GetLocks(0).SerializeAsString();
    snapshots.clear();
    RebootTablet(runtime, source, env.Sender);
    runtime.WaitFor("resent snapshots", [&] { return snapshots.size() >= 2; });
    for (const auto& snapshot : snapshots) {
        UNIT_ASSERT_VALUES_EQUAL(snapshot->Get()->Record.GetLocks(0).SerializeAsString(), metadata);
    }
    snapshots.Stop().Unblock();
    WaitTxNotification(env.Server, env.Sender, splitTx);
    env.Shards = GetTableShards(env.Server, env.Sender, "/Root/table");
    auto lock = WriteAt(env, tx, env.Shards[0], {owner, source}, 2, Increment(1, 999));
    UNIT_ASSERT_VALUES_EQUAL(lock.GetWriteSeqNums(0).GetWriteSeqNum(), 2);
    UNIT_ASSERT_VALUES_EQUAL(tx.ReadKey(env.TableId, env.Shards[0], 1), "1, 110\n");
}

Y_UNIT_TEST(LocksTransferSimple) {
    // Create a table with 3 shards
    TTestEnv env({10, 20});
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // Lock and upsert a row in shards 0 and 1
    tx.LockRows(tableId, shards.at(0), {1});
    tx.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 100));

    tx.LockRows(tableId, shards.at(1), {15});
    tx.Write(tableId, shards.at(1), TWriteOperation::Upsert(15, 1500));

    // After a split-merge we end up with 3 shards as well, shard 0 got
    // data from old shards 0 and 1, and shard 1 from old shard 1
    auto oldShards = shards;
    env.Split(1, 15);
    env.Merge(0, 1);

    tx.MapAncestorShard(shards.at(0), oldShards.at(0));
    tx.MapAncestorShard(shards.at(0), oldShards.at(1));
    tx.MapAncestorShard(shards.at(1), oldShards.at(1));

    // Reboot one of the tablets to check loading locks from localdb
    RebootTablet(runtime, shards.at(1), sender);

    // Check that we can still read uncommitted data on new shards
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 1),
        "1, 100\n");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(1), 15),
        "15, 1500\n");

    // Commit the transaction and check that the results are readable
    tx.InitCommit({shards.at(0), shards.at(1)});
    auto prepare1 = tx.PrepareCommit(tableId, shards.at(0));
    auto prepare2 = tx.PrepareCommit(tableId, shards.at(1));
    tx.SendPlan();
    UNIT_ASSERT_VALUES_EQUAL(prepare1.NextString(), "OK");
    UNIT_ASSERT_VALUES_EQUAL(prepare2.NextString(), "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 15 } items { int32_value: 1500 } }");
}

Y_UNIT_TEST_TWIN(WriteSeqNums, PendingRowLock) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    // Start with a table with 2 shards
    env.Split(0, 10);

    // Use write seqnums in this tx
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // Apply shard 0's write and lose its result. Keep shard 1's first write
    // pending so its retry creates a new ancestor after the merge.
    auto shard0Actor = ResolveTablet(runtime, shards.at(0));
    TBlockEvents<NEvents::TDataEvents::TEvWriteResult> blockWriteResToShard0(runtime, [&](auto& ev) {
        return ev->Sender == shard0Actor;
    });
    auto shard1Actor = ResolveTablet(runtime, shards.at(1));
    TBlockEvents<NEvents::TDataEvents::TEvWrite> blockWritesToShard1(runtime, [&](auto& ev) {
        return ev->GetRecipientRewrite() == shard1Actor;
    });

    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(0), {1}), "OK");
    auto write1Fut = tx.SendWrite(tableId, shards.at(0), TWriteOperation::Upsert(1, 100));
    UNIT_ASSERT_VALUES_EQUAL(
        write1Fut.NextString(TDuration::Seconds(1)),
        "<timeout>");

    if (PendingRowLock) {
        // LockRows writes a row-lock delta before the first WSN is applied.
        // This WSN-less lock is discarded by merge and aborts the shared id.
        UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(1), {15}), "OK");
    }
    auto write2Fut = tx.SendWrite(tableId, shards.at(1), TWriteOperation::Upsert(15, 1500));
    UNIT_ASSERT_VALUES_EQUAL(
        write2Fut.NextString(TDuration::Seconds(1)),
        "<timeout>");

    runtime.WaitFor("Blocked writes", [&] {
        return !blockWriteResToShard0.empty() && !blockWritesToShard1.empty();
    });
    blockWriteResToShard0.Stop();
    blockWritesToShard1.Stop();

    // Merge into a single shard
    auto oldShards = shards;
    env.Merge(0, 1);

    tx.MapAncestorShard(shards.at(0), oldShards.at(0));
    tx.MapAncestorShard(shards.at(0), oldShards.at(1));

    const TString expectedRetryStatus = PendingRowLock ? "ERROR: STATUS_LOCKS_BROKEN" : "OK";
    // Re-send the write to old shard 0. It is a duplicate.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.RetryWriteToAnotherShard(
            tableId, oldShards.at(0), shards.at(0), TWriteOperation::Upsert(1, 100)),
        expectedRetryStatus);
    // Re-send the write to old shard 1.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.RetryWriteToAnotherShard(
            tableId, oldShards.at(1), shards.at(0), TWriteOperation::Upsert(15, 1500)),
        expectedRetryStatus);

    // Stale request is an error now that the old shard 1 is merged
    blockWritesToShard1.Unblock();
    UNIT_ASSERT_VALUES_EQUAL(
        write2Fut.NextString(TDuration::Seconds(1)),
        "ERROR: STATUS_WRONG_SHARD_STATE");

    if (PendingRowLock) {
        CheckNoOpenTxs(env, shards.at(0));
        UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(tableId, shards.at(0)), "ERROR: STATUS_LOCKS_BROKEN");
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT key, value FROM `/Root/table` ORDER BY key;"), "");
        return;
    }

    // Write something to the new shard as well.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(0), {20}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, shards.at(0), TWriteOperation::Upsert(20, 2000)),
        "OK");

    // Check that the uncommitted data is readable.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 1),
        "1, 100\n");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 15),
        "15, 1500\n");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 20),
        "20, 2000\n");

    // Do an immediate commit and check that the data was committed.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.WriteCommit(tableId, shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 15 } items { int32_value: 1500 } }, "
        "{ items { uint32_value: 20 } items { int32_value: 2000 } }");
}

// A re-routed batch after a split may merge two pending positions of the removed
// shard's write chain in a single EvWrite: the already applied, unacknowledged
// position 1 (its result was lost, its rows were moved by the split) and the still
// pending position 2 that was queued behind it. Such a mixed group starts at an
// already applied position: the leading position is a duplicate (its ops are
// skipped, the rows are not written again) and the tail is applied as a
// continuation of the transferred ancestor chain. Before the DataShard accepted
// mixed groups, every covering shard answered the merged batch with
// STATUS_BAD_REQUEST ("must continue the writer chain at 2").
Y_UNIT_TEST(WriteSeqNumsRerouteMergesAppliedAndPending) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, initialShards] = env.GetAll();
    UNIT_ASSERT_VALUES_EQUAL(initialShards.size(), 1u);

    // Start with a table with 2 shards
    env.Split(0, 10);
    TVector<ui64> shards = env.Shards;
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

    // Use write seqnums in this tx
    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // The first write (WriteSeqNum 1) is applied on the shard that is about to be
    // split, but its result is blocked: the batch stays unacknowledged, like a lost
    // reply of an in-flight re-routed batch.
    auto splitShardActor = ResolveTablet(runtime, shards.at(0));
    TBlockEvents<NEvents::TDataEvents::TEvWriteResult> blockWriteResToSplitShard(runtime, [&](auto& ev) {
        return ev->Sender == splitShardActor;
    });

    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(0), {1}), "OK");
    auto write1Fut = tx.SendWrite(tableId, shards.at(0), TWriteOperation::Upsert(1, 100));
    UNIT_ASSERT_VALUES_EQUAL(
        write1Fut.NextString(TDuration::Seconds(1)),
        "<timeout>");

    runtime.WaitFor("Blocked write result", [&] {
        return !blockWriteResToSplitShard.empty();
    });

    // Release the blocked reply of the original write: it is a stale duplicate of
    // position 1 once the re-routed batches arrive; nothing must break. Draining it
    // before the re-routes also keeps the harness' lock echo bookkeeping fresh.
    blockWriteResToSplitShard.Stop();
    blockWriteResToSplitShard.Unblock();
    UNIT_ASSERT_VALUES_EQUAL(
        write1Fut.NextString(TDuration::Seconds(1)),
        "OK");

    // Quiet split of the shard: the lock with its WriteSeqNum chain (at 1) and the
    // rows transfer to the two new covering shards.
    auto oldShards = shards;
    env.Split(0, 5);
    shards = env.Shards;
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 3u);

    tx.MapAncestorShard(shards.at(0), oldShards.at(0));
    tx.MapAncestorShard(shards.at(1), oldShards.at(0));

    // The drained reply advanced the test's write seq num bookkeeping for the
    // removed shard; reset it so the re-routed batch is rebuilt with the pending
    // positions [1, 2] of the removed shard's chain: the applied position 1 (a
    // duplicate now, its result was lost) and the queued position 2.
    tx.Shard2SeqNum[oldShards.at(0)] = 0;

    // The re-routed batch merges the applied position 1 with the pending position 2
    // of the removed shard's chain. The op of the duplicate position carries a
    // different value (999): if it were applied again, key 1 would be overwritten.
    {
        auto promise = tx.SendWriteToAnotherShard(
            tableId, oldShards.at(0), shards.at(0),
            TWriteOperation::Upsert(1, 999), TWriteOperation::Upsert(2, 200));
        auto result = promise.NextResult();
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        // The reply reports the sum of the batch's statistics: the duplicate
        // position's original delivery (1 written row, its reply was lost) plus
        // the fresh tail op for key 2.
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetTxStats().GetTableAccessStats(0).GetUpdateRow().GetRows(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetTxLocks().size(), 1u);
        bool chainAdvanced = false;
        for (const auto& lock : result->Record.GetTxLocks()) {
            if (lock.WriteSeqNumsSize() == 1
                    && lock.GetWriteSeqNums(0).GetWriterIndex() == 123u
                    && lock.GetWriteSeqNums(0).GetWriteSeqNum() == 2u) {
                chainAdvanced = true;
            }
        }
        UNIT_ASSERT_C(chainAdvanced,
            "the transferred chain did not advance to position 2");
    }

    // The second covering shard receives the same merged positions without data (the
    // empty re-routed batch): the duplicate position is skipped, the empty tail
    // advances the transferred chain. The reply echoes the transferred ancestor
    // lock carrying the advanced chain position. The first reply advanced the
    // test's write seq num bookkeeping for the removed shard;
    // reset it so the same merged positions are rebuilt for the empty batch.
    tx.Shard2SeqNum[oldShards.at(0)] = 0;
    {
        const TVector<TWriteOperation::TKeyValue> noRows;
        auto promise = tx.SendWriteToAnotherShard(
            tableId, oldShards.at(0), shards.at(1),
            TWriteOperation::Upsert(noRows), TWriteOperation::Upsert(noRows));
        auto result = promise.NextResult();
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        // The duplicate position's stored statistics are reported even when the
        // batch's own ops are empty: the writer still never saw the lost delivery.
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetTxStats().GetTableAccessStats(0).GetUpdateRow().GetRows(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetTxLocks().size(), 1u);
        bool chainAdvanced = false;
        for (const auto& lock : result->Record.GetTxLocks()) {
            if (lock.WriteSeqNumsSize() == 1
                    && lock.GetWriteSeqNums(0).GetWriterIndex() == 123u
                    && lock.GetWriteSeqNums(0).GetWriteSeqNum() == 2u) {
                chainAdvanced = true;
            }
        }
        UNIT_ASSERT_C(chainAdvanced,
            "the echoed transferred chain did not advance to position 2");
    }

    // The duplicate position was not applied again: key 1 keeps its value; the tail
    // is applied as a continuation.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 1),
        "1, 100\n");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 2),
        "2, 200\n");

    // Commit the transaction and check that the data is committed.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.WriteCommit(tableId, shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` WHERE key = 1 OR key = 2 ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 2 } items { int32_value: 200 } }");
}

// Operations of the current shard must come last in a request: the writer
// flushes the re-routed batches of removed shards (ancestor chains) before its
// fresh current-shard writes, so an ancestor operation positioned after a
// current-shard operation is a writer bug and must be rejected.
Y_UNIT_TEST(WriteSeqNumsAncestorAfterCurrentShardRejected) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, initialShards] = env.GetAll();
    UNIT_ASSERT_VALUES_EQUAL(initialShards.size(), 1u);

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // Apply positions 1 and 2 of the writer chain on the shard: the delivery is
    // acknowledged normally, so the shard's chain is at 2.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, env.Shards.at(0), {1}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, env.Shards.at(0), TWriteOperation::Upsert(1, 100), TWriteOperation::Upsert(2, 200)),
        "OK");

    // Split the shard: the lock with its WriteSeqNum chain (at 2) transfers to
    // the new covering shards as an ancestor state.
    const ui64 oldShard = env.Shards.at(0);
    env.Split(0, 5);
    auto shards = env.Shards;
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
    tx.MapAncestorShard(shards.at(0), oldShard);

    tx.Shard2SeqNum[oldShard] = 0;
    {
        auto req = tx.CreateWrite(tableId, oldShard, shards.at(0),
            TWriteOperation::Upsert(3, 300), TWriteOperation::Upsert(4, 400));
        // The first op is re-labeled as a fresh position of the current shard's
        // own chain, the second stays on the transferred ancestor chain as its
        // already applied position 2: the ancestor op comes after the
        // current-shard op and must be rejected.
        req->Record.MutableOperations(0)->ClearAncestors();

        auto reqSender = runtime.AllocateEdgeActor();
        ui32 nodeIndex = reqSender.NodeId() - runtime.GetNodeId(0);
        runtime.SendToPipe(shards.at(0), reqSender, req.release(), nodeIndex);

        TTransactionState::TWritePromise promise{tx, reqSender};
        auto result = promise.NextResult();
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        UNIT_ASSERT(result->Record.GetIssues(0).message().Contains(
            "Ancestor operations must precede current operations"));
    }

    // Neither row of the rejected request was applied.
    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` WHERE key = 3 OR key = 4 ORDER BY key;
        )"),
        "");
}

// Already applied ancestor positions may be replayed after the current shard
// chain has started, but a new ancestor position must still be rejected: the
// writer sends the re-routed ancestor chain before its fresh current-shard writes.
Y_UNIT_TEST(WriteSeqNumsAncestorContinuationAfterCurrentShardRejected) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, initialShards] = env.GetAll();
    UNIT_ASSERT_VALUES_EQUAL(initialShards.size(), 1u);

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // Apply position 1 of the writer chain on the shard.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, env.Shards.at(0), {1}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, env.Shards.at(0), TWriteOperation::Insert(1, 100)),
        "OK");

    // Split the shard: the lock with its WriteSeqNum chain (at 1) transfers to
    // the new covering shards as an ancestor state.
    const ui64 oldShard = env.Shards.at(0);
    env.Split(0, 5);
    auto shards = env.Shards;
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
    tx.MapAncestorShard(shards.at(0), oldShard);

    // Start the current shard's own chain at position 1.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(0), {3}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, shards.at(0), TWriteOperation::Insert(3, 300)),
        "OK");

    // Position 2 is a fresh continuation of the transferred ancestor chain and is
    // not allowed after the current shard chain has started.
    {
        auto req = tx.CreateWrite(tableId, oldShard, shards.at(0),
            TWriteOperation::Insert(6, 600));

        auto reqSender = runtime.AllocateEdgeActor();
        ui32 nodeIndex = reqSender.NodeId() - runtime.GetNodeId(0);
        runtime.SendToPipe(shards.at(0), reqSender, req.release(), nodeIndex);

        TTransactionState::TWritePromise promise{tx, reqSender};
        auto result = promise.NextResult();
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        UNIT_ASSERT(result->Record.GetIssues(0).message().Contains(
            "Fresh ancestor writes are forbidden after current writes"));
    }

    // Commit the current shard's write and check that the rejected ancestor row
    // was not applied.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.WriteCommit(tableId, shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` WHERE key = 3 OR key = 6 ORDER BY key;
        )"),
        "{ items { uint32_value: 3 } items { int32_value: 300 } }");
}

// The legitimate shape of a merged request: the re-routed operations of the
// removed shard's chain come first (unacknowledged batches are re-sent before
// fresh writes), the fresh operations of the current shard's own chain come
// last. The duplicate position is skipped, the tails of both chains apply and
// advance.
Y_UNIT_TEST(WriteSeqNumsAncestorOpsBeforeCurrentShardOps) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, initialShards] = env.GetAll();
    UNIT_ASSERT_VALUES_EQUAL(initialShards.size(), 1u);

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // Apply position 1 of the writer chain on the shard.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, env.Shards.at(0), {1}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, env.Shards.at(0), TWriteOperation::Upsert(1, 100)),
        "OK");

    // Split the shard: the lock with its WriteSeqNum chain (at 1) transfers to
    // the new covering shards as an ancestor state.
    const ui64 oldShard = env.Shards.at(0);
    env.Split(0, 5);
    auto shards = env.Shards;
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
    tx.MapAncestorShard(shards.at(0), oldShard);

    tx.Shard2SeqNum[oldShard] = 0;
    {
        auto sendWrite = [&]() {
            auto req = tx.CreateWrite(tableId, oldShard, shards.at(0),
                TWriteOperation::Upsert(1, 999), TWriteOperation::Upsert(3, 300), TWriteOperation::Upsert(4, 400));
            // The first op is the already applied position 1 of the transferred
            // chain: a duplicate whose row must be skipped. The second op continues
            // the transferred chain. The third op is re-labeled as a fresh position
            // of the current shard's own chain and comes last.
            req->Record.MutableOperations(2)->ClearAncestors();
            req->Record.MutableOperations(2)->MutableWriteSeqNum()->SetWriteSeqNum(1);

            auto reqSender = runtime.AllocateEdgeActor();
            ui32 nodeIndex = reqSender.NodeId() - runtime.GetNodeId(0);
            runtime.SendToPipe(shards.at(0), reqSender, req.release(), nodeIndex);

            return TTransactionState::TWritePromise{tx, reqSender};
        };

        // Hold the first reply to simulate a lost response, then replay the exact
        // same request: the already applied ancestor positions must be accepted as
        // duplicates instead of being rejected after the current shard chain started.
        auto shardActor = ResolveTablet(runtime, shards.at(0));
        TBlockEvents<NEvents::TDataEvents::TEvWriteResult> blockWriteResToShard(runtime, [&](auto& ev) {
            return ev->Sender == shardActor;
        });

        auto lostReply = sendWrite();
        UNIT_ASSERT_VALUES_EQUAL(lostReply.NextString(TDuration::Seconds(1)), "<timeout>");
        runtime.WaitFor("Blocked write result", [&] {
            return !blockWriteResToShard.empty();
        });
        blockWriteResToShard.Stop();

        auto replay = sendWrite();
        auto result = replay.NextResult();
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        // Both chains stay advanced: the transferred one at position 2, the current
        // shard's own one at position 1.
        bool chain1 = false;
        bool chain2 = false;
        for (const auto& lock : result->Record.GetTxLocks()) {
            for (const auto& seqNum : lock.GetWriteSeqNums()) {
                if (seqNum.GetWriterIndex() == 123u) {
                    chain1 |= (seqNum.GetWriteSeqNum() == 1u);
                    chain2 |= (seqNum.GetWriteSeqNum() == 2u);
                }
            }
        }
        UNIT_ASSERT_C(chain1, "the current shard's chain did not advance to position 1");
        UNIT_ASSERT_C(chain2, "the transferred chain did not advance to position 2");
    }

    // Commit the transaction and check that only the fresh rows were applied:
    // the duplicate's row keeps its original value.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.WriteCommit(tableId, shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` WHERE key = 1 OR key = 3 OR key = 4 ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 3 } items { int32_value: 300 } }, "
        "{ items { uint32_value: 4 } items { int32_value: 400 } }");
}

// A successfully applied mixed request may be retried as is. The duplicate INSERT
// operations must not be executed again: they would conflict with the rows the
// first delivery inserted.
Y_UNIT_TEST(WriteSeqNumsAncestorOpsBeforeCurrentShardOpsInsertReplay) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, initialShards] = env.GetAll();
    UNIT_ASSERT_VALUES_EQUAL(initialShards.size(), 1u);

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // Apply position 1 of the writer chain on the shard.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, env.Shards.at(0), {1}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, env.Shards.at(0), TWriteOperation::Insert(1, 100)),
        "OK");

    // Split the shard: the lock with its WriteSeqNum chain (at 1) transfers to
    // the new covering shards as an ancestor state.
    const ui64 oldShard = env.Shards.at(0);
    env.Split(0, 5);
    auto shards = env.Shards;
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
    tx.MapAncestorShard(shards.at(0), oldShard);

    auto sendWrite = [&]() {
        auto req = tx.CreateWrite(tableId, oldShard, shards.at(0),
            TWriteOperation::Insert(1, 999), TWriteOperation::Insert(3, 300), TWriteOperation::Insert(4, 400));
        // The first op is the already applied position 1 of the transferred
        // chain: a duplicate whose row must be skipped. The second op continues
        // the transferred chain. The third op is re-labeled as a fresh position
        // of the current shard's own chain and comes last.
        req->Record.MutableOperations(2)->ClearAncestors();
        req->Record.MutableOperations(2)->MutableWriteSeqNum()->SetWriteSeqNum(1);

        auto reqSender = runtime.AllocateEdgeActor();
        ui32 nodeIndex = reqSender.NodeId() - runtime.GetNodeId(0);
        runtime.SendToPipe(shards.at(0), reqSender, req.release(), nodeIndex);

        return TTransactionState::TWritePromise{tx, reqSender};
    };

    auto checkChains = [](const NKikimrDataEvents::TEvWriteResult& result) {
        bool chain1 = false;
        bool chain2 = false;
        for (const auto& lock : result.GetTxLocks()) {
            for (const auto& seqNum : lock.GetWriteSeqNums()) {
                if (seqNum.GetWriterIndex() == 123u) {
                    chain1 |= (seqNum.GetWriteSeqNum() == 1u);
                    chain2 |= (seqNum.GetWriteSeqNum() == 2u);
                }
            }
        }
        UNIT_ASSERT_C(chain1, "the current shard's chain did not advance to position 1");
        UNIT_ASSERT_C(chain2, "the transferred chain did not advance to position 2");
    };

    tx.Shard2SeqNum[oldShard] = 0;
    {
        auto first = sendWrite();
        auto result = first.NextResult();
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        checkChains(result->Record);
    }

    // The successful reply advanced the test's write seq num bookkeeping; reset it
    // so the retry rebuilds the identical positions.
    tx.Shard2SeqNum[oldShard] = 0;
    {
        auto replay = sendWrite();
        auto result = replay.NextResult();
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        checkChains(result->Record);
    }

    // Commit the transaction and check that the duplicate INSERT was not executed:
    // key 1 keeps its original value and no insert conflict was reported.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.WriteCommit(tableId, shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` WHERE key = 1 OR key = 3 OR key = 4 ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 3 } items { int32_value: 300 } }, "
        "{ items { uint32_value: 4 } items { int32_value: 400 } }");
}

// A merge re-route mixes the chains of several removed shards in one EvWrite,
// and the order of operations depends on the re-route's map iteration.
// Duplicates must form a prefix of their own shard's operations, which holds
// per shard even when the request interleaves the chains: a duplicate of one
// chain may follow a fresh operation of another.
Y_UNIT_TEST(WriteSeqNumsMergedAncestorChainsInterleaved) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, initialShards] = env.GetAll();
    UNIT_ASSERT_VALUES_EQUAL(initialShards.size(), 1u);

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // Apply position 1 of the writer chain on the shard.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, env.Shards.at(0), {1}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, env.Shards.at(0), TWriteOperation::Upsert(1, 100)),
        "OK");

    // Split the shard: the lock with its WriteSeqNum chain (at 1) transfers to
    // the new covering shards as an ancestor state.
    const ui64 oldShard0 = env.Shards.at(0);
    env.Split(0, 10);
    UNIT_ASSERT_VALUES_EQUAL(env.Shards.size(), 2u);

    // Apply position 1 of the writer chain on the second covering shard: a
    // fresh chain of the current shard.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, env.Shards.at(1), {15}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, env.Shards.at(1), TWriteOperation::Upsert(15, 1500)),
        "OK");

    // Merge the shards: both chains transfer to the merged shard as ancestor
    // states.
    auto oldShards = env.Shards;
    env.Merge(0, 1);
    auto shards = env.Shards;
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
    tx.MapAncestorShard(shards.at(0), oldShards.at(0));
    tx.MapAncestorShard(shards.at(0), oldShards.at(1));

    tx.Shard2SeqNum[oldShard0] = 0;
    {
        auto req = tx.CreateWrite(tableId, oldShard0, shards.at(0),
            TWriteOperation::Upsert(1, 999), TWriteOperation::Upsert(3, 300), TWriteOperation::Upsert(15, 999));
        // The first two ops continue the first transferred chain: position 1 is
        // already applied (a duplicate whose row must be skipped), position 2 is
        // fresh. The third op is re-labeled to the second transferred chain as
        // its already applied position 1 and comes after the fresh operation of
        // the first chain, which the shard must accept.
        req->Record.MutableOperations(2)->SetAncestors(0, oldShards.at(1));
        req->Record.MutableOperations(2)->MutableWriteSeqNum()->SetWriteSeqNum(1);

        auto reqSender = runtime.AllocateEdgeActor();
        ui32 nodeIndex = reqSender.NodeId() - runtime.GetNodeId(0);
        runtime.SendToPipe(shards.at(0), reqSender, req.release(), nodeIndex);

        TTransactionState::TWritePromise promise{tx, reqSender};
        auto result = promise.NextResult();
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            result->Record.GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    // Commit the transaction and check that only the fresh row was applied: the
    // duplicates' rows keep their original values.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.WriteCommit(tableId, shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` WHERE key = 1 OR key = 3 OR key = 15 ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 3 } items { int32_value: 300 } }, "
        "{ items { uint32_value: 15 } items { int32_value: 1500 } }");
}

Y_UNIT_TEST(RollbackSimple) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    // Start with a table with 2 shards
    env.Split(0, 10);

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx.WriterIndex = 123;

    // Lock and upsert a row to each of the shards.
    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(0), {1}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 100)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(tx.LockRows(tableId, shards.at(1), {15}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, shards.at(1), TWriteOperation::Upsert(15, 1500)),
        "OK");

    // Merge into a single shard.
    auto oldShards = shards;
    env.Merge(0, 1);

    tx.MapAncestorShard(shards.at(0), oldShards.at(0));
    tx.MapAncestorShard(shards.at(0), oldShards.at(1));

    // Check that the uncommitted data is there.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 1),
        "1, 100\n");
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 15),
        "15, 1500\n");

    // Rollback the transaction and check that everything gets rolled back.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Rollback(shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` ORDER BY key;
        )"),
        "");

    runtime.SendToPipe(shards.at(0), sender, new TEvDataShard::TEvGetOpenTxs(tableId.PathId));
    auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(sender);
    UNIT_ASSERT(ev->Get()->OpenTxs.empty());
}

Y_UNIT_TEST_TWIN(ConflictsSimple, CommitInWriteOrder) {
    TTestEnv env({});
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    // Start with a table with 2 shards
    env.Split(0, 10);

    TTransactionState tx1(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    tx1.WriterIndex = 123;
    TTransactionState tx2(runtime, NKikimrDataEvents::OPTIMISTIC);
    tx2.WriterIndex = 234;

    // Tx1: lock and upsert a row to each of the shards.
    UNIT_ASSERT_VALUES_EQUAL(tx1.LockRows(tableId, shards.at(0), {1}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx1.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 100)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(tx1.LockRows(tableId, shards.at(1), {15}), "OK");
    UNIT_ASSERT_VALUES_EQUAL(
        tx1.Write(tableId, shards.at(1), TWriteOperation::Upsert(15, 1500)),
        "OK");

    // Tx2: upsert the same key on the second shard.
    UNIT_ASSERT_VALUES_EQUAL(
        tx2.Write(tableId, shards.at(1), TWriteOperation::Upsert(15, 1501)),
        "OK");

    // Merge into a single shard.
    auto oldShards = shards;
    env.Merge(0, 1);

    for (auto tx : {&tx1, &tx2}) {
        tx->MapAncestorShard(shards.at(0), oldShards.at(0));
        tx->MapAncestorShard(shards.at(0), oldShards.at(1));
    }

    if (CommitInWriteOrder) {
        // Commit tx1 first, tx2 should still be live.
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.WriteCommit(tableId, shards.at(0)),
            "OK");

        UNIT_ASSERT_VALUES_EQUAL(
            tx2.Write(tableId, shards.at(0), TWriteOperation::Upsert(15, 1502)),
            "OK");

        // Commit tx2 and observe the results.
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.WriteCommit(tableId, shards.at(0)),
            "OK");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
            "{ items { uint32_value: 15 } items { int32_value: 1502 } }");
    } else {
        // Commit tx2 first, this should abort tx1.
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.WriteCommit(tableId, shards.at(0)),
            "OK");

        // tx1 should be aborted, so additional updates should fail.
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 101)),
            "ERROR: STATUS_LOCKS_BROKEN");

        UNIT_ASSERT_VALUES_EQUAL(
            tx1.WriteCommit(tableId, shards.at(0)),
            "ERROR: STATUS_LOCKS_BROKEN");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { uint32_value: 15 } items { int32_value: 1501 } }");
    }
}

} // Y_UNIT_TEST_SUITE(DataShardLocksTransfer)

} // namespace NKikimr
