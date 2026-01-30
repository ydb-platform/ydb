#include "datashard_ut_common_kqp.h"
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardSnapshotIsolation) {

    std::tuple<TTestActorRuntime&, Tests::TServer::TPtr, TActorId> TestCreateServer(const TServerSettings& serverSettings) {
        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        return {runtime, server, sender};
    }

    ui64 AllocateTxId(TTestActorRuntime& runtime, const TActorId& sender) {
        ui32 nodeId = sender.NodeId();
        ui32 nodeIndex = nodeId - runtime.GetNodeId(0);
        runtime.Send(new IEventHandle(MakeTxProxyID(), sender, new TEvTxUserProxy::TEvAllocateTxId), nodeIndex, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvAllocateTxIdResult>(sender);
        auto* msg = ev->Get();
        return msg->TxId;
    }

    TRowVersion AcquireSnapshot(TTestActorRuntime& runtime, const TActorId& sender, const TString& databaseName = "/Root") {
        ui32 nodeId = sender.NodeId();
        ui32 nodeIndex = nodeId - runtime.GetNodeId(0);
        auto* req = new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(databaseName);
        runtime.Send(new IEventHandle(NLongTxService::MakeLongTxServiceID(nodeId), sender, req), nodeIndex, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvAcquireReadSnapshotResult>(sender);
        auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        return TRowVersion(msg->Record.GetSnapshotStep(), msg->Record.GetSnapshotTxId());
    }

    NLongTxService::TLockHandle MakeLockHandle(TTestActorRuntime& runtime, ui64 lockId, ui32 nodeIndex = 0) {
        return NLongTxService::TLockHandle(lockId, runtime.GetActorSystem(nodeIndex));
    }

    TString FormatReadResult(const TEvDataShard::TEvReadResult* msg) {
        TStringBuilder sb;
        if (msg->Record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
            size_t count = msg->GetRowsCount();
            for (size_t i = 0; i < count; ++i) {
                auto cells = msg->GetCells(i);
                for (size_t j = 0; j < cells.size(); ++j) {
                    if (j != 0) {
                        sb << ", ";
                    }
                    sb << cells[j].AsValue<i32>();
                }
                sb << "\n";
            }
        } else {
            sb << "ERROR: " << msg->Record.GetStatus().GetCode();
        }
        return sb;
    }

    struct TKeyValue {
        i32 Key;
        i32 Value;
    };

    struct TOperation {
        NKikimrDataEvents::TEvWrite::TOperation::EOperationType Type;
        TVector<TKeyValue> Rows;

        void ApplyTo(const TTableId& tableId, NEvents::TDataEvents::TEvWrite* req) {
            TVector<TCell> cells;
            cells.reserve(Rows.size() * 2);
            for (const auto& row : Rows) {
                cells.push_back(TCell::Make(row.Key));
                cells.push_back(TCell::Make(row.Value));
            }

            TSerializedCellMatrix matrix(cells, Rows.size(), 2);
            TString blobData = matrix.ReleaseBuffer();

            ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*req).AddDataToPayload(std::move(blobData));
            req->AddOperation(Type, tableId, { 1, 2 }, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);
        }

        static TOperation Upsert(i32 key, i32 value) {
            return Upsert({ { key, value } });
        }

        static TOperation Upsert(TVector<TKeyValue> rows) {
            return TOperation{
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                std::move(rows),
            };
        }

        static TOperation Insert(i32 key, i32 value) {
            return Insert({ { key, value } } );
        }

        static TOperation Insert(TVector<TKeyValue> rows) {
            return TOperation{
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                std::move(rows),
            };
        }
    };

    class TTransactionState {
    public:
        TTransactionState(TTestActorRuntime& runtime)
            : Runtime(runtime)
        {
            Sender = Runtime.AllocateEdgeActor();
            LockTxId = AllocateTxId(Runtime, Sender);
            LockNodeId = Sender.NodeId();
            LockHandle = MakeLockHandle(Runtime, LockTxId, LockNodeId - Runtime.GetNodeId(0));
        }

        struct TReadPromise {
            TTransactionState& State;
            TActorId Sender;
            TActorId LastSender = TActorId();
            ui64 LastSeqNo = 0;

            void SendAck(ui64 maxRows = Max<ui64>(), ui64 maxBytes = Max<ui64>()) {
                auto* msg = new TEvDataShard::TEvReadAck();
                msg->Record.SetReadId(0);
                msg->Record.SetSeqNo(LastSeqNo);
                msg->Record.SetMaxRows(maxRows);
                msg->Record.SetMaxBytes(maxBytes);
                ui32 nodeIndex = Sender.NodeId() - State.Runtime.GetNodeId(0);
                State.Runtime.Send(new IEventHandle(LastSender, Sender, msg), nodeIndex, true);
            }

            std::unique_ptr<TEvDataShard::TEvReadResult> NextResult(TDuration simTimeout = TDuration::Max()) {
                auto ev = State.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(Sender, simTimeout);
                if (!ev) {
                    return nullptr;
                }
                LastSender = ev->Sender;
                std::unique_ptr<TEvDataShard::TEvReadResult> msg(ev->Release().Release());
                LastSeqNo = msg->Record.GetSeqNo();
                for (const auto& lock : msg->Record.GetTxLocks()) {
                    State.Locks.push_back(lock);
                }
                for (const auto& lock : msg->Record.GetBrokenTxLocks()) {
                    State.Locks.push_back(lock);
                }
                return msg;
            }

            TString NextString(TDuration simTimeout = TDuration::Max()) {
                auto msg = NextResult(simTimeout);
                if (!msg) {
                    return "<timeout>";
                }
                auto status = msg->Record.GetStatus().GetCode();
                if (status != Ydb::StatusIds::SUCCESS) {
                    return TStringBuilder() << "ERROR: " << status;
                }
                TString res = FormatReadResult(msg.get());
                if (msg->Record.GetFinished()) {
                    res += "<end>";
                }
                return res;
            }

            TString AllString() {
                TString res;
                for (;;) {
                    auto msg = NextResult();
                    auto status = msg->Record.GetStatus().GetCode();
                    if (status != Ydb::StatusIds::SUCCESS) {
                        res += TStringBuilder() << "ERROR: " << status;
                        break;
                    }
                    res += FormatReadResult(msg.get());
                    if (msg->Record.GetFinished()) {
                        break;
                    }
                    SendAck();
                }
                return res;
            }
        };

        TReadPromise SendReadKey(const TTableId& tableId, ui64 shardId, i32 key) {
            if (!Snapshot) {
                Snapshot = AcquireSnapshot(Runtime, Sender);
            }

            TActorId sender = Runtime.AllocateEdgeActor();
            ui32 nodeIndex = sender.NodeId() - Runtime.GetNodeId(0);

            auto* req = new TEvDataShard::TEvRead();
            req->Record.SetReadId(0);
            req->Record.MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
            req->Record.MutableTableId()->SetTableId(tableId.PathId.LocalPathId);
            req->Record.MutableTableId()->SetSchemaVersion(tableId.SchemaVersion);
            req->Record.MutableSnapshot()->SetStep(Snapshot->Step);
            req->Record.MutableSnapshot()->SetTxId(Snapshot->TxId);
            req->Record.SetLockTxId(LockTxId);
            req->Record.SetLockNodeId(LockNodeId);
            req->Record.SetLockMode(NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
            req->Record.AddColumns(1);
            req->Record.AddColumns(2);
            req->Record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

            TVector<TCell> cells;
            cells.emplace_back(TCell::Make(key));
            req->Keys.emplace_back(cells);

            Runtime.SendToPipe(shardId, sender, req, nodeIndex);
            return { *this, sender };
        }

        TString ReadKey(const TTableId& tableId, ui64 shardId, i32 key) {
            auto promise = SendReadKey(tableId, shardId, key);
            return promise.AllString();
        }

        TReadPromise SendReadRange(const TTableId& tableId, ui64 shardId, i32 minKey, i32 maxKey, ui64 maxRowsQuota = Max<ui64>()) {
            if (!Snapshot) {
                Snapshot = AcquireSnapshot(Runtime, Sender);
            }

            TActorId sender = Runtime.AllocateEdgeActor();
            ui32 nodeIndex = sender.NodeId() - Runtime.GetNodeId(0);

            auto* req = new TEvDataShard::TEvRead();
            req->Record.SetReadId(0);
            req->Record.MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
            req->Record.MutableTableId()->SetTableId(tableId.PathId.LocalPathId);
            req->Record.MutableTableId()->SetSchemaVersion(tableId.SchemaVersion);
            req->Record.MutableSnapshot()->SetStep(Snapshot->Step);
            req->Record.MutableSnapshot()->SetTxId(Snapshot->TxId);
            req->Record.SetLockTxId(LockTxId);
            req->Record.SetLockNodeId(LockNodeId);
            req->Record.SetLockMode(NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
            req->Record.AddColumns(1);
            req->Record.AddColumns(2);
            req->Record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            req->Record.SetMaxRowsInResult(1);
            req->Record.SetMaxRows(maxRowsQuota);

            TVector<TCell> minCells, maxCells;
            minCells.emplace_back(TCell::Make(minKey));
            maxCells.emplace_back(TCell::Make(maxKey));
            req->Ranges.emplace_back(minCells, true, maxCells, true);

            Runtime.SendToPipe(shardId, sender, req, nodeIndex);
            return { *this, sender };
        }

        TString ReadRange(const TTableId& tableId, ui64 shardId, i32 minKey, i32 maxKey) {
            auto promise = SendReadRange(tableId, shardId, minKey, maxKey);
            return promise.AllString();
        }

        bool HasLockConflicts(ui64 shardId) const {
            const NKikimrDataEvents::TLock* prev = nullptr;
            for (auto it = Locks.begin(); it != Locks.end(); ++it) {
                if (it->GetDataShard() == shardId) {
                    if (prev) {
                        if (prev->GetGeneration() != it->GetGeneration()) {
                            return true;
                        }
                        if (prev->GetCounter() != it->GetCounter()) {
                            return true;
                        }
                    }
                    prev = &*it;
                }
            }
            return false;
        }

        const NKikimrDataEvents::TLock* FindLastLock(ui64 shardId) const {
            for (auto it = Locks.rbegin(); it != Locks.rend(); ++it) {
                if (it->GetDataShard() == shardId) {
                    return &*it;
                }
            }
            return nullptr;
        }

        struct TWritePromise {
            TTransactionState& State;
            TActorId Sender;

            std::unique_ptr<NEvents::TDataEvents::TEvWriteResult> NextResult(TDuration simTimeout = TDuration::Max()) {
                auto ev = State.Runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(Sender, simTimeout);
                if (!ev) {
                    return nullptr;
                }
                std::unique_ptr<NEvents::TDataEvents::TEvWriteResult> msg(ev->Release().Release());
                for (const auto& lock : msg->Record.GetTxLocks()) {
                    State.Locks.push_back(lock);
                }
                return msg;
            }

            TString NextString(TDuration simTimeout = TDuration::Max()) {
                auto msg = NextResult(simTimeout);
                if (!msg) {
                    return "<timeout>";
                }
                auto status = msg->Record.GetStatus();
                if (status != NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
                    return TStringBuilder() << "ERROR: " << status;
                }
                return "OK";
            }
        };

        template<class... TOps>
        TWritePromise SendWrite(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
            auto sender = Runtime.AllocateEdgeActor();
            ui32 nodeIndex = sender.NodeId() - Runtime.GetNodeId(0);

            auto* req = new NEvents::TDataEvents::TEvWrite(0, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            req->Record.SetLockTxId(LockTxId);
            req->Record.SetLockNodeId(LockNodeId);
            req->Record.SetLockMode(NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
            if (Snapshot) {
                req->Record.MutableMvccSnapshot()->SetStep(Snapshot->Step);
                req->Record.MutableMvccSnapshot()->SetTxId(Snapshot->TxId);
            }

            (..., ops.ApplyTo(tableId, req));

            Runtime.SendToPipe(shardId, sender, req, nodeIndex);
            return { *this, sender };
        }

        template<class... TOps>
        TString Write(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
            auto promise = SendWrite(tableId, shardId, std::forward<TOps>(ops)...);
            return promise.NextString();
        }

        template<class... TOps>
        TWritePromise SendWriteCommit(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
            auto sender = Runtime.AllocateEdgeActor();
            ui32 nodeIndex = sender.NodeId() - Runtime.GetNodeId(0);

            auto* req = new NEvents::TDataEvents::TEvWrite(0, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            req->Record.SetLockMode(NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
            if (Snapshot) {
                req->Record.MutableMvccSnapshot()->SetStep(Snapshot->Step);
                req->Record.MutableMvccSnapshot()->SetTxId(Snapshot->TxId);
            }

            // Try to find the last known lock state
            if (const auto* pLock = FindLastLock(shardId)) {
                req->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
                req->Record.MutableLocks()->AddSendingShards(shardId);
                req->Record.MutableLocks()->AddReceivingShards(shardId);
                *req->Record.MutableLocks()->AddLocks() = *pLock;
            }

            (..., ops.ApplyTo(tableId, req));

            Runtime.SendToPipe(shardId, sender, req, nodeIndex);
            return { *this, sender };
        }

        template<class... TOps>
        TString WriteCommit(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
            auto promise = SendWriteCommit(tableId, shardId, std::forward<TOps>(ops)...);
            return promise.NextString();
        }

        void InitCommit(std::vector<ui64> participants) {
            CommitTxId = AllocateTxId(Runtime, Sender);
            Participants = std::move(participants);
        }

        template<class... TOps>
        TWritePromise SendPrepareCommit(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
            auto sender = Runtime.AllocateEdgeActor();
            ui32 nodeIndex = sender.NodeId() - Runtime.GetNodeId(0);

            auto* req = new NEvents::TDataEvents::TEvWrite(CommitTxId, NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE);
            req->Record.SetLockMode(NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
            if (Snapshot) {
                req->Record.MutableMvccSnapshot()->SetStep(Snapshot->Step);
                req->Record.MutableMvccSnapshot()->SetTxId(Snapshot->TxId);
            }

            req->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            for (ui64 participant : Participants) {
                req->Record.MutableLocks()->AddSendingShards(participant);
                req->Record.MutableLocks()->AddReceivingShards(participant);
            }
            if (const auto* pLock = FindLastLock(shardId)) {
                *req->Record.MutableLocks()->AddLocks() = *pLock;
            }

            (..., ops.ApplyTo(tableId, req));

            Runtime.SendToPipe(shardId, sender, req, nodeIndex);
            return { *this, sender };
        }

        template<class... TOps>
        TWritePromise PrepareCommit(const TTableId& tableId, ui64 shardId, TOps&&... ops) {
            auto promise = SendPrepareCommit(tableId, shardId, std::forward<TOps>(ops)...);
            auto msg = promise.NextResult();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
            MinStep = Max(MinStep, msg->Record.GetMinStep());
            MaxStep = Min(MaxStep, msg->Record.GetMaxStep());
            for (ui64 coordinator : msg->Record.GetDomainCoordinators()) {
                Coordinator = coordinator;
            }
            return promise;
        }

        void SendPlan() {
            UNIT_ASSERT(Coordinator != 0);
            UNIT_ASSERT(MinStep <= MaxStep);
            UNIT_ASSERT(!Participants.empty());
            SendProposeToCoordinator(
                Runtime, Sender, Participants, {
                    .TxId = CommitTxId,
                    .Coordinator = Coordinator,
                    .MinStep = MinStep,
                    .MaxStep = MaxStep,
                    .Volatile = true,
                });
        }

    public:
        TTestActorRuntime& Runtime;
        TActorId Sender;
        ui64 LockTxId = 0;
        ui32 LockNodeId = 0;
        NLongTxService::TLockHandle LockHandle;
        std::optional<TRowVersion> Snapshot;
        ui64 Counter = 0;
        std::vector<NKikimrDataEvents::TLock> Locks;
        std::vector<ui64> Participants;
        ui64 CommitTxId = 0;
        ui64 MinStep = 0;
        ui64 MaxStep = Max<ui64>();
        ui64 Coordinator = 0;
    };

    Y_UNIT_TEST(ReadWriteNoLocksNoConflict) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Write to key 1, this shouldn't break anything
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 3001);
        )");

        // Commit along with writing to key 2, which has no conflicts
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.WriteCommit(tableId, shards.at(0), TOperation::Upsert(2, 2001)),
            "OK");

        // We should observe a successful commit to both keys
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 3001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 2001 } }");
    }

    Y_UNIT_TEST(ReadWriteConflictOnUncommittedWrite) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Write to key 2, which would be with version above the snapshot
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (2, 3001);
        )");

        // Perform uncommitted write to key 2. Since this key is modifed
        // between the snapshot and commit timestamps it must abort.
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TOperation::Upsert(2, 2001)),
            "ERROR: STATUS_LOCKS_BROKEN");
    }

    Y_UNIT_TEST(ReadWriteConflictOnCommitWithEffects) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Write to key 2, which would be with version above the snapshot
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (2, 3001);
        )");

        // Commit along with writing a key 2. Since this key is modifed
        // between the snapshot and commit timestamps it must abort.
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.WriteCommit(tableId, shards.at(0), TOperation::Upsert(2, 2001)),
            "ERROR: STATUS_LOCKS_BROKEN");

        // We should not observe a commit by tx1.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 3001 } }");
    }

    Y_UNIT_TEST(ReadWriteConflictOnCommitAfterAnotherCommit) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        TTransactionState tx2(runtime);

        // Make an uncommitted write in tx2 to key 2
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.Write(tableId, shards.at(0), TOperation::Upsert(2, 2001)),
            "OK");
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.back().GetHasWrites(), true);

        // Make an uncommitted write in tx1 to key 2
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TOperation::Upsert(2, 3001)),
            "OK");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.back().GetHasWrites(), true);

        // Commit tx2, it must succeed
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.WriteCommit(tableId, shards.at(0)),
            "OK");

        // Try committing tx1, it must be broken now
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.WriteCommit(tableId, shards.at(0)),
            "ERROR: STATUS_LOCKS_BROKEN");

        // We should not observe a commit by tx1.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 2001 } }");
    }

    Y_UNIT_TEST(ReadWriteUpsertTwiceThenCommit) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Make an uncommitted write in tx1 to key 2
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TOperation::Upsert(2, 2001)),
            "OK");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.back().GetHasWrites(), true);

        // Make another uncommitted write to key 2
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TOperation::Upsert(2, 2002)),
            "OK");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.back().GetHasWrites(), true);

        // Commit must succeed
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.WriteCommit(tableId, shards.at(0)),
            "OK");

        // We should observe the final effect
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 2002 } }");
    }

    Y_UNIT_TEST(ReadWriteUpsertAgainOnCommitNoConflict) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Make an uncommitted write in tx1 to key 2
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TOperation::Upsert(2, 2001)),
            "OK");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.back().GetHasWrites(), true);

        // Commit while writing to key 2 again
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.WriteCommit(tableId, shards.at(0), TOperation::Upsert(2, 2002)),
            "OK");

        // We should observe the final effect
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 2002 } }");
    }

    Y_UNIT_TEST(ReadWriteUpsertAgainTwiceOnCommitNoConflict) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Make an uncommitted write in tx1 to key 2
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TOperation::Upsert(2, 2001)),
            "OK");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.back().GetHasWrites(), true);

        // Commit while writing to key 2 again twice
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.WriteCommit(tableId, shards.at(0), TOperation::Upsert({ { 2, 2002 }, { 2, 2003 } })),
            "OK");

        // We should observe the final effect
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 2003 } }");
    }

    Y_UNIT_TEST(ReadWriteUncommittedUpsertBlockedByVolatileConflict) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Start blocking readsets (non expectations)
        TBlockEvents<TEvTxProcessing::TEvReadSet> blockedReadSets(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            return !(msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET);
        });

        TTransactionState tx2(runtime);
        tx2.InitCommit(shards);
        auto write1 = tx2.PrepareCommit(tableId, shards.at(0), TOperation::Upsert(2, 2001));
        auto write2 = tx2.PrepareCommit(tableId, shards.at(1), TOperation::Upsert(11, 2002));
        tx2.SendPlan();

        runtime.WaitFor("blocked readsets", [&]{ return blockedReadSets.size() >= 2; });
        blockedReadSets.Stop();

        auto write3 = tx1.SendWrite(tableId, shards.at(0), TOperation::Upsert(2, 3001));

        // Write of (2, 3001) should be blocked until tx2 is resolved
        UNIT_ASSERT_VALUES_EQUAL(write3.NextString(TDuration::Seconds(1)), "<timeout>");

        blockedReadSets.Unblock();

        Cerr << "... waiting for OK 1" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write1.NextString(), "OK");
        Cerr << "... waiting for OK 2" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write2.NextString(), "OK");
        Cerr << "... waiting for LOCKS BROKEN 3" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write3.NextString(), "ERROR: STATUS_LOCKS_BROKEN");

        // We should observe an effect from tx2
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 2001 } }, "
            "{ items { int32_value: 11 } items { int32_value: 2002 } }");
    }

    Y_UNIT_TEST(ReadWriteUncommittedUpsertBlockedByVolatileNoConflict) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Start blocking readsets (non expectations)
        TBlockEvents<TEvTxProcessing::TEvReadSet> blockedReadSets(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            return !(msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET);
        });

        TTransactionState tx2(runtime);
        tx2.InitCommit(shards);
        auto write1 = tx2.PrepareCommit(tableId, shards.at(0), TOperation::Upsert(2, 2001));
        auto write2 = tx2.PrepareCommit(tableId, shards.at(1), TOperation::Upsert(11, 2002));
        tx2.SendPlan();

        runtime.WaitFor("blocked readsets", [&]{ return blockedReadSets.size() >= 2; });
        blockedReadSets.Stop();

        auto write3 = tx1.SendWrite(tableId, shards.at(0), TOperation::Upsert(2, 3001));

        // Write of (2, 3001) should be blocked until tx2 is resolved
        UNIT_ASSERT_VALUES_EQUAL(write3.NextString(TDuration::Seconds(1)), "<timeout>");

        // Rewrite readsets into DECISION_UNKNOWN, which would force tx2 to abort
        for (auto& ev : blockedReadSets) {
            auto* msg = ev->Get();
            msg->Record.ClearReadSet();
        }
        blockedReadSets.Unblock();

        Cerr << "... waiting for ABORTED 1" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write1.NextString(), "ERROR: STATUS_ABORTED");
        Cerr << "... waiting for ABORTED 2" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write2.NextString(), "ERROR: STATUS_ABORTED");
        Cerr << "... waiting for OK 3" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write3.NextString(), "OK");

        // We should not observe effects from either tx
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }");

        // Commit tx1
        UNIT_ASSERT_VALUES_EQUAL(tx1.WriteCommit(tableId, shards.at(0)), "OK");

        // We should observe effects from tx1
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 3001 } }");
    }

    Y_UNIT_TEST(ReadWriteUpsertOnCommitBlockedByVolatileConflict) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Start blocking readsets (non expectations)
        TBlockEvents<TEvTxProcessing::TEvReadSet> blockedReadSets(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            return !(msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET);
        });

        TTransactionState tx2(runtime);
        tx2.InitCommit(shards);
        auto write1 = tx2.PrepareCommit(tableId, shards.at(0), TOperation::Upsert(2, 2001));
        auto write2 = tx2.PrepareCommit(tableId, shards.at(1), TOperation::Upsert(11, 2002));
        tx2.SendPlan();

        runtime.WaitFor("blocked readsets", [&]{ return blockedReadSets.size() >= 2; });
        blockedReadSets.Stop();

        auto write3 = tx1.SendWriteCommit(tableId, shards.at(0), TOperation::Upsert(2, 3001));

        // Commit of (2, 3001) should be blocked until tx2 is resolved
        UNIT_ASSERT_VALUES_EQUAL(write3.NextString(TDuration::Seconds(1)), "<timeout>");

        // Rewrite readsets into DECISION_UNKNOWN, which would force tx2 to abort
        blockedReadSets.Unblock();

        Cerr << "... waiting for OK 1" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write1.NextString(), "OK");
        Cerr << "... waiting for OK 2" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write2.NextString(), "OK");
        Cerr << "... waiting for LOCKS_BROKEN 3" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write3.NextString(), "ERROR: STATUS_LOCKS_BROKEN");

        // We should observe effects from tx2, but not from tx1
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 2001 } }, "
            "{ items { int32_value: 11 } items { int32_value: 2002 } }");
    }

    Y_UNIT_TEST(ReadWriteUpsertOnCommitBlockedByVolatileNoConflict) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Start blocking readsets (non expectations)
        TBlockEvents<TEvTxProcessing::TEvReadSet> blockedReadSets(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            return !(msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET);
        });

        TTransactionState tx2(runtime);
        tx2.InitCommit(shards);
        auto write1 = tx2.PrepareCommit(tableId, shards.at(0), TOperation::Upsert(2, 2001));
        auto write2 = tx2.PrepareCommit(tableId, shards.at(1), TOperation::Upsert(11, 2002));
        tx2.SendPlan();

        runtime.WaitFor("blocked readsets", [&]{ return blockedReadSets.size() >= 2; });
        blockedReadSets.Stop();

        auto write3 = tx1.SendWriteCommit(tableId, shards.at(0), TOperation::Upsert(2, 3001));

        // Commit of (2, 3001) should be blocked until tx2 is resolved
        UNIT_ASSERT_VALUES_EQUAL(write3.NextString(TDuration::Seconds(1)), "<timeout>");

        // Rewrite readsets into DECISION_UNKNOWN, which would force tx2 to abort
        for (auto& ev : blockedReadSets) {
            auto* msg = ev->Get();
            msg->Record.ClearReadSet();
        }
        blockedReadSets.Unblock();

        Cerr << "... waiting for ABORTED 1" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write1.NextString(), "ERROR: STATUS_ABORTED");
        Cerr << "... waiting for ABORTED 2" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write2.NextString(), "ERROR: STATUS_ABORTED");
        Cerr << "... waiting for OK 3" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(write3.NextString(), "OK");

        // We should observe an effect from tx1, but not from tx2
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 3001 } }");
    }

    Y_UNIT_TEST(ReadWriteUncommittedUpsertNotBlockedByOlderVolatile) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        // Start blocking readsets (non expectations)
        TBlockEvents<TEvTxProcessing::TEvReadSet> blockedReadSets(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            return !(msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET);
        });

        TTransactionState tx1(runtime);
        tx1.InitCommit(shards);
        auto write1 = tx1.PrepareCommit(tableId, shards.at(0), TOperation::Upsert(2, 2001));
        auto write2 = tx1.PrepareCommit(tableId, shards.at(1), TOperation::Upsert(11, 2002));
        tx1.SendPlan();

        runtime.WaitFor("blocked readsets", [&]{ return blockedReadSets.size() >= 2; });
        blockedReadSets.Stop();

        TTransactionState tx2(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 0u);

        // Write to key 2, this should not block on earlier volatile write to key 2
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.Write(tableId, shards.at(0), TOperation::Upsert(2, 3001)),
            "OK");

        // We should be able to fully commit the transaction as well
        UNIT_ASSERT_VALUES_EQUAL(tx2.WriteCommit(tableId, shards.at(0)), "OK");

        blockedReadSets.Unblock();

        UNIT_ASSERT_VALUES_EQUAL(write1.NextString(), "OK");
        UNIT_ASSERT_VALUES_EQUAL(write2.NextString(), "OK");

        // We should observe all effects in the correct order
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 3001 } }, "
            "{ items { int32_value: 11 } items { int32_value: 2002 } }");
    }

    Y_UNIT_TEST(ReadWriteUpsertOnCommitNotBlockedByOlderVolatile) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        // Start blocking readsets (non expectations)
        TBlockEvents<TEvTxProcessing::TEvReadSet> blockedReadSets(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            return !(msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET);
        });

        TTransactionState tx1(runtime);
        tx1.InitCommit(shards);
        auto write1 = tx1.PrepareCommit(tableId, shards.at(0), TOperation::Upsert(2, 2001));
        auto write2 = tx1.PrepareCommit(tableId, shards.at(1), TOperation::Upsert(11, 2002));
        tx1.SendPlan();

        runtime.WaitFor("blocked readsets", [&]{ return blockedReadSets.size() >= 2; });
        blockedReadSets.Stop();

        TTransactionState tx2(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 0u);

        // Commit to key 2, this should not block on earlier volatile write to key 2
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.WriteCommit(tableId, shards.at(0), TOperation::Upsert(2, 3001)),
            "OK");

        blockedReadSets.Unblock();

        UNIT_ASSERT_VALUES_EQUAL(write1.NextString(), "OK");
        UNIT_ASSERT_VALUES_EQUAL(write2.NextString(), "OK");

        // We should observe all effects in the correct order
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 3001 } }, "
            "{ items { int32_value: 11 } items { int32_value: 2002 } }");
    }

    Y_UNIT_TEST(ReadWriteUncommittedInsertBlockedByOlderVolatile) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        // Start blocking readsets (non expectations)
        TBlockEvents<TEvTxProcessing::TEvReadSet> blockedReadSets(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            return !(msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET);
        });

        TTransactionState tx1(runtime);
        tx1.InitCommit(shards);
        auto write1 = tx1.PrepareCommit(tableId, shards.at(0), TOperation::Upsert(2, 2001));
        auto write2 = tx1.PrepareCommit(tableId, shards.at(1), TOperation::Upsert(11, 2002));
        tx1.SendPlan();

        runtime.WaitFor("blocked readsets", [&]{ return blockedReadSets.size() >= 2; });
        blockedReadSets.Stop();

        TTransactionState tx2(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 0u);

        // Insert to key 2, this should block on earlier volatile write to key 2
        auto write3 = tx2.SendWrite(tableId, shards.at(0), TOperation::Insert(2, 3001));
        UNIT_ASSERT_VALUES_EQUAL(write3.NextString(TDuration::Seconds(1)), "<timeout>");

        blockedReadSets.Unblock();

        UNIT_ASSERT_VALUES_EQUAL(write1.NextString(), "OK");
        UNIT_ASSERT_VALUES_EQUAL(write2.NextString(), "OK");

        // Write should now fail with unique constraint violation
        UNIT_ASSERT_VALUES_EQUAL(write3.NextString(), "ERROR: STATUS_CONSTRAINT_VIOLATION");

        // We should observe effects from tx1
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 2001 } }, "
            "{ items { int32_value: 11 } items { int32_value: 2002 } }");
    }

    Y_UNIT_TEST(ReadWriteUncommittedInsertDuplicateKeyAtSnapshot) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Erase key 1 in another transaction
        ExecSQL(server, sender, R"(
            DELETE FROM `/Root/table` WHERE key = 1;
        )");

        // Try inserting key 1, it should fail with constraint violation (not locks aborted)
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TOperation::Insert(1, 2001)),
            "ERROR: STATUS_CONSTRAINT_VIOLATION");
    }

    Y_UNIT_TEST(ReadWriteUncommittedInsertMissingKeyAtSnapshot) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        TTransactionState tx1(runtime);

        // Start transaction by reading key 1, this should not acquire locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(tableId, shards.at(0), 1),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Add key 2 in another transaction
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (2, 2001);
        )");

        // Try inserting key 2, it should fail with locks broken (no duplicate key at snapshot)
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TOperation::Insert(2, 3001)),
            "ERROR: STATUS_LOCKS_BROKEN");
    }

    Y_UNIT_TEST(ReadWriteObserveOwnChanges) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        TTransactionState tx1(runtime);

        // Read range, we should observe initial values
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadRange(tableId, shards.at(0), 1, 10),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 0u);

        // Add key 2 as an uncommitted change
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TOperation::Upsert(2, 2001)),
            "OK");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1u);

        // Read range again: we should observe our uncommitted changes, but not acquire more locks
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadRange(tableId, shards.at(0), 1, 10),
            "1, 1001\n"
            "2, 2001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1u);

        // Write some new data to the table
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (4, 3001);
        )");

        // Read range again: we should neither observe new data, break locks or fail
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadRange(tableId, shards.at(0), 1, 10),
            "1, 1001\n"
            "2, 2001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1u);

        // We should be able to commit
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.WriteCommit(tableId, shards.at(0)),
            "OK");
    }

    Y_UNIT_TEST(ReadWriteCommitConflictThenRead) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        TTransactionState tx1(runtime);
        TTransactionState tx2(runtime);

        // Make an uncommitted write to key 2 in tx1
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TOperation::Upsert(2, 2001)),
            "OK");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1u);

        // Make a read in tx2 (acquires snapshot, not observing tx1)
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.ReadRange(tableId, shards.at(0), 1, 10),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 0u);

        // Make an uncommitted write to key 2 in tx2
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.Write(tableId, shards.at(0), TOperation::Upsert(2, 3001)),
            "OK");
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 1u);

        // Commit tx1 which makes it impossible for tx2 to commit
        UNIT_ASSERT_VALUES_EQUAL(tx1.WriteCommit(tableId, shards.at(0)), "OK");

        // Trying to read in tx2 should immediately abort
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.ReadRange(tableId, shards.at(0), 1, 10),
            "ERROR: ABORTED");

        // Note: currently read iterator doesn't report broken locks in this case
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 1u);
    }

    Y_UNIT_TEST(ReadWriteCommitConflictWhileReading) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        TTransactionState tx1(runtime);
        TTransactionState tx2(runtime);

        // Make an uncommitted write to key 2 in tx1
        UNIT_ASSERT_VALUES_EQUAL(
            tx1.Write(tableId, shards.at(0), TOperation::Upsert(2, 2001)),
            "OK");
        UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1u);

        // Make a read in tx2 (acquires snapshot, not observing tx1)
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.ReadRange(tableId, shards.at(0), 1, 10),
            "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 0u);

        // Make an uncommitted write to key 2 in tx2
        UNIT_ASSERT_VALUES_EQUAL(
            tx2.Write(tableId, shards.at(0), TOperation::Upsert(2, 3001)),
            "OK");
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 1u);

        // Start reading in tx2, use quota to block it after the first row
        auto read = tx2.SendReadRange(tableId, shards.at(0), 1, 10, /* maxRowsQuota */ 1);
        UNIT_ASSERT_VALUES_EQUAL(read.NextString(TDuration::Seconds(1)), "1, 1001\n");
        UNIT_ASSERT_VALUES_EQUAL(read.NextString(TDuration::Seconds(1)), "<timeout>");
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 1u);

        // Commit tx1 which makes it impossible for tx2 to commit
        UNIT_ASSERT_VALUES_EQUAL(tx1.WriteCommit(tableId, shards.at(0)), "OK");

        // When continue reading it should abort immediately
        read.SendAck(1);
        UNIT_ASSERT_VALUES_EQUAL(read.AllString(), "ERROR: ABORTED");

        // Note: currently read iterator reports broken locks discovered in subsequent chunks
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.back().GetCounter(), ui64(TSysTables::TLocksTable::TLock::ErrorBroken));
    }

} // Y_UNIT_TEST_SUITE(DataShardSnapshotIsolation)

} // namespace NKikimr
