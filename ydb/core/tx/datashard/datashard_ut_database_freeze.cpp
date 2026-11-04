#include "datashard_impl.h"
#include "change_exchange_impl.h"
#include "datashard_ut_common_kqp.h"

#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/change_exchange/change_sender.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tablet_flat/flat_boot_cookie.h>
#include <ydb/core/tablet_flat/flat_redo_player.h>
#include <ydb/core/tablet_flat/flat_sausage_slicer.h>
#include <ydb/core/tx/datashard/ut_common/database_freeze.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <google/protobuf/descriptor.h>

#include <util/string/cast.h>

namespace NKikimr {

using namespace NDataShard;
using namespace NDataShard::NTest;
using namespace NDataShard::NKqpHelpers;
using namespace Tests;

namespace {

using TStage = NKikimrTxDataShard::TDatabaseFreezeState;

struct TFreezeEnv {
    TPortManager Ports;
    TServer::TPtr Server;
    TTestActorRuntime& Runtime;
    TActorId Sender;
    TVector<ui64> Shards;
    TTableId TableId;

    explicit TFreezeEnv(TShardedTableOptions options = {}, const NKikimrPQ::TPQConfig& pqConfig = {})
        : Server(new TServer(TServerSettings(Ports.GetPort(2134), {}, pqConfig)
            .SetDomainName("Root").SetUseRealThreads(false).SetEnableChangefeedInitialScan(true)))
        , Runtime(*Server->GetRuntime())
        , Sender(Runtime.AllocateEdgeActor())
    {
        InitRoot(Server, Sender);
        std::tie(Shards, TableId) = CreateShardedTable(Server, Sender, "/Root", "Table", options);
    }

    TPathId Domain() const { return TPathId(TableId.PathId.OwnerId, 1); }

    void Freeze(ui64 epoch = 1) {
        FreezeDatabaseShard(Runtime, Shards.at(0), TableId.PathId.OwnerId, Domain(), epoch);
    }

    void Thaw(ui64 epoch = 2) {
        WaitDatabaseFreezeStage(Runtime, Shards.at(0), TableId.PathId.OwnerId, Domain(), epoch, TStage::OPEN);
    }

    auto Write(ui32 key, ui32 value, NKikimrDataEvents::TEvWrite::ETxMode mode = NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE) {
        auto request = MakeWriteRequest(1000 + key, mode,
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, TableId,
            std::vector<ui32>{1, 2}, std::vector<TCell>{TCell::Make(key), TCell::Make(value)});
        Runtime.SendToPipe(Shards.at(0), Sender, request.release(), 0, GetPipeConfigWithRetries());
        return Runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(Sender)->Get()->Record;
    }

    NKikimrTxDataShard::TEvUploadRowsRequest UploadRequest(ui32 key, ui32 value) const {
        NKikimrTxDataShard::TEvUploadRowsRequest request;
        request.SetTableId(TableId.PathId.LocalPathId);
        request.SetSchemaVersion(TableId.SchemaVersion);
        request.MutableRowScheme()->AddKeyColumnIds(1);
        request.MutableRowScheme()->AddValueColumnIds(2);
        auto* row = request.AddRows();
        row->SetKeyColumns(TSerializedCellVec::Serialize({TCell::Make(key)}));
        row->SetValueColumns(TSerializedCellVec::Serialize({TCell::Make(value)}));
        return request;
    }

    auto Upload(ui32 key, ui32 value) {
        auto request = MakeHolder<TEvDataShard::TEvUploadRowsRequest>();
        request->Record.CopyFrom(UploadRequest(key, value));
        Runtime.SendToPipe(Shards.at(0), Sender, request.Release(), 0, GetPipeConfigWithRetries());
        return Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvUploadRowsResponse>(Sender)->Get()->Record;
    }

    auto Erase(ui32 key) {
        auto request = MakeHolder<TEvDataShard::TEvEraseRowsRequest>();
        request->Record.SetTableId(TableId.PathId.LocalPathId);
        request->Record.SetSchemaVersion(TableId.SchemaVersion);
        request->Record.AddKeyColumnIds(1);
        request->Record.AddKeyColumns(TSerializedCellVec::Serialize({TCell::Make(key)}));
        Runtime.SendToPipe(Shards.at(0), Sender, request.Release(), 0, GetPipeConfigWithRetries());
        return Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvEraseRowsResponse>(Sender)->Get()->Record;
    }

    void LegacyFreeze(bool freeze) {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
        tx.SetWorkingDir("/Root");
        tx.MutableAlterTable()->SetName("Table");
        tx.MutableAlterTable()->MutablePartitionConfig()->SetFreezeState(freeze
            ? NKikimrSchemeOp::EFreezeState::Freeze : NKikimrSchemeOp::EFreezeState::Unfreeze);
        Runtime.Send(new IEventHandle(MakeTxProxyID(), Sender, request.Release()));
        const auto response = Runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(Sender);
        WaitTxNotification(Server, Sender, response->Get()->Record.GetTxId());
        TableId = ResolveTableId(Server, Sender, "/Root/Table");
    }

    template<class TRequest, class TResponse>
    void CheckBackgroundScanRejected() {
        auto request = MakeHolder<TRequest>();
        request->Record.SetId(1);
        request->Record.SetTabletId(Shards.at(0));
        if constexpr (requires { request->Record.AddIndexColumns("value"); }) {
            request->Record.AddIndexColumns("value");
        }
        if constexpr (requires { request->Record.SetOwnerId(ui64{}); }) {
            request->Record.SetOwnerId(TableId.PathId.OwnerId);
            request->Record.SetPathId(TableId.PathId.LocalPathId);
        } else {
            TableId.PathId.ToProto(request->Record.MutablePathId());
        }
        Runtime.SendToPipe(Shards.at(0), Sender, request.Release(), 0, GetPipeConfigWithRetries());
        auto response = Runtime.GrabEdgeEventRethrow<TResponse>(Sender);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
        NYql::TIssues issues;
        NYql::IssuesFromMessage(response->Get()->Record.GetIssues(), issues);
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "not ready for requests");
    }
};

NKikimrPQ::TPQConfig CdcConfig() {
    NKikimrPQ::TPQConfig config;
    config.SetEnabled(true);
    config.SetEnableProtoSourceIdInfo(true);
    config.SetTopicsAreFirstClassCitizen(true);
    config.SetMaxReadCookies(10);
    config.AddClientServiceType()->SetName("data-streams");
    config.SetCheckACL(false);
    config.SetRequireCredentialsInNewProtocol(false);
    config.MutableQuotingConfig()->SetEnableQuoting(false);
    return config;
}

struct TCdcFreezeEnv : TFreezeEnv {
    explicit TCdcFreezeEnv(TShardedTableOptions options = {})
        : TFreezeEnv(std::move(options), CdcConfig())
    {}

    void AddStream(bool resolved = false, bool initialScan = false, ui32 partitions = 1) {
        TShardedTableOptions::TCdcStream stream{
            .Name = "Stream",
            .Mode = NKikimrSchemeOp::ECdcStreamModeUpdate,
            .Format = NKikimrSchemeOp::ECdcStreamFormatJson,
            .TopicPartitions = partitions,
        };
        if (resolved) {
            stream.VirtualTimestamps = true;
            // Keep periodic resolved records out of the final-ACK assertions.
            stream.ResolvedTimestamps = TDuration::Hours(1);
        }
        if (initialScan) {
            stream.InitialState = NKikimrSchemeOp::ECdcStreamStateScan;
        }
        WaitTxNotification(Server, Sender, AsyncAlterAddStream(Server, "/Root", "Table", stream));
        TableId = ResolveTableId(Server, Sender, "/Root/Table");
    }

    auto Stage(TStage::EStage stage, ui64 epoch = 1) {
        return SendDatabaseFreeze(Runtime, Shards.at(0), TableId.PathId.OwnerId, Domain(), epoch, stage);
    }

    void WaitStage(TStage::EStage stage, ui64 epoch = 1) {
        for (const auto shard : Shards) {
            WaitDatabaseFreezeStage(Runtime, shard, TableId.PathId.OwnerId, Domain(), epoch, stage);
        }
    }

    void Freeze(ui64 epoch = 1) {
        for (const auto stage : {TStage::FENCE_WRITES, TStage::DRAIN_INDEXES, TStage::DRAIN_CDC}) {
            WaitStage(stage, epoch);
        }
    }

    void Thaw(ui64 epoch = 2) {
        WaitStage(TStage::OPEN, epoch);
    }
};

// Inspect actual durable updates without adding instrumentation to DataShard.
// Schema ids: Sys = 1, Sys::Bytes = 2, Sys_DatabaseFreeze = 49,
// ChangeRecords = 17.
struct TFreezeRemovalLog {
    TMap<ui64, size_t> DeliveryLostWrites;
    size_t RemovedRecords = 0;

    bool NeedIn(ui32 table) const {
        return table == 1 || table == 17;
    }

    void DoUpdate(ui32 table, NTable::ERowOp op, TArrayRef<const TRawTypeValue> keys,
            TArrayRef<const NTable::TUpdateOp> updates, TRowVersion) {
        if (table == 17 && op == NTable::ERowOp::Erase) {
            ++RemovedRecords;
        } else if (table == 1 && keys.size() == 1
                && TCell(static_cast<const char*>(keys[0].Data()), keys[0].Size()).AsValue<ui64>() == 49) {
            for (const auto& update : updates) {
                if (update.Tag == 2) {
                    TStage state;
                    UNIT_ASSERT(state.ParseFromArray(update.Value.Data(), update.Value.Size()));
                    if (state.GetDeliveryLost()) {
                        ++DeliveryLostWrites[state.GetEpoch()];
                    }
                }
            }
        }
    }

    template<class... TArgs> void DoBegin(TArgs&&...) {}
    void DoAnnex(TArrayRef<const TStdPad<NPageCollection::TGlobId>>) {}
    template<class... TArgs> void DoFlush(TArgs&&...) {}
    template<class... TArgs> void DoUpdateTx(TArgs&&...) {}
    template<class... TArgs> void DoCommitTx(TArgs&&...) {}
    template<class... TArgs> void DoRemoveTx(TArgs&&...) {}
    template<class... TArgs> void DoLockRowTx(TArgs&&...) {}

    void Observe(const TEvTablet::TEvCommit& commit) {
        if (commit.IsSnapshot) {
            return;
        }
        TString redo = commit.EmbeddedLogBody;
        for (const auto& ref : commit.References) {
            using TCookie = NTabletFlatExecutor::NBoot::TCookie;
            const TCookie cookie(ref.Id.Cookie());
            if (cookie.Type() == TCookie::EType::Log && cookie.Index() == TCookie::EIdx::RedoLz4) {
                redo.append(ref.Buffer);
            }
        }
        if (redo) {
            const TString plain = NPageCollection::TSlicer::Lz4()->Decode(redo);
            NTable::NRedo::TPlayer<TFreezeRemovalLog>(*this).Replay({plain.data(), plain.size()});
        }
    }
};

// Fault injection uses the persistent Sys keys from TDataShard::Schema:
// CurrentSchemeShardId = 11, SubDomainOwnerId = 33, SubDomainLocalPathId = 34.
void SetFreezeTestSys(TFreezeEnv& env, std::initializer_list<std::pair<ui64, ui64>> values) {
    TStringBuilder program;
    program << "((return (AsList";
    for (const auto& [key, value] : values) {
        program << " (UpdateRow 'Sys '('('Id (Uint64 '" << key << ")))"
            << " '('('Uint64 (Uint64 '" << value << "))))";
    }
    program << ")))";
    auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
    request->Record.MutableProgram()->MutableProgram()->SetText(program);
    const auto sender = env.Runtime.AllocateEdgeActor();
    env.Runtime.SendToPipe(env.Shards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());
    auto result = env.Runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Record.GetStatus(), NKikimrProto::OK, result->Get()->Record.ShortDebugString());
}

auto SendFreezeTestWrite(TFreezeEnv& env, ui64 shard, ui64 txId, ui32 key, ui32 value,
        NKikimrDataEvents::TEvWrite::ETxMode mode, TActorId sender = {})
{
    if (!sender) {
        sender = env.Runtime.AllocateEdgeActor();
    }
    auto request = MakeWriteRequest(txId, mode,
        NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, env.TableId,
        std::vector<ui32>{1, 2}, std::vector<TCell>{TCell::Make(key), TCell::Make(value)});
    if (mode != NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE) {
        auto* locks = request->Record.MutableLocks();
        locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
        for (const auto participant : env.Shards) {
            locks->AddSendingShards(participant);
            locks->AddReceivingShards(participant);
        }
    }
    env.Runtime.SendToPipe(shard, sender, request.release(), 0, GetPipeConfigWithRetries());
    return env.Runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(sender)->Get()->Record;
}

void FinishFreezeTestDrain(TFreezeEnv& env) {
    for (const auto stage : {TStage::FENCE_WRITES, TStage::DRAIN_INDEXES, TStage::DRAIN_CDC}) {
        for (const auto shard : env.Shards) {
            WaitDatabaseFreezeStage(env.Runtime, shard, env.TableId.PathId.OwnerId, env.Domain(), 1, stage);
        }
    }
}

void CheckPreparedTransactionFreezeRestart(bool afterPlan, bool isVolatile = false) {
    TFreezeEnv env(TShardedTableOptions().Shards(2));
    // Exercise recovery from storage, without migrating the old actor's memory.
    env.Runtime.GetAppData(0).FeatureFlags.SetEnableDataShardInMemoryStateMigration(false);
    const auto mode = isVolatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE
        : NKikimrDataEvents::TEvWrite::MODE_PREPARE;
    const ui64 txId = 1001;
    const auto first = SendFreezeTestWrite(env, env.Shards.at(0), txId, 1, 10, mode, env.Sender);
    const auto second = SendFreezeTestWrite(env, env.Shards.at(1), txId, 2147483648u, 20, mode, env.Sender);
    UNIT_ASSERT_VALUES_EQUAL(first.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
    UNIT_ASSERT_VALUES_EQUAL(second.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
    for (const auto shard : env.Shards) {
        const auto fence = SendDatabaseFreeze(env.Runtime, shard, env.TableId.PathId.OwnerId, env.Domain(), 1, TStage::FENCE_WRITES);
        UNIT_ASSERT_VALUES_EQUAL(fence.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
        UNIT_ASSERT(!fence.GetReady());
    }

    TBlockEvents<TEvTxProcessing::TEvReadSet> readSets(env.Runtime);
    auto plan = [&] {
        SendProposeToCoordinator(env.Runtime, env.Sender, env.Shards, {
            .TxId = txId,
            .Coordinator = first.GetDomainCoordinators(0),
            .MinStep = Max(first.GetMinStep(), second.GetMinStep()),
            .MaxStep = Min(first.GetMaxStep(), second.GetMaxStep()),
            .Volatile = isVolatile,
        });
        env.Runtime.WaitFor("prepared transaction readsets before restart", [&] { return !readSets.empty(); }, TDuration::Seconds(10));
    };
    if (afterPlan) {
        plan();
        if (isVolatile) {
            const auto actor = ResolveTablet(env.Runtime, env.Shards.at(0));
            auto* shard = dynamic_cast<TDataShard*>(env.Runtime.FindActor(actor));
            UNIT_ASSERT(shard);
            env.Runtime.WaitFor("persisted volatile transaction awaiting readsets", [&] {
                return bool(shard->GetVolatileTxManager().GetTxMap());
            }, TDuration::Seconds(10));
            // The same-stage read-only response waits behind earlier local
            // commits, including persistence of the volatile transaction.
            const auto durable = SendDatabaseFreeze(env.Runtime, env.Shards.at(0), env.TableId.PathId.OwnerId,
                env.Domain(), 1, TStage::FENCE_WRITES);
            UNIT_ASSERT_VALUES_EQUAL(durable.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
            UNIT_ASSERT(!durable.GetReady());
        }
    }

    RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
    const auto pending = SendDatabaseFreeze(env.Runtime, env.Shards.at(0), env.TableId.PathId.OwnerId,
        env.Domain(), 1, TStage::FENCE_WRITES);
    UNIT_ASSERT_VALUES_EQUAL_C(pending.GetStatus(), ui32(Ydb::StatusIds::SUCCESS), pending.ShortDebugString());
    UNIT_ASSERT_C(!pending.GetReady(), "Freeze lost an in-flight transaction during restart: " << pending.ShortDebugString());

    // A fresh proposal cannot borrow the restored operation's allowance, even
    // when it reuses its txId. Use a separate recipient for these responses.
    for (const auto id : {txId, ui64(1002)}) {
        const auto rejected = SendFreezeTestWrite(env, env.Shards.at(0), id, 2, 200, mode);
        UNIT_ASSERT_VALUES_EQUAL_C(rejected.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE,
            "Fresh proposal bypassed the restored freeze: " << rejected.ShortDebugString());
    }
    if (!afterPlan) {
        plan();
    }
    readSets.Stop().Unblock();
    FinishFreezeTestDrain(env);
    // The old pipe may disappear at reboot. Verify completion through durable
    // rows and drain readiness, rather than replies addressed to that pipe.
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId),
        "key = 1, value = 10\nkey = 2147483648, value = 20\n");
    const auto rejected = SendFreezeTestWrite(env, env.Shards.at(0), 1003, 3, 30, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
    UNIT_ASSERT_VALUES_EQUAL(rejected.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
    for (const auto shard : env.Shards) {
        WaitDatabaseFreezeStage(env.Runtime, shard, env.TableId.PathId.OwnerId, env.Domain(), 2, TStage::OPEN);
    }
    UNIT_ASSERT_VALUES_EQUAL(env.Write(3, 30).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
}

void CheckDiscardFailsFreeze(TCdcFreezeEnv& env,
        TBlockEvents<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>& removals,
        ui64 order)
{
    env.Runtime.WaitFor("discarded change record removal", [&] { return !removals.empty(); }, TDuration::Seconds(10));
    UNIT_ASSERT_VALUES_EQUAL(removals.size(), 1);
    const auto& removal = *removals.front()->Get();
    UNIT_ASSERT_VALUES_EQUAL(removal.Records.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(removal.Records.front(), order);
    UNIT_ASSERT_C(!removal.DeliveryConfirmed,
        "Discarded change record is incorrectly reported as delivered");

    removals.Stop().Unblock();
    const auto result = env.Stage(TStage::DRAIN_INDEXES);
    UNIT_ASSERT(result.GetReady());
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), ui32(Ydb::StatusIds::PRECONDITION_FAILED), result.ShortDebugString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssue(), "Change records were discarded during database freeze");
}

void CheckPreparedFinalResolvedAfterWriterDisconnect(ui32 partitions) {
    TCdcFreezeEnv env;
    env.AddStream(true, false, partitions);
    const auto shardActor = ResolveTablet(env.Runtime, env.Shards.at(0));

    ui64 finalOrder = 0;
    TActorId streamSender;
    auto records = env.Runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvRecords>([&](auto& ev) {
        if (ev->Sender != shardActor) {
            return;
        }
        for (const auto& record : ev->Get()->Records) {
            if (record->GetKind() == NChangeExchange::IChangeRecord::EKind::CdcHeartbeat) {
                UNIT_ASSERT(!finalOrder || finalOrder == record->GetOrder());
                finalOrder = record->GetOrder();
                streamSender = ev->GetRecipientRewrite();
            }
        }
    });

    ui64 failedPartition = Max<ui64>();
    bool releaseOtherAcks = false;
    THashMap<TActorId, ui64> partitionIds;
    THashSet<ui64> otherAcks;
    auto ready = env.Runtime.AddObserver<NChangeExchange::TEvChangeExchangePrivate::TEvReady>([&](auto& ev) {
        const auto partitionId = ev->Get()->PartitionId;
        partitionIds[ev->Sender] = partitionId;
        if (releaseOtherAcks && ev->GetRecipientRewrite() == streamSender && partitionId != failedPartition) {
            otherAcks.insert(partitionId);
        }
    });
    bool finalRemoved = false;
    bool finalRemovalConfirmed = false;
    auto removals = env.Runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>([&](auto& ev) {
        for (const auto order : ev->Get()->Records) {
            if (finalOrder && order == finalOrder) {
                finalRemoved = true;
                finalRemovalConfirmed |= ev->Get()->DeliveryConfirmed;
                Cerr << "... final resolved removal: order# " << order
                    << " deliveryConfirmed# " << ev->Get()->DeliveryConfirmed << Endl;
            }
        }
    });

    TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    env.Runtime.WaitFor("data ACK keeping the CDC writer busy", [&] { return !ack.empty(); }, TDuration::Seconds(10));
    UNIT_ASSERT_VALUES_EQUAL(ack.size(), 1);
    UNIT_ASSERT(ack.front()->Get()->IsSuccess());
    const auto oldWriter = ack.front()->Sender;
    const auto oldPartitionSender = ack.front()->GetRecipientRewrite();
    // Withhold this ACK so the final resolved remains in Prepared for this partition.
    ack.pop_front();

    env.WaitStage(TStage::FENCE_WRITES);
    env.WaitStage(TStage::DRAIN_INDEXES);
    const auto draining = env.Stage(TStage::DRAIN_CDC);
    UNIT_ASSERT_VALUES_EQUAL(draining.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
    UNIT_ASSERT(!draining.GetReady());
    env.Runtime.WaitFor("final resolved prepared by the CDC sender", [&] { return finalOrder != 0; }, TDuration::Seconds(10));
    env.Runtime.WaitFor("final resolved ACKs from the other partitions", [&] { return ack.size() == partitions - 1; }, TDuration::Seconds(10));
    for (const auto& event : ack) {
        UNIT_ASSERT(event->Get()->IsSuccess());
        const auto& response = event->Get()->Record.GetPartitionResponse();
        UNIT_ASSERT_VALUES_EQUAL(response.CmdWriteResultSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetCmdWriteResult(0).GetSeqNo(), i64(finalOrder));
        UNIT_ASSERT(event->GetRecipientRewrite() != oldPartitionSender);
    }

    // Keep recreation paused while the other partition completes its broadcast ACK.
    // This also checks that a lost delivery is retained until that later completion.
    TBlockEvents<TEvTxProxySchemeCache::TEvNavigateKeySetResult> resolving(env.Runtime, [&](const auto& ev) {
        return ev->GetRecipientRewrite() == streamSender;
    });
    bool writerGone = false;
    auto gone = env.Runtime.AddObserver<NChangeExchange::TEvChangeExchangePrivate::TEvGone>([&](auto& ev) {
        if (ev->Sender == oldPartitionSender && ev->GetRecipientRewrite() == streamSender) {
            failedPartition = ev->Get()->PartitionId;
            writerGone = true;
        }
    });
    env.Runtime.Send(new IEventHandle(oldPartitionSender, oldWriter,
        new NPQ::TEvPartitionWriter::TEvDisconnected(NPQ::TEvPartitionWriter::TEvWriteResponse::EErrorCode::PartitionDisconnected)), 0, true);
    env.Runtime.WaitFor("CDC sender resolving after writer disconnect", [&] { return !resolving.empty(); }, TDuration::Seconds(10));
    UNIT_ASSERT(writerGone);

    releaseOtherAcks = true;
    ack.Unblock();
    env.Runtime.WaitFor("other partitions completing after writer disconnect", [&] { return otherAcks.size() == partitions - 1; }, TDuration::Seconds(10));
    UNIT_ASSERT_C(!finalRemoved, "Unsent final resolved record was removed before writer recreation");
    const auto resolvingDrain = env.Stage(TStage::DRAIN_CDC);
    UNIT_ASSERT_VALUES_EQUAL(resolvingDrain.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
    UNIT_ASSERT(!resolvingDrain.GetReady());
    resolving.Stop().Unblock();
    env.Runtime.WaitFor("replayed final resolved ACK", [&] {
        return !ack.empty() || finalRemoved;
    }, TDuration::Seconds(10));
    UNIT_ASSERT_C(!finalRemoved, "Final resolved record was removed before the replayed ACK");

    UNIT_ASSERT_VALUES_EQUAL(ack.size(), 1);
    UNIT_ASSERT(ack.front()->Get()->IsSuccess());
    const auto& replayed = ack.front()->Get()->Record.GetPartitionResponse();
    UNIT_ASSERT_VALUES_EQUAL(replayed.CmdWriteResultSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(replayed.GetCmdWriteResult(0).GetSeqNo(), i64(finalOrder));
    const auto newPartitionSender = ack.front()->GetRecipientRewrite();
    UNIT_ASSERT(newPartitionSender != oldPartitionSender);
    UNIT_ASSERT_VALUES_EQUAL(partitionIds.at(newPartitionSender), failedPartition);

    const auto pending = env.Stage(TStage::DRAIN_CDC);
    UNIT_ASSERT_VALUES_EQUAL(pending.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
    UNIT_ASSERT_C(!pending.GetReady(), "CDC drain completed before the replayed final resolved ACK");
    ack.Stop().Unblock();
    env.WaitStage(TStage::DRAIN_CDC);
    UNIT_ASSERT(finalRemoved);
    UNIT_ASSERT(finalRemovalConfirmed);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(DataShardDatabaseFreeze) {
    Y_UNIT_TEST(CdcDrainWaitsForTopicAcknowledgementAndThawResumesWrites) {
        TCdcFreezeEnv env;
        env.AddStream();
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Runtime.WaitFor("CDC topic write acknowledgement", [&] { return !ack.empty(); });
        env.WaitStage(TStage::FENCE_WRITES);
        env.WaitStage(TStage::DRAIN_INDEXES);
        const auto pending = env.Stage(TStage::DRAIN_CDC);
        UNIT_ASSERT_VALUES_EQUAL(pending.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
        UNIT_ASSERT(!pending.GetReady());
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\n");

        ack.Stop().Unblock();
        env.WaitStage(TStage::DRAIN_CDC);
        env.Thaw();
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> resumed(env.Runtime);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Runtime.WaitFor("CDC write after thaw", [&] { return !resumed.empty(); });
        env.WaitStage(TStage::FENCE_WRITES, 3);
        env.WaitStage(TStage::DRAIN_INDEXES, 3);
        UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC, 3).GetReady());
        resumed.Stop().Unblock();
        env.WaitStage(TStage::DRAIN_CDC, 3);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\nkey = 2, value = 20\n");
    }

    Y_UNIT_TEST(CdcDrainWaitsForEveryFinalResolvedAcknowledgement) {
        TCdcFreezeEnv env;
        env.AddStream(true, false, 2);
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        env.WaitStage(TStage::FENCE_WRITES);
        env.WaitStage(TStage::DRAIN_INDEXES);
        UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());
        env.Runtime.WaitFor("final resolved ACKs from both topic partitions", [&] { return ack.size() >= 2; });
        UNIT_ASSERT_VALUES_EQUAL(ack.size(), 2);
        UNIT_ASSERT(ack[0]->Sender != ack[1]->Sender);
        for (const auto& event : ack) {
            UNIT_ASSERT(event->Get()->IsSuccess());
        }
        ack.Unblock(1);
        UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());
        ack.Stop().Unblock();
        env.WaitStage(TStage::DRAIN_CDC);
        env.Thaw();
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(PreparedFinalResolvedSurvivesWriterDisconnect) {
        CheckPreparedFinalResolvedAfterWriterDisconnect(1);
    }

    Y_UNIT_TEST(PreparedFinalResolvedSurvivesWriterDisconnectBeforeOtherPartitionAck) {
        CheckPreparedFinalResolvedAfterWriterDisconnect(2);
    }

    Y_UNIT_TEST(CdcFinalResolvedAllowsDistributedWriteAfterThaw) {
        TCdcFreezeEnv env(TShardedTableOptions().Shards(2));
        env.AddStream(true, false, 2);
        env.Freeze();
        env.Thaw();
        ExecSQL(env.Server, env.Sender,
            "UPSERT INTO `/Root/Table` (key, value) VALUES (1, 10), (2147483648, 20);");
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId),
            "key = 1, value = 10\nkey = 2147483648, value = 20\n");
    }

    Y_UNIT_TEST(PendingCdcDrainSurvivesDataShardRestart) {
        TCdcFreezeEnv env;
        env.AddStream();
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Runtime.WaitFor("unconfirmed CDC write before restart", [&] { return !ack.empty(); });
        env.WaitStage(TStage::FENCE_WRITES);
        env.WaitStage(TStage::DRAIN_INDEXES);
        UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());
        TBlockEvents<NPQ::TEvPartitionWriter::TEvInitResult> recovered(env.Runtime);
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        env.Runtime.WaitFor("recovered CDC writer confirmation", [&] { return !recovered.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        const auto pending = env.Stage(TStage::DRAIN_CDC);
        UNIT_ASSERT_VALUES_EQUAL(pending.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
        UNIT_ASSERT(!pending.GetReady());
        UNIT_ASSERT(ack.front()->Get()->IsSuccess());
        const auto seqNo = ack.front()->Get()->Record.GetPartitionResponse().GetCmdWriteResult(0).GetSeqNo();
        for (const auto& event : recovered) {
            UNIT_ASSERT(event->Get()->IsSuccess());
            UNIT_ASSERT(event->Get()->GetResult().SourceIdInfo.GetSeqNo() >= seqNo);
        }
        // Reinitialization confirms the durable sequence number in the topic.
        // Drain can finish without delivering the old writer's delayed ACK.
        recovered.Stop().Unblock();
        env.WaitStage(TStage::DRAIN_CDC);
        ack.Stop().Unblock();
        env.Thaw();
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Freeze(3);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\nkey = 2, value = 20\n");
    }

    Y_UNIT_TEST(UndeliveredCdcRecordsSurviveDataShardRestart) {
        TCdcFreezeEnv env;
        env.AddStream();
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> writes(env.Runtime);
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Runtime.WaitFor("CDC write before reaching the topic", [&] { return !writes.empty(); });
        env.WaitStage(TStage::FENCE_WRITES);
        env.WaitStage(TStage::DRAIN_INDEXES);
        UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());
        const auto oldRequests = writes.size();
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        env.Runtime.WaitFor("replayed CDC write after restart", [&] { return writes.size() > oldRequests; });
        const auto pending = env.Stage(TStage::DRAIN_CDC);
        UNIT_ASSERT_VALUES_EQUAL(pending.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
        UNIT_ASSERT(!pending.GetReady());
        UNIT_ASSERT(ack.empty());
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        writes.Stop().Unblock();
        env.Runtime.WaitFor("replayed CDC topic acknowledgement", [&] { return !ack.empty(); });
        UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());
        ack.Stop().Unblock();
        env.WaitStage(TStage::DRAIN_CDC);
        env.Thaw();
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Freeze(3);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\nkey = 2, value = 20\n");
    }

    Y_UNIT_TEST(MissingSenderDiscardFailsFreeze) {
        TCdcFreezeEnv env;
        env.AddStream();

        TBlockEvents<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords> enqueued(env.Runtime);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Runtime.WaitFor("change record before routing to its sender", [&] { return !enqueued.empty(); }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(enqueued.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(enqueued.front()->Get()->Records.size(), 1);
        const auto changeSender = enqueued.front()->GetRecipientRewrite();
        const auto pathId = enqueued.front()->Get()->Records.front().PathId;
        const auto order = enqueued.front()->Get()->Records.front().Order;
        env.WaitStage(TStage::FENCE_WRITES);

        // Delay the enqueue until the router has removed its destination sender.
        bool senderRemoved = false;
        auto removeSender = env.Runtime.AddObserver<TEvChangeExchange::TEvRemoveSender>([&](auto& ev) {
            if (ev->Sender == changeSender && ev->Get()->PathId == pathId) {
                senderRemoved = true;
            }
        });
        env.Runtime.Send(new IEventHandle(changeSender, env.Sender,
            new TEvChangeExchange::TEvRemoveSender(pathId)), 0, true);
        env.Runtime.WaitFor("sender removal forwarded by the router", [&] { return senderRemoved; }, TDuration::Seconds(10));
        removeSender.Remove();

        TBlockEvents<NChangeExchange::TEvChangeExchange::TEvRemoveRecords> removals(env.Runtime, [&](const auto& ev) {
            return ev->Sender == changeSender;
        });
        enqueued.Stop().Unblock();
        CheckDiscardFailsFreeze(env, removals, order);
    }

    Y_UNIT_TEST(PendingAsyncIndexSenderDiscardFailsFreeze) {
        TCdcFreezeEnv env(TShardedTableOptions().Indexes({
            TShardedTableOptions::TIndex{"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
        }));

        TBlockEvents<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords> enqueued(env.Runtime);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Runtime.WaitFor("async index record before routing to its sender", [&] { return !enqueued.empty(); }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(enqueued.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(enqueued.front()->Get()->Records.size(), 1);
        const auto indexPathId = enqueued.front()->Get()->Records.front().PathId;
        const auto order = enqueued.front()->Get()->Records.front().Order;
        env.WaitStage(TStage::FENCE_WRITES);

        // Model a cached drop notification without issuing DDL on the fenced shard.
        // The real async-index sender must enter StatePendingRemove before enqueue.
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        TActorId indexSender;
        bool indexUnderRemove = false;
        auto navigate = env.Runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>([&](auto& ev) {
            if (ev->GetRecipientRewrite() != indexSender) {
                return;
            }
            auto& entries = ev->Get()->Request->ResultSet;
            UNIT_ASSERT_VALUES_EQUAL(entries.size(), 1);
            auto& entry = entries.front();
            if (entry.TableId.PathId != indexPathId) {
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(entry.Status, TNavigate::EStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(entry.Kind, TNavigate::KindIndex);
            UNIT_ASSERT(entry.Self);
            auto self = MakeIntrusive<TNavigate::TDirEntryInfo>();
            self->Info.CopyFrom(entry.Self->Info);
            self->Info.SetPathState(NKikimrSchemeOp::EPathStateDrop);
            entry.Self = std::move(self);
            indexUnderRemove = true;
        });
        const auto shardActor = ResolveTablet(env.Runtime, env.Shards.at(0));
        indexSender = env.Runtime.Register(CreateAsyncIndexChangeSender(
            TDataShardId{env.Shards.at(0), 1, shardActor}, env.TableId, indexPathId));
        env.Runtime.WaitFor("async index sender entering pending removal", [&] { return indexUnderRemove; }, TDuration::Seconds(10));
        navigate.Remove();

        TBlockEvents<NChangeExchange::TEvChangeExchange::TEvRemoveRecords> removals(env.Runtime, [&](const auto& ev) {
            return ev->Sender == indexSender;
        });
        enqueued.Stop();
        auto records = std::move(enqueued.front()->Get()->Records);
        enqueued.pop_front();
        env.Runtime.Send(new IEventHandle(indexSender, env.Sender,
            new NChangeExchange::TEvChangeExchange::TEvEnqueueRecords(std::move(records))), 0, true);
        CheckDiscardFailsFreeze(env, removals, order);
    }

    Y_UNIT_TEST(UnknownTableChangeRecordReportsDrainIssue) {
        TCdcFreezeEnv env;
        env.AddStream();

        TBlockEvents<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords> enqueued(env.Runtime);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Runtime.WaitFor("change record before routing to its sender", [&] { return !enqueued.empty(); }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(enqueued.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(enqueued.front()->Get()->Records.size(), 1);
        const auto order = enqueued.front()->Get()->Records.front().Order;
        const auto pathId = enqueued.front()->Get()->Records.front().PathId;
        const auto shardActor = ResolveTablet(env.Runtime, env.Shards.at(0));
        const TPathId unknownTableId(env.TableId.PathId.OwnerId, Max<ui64>() - 1);

        // Model a stale queue entry without dropping the actual table: DropTable
        // would also change the shard state and trigger sender cleanup.
        env.Runtime.RunCall([&] {
            auto* shard = dynamic_cast<TDataShard*>(env.Runtime.FindActor(shardActor));
            UNIT_ASSERT(shard);
            UNIT_ASSERT(!shard->HasUserTable(unknownTableId));
            auto queue = shard->TakeChangesQueue();
            UNIT_ASSERT_VALUES_EQUAL(queue.size(), 1);
            auto& record = queue.at(order);
            UNIT_ASSERT_VALUES_EQUAL(record.TableId, env.TableId.PathId);
            UNIT_ASSERT_VALUES_EQUAL(record.PathId, pathId);
            record.TableId = unknownTableId;
            shard->SetChangesQueue(std::move(queue));
            return true;
        });

        env.WaitStage(TStage::FENCE_WRITES);
        const auto pendingIndexes = env.Stage(TStage::DRAIN_INDEXES);
        const auto pendingCdc = env.Stage(TStage::DRAIN_CDC);

        // Restore the real table before delivery can remove the queue entry and
        // release its schema snapshot reference.
        env.Runtime.RunCall([&] {
            auto* shard = dynamic_cast<TDataShard*>(env.Runtime.FindActor(shardActor));
            UNIT_ASSERT(shard);
            auto queue = shard->TakeChangesQueue();
            UNIT_ASSERT_VALUES_EQUAL(queue.size(), 1);
            auto& record = queue.at(order);
            UNIT_ASSERT_VALUES_EQUAL(record.TableId, unknownTableId);
            record.TableId = env.TableId.PathId;
            shard->SetChangesQueue(std::move(queue));
            return true;
        });

        // The same undelivered record is now recognizable as CDC, so it must
        // no longer block DRAIN_INDEXES.
        const auto resumedIndexes = env.Stage(TStage::DRAIN_INDEXES);
        UNIT_ASSERT_VALUES_EQUAL_C(resumedIndexes.GetStatus(), ui32(Ydb::StatusIds::SUCCESS), resumedIndexes.ShortDebugString());
        UNIT_ASSERT_C(resumedIndexes.GetReady(), resumedIndexes.ShortDebugString());
        UNIT_ASSERT_C(resumedIndexes.GetIssue().empty(), resumedIndexes.ShortDebugString());
        enqueued.Stop().Unblock();
        env.WaitStage(TStage::DRAIN_CDC);

        for (const auto* result : {&pendingIndexes, &pendingCdc}) {
            UNIT_ASSERT_VALUES_EQUAL_C(result->GetStatus(), ui32(Ydb::StatusIds::SUCCESS), result->ShortDebugString());
            UNIT_ASSERT_C(!result->GetReady(), result->ShortDebugString());
            UNIT_ASSERT_C(!result->GetIssue().empty(),
                "Drain is blocked by a change record for an unknown table, but no diagnostic is reported: "
                << result->ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(result->GetIssue(), unknownTableId.ToString());
            UNIT_ASSERT_STRING_CONTAINS(result->GetIssue(), pathId.ToString());
            UNIT_ASSERT_STRING_CONTAINS(result->GetIssue(), "order# " + ToString(order));
        }
    }

    Y_UNIT_TEST(DeliveryLostIsPersistedOnceAcrossRemovalBatches) {
        TCdcFreezeEnv env;
        env.Runtime.GetAppData(0).FeatureFlags.SetEnableDataShardInMemoryStateMigration(false);
        env.AddStream();
        TBlockEvents<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords> enqueued(env.Runtime);

        // More than two removal buckets, plus one record to remove after reboot.
        constexpr ui32 recordCount = 2002;
        std::vector<TCell> cells;
        for (ui32 key = 1; key <= recordCount; ++key) {
            cells.push_back(TCell::Make(key));
            cells.push_back(TCell::Make(key));
        }
        auto write = MakeWriteRequest(5000, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, env.TableId,
            std::vector<ui32>{1, 2}, cells);
        env.Runtime.SendToPipe(env.Shards.at(0), env.Sender, write.release(), 0, GetPipeConfigWithRetries());
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(env.Sender)
            ->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        TVector<ui64> orders;
        env.Runtime.WaitFor("all CDC records enqueued", [&] {
            orders.clear();
            for (const auto& event : enqueued) {
                for (const auto& record : event->Get()->Records) {
                    orders.push_back(record.Order);
                }
            }
            return orders.size() == recordCount;
        });
        env.WaitStage(TStage::FENCE_WRITES);

        TFreezeRemovalLog log;
        auto commits = env.Runtime.AddObserver<TEvTablet::TEvCommit>([&](auto& ev) {
            if (ev->Get()->TabletID == env.Shards.at(0)) {
                log.Observe(*ev->Get());
            }
        });
        const ui64 lastOrder = orders.back();
        orders.pop_back();
        auto discard = MakeHolder<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>(std::move(orders));
        discard->DeliveryConfirmed = false;
        env.Runtime.Send(new IEventHandle(ResolveTablet(env.Runtime, env.Shards.at(0)), env.Sender, discard.Release()));
        env.Runtime.WaitFor("three change record removal batches", [&] { return log.RemovedRecords == recordCount - 1; });
        UNIT_ASSERT_VALUES_EQUAL(env.Stage(TStage::FENCE_WRITES).GetStatus(), ui32(Ydb::StatusIds::PRECONDITION_FAILED));

        // The sticky failure must survive actual disk recovery. A subsequent
        // confirmed removal must not rewrite the already durable flag either.
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(env.Stage(TStage::FENCE_WRITES).GetStatus(), ui32(Ydb::StatusIds::PRECONDITION_FAILED));
        UNIT_ASSERT_VALUES_EQUAL(env.Write(recordCount + 1, 1).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        auto confirmed = MakeHolder<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>(TVector<ui64>{lastOrder});
        env.Runtime.Send(new IEventHandle(ResolveTablet(env.Runtime, env.Shards.at(0)), env.Sender, confirmed.Release()));
        env.Runtime.WaitFor("confirmed removal after restart", [&] { return log.RemovedRecords == recordCount; });
        UNIT_ASSERT_VALUES_EQUAL(env.Stage(TStage::FENCE_WRITES).GetStatus(), ui32(Ydb::StatusIds::PRECONDITION_FAILED));

        // A new freeze epoch must persist a new delivery loss independently.
        env.Thaw();
        enqueued.clear();
        UNIT_ASSERT_VALUES_EQUAL(env.Write(recordCount + 1, 1).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Runtime.WaitFor("next epoch CDC record", [&] { return !enqueued.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(enqueued.front()->Get()->Records.size(), 1);
        const ui64 nextOrder = enqueued.front()->Get()->Records.front().Order;
        env.WaitStage(TStage::FENCE_WRITES, 3);
        auto nextDiscard = MakeHolder<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>(TVector<ui64>{nextOrder});
        nextDiscard->DeliveryConfirmed = false;
        env.Runtime.Send(new IEventHandle(ResolveTablet(env.Runtime, env.Shards.at(0)), env.Sender, nextDiscard.Release()));
        env.Runtime.WaitFor("new epoch delivery loss", [&] { return log.RemovedRecords == recordCount + 1; });
        UNIT_ASSERT_VALUES_EQUAL(env.Stage(TStage::FENCE_WRITES, 3).GetStatus(), ui32(Ydb::StatusIds::PRECONDITION_FAILED));

        UNIT_ASSERT_VALUES_EQUAL_C(log.DeliveryLostWrites[1], 1,
            "DeliveryLost must be persisted once, atomically with the first removal; "
            "later buckets and confirmed removals after restart must not rewrite it");
        UNIT_ASSERT_VALUES_EQUAL(log.DeliveryLostWrites[3], 1);
    }

    Y_UNIT_TEST(UnconfirmedCdcRemovalFailsAcrossRestartUntilThaw) {
        TCdcFreezeEnv env;
        env.AddStream();
        TVector<ui64> orders;
        auto observer = env.Runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords>([&](auto& ev) {
            if (orders.empty()) {
                for (const auto& record : ev->Get()->Records) {
                    orders.push_back(record.Order);
                }
            }
        });
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Runtime.WaitFor("unconfirmed CDC record", [&] { return !orders.empty() && !ack.empty(); });
        env.WaitStage(TStage::FENCE_WRITES);
        auto discard = MakeHolder<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>(std::move(orders));
        discard->DeliveryConfirmed = false;
        const auto shardActor = ResolveTablet(env.Runtime, env.Shards.at(0));
        env.Runtime.Send(new IEventHandle(shardActor, env.Sender, discard.Release()));
        UNIT_ASSERT_VALUES_EQUAL(env.Stage(TStage::DRAIN_INDEXES).GetStatus(), ui32(Ydb::StatusIds::PRECONDITION_FAILED));
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(env.Stage(TStage::FENCE_WRITES).GetStatus(), ui32(Ydb::StatusIds::PRECONDITION_FAILED));
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        ack.Stop().Unblock();
        env.Thaw();
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Freeze(3);
    }

    Y_UNIT_TEST(ThawSupersedesPendingCdcDrainAndIgnoresLateAcknowledgements) {
        TCdcFreezeEnv env;
        env.AddStream();
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Runtime.WaitFor("unconfirmed CDC write", [&] { return !ack.empty(); });
        env.WaitStage(TStage::FENCE_WRITES);
        env.WaitStage(TStage::DRAIN_INDEXES);
        UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());
        env.Thaw();
        UNIT_ASSERT_VALUES_EQUAL(env.Stage(TStage::DRAIN_CDC).GetStatus(), ui32(Ydb::StatusIds::ABORTED));
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        ack.Stop().Unblock();
        env.Freeze(3);
        env.Thaw(4);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\nkey = 2, value = 20\n");
    }

    Y_UNIT_TEST(InitialCdcScanMustFinishBeforeDrain) {
        TCdcFreezeEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        TBlockEvents<TEvDataShard::TEvCdcStreamScanResponse> scan(env.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TEvCdcStreamScanResponse::DONE;
        });
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        env.AddStream(false, true);
        env.Runtime.WaitFor("initial CDC scan completion", [&] { return !scan.empty(); });
        env.WaitStage(TStage::FENCE_WRITES);
        UNIT_ASSERT(!env.Stage(TStage::DRAIN_INDEXES).GetReady());
        UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());
        scan.Stop().Unblock();
        env.WaitStage(TStage::DRAIN_INDEXES);
        env.Runtime.WaitFor("initial CDC scan topic ACK", [&] { return !ack.empty(); });
        UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());
        ack.Stop().Unblock();
        env.WaitStage(TStage::DRAIN_CDC);
    }

    Y_UNIT_TEST(FreezeReadWriteThaw) {
        TFreezeEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Freeze();
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(3, 30, NKikimrDataEvents::TEvWrite::MODE_PREPARE).GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        ExecSQL(env.Server, env.Sender, "SELECT * FROM `/Root/Table`;");
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\n");
        env.Thaw();
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\nkey = 2, value = 20\n");
    }

    Y_UNIT_TEST(DirectUploadAndEraseAreFenced) {
        TFreezeEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.Upload(1, 10).GetStatus(), ui32(NKikimrTxDataShard::TError::OK));
        env.Freeze();
        UNIT_ASSERT_VALUES_EQUAL(env.Upload(2, 20).GetStatus(), ui32(NKikimrTxDataShard::TError::WRONG_SHARD_STATE));
        UNIT_ASSERT_VALUES_EQUAL(env.Erase(1).GetStatus(), NKikimrTxDataShard::TEvEraseRowsResponse::WRONG_SHARD_STATE);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\n");
        env.Thaw();
        UNIT_ASSERT_VALUES_EQUAL(env.Upload(2, 20).GetStatus(), ui32(NKikimrTxDataShard::TError::OK));
        UNIT_ASSERT_VALUES_EQUAL(env.Erase(1).GetStatus(), NKikimrTxDataShard::TEvEraseRowsResponse::OK);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 2, value = 20\n");
    }

    Y_UNIT_TEST(UncommittedLockCannotBeCommittedWhileFrozen) {
        TFreezeEnv env;
        NLongTxService::TLockHandle lockHandle(42, env.Runtime.GetActorSystem(0));
        const auto written = UncommittedWrite(env.Runtime, env.Sender, env.Shards.at(0),
            env.TableId, TShardedTableOptions().Columns_, 42, env.Sender.NodeId(), 1, 10, 0, 0,
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(written.TxLocksSize(), 1);
        const auto lock = written.GetTxLocks(0);
        env.Freeze();
        CommitLock(env.Runtime, env.Sender, env.Shards.at(0), lock,
            NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "");
        env.Thaw();
        CommitLock(env.Runtime, env.Sender, env.Shards.at(0), lock,
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\n");
    }

    Y_UNIT_TEST(S3UploadsAreFenced) {
        TFreezeEnv env;
        env.Freeze();
        TS3Download info;
        info.DataETag = "etag";
        auto rows = std::make_shared<NKikimrTxDataShard::TEvUploadRowsRequest>(env.UploadRequest(1, 10));
        auto request = MakeHolder<TEvDataShard::TEvS3UploadRowsRequest>(100, rows, info);
        env.Runtime.SendToPipe(env.Shards.at(0), env.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        auto response = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvS3UploadRowsResponse>(env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), ui32(NKikimrTxDataShard::TError::WRONG_SHARD_STATE));
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "");
    }

    Y_UNIT_TEST(BackgroundIndexScansAreFenced) {
        TFreezeEnv env;
        env.Freeze();
        env.CheckBackgroundScanRejected<TEvDataShard::TEvBuildIndexCreateRequest, TEvDataShard::TEvBuildIndexProgressResponse>();
        env.CheckBackgroundScanRejected<TEvDataShard::TEvValidateUniqueIndexRequest, TEvDataShard::TEvValidateUniqueIndexResponse>();
        env.CheckBackgroundScanRejected<TEvDataShard::TEvSampleKRequest, TEvDataShard::TEvSampleKResponse>();
        env.CheckBackgroundScanRejected<TEvDataShard::TEvReshuffleKMeansRequest, TEvDataShard::TEvReshuffleKMeansResponse>();
        env.CheckBackgroundScanRejected<TEvDataShard::TEvRecomputeKMeansRequest, TEvDataShard::TEvRecomputeKMeansResponse>();
        env.CheckBackgroundScanRejected<TEvDataShard::TEvLocalKMeansRequest, TEvDataShard::TEvLocalKMeansResponse>();
        env.CheckBackgroundScanRejected<TEvDataShard::TEvPrefixKMeansRequest, TEvDataShard::TEvPrefixKMeansResponse>();
        env.CheckBackgroundScanRejected<TEvDataShard::TEvFilterKMeansRequest, TEvDataShard::TEvFilterKMeansResponse>();
        env.CheckBackgroundScanRejected<TEvDataShard::TEvBuildFulltextIndexRequest, TEvDataShard::TEvBuildFulltextIndexResponse>();
        env.CheckBackgroundScanRejected<TEvDataShard::TEvBuildFulltextDictRequest, TEvDataShard::TEvBuildFulltextDictResponse>();
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "");
    }

    Y_UNIT_TEST(CommitWritesAndDistributedEraseAreFenced) {
        TFreezeEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Freeze();
        NKikimrTxDataShard::TCommitWritesTransaction commit;
        commit.MutableTableId()->SetOwnerId(env.TableId.PathId.OwnerId);
        commit.MutableTableId()->SetTableId(env.TableId.PathId.LocalPathId);
        commit.SetWriteTxId(42);
        NKikimrTxDataShard::TDistributedEraseTransaction erase;
        auto* rows = erase.MutableEraseRowsRequest();
        rows->SetTableId(env.TableId.PathId.LocalPathId);
        rows->SetSchemaVersion(env.TableId.SchemaVersion);
        rows->AddKeyColumnIds(1);
        rows->AddKeyColumns(TSerializedCellVec::Serialize({TCell::Make(ui32(1))}));
        ui64 txId = 2000;
        for (const auto& [kind, body] : {
            std::pair{NKikimrTxDataShard::TX_KIND_COMMIT_WRITES, commit.SerializeAsString()},
            std::pair{NKikimrTxDataShard::TX_KIND_DISTRIBUTED_ERASE, erase.SerializeAsString()}})
        {
            auto request = MakeHolder<TEvDataShard::TEvProposeTransaction>(kind, env.Sender, ++txId, body);
            env.Runtime.SendToPipe(env.Shards.at(0), env.Sender, request.Release(), 0, GetPipeConfigWithRetries());
            auto response = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(env.Sender);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);
            UNIT_ASSERT_STRING_CONTAINS(response->Get()->Record.ShortDebugString(), "Database is frozen");
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\n");
    }

    Y_UNIT_TEST(SchemeWriteProtocolsAreFencedAtTheShard) {
        TFreezeEnv env;
        env.Freeze();
        ui64 txId = 5000;
        for (const auto* name : {"CreateTable", "DropTable", "AlterTable", "Backup", "Restore", "SendSnapshot",
            "CreatePersistentSnapshot", "DropPersistentSnapshot", "InitiateBuildIndex", "PrepareIndexValidation",
            "FinalizeBuildIndex", "DropIndexNotice", "MoveTable", "CreateCdcStreamNotice", "AlterCdcStreamNotice",
            "DropCdcStreamNotice", "RotateCdcStreamNotice", "MoveIndex", "CreateIncrementalBackupSrc", "TruncateTable"})
        {
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            const auto* field = tx.GetDescriptor()->FindFieldByName(name);
            UNIT_ASSERT_C(field, name);
            tx.GetReflection()->MutableMessage(&tx, field);
            if (tx.HasAlterCdcStreamNotice()) {
                tx.MutableAlterCdcStreamNotice()->MutableStreamDescription()->SetState(NKikimrSchemeOp::ECdcStreamStateDisabled);
            }
            auto request = MakeHolder<TEvDataShard::TEvProposeTransaction>(NKikimrTxDataShard::TX_KIND_SCHEME,
                env.TableId.PathId.OwnerId, env.Sender, ++txId, tx.SerializeAsString(), NKikimrSubDomains::TProcessingParams());
            env.Runtime.SendToPipe(env.Shards.at(0), env.Sender, request.Release(), 0, GetPipeConfigWithRetries());
            const auto response = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(env.Sender);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrTxDataShard::TEvProposeTransactionResult::ERROR, name);
            UNIT_ASSERT_STRING_CONTAINS(response->Get()->Record.ShortDebugString(), "Wrong shard state for scheme transaction");
        }
    }

    Y_UNIT_TEST(UnsafeTruncateIsFenced) {
        TFreezeEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        auto truncate = [&] {
            auto request = MakeHolder<NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            auto* operation = request->Record.AddOperations();
            operation->SetType(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UNSAFE_TRUNCATE);
            operation->MutableTableId()->SetOwnerId(env.TableId.PathId.OwnerId);
            operation->MutableTableId()->SetTableId(env.TableId.PathId.LocalPathId);
            operation->MutableTableId()->SetSchemaVersion(env.TableId.SchemaVersion);
            env.Runtime.SendToPipe(env.Shards.at(0), env.Sender, request.Release(), 0, GetPipeConfigWithRetries());
            return env.Runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(env.Sender)->Get()->Record;
        };
        env.Freeze();
        UNIT_ASSERT_VALUES_EQUAL(truncate().GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\n");
        env.Thaw();
        UNIT_ASSERT_VALUES_EQUAL(truncate().GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "");
    }

    Y_UNIT_TEST(TtlRequestsAreFenced) {
        TFreezeEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Freeze();
        auto request = MakeHolder<TEvDataShard::TEvConditionalEraseRowsRequest>();
        request->Record.SetTableId(env.TableId.PathId.LocalPathId);
        request->Record.SetSchemaVersion(env.TableId.SchemaVersion);
        request->Record.MutableExpiration()->SetColumnId(2);
        request->Record.MutableExpiration()->SetColumnUnit(NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS);
        request->Record.MutableExpiration()->SetWallClockTimestamp(TInstant::Now().MicroSeconds());
        env.Runtime.SendToPipe(env.Shards.at(0), env.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        auto response = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvConditionalEraseRowsResponse>(env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrTxDataShard::TEvConditionalEraseRowsResponse::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\n");
    }

    Y_UNIT_TEST(ReplicationApplyIsFenced) {
        TFreezeEnv env(TShardedTableOptions().Replicated(true));
        env.Freeze();
        auto request = MakeHolder<TEvDataShard::TEvApplyReplicationChanges>();
        request->Record.MutableTableId()->SetOwnerId(env.TableId.PathId.OwnerId);
        request->Record.MutableTableId()->SetTableId(env.TableId.PathId.LocalPathId);
        env.Runtime.SendToPipe(env.Shards.at(0), env.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        auto response = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvApplyReplicationChangesResult>(env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(ui32(response->Get()->Record.GetStatus()), ui32(NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED));
        UNIT_ASSERT_VALUES_EQUAL(ui32(response->Get()->Record.GetReason()), ui32(NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_WRONG_STATE));
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "");
    }

    Y_UNIT_TEST(WriteAlreadyInProposeQueueIsRejected) {
        TFreezeEnv env;
        const auto shardActor = ResolveTablet(env.Runtime, env.Shards.at(0), 0, false);
        auto request = MakeWriteRequest(100, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, env.TableId,
            std::vector<ui32>{1, 2}, std::vector<TCell>{TCell::Make(ui32(1)), TCell::Make(ui32(10))});
        // Queue both public requests before dispatch. The write handler appends its
        // delayed proposal after the fence that is already in the same mailbox.
        env.Runtime.Send(new IEventHandle(shardActor, env.Sender, request.release()), 0, true);
        auto fence = MakeHolder<TEvDataShard::TEvDatabaseFreeze>();
        fence->Record.SetEpoch(1);
        fence->Record.SetSchemeShardId(env.TableId.PathId.OwnerId);
        fence->Record.SetDomainOwnerId(env.Domain().OwnerId);
        fence->Record.SetDomainLocalPathId(env.Domain().LocalPathId);
        fence->Record.SetStage(TStage::FENCE_WRITES);
        env.Runtime.Send(new IEventHandle(shardActor, env.Sender, fence.Release()), 0, true);
        auto frozen = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvDatabaseFreezeResult>(env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(frozen->Get()->Record.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
        auto result = env.Runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        env.Freeze();
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "");
    }

    Y_UNIT_TEST(MissingSubdomainReportsFreezeWaitReason) {
        TFreezeEnv env;
        SetFreezeTestSys(env, {{33, 0}, {34, 0}});
        TBlockEvents<NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound> discovery(env.Runtime);
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        const auto actor = ResolveTablet(env.Runtime, env.Shards.at(0));
        env.Runtime.RunCall([&] {
            auto* shard = dynamic_cast<TDataShard*>(env.Runtime.FindActor(actor));
            UNIT_ASSERT(shard);
            shard->StopFindSubDomainPathId();
            shard->StartFindSubDomainPathId(false);
            return true;
        });
        env.Runtime.WaitFor("subdomain discovery held before freeze", [&] { return !discovery.empty(); }, TDuration::Seconds(10));
        const auto pending = SendDatabaseFreeze(env.Runtime, env.Shards.at(0), env.TableId.PathId.OwnerId,
            env.Domain(), 1, TStage::FENCE_WRITES);
        discovery.Stop().Unblock();
        env.Freeze();
        UNIT_ASSERT_VALUES_EQUAL(pending.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
        UNIT_ASSERT(!pending.GetReady());
        UNIT_ASSERT_C(!pending.GetIssue().empty(), "Missing diagnostic while SubDomainPathId is unknown: " << pending.ShortDebugString());
        UNIT_ASSERT_STRING_CONTAINS(pending.GetIssue(), "SubDomainPathId is unknown");
    }

    Y_UNIT_TEST_TWIN(MissingSchemeShardReportsFreezeWaitReason, InvalidId) {
        TFreezeEnv env;
        SetFreezeTestSys(env, {{11, InvalidId ? INVALID_TABLET_ID : 0}});
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        const auto pending = SendDatabaseFreeze(env.Runtime, env.Shards.at(0), env.TableId.PathId.OwnerId,
            env.Domain(), 1, TStage::FENCE_WRITES);
        SetFreezeTestSys(env, {{11, env.TableId.PathId.OwnerId}});
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        env.Freeze();
        UNIT_ASSERT_VALUES_EQUAL(pending.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
        UNIT_ASSERT(!pending.GetReady());
        UNIT_ASSERT_C(!pending.GetIssue().empty(), "Missing diagnostic while CurrentSchemeShardId is unknown: " << pending.ShortDebugString());
        UNIT_ASSERT_STRING_CONTAINS(pending.GetIssue(), "CurrentSchemeShardId is unknown");
    }

    Y_UNIT_TEST_TWIN(PreparedTransactionSurvivesFreezeRestart, AfterPlan) {
        CheckPreparedTransactionFreezeRestart(AfterPlan);
    }

    Y_UNIT_TEST(PlannedVolatileTransactionSurvivesFreezeRestart) {
        CheckPreparedTransactionFreezeRestart(true, true);
    }

    Y_UNIT_TEST(LostVolatilePrepareDoesNotBypassFreezeAfterRestart) {
        TFreezeEnv env(TShardedTableOptions().Shards(2));
        env.Runtime.GetAppData(0).FeatureFlags.SetEnableDataShardInMemoryStateMigration(false);
        const auto mode = NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE;
        const ui64 txId = 1001;
        UNIT_ASSERT_VALUES_EQUAL(SendFreezeTestWrite(env, env.Shards.at(0), txId, 1, 10, mode).GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        UNIT_ASSERT_VALUES_EQUAL(SendFreezeTestWrite(env, env.Shards.at(1), txId, 2147483648u, 20, mode).GetStatus(),
            NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        for (const auto shard : env.Shards) {
            const auto fence = SendDatabaseFreeze(env.Runtime, shard, env.TableId.PathId.OwnerId,
                env.Domain(), 1, TStage::FENCE_WRITES);
            UNIT_ASSERT_VALUES_EQUAL(fence.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
            UNIT_ASSERT(!fence.GetReady());
        }
        // Volatile prepares have no durable body before plan. The epoch retains
        // their accepted txIds, but the operations themselves disappear.
        for (const auto shard : env.Shards) {
            RebootTablet(env.Runtime, shard, env.Sender);
        }
        for (const auto id : {txId, ui64(1002)}) {
            const auto rejected = SendFreezeTestWrite(env, env.Shards.at(0), id, 2, 200, mode);
            UNIT_ASSERT_VALUES_EQUAL_C(rejected.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE,
                "Lost volatile prepare allowed a new proposal through the fence: " << rejected.ShortDebugString());
        }
        FinishFreezeTestDrain(env);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "");
        for (const auto shard : env.Shards) {
            WaitDatabaseFreezeStage(env.Runtime, shard, env.TableId.PathId.OwnerId, env.Domain(), 2, TStage::OPEN);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Write(3, 30).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(RestartAndStaleEpoch) {
        TFreezeEnv env;
        env.Freeze(10);
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        const auto stale = SendDatabaseFreeze(env.Runtime, env.Shards.at(0), env.TableId.PathId.OwnerId,
            env.Domain(), 9, TStage::OPEN);
        UNIT_ASSERT_VALUES_EQUAL(stale.GetStatus(), ui32(Ydb::StatusIds::ABORTED));
        env.Thaw(11);
        const auto late = SendDatabaseFreeze(env.Runtime, env.Shards.at(0), env.TableId.PathId.OwnerId,
            env.Domain(), 10, TStage::DRAIN_CDC);
        UNIT_ASSERT_VALUES_EQUAL(late.GetStatus(), ui32(Ydb::StatusIds::ABORTED));
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(CannotSkipFenceStage) {
        TFreezeEnv env;
        const auto result = SendDatabaseFreeze(env.Runtime, env.Shards.at(0), env.TableId.PathId.OwnerId,
            env.Domain(), 1, TStage::DRAIN_CDC);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), ui32(Ydb::StatusIds::BAD_REQUEST));
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(LegacyTableThawPreservesDatabaseFreeze) {
        TFreezeEnv env;
        env.Runtime.GetAppData(0).FeatureFlags.SetEnableDataShardInMemoryStateMigration(false);
        env.LegacyFreeze(true);
        env.Freeze();

        // SchemeShard retries rejected proposals. Check the first rejection so
        // a regression fails here instead of timing out in WaitTxNotification.
        TString unfreezeBody;
        auto proposals = env.Runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            if (ev->Get()->Record.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
                unfreezeBody = ev->Get()->Record.GetTxBody();
            }
        });
        auto results = env.Runtime.AddObserver<TEvDataShard::TEvProposeTransactionResult>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetOrigin() == env.Shards.at(0) && record.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
                NKikimrTxDataShard::TFlatSchemeTransaction tx;
                UNIT_ASSERT(tx.ParseFromString(unfreezeBody));
                UNIT_ASSERT_C(record.GetStatus() != NKikimrTxDataShard::TEvProposeTransactionResult::ERROR,
                    "Legacy table unfreeze must be admitted while the database remains frozen: "
                    << record.ShortDebugString() << "; proposal: " << tx.ShortDebugString());
            }
        });
        env.LegacyFreeze(false);
        proposals.Remove();
        results.Remove();

        const auto shardActor = ResolveTablet(env.Runtime, env.Shards.at(0));
        env.Runtime.RunCall([&] {
            auto* shard = dynamic_cast<TDataShard*>(env.Runtime.FindActor(shardActor));
            UNIT_ASSERT(shard);
            UNIT_ASSERT(!shard->IsStateFrozen());
            UNIT_ASSERT(shard->IsDatabaseFrozen());
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        env.Thaw();
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId), "key = 1, value = 10\n");
    }

    Y_UNIT_TEST(LegacyUnfreezeWithOtherAlterOptionsIsFenced) {
        TFreezeEnv env;
        env.LegacyFreeze(true);
        env.Freeze();
        ui64 txId = 5000;
        for (const auto* name : {"TxReadSizeLimit", "TTLSettings", "ReplicationConfig"}) {
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            tx.MutableSeqNo()->SetGeneration(Max<ui64>());
            tx.MutableSeqNo()->SetRound(++txId);
            auto* alter = tx.MutableAlterTable();
            env.TableId.PathId.ToProto(alter->MutablePathId());
            alter->SetTableSchemaVersion(env.TableId.SchemaVersion + 1);
            auto* config = alter->MutablePartitionConfig();
            config->SetFreezeState(NKikimrSchemeOp::EFreezeState::Unfreeze);
            if (TStringBuf(name) == "TxReadSizeLimit") {
                config->SetTxReadSizeLimit(1024);
            } else {
                const auto* field = alter->GetDescriptor()->FindFieldByName(name);
                UNIT_ASSERT_C(field, name);
                alter->GetReflection()->MutableMessage(alter, field);
            }
            auto request = MakeHolder<TEvDataShard::TEvProposeTransaction>(NKikimrTxDataShard::TX_KIND_SCHEME,
                env.TableId.PathId.OwnerId, env.Sender, txId, tx.SerializeAsString(), NKikimrSubDomains::TProcessingParams());
            env.Runtime.SendToPipe(env.Shards.at(0), env.Sender, request.Release(), 0, GetPipeConfigWithRetries());
            const auto response = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(env.Sender);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrTxDataShard::TEvProposeTransactionResult::ERROR, name);
            UNIT_ASSERT_STRING_CONTAINS(response->Get()->Record.ShortDebugString(), "Wrong shard state for scheme transaction");
        }
    }

    Y_UNIT_TEST(DatabaseThawPreservesManualTableFreeze) {
        TFreezeEnv env;
        env.LegacyFreeze(true);
        UNIT_ASSERT_VALUES_EQUAL(env.Upload(1, 10).GetStatus(), ui32(NKikimrTxDataShard::TError::WRONG_SHARD_STATE));
        UNIT_ASSERT_VALUES_EQUAL(env.Erase(1).GetStatus(), NKikimrTxDataShard::TEvEraseRowsResponse::WRONG_SHARD_STATE);
        env.Freeze();
        env.Thaw();
        UNIT_ASSERT(env.Write(1, 10).GetStatus() != NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.LegacyFreeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST_TWIN(PreparedTransactionFinishesBeforeFreeze, Volatile) {
        TFreezeEnv env(TShardedTableOptions().Shards(2));
        const auto mode = Volatile
            ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE
            : NKikimrDataEvents::TEvWrite::MODE_PREPARE;
        auto prepare = [&](ui64 shard, ui32 key, ui32 value) {
            auto request = MakeWriteRequest(1001, mode,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, env.TableId,
                std::vector<ui32>{1, 2}, std::vector<TCell>{TCell::Make(key), TCell::Make(value)});
            auto* locks = request->Record.MutableLocks();
            locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            for (const auto participant : env.Shards) {
                locks->AddSendingShards(participant);
                locks->AddReceivingShards(participant);
            }
            env.Runtime.SendToPipe(shard, env.Sender, request.release(), 0, GetPipeConfigWithRetries());
            return env.Runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(env.Sender)->Get()->Record;
        };
        const auto prepared = prepare(env.Shards.at(0), 1, 10);
        UNIT_ASSERT_VALUES_EQUAL(prepared.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        const auto second = prepare(env.Shards.at(1), 2147483648u, 20);
        UNIT_ASSERT_VALUES_EQUAL(second.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        for (const auto shard : env.Shards) {
            const auto pending = SendDatabaseFreeze(env.Runtime, shard, env.TableId.PathId.OwnerId,
                env.Domain(), 1, TStage::FENCE_WRITES);
            UNIT_ASSERT_VALUES_EQUAL(pending.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
            UNIT_ASSERT(!pending.GetReady());
        }
        TBlockEvents<TEvTxProcessing::TEvReadSet> readSets(env.Runtime);
        SendProposeToCoordinator(env.Runtime, env.Sender, env.Shards, {
            .TxId = prepared.GetTxId(),
            .Coordinator = prepared.GetDomainCoordinators(0),
            .MinStep = Max(prepared.GetMinStep(), second.GetMinStep()),
            .MaxStep = Min(prepared.GetMaxStep(), second.GetMaxStep()),
            .Volatile = Volatile,
        });
        env.Runtime.WaitFor("distributed transaction readsets", [&] { return !readSets.empty(); });
        for (const auto shard : env.Shards) {
            UNIT_ASSERT(!SendDatabaseFreeze(env.Runtime, shard, env.TableId.PathId.OwnerId,
                env.Domain(), 1, TStage::FENCE_WRITES).GetReady());
        }
        readSets.Stop().Unblock();
        for (size_t i = 0; i < env.Shards.size(); ++i) {
            WaitForWriteCompleted(env.Runtime, env.Sender);
        }
        for (const auto stage : {TStage::FENCE_WRITES, TStage::DRAIN_INDEXES, TStage::DRAIN_CDC}) {
            for (const auto shard : env.Shards) {
                WaitDatabaseFreezeStage(env.Runtime, shard, env.TableId.PathId.OwnerId, env.Domain(), 1, stage);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, env.Shards, env.TableId),
            "key = 1, value = 10\nkey = 2147483648, value = 20\n");
    }
}

} // namespace NKikimr
