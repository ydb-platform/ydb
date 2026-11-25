#include "datashard_impl.h"
#include "change_exchange_impl.h"
#include "datashard_ut_common_kqp.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/change_exchange/change_sender.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/core/tx/datashard/ut_common/database_freeze.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/unknown_field_set.h>

#include <util/string/cast.h>
#include <util/stream/str.h>
#include <util/string/printf.h>

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

    void AddStream(bool resolved = false, bool initialScan = false, ui32 partitions = 1, bool topicAutoPartitioning = false) {
        TShardedTableOptions::TCdcStream stream{
            .Name = "Stream",
            .Mode = NKikimrSchemeOp::ECdcStreamModeUpdate,
            .Format = NKikimrSchemeOp::ECdcStreamFormatJson,
            .TopicPartitions = partitions,
        };
        stream.TopicAutoPartitioning = topicAutoPartitioning;
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
    auto removals = env.Runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>([&](auto& ev) {
        for (const auto order : ev->Get()->Records) {
            if (finalOrder && order == finalOrder) {
                finalRemoved = true;
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
    // The disconnected partition must still owe its final resolved ACK.
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
}

void CheckFinalResolvedAfterTopicSplit(bool restartDataShard) {
    TCdcFreezeEnv env;
    env.AddStream(true, false, 2, true);
    const auto shardActor = ResolveTablet(env.Runtime, env.Shards.at(0));

    TActorId streamSender;
    ui64 finalOrder = 0;
    THashMap<TActorId, ui64> partitionIds;
    THashSet<TActorId> finalRecipients;
    THashSet<ui64> completedPartitions;
    auto records = env.Runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvRecords>([&](auto& ev) {
        if (ev->Sender == shardActor) {
            streamSender = ev->GetRecipientRewrite();
        }
        for (const auto& record : ev->Get()->Records) {
            if (record->GetKind() == NChangeExchange::IChangeRecord::EKind::CdcHeartbeat) {
                UNIT_ASSERT(!finalOrder || finalOrder == record->GetOrder());
                finalOrder = record->GetOrder();
                if (partitionIds.contains(ev->GetRecipientRewrite())) {
                    finalRecipients.insert(ev->GetRecipientRewrite());
                }
            }
        }
    });
    bool priming = false;
    bool primed = false;
    auto ready = env.Runtime.AddObserver<NChangeExchange::TEvChangeExchangePrivate::TEvReady>([&](auto& ev) {
        const auto partitionId = ev->Get()->PartitionId;
        partitionIds[ev->Sender] = partitionId;
        if (priming && !finalOrder && ev->GetRecipientRewrite() == streamSender) {
            primed = true;
        }
        if (finalRecipients.contains(ev->Sender)) {
            completedPartitions.insert(partitionId);
        }
    });
    bool finalRemoved = false;
    auto removals = env.Runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>([&](auto& ev) {
        for (const auto order : ev->Get()->Records) {
            if (finalOrder && order == finalOrder) {
                finalRemoved = true;
            }
        }
    });

    // Resolve the original P0/P1 topology before splitting P1. Keep the sender's
    // metadata refresh paused, while the actual scheme operation completes.
    TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> initialAck(env.Runtime);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    env.Runtime.WaitFor("initial CDC write acknowledgement", [&] { return !initialAck.empty(); }, TDuration::Seconds(10));
    UNIT_ASSERT(streamSender);
    UNIT_ASSERT(initialAck.front()->Get()->IsSuccess());
    priming = true;
    initialAck.Stop().Unblock();
    env.Runtime.WaitFor("CDC sender using the original topic topology", [&] { return primed; }, TDuration::Seconds(10));
    TBlockEvents<TEvTxProxySchemeCache::TEvNavigateKeySetResult> resolving(env.Runtime, [&](const auto& ev) {
        return ev->GetRecipientRewrite() == streamSender;
    });

    auto splitRequest = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    auto& modify = *splitRequest->Record.MutableTransaction()->MutableModifyScheme();
    modify.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup);
    modify.SetWorkingDir("/Root/Table/Stream");
    modify.SetInternal(true);
    auto& topic = *modify.MutableAlterPersQueueGroup();
    topic.SetName("streamImpl");
    auto& split = *topic.AddSplit();
    split.SetPartition(1);
    // P1 is the upper half of the 128-bit hash space; split it at 3/4.
    split.SetSplitBoundary(TString(1, '\xc0'));
    env.Runtime.Send(new IEventHandle(MakeTxProxyID(), env.Sender, splitRequest.Release()));
    const auto splitResponse = env.Runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(env.Sender);
    UNIT_ASSERT_VALUES_EQUAL_C(splitResponse->Get()->Record.GetStatus(),
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress,
        splitResponse->Get()->Record.ShortDebugString());
    WaitTxNotification(env.Server, env.Sender, splitResponse->Get()->Record.GetTxId());
    UNIT_ASSERT_VALUES_EQUAL(finalOrder, 0);

    TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> childAcks(env.Runtime, [&](const auto& ev) {
        if (!finalOrder || !ev->Get()->IsSuccess()) {
            return false;
        }
        const auto& response = ev->Get()->Record.GetPartitionResponse();
        if (response.CmdWriteResultSize() != 1 || response.GetCmdWriteResult(0).GetSeqNo() != i64(finalOrder)) {
            return false;
        }
        const auto partitionId = partitionIds.at(ev->GetRecipientRewrite());
        // The unaffected P0 may confirm the broadcast while topology resolution
        // is paused. Both new children, P2 and P3, must remain unconfirmed.
        return partitionId != 0;
    });
    env.WaitStage(TStage::FENCE_WRITES);
    env.WaitStage(TStage::DRAIN_INDEXES);
    const auto draining = env.Stage(TStage::DRAIN_CDC);
    UNIT_ASSERT_VALUES_EQUAL(draining.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
    UNIT_ASSERT(!draining.GetReady());
    env.Runtime.WaitFor("final resolved confirmed by P0", [&] { return completedPartitions.contains(0); }, TDuration::Seconds(10));
    env.Runtime.WaitFor("CDC sender refreshing the old topology", [&] { return !resolving.empty(); }, TDuration::Seconds(10));
    UNIT_ASSERT(!finalRemoved);
    UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());

    resolving.Stop().Unblock();
    env.Runtime.WaitFor("final resolved acknowledgements from both split children", [&] { return childAcks.size() == 2; }, TDuration::Seconds(10));
    THashSet<ui64> childPartitions;
    for (const auto& event : childAcks) {
        childPartitions.insert(partitionIds.at(event->GetRecipientRewrite()));
    }
    UNIT_ASSERT_VALUES_EQUAL(childPartitions.size(), 2);
    UNIT_ASSERT(childPartitions.contains(2));
    UNIT_ASSERT(childPartitions.contains(3));
    UNIT_ASSERT(!finalRemoved);
    UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());

    const auto firstChild = partitionIds.at(childAcks.front()->GetRecipientRewrite());
    childAcks.Unblock(1);
    env.Runtime.WaitFor("one split child confirming the final resolved", [&] { return completedPartitions.contains(firstChild); }, TDuration::Seconds(10));
    UNIT_ASSERT(!finalRemoved);
    UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());

    if (restartDataShard) {
        // Both children have persisted the record, but the last ACK was not
        // delivered. After reboot, MaxSeqNo is an alternative delivery proof.
        TBlockEvents<NPQ::TEvPartitionWriter::TEvInitResult> recovered(env.Runtime);
        childAcks.Stop();
        childAcks.clear();
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        env.Runtime.WaitFor("recovered writers for P0 and both split children", [&] { return recovered.size() == 3; }, TDuration::Seconds(10));
        for (const auto& event : recovered) {
            UNIT_ASSERT(event->Get()->IsSuccess());
            UNIT_ASSERT(event->Get()->GetResult().SourceIdInfo.GetSeqNo() >= i64(finalOrder));
        }
        UNIT_ASSERT(!finalRemoved);
        UNIT_ASSERT(!env.Stage(TStage::DRAIN_CDC).GetReady());
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        recovered.Stop().Unblock();
    } else {
        childAcks.Stop().Unblock();
    }
    env.WaitStage(TStage::DRAIN_CDC);
    UNIT_ASSERT(finalRemoved);
    UNIT_ASSERT(completedPartitions.contains(0));
    UNIT_ASSERT(completedPartitions.contains(2));
    UNIT_ASSERT(completedPartitions.contains(3));
    records.Remove();
    ready.Remove();
    removals.Remove();
    env.Thaw();
    UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
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

    Y_UNIT_TEST(FinalResolvedWaitsForSplitTopicPartitions) {
        CheckFinalResolvedAfterTopicSplit(false);
    }

    Y_UNIT_TEST(FinalResolvedWaitsForSplitTopicPartitionsAfterDataShardRestart) {
        CheckFinalResolvedAfterTopicSplit(true);
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

    Y_UNIT_TEST_TWIN(SplitCannotStartAfterDatabaseFence, Reboot) {
        TFreezeEnv env;
        SetSplitMergePartCountLimit(&env.Runtime, -1);
        auto& flags = env.Runtime.GetAppData(0).FeatureFlags;
        flags.SetEnableDataShardInMemoryStateMigration(false);
        // Exercise the invariant without terminating the whole test process.
        flags.SetEnableTabletRestartOnUnhandledExceptions(true);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        const auto shardId = env.Shards.at(0);
        // Hold a real, otherwise valid split request before it reaches the source.
        TBlockEvents<TEvDataShard::TEvSplit> split(env.Runtime, [&](const auto& ev) {
            return ev->Sender != env.Sender;
        });
        const auto splitTxId = AsyncSplitTable(env.Server, env.Sender, "/Root/Table", shardId, 2);
        env.Runtime.WaitFor("split request before database fence", [&] { return !split.empty(); }, TDuration::Seconds(10));
        const auto splitRecord = split.front()->Get()->Record;
        const auto proposer = split.front()->Sender;
        WaitDatabaseFreezeStage(env.Runtime, shardId, env.TableId.PathId.OwnerId, env.Domain(), 1, TStage::FENCE_WRITES);
        if (Reboot) {
            RebootTablet(env.Runtime, shardId, env.Sender);
        }

        const auto alerts = GetServiceCounters(env.Runtime.GetAppData(0).Counters, "tablets")
            ->GetCounter("alerts_exception", true);
        const auto initialAlerts = alerts->Val();
        bool restarted = false;
        auto boots = env.Runtime.AddObserver<TEvTablet::TEvBoot>([&](auto& ev) {
            if (ev->Get()->TabletID == shardId) {
                restarted = true;
            }
        });
        size_t splitAcks = 0;
        auto acks = env.Runtime.AddObserver<TEvDataShard::TEvSplitAck>([&](auto& ev) {
            if (ev->Get()->Record.GetTabletId() == shardId
                && ev->Get()->Record.GetOperationCookie() == splitRecord.GetOperationCookie())
            {
                ++splitAcks;
            }
        });
        auto request = MakeHolder<TEvDataShard::TEvSplit>();
        request->Record.CopyFrom(splitRecord);
        env.Runtime.SendToPipe(shardId, env.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        env.Runtime.WaitFor("split protocol violation restarting the frozen shard", [&] {
            return restarted && alerts->Val() == initialAlerts + 1;
        }, TDuration::Seconds(10));
        WaitDatabaseFreezeStage(env.Runtime, shardId, env.TableId.PathId.OwnerId, env.Domain(), 1, TStage::FENCE_WRITES);
        UNIT_ASSERT_VALUES_EQUAL(splitAcks, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        const auto actor = ResolveTablet(env.Runtime, shardId);
        env.Runtime.RunCall([&] {
            auto* shard = dynamic_cast<TDataShard*>(env.Runtime.FindActor(actor));
            UNIT_ASSERT(shard);
            UNIT_ASSERT_VALUES_EQUAL(shard->GetState(), ui32(TShardState::Ready));
            UNIT_ASSERT(shard->IsDatabaseFrozen());
            return true;
        });
        boots.Remove();
        acks.Remove();

        // After an explicit thaw, the same real split can still finish normally.
        env.Thaw();
        split.Stop();
        split.clear();
        auto retry = MakeHolder<TEvDataShard::TEvSplit>();
        retry->Record.CopyFrom(splitRecord);
        env.Runtime.Send(new IEventHandle(actor, proposer, retry.Release()), 0, true);
        WaitTxNotification(env.Server, env.Sender, splitTxId);
        UNIT_ASSERT_VALUES_EQUAL(GetTableShards(env.Server, env.Sender, "/Root/Table").size(), 2);
        ExecSQL(env.Server, env.Sender, "UPSERT INTO `/Root/Table` (key, value) VALUES (2, 20);");
    }

    Y_UNIT_TEST_TWIN(DatabaseFreezeRejectsStartedSplit, Reboot) {
        TFreezeEnv env;
        SetSplitMergePartCountLimit(&env.Runtime, -1);
        env.Runtime.GetAppData(0).FeatureFlags.SetEnableDataShardInMemoryStateMigration(false);
        const auto shardId = env.Shards.at(0);
        const ui64 txId = 1001;
        const auto prepared = SendFreezeTestWrite(env, shardId, txId, 1, 10,
            NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        UNIT_ASSERT_VALUES_EQUAL(prepared.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        NKikimrTxDataShard::TEvSplit splitRecord;
        auto splits = env.Runtime.AddObserver<TEvDataShard::TEvSplit>([&](auto& ev) {
            splitRecord.CopyFrom(ev->Get()->Record);
        });
        const auto actor = ResolveTablet(env.Runtime, shardId);
        auto* shard = dynamic_cast<TDataShard*>(env.Runtime.FindActor(actor));
        UNIT_ASSERT(shard);
        const auto splitTxId = AsyncSplitTable(env.Server, env.Sender, "/Root/Table", shardId, 2);
        env.Runtime.WaitFor("split waiting for the prepared transaction", [&] {
            return shard->GetState() == TShardState::SplitSrcWaitForNoTxInFlight;
        }, TDuration::Seconds(10));
        UNIT_ASSERT(splitRecord.HasOperationCookie());
        splits.Remove();
        if (Reboot) {
            RebootTablet(env.Runtime, shardId, env.Sender);
        }
        const auto checkRejected = [&] {
            const auto result = SendDatabaseFreeze(env.Runtime, shardId, env.TableId.PathId.OwnerId,
                env.Domain(), 1, TStage::FENCE_WRITES);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), ui32(Ydb::StatusIds::PRECONDITION_FAILED), result.ShortDebugString());
            UNIT_ASSERT(result.GetReady());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssue(), "stable shard topology");
            const auto current = ResolveTablet(env.Runtime, shardId);
            env.Runtime.RunCall([&] {
                auto* source = dynamic_cast<TDataShard*>(env.Runtime.FindActor(current));
                UNIT_ASSERT(source);
                UNIT_ASSERT(!source->IsDatabaseFrozen());
                return true;
            });
        };
        checkRejected();
        // Retrying an already accepted split is still allowed.
        auto retry = MakeHolder<TEvDataShard::TEvSplit>();
        retry->Record.CopyFrom(splitRecord);
        env.Runtime.SendToPipe(shardId, env.Sender, retry.Release(), 0, GetPipeConfigWithRetries());
        checkRejected();
        SendProposeToCoordinator(env.Runtime, env.Sender, {shardId}, {
            .TxId = txId,
            .Coordinator = prepared.GetDomainCoordinators(0),
            .MinStep = prepared.GetMinStep(),
            .MaxStep = prepared.GetMaxStep(),
        });
        WaitTxNotification(env.Server, env.Sender, splitTxId);
        const auto children = GetTableShards(env.Server, env.Sender, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(children.size(), 2);
        for (const auto child : children) {
            FreezeDatabaseShard(env.Runtime, child, env.TableId.PathId.OwnerId, env.Domain(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, children, env.TableId), "key = 1, value = 10\n");
    }

    Y_UNIT_TEST(DatabaseFreezeRejectsReceivingSplitDestination) {
        TFreezeEnv env;
        SetSplitMergePartCountLimit(&env.Runtime, -1);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        THashSet<ui64> destinations;
        auto inits = env.Runtime.AddObserver<TEvDataShard::TEvInitSplitMergeDestination>([&](auto& ev) {
            for (const auto& range : ev->Get()->Record.GetSplitDescription().GetDestinationRanges()) {
                destinations.insert(range.GetTabletID());
            }
        });
        TBlockEvents<TEvDataShard::TEvSplitTransferSnapshot> snapshots(env.Runtime);
        const auto splitTxId = AsyncSplitTable(env.Server, env.Sender, "/Root/Table", env.Shards.at(0), 2);
        env.Runtime.WaitFor("split snapshots withheld from the destinations", [&] {
            return snapshots.size() == 2;
        }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(destinations.size(), 2);
        for (const auto child : destinations) {
            const auto result = SendDatabaseFreeze(env.Runtime, child, env.TableId.PathId.OwnerId,
                env.Domain(), 1, TStage::FENCE_WRITES);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), ui32(Ydb::StatusIds::PRECONDITION_FAILED), result.ShortDebugString());
            UNIT_ASSERT(result.GetReady());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssue(), "stable shard topology");
        }
        inits.Remove();
        snapshots.Stop().Unblock();
        WaitTxNotification(env.Server, env.Sender, splitTxId);
        for (const auto child : destinations) {
            FreezeDatabaseShard(env.Runtime, child, env.TableId.PathId.OwnerId, env.Domain(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, GetTableShards(env.Server, env.Sender, "/Root/Table"), env.TableId),
            "key = 1, value = 10\n");
    }

    Y_UNIT_TEST_TWIN(LargeAcceptedTxIdsSurviveRestart, MaxConfiguredLimit) {
        TFreezeEnv env;
        env.Runtime.GetAppData(0).FeatureFlags.SetEnableDataShardInMemoryStateMigration(false);
        const auto* field = NKikimrConfig::TImmediateControlsConfig::TDataShardControls::descriptor()
            ->FindFieldByName("MaxTxInFly");
        UNIT_ASSERT(field);
        UNIT_ASSERT(field->options().HasExtension(NKikimrConfig::ControlOptions));
        const auto& limits = field->options().GetExtension(NKikimrConfig::ControlOptions);
        // CheckInflightLimit uses '>', so include the boundary transaction.
        const ui64 txCount = (MaxConfiguredLimit ? limits.GetMaxValue() : limits.GetDefaultValue()) + 1;
        UNIT_ASSERT(txCount > 1);
        const auto shardActor = ResolveTablet(env.Runtime, env.Shards.at(0));
        env.Runtime.RunCall([&] {
            auto* shard = dynamic_cast<TDataShard*>(env.Runtime.FindActor(shardActor));
            UNIT_ASSERT(shard);
            UNIT_ASSERT_VALUES_EQUAL(shard->GetMaxTxInFly(), limits.GetDefaultValue());
            return true;
        });

        // Exercise the actual Sys cell without starting thousands of user
        // transactions. All IDs need ten varint bytes, the worst case for uint64.
        TStage state;
        state.SetEpoch(1);
        state.SetStage(TStage::FENCE_WRITES);
        for (ui64 i = 0; i < txCount; ++i) {
            state.AddAcceptedTxIds(Max<ui64>() - i);
        }
        const auto serialized = state.SerializeAsString();
        UNIT_ASSERT_C(serialized.size() >= 10 * txCount,
            "Expected worst-case txId payload: count# " << txCount << " bytes# " << serialized.size());
        const auto query = [&](const TString& program) {
            auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
            request->Record.MutableProgram()->MutableProgram()->SetText(program);
            const auto sender = env.Runtime.AllocateEdgeActor();
            env.Runtime.SendToPipe(env.Shards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());
            const auto response = env.Runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrProto::OK,
                response->Get()->Record.ShortDebugString());
            return response->Get()->Record.GetExecutionEngineEvaluatedResponse();
        };
        // Sys_DatabaseFreeze = 49; store the same Bytes cell as PersistSys.
        TStringStream literal;
        NYql::EscapeArbitraryAtom(serialized, '"', &literal);
        query(Sprintf(R"(
            (
                (let key '('('Id (Uint64 '49))))
                (let update '('('Bytes (String '%s))))
                (return (AsList (UpdateRow 'Sys key update)))
            )
        )", literal.Str().c_str()));
        const auto readState = [&] {
            const auto result = query(R"(
                (
                    (let key '('('Id (Uint64 '49))))
                    (return (AsList (SetResult 'Freeze (SelectRow 'Sys key '('Bytes)))))
                )
            )");
            const TString bytes = NClient::TValue::Create(result)["Freeze"]["Bytes"];
            TStage restored;
            UNIT_ASSERT(restored.ParseFromString(bytes));
            return restored;
        };
        const auto checkState = [&](TStage::EStage stage) {
            const auto restored = readState();
            UNIT_ASSERT_VALUES_EQUAL(restored.GetEpoch(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ui32(restored.GetStage()), ui32(stage));
            UNIT_ASSERT_VALUES_EQUAL(ui64(restored.AcceptedTxIdsSize()), txCount);
            for (size_t i = 0; i < restored.AcceptedTxIdsSize(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(restored.GetAcceptedTxIds(i), Max<ui64>() - ui64(i));
            }
        };
        checkState(TStage::FENCE_WRITES);
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        checkState(TStage::FENCE_WRITES);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);

        // The normal freeze handler must also be able to rewrite the large
        // protobuf on stage transitions, and restore it on the next boot.
        env.Freeze();
        checkState(TStage::DRAIN_CDC);
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        checkState(TStage::DRAIN_CDC);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
        env.Thaw();
        const auto opened = readState();
        UNIT_ASSERT_VALUES_EQUAL(ui32(opened.GetStage()), ui32(TStage::OPEN));
        UNIT_ASSERT_VALUES_EQUAL(opened.GetEpoch(), 2);
        UNIT_ASSERT_VALUES_EQUAL(opened.AcceptedTxIdsSize(), 0);
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(UnknownFreezeStageDoesNotDeserializeAsOpen) {
        const auto check = [](auto message) {
            UNIT_ASSERT(!message.HasStage());
            UNIT_ASSERT_VALUES_EQUAL(ui32(message.GetStage()), ui32(TStage::UNKNOWN));
            const auto* field = message.GetDescriptor()->FindFieldByName("Stage");
            UNIT_ASSERT(field);
            message.GetReflection()->MutableUnknownFields(&message)->AddVarint(field->number(), Max<ui32>());
            const auto serialized = message.SerializeAsString();
            message.SetStage(TStage::OPEN);
            UNIT_ASSERT(message.ParseFromString(serialized));
            UNIT_ASSERT(!message.HasStage());
            UNIT_ASSERT_VALUES_EQUAL(ui32(message.GetStage()), ui32(TStage::UNKNOWN));
            const auto& unknown = message.GetReflection()->GetUnknownFields(message);
            UNIT_ASSERT_VALUES_EQUAL(unknown.field_count(), 1);
            UNIT_ASSERT_VALUES_EQUAL(unknown.field(0).number(), field->number());
            UNIT_ASSERT_VALUES_EQUAL(unknown.field(0).varint(), Max<ui32>());
        };
        check(TStage{});
        check(NKikimrTxDataShard::TEvDatabaseFreeze{});
        check(NKikimrTxDataShard::TEvDatabaseFreezeResult{});
    }

    Y_UNIT_TEST(InvalidFreezeStageCannotThawOrAdvanceEpoch) {
        TFreezeEnv env;
        env.Runtime.GetAppData(0).FeatureFlags.SetEnableDataShardInMemoryStateMigration(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(1, 10).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        env.Freeze(10);

        // Cover a missing stage, explicit UNKNOWN, and an unsupported wire value.
        TVector<NKikimrTxDataShard::TEvDatabaseFreeze> invalid(3);
        invalid[1].SetStage(TStage::UNKNOWN);
        invalid[2].GetReflection()->MutableUnknownFields(&invalid[2])->AddVarint(
            NKikimrTxDataShard::TEvDatabaseFreeze::kStageFieldNumber, Max<ui32>());
        for (const auto& value : invalid) {
            auto request = MakeHolder<TEvDataShard::TEvDatabaseFreeze>();
            UNIT_ASSERT(request->Record.ParseFromString(value.SerializeAsString()));
            UNIT_ASSERT_VALUES_EQUAL(ui32(request->Record.GetStage()), ui32(TStage::UNKNOWN));
            request->Record.SetEpoch(11);
            request->Record.SetSchemeShardId(env.TableId.PathId.OwnerId);
            request->Record.SetDomainOwnerId(env.Domain().OwnerId);
            request->Record.SetDomainLocalPathId(env.Domain().LocalPathId);
            env.Runtime.SendToPipe(env.Shards.at(0), env.Sender, request.Release(), 0, GetPipeConfigWithRetries());
            const auto response = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvDatabaseFreezeResult>(env.Sender);
            const auto& result = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), ui32(Ydb::StatusIds::BAD_REQUEST), result.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
            const auto current = SendDatabaseFreeze(env.Runtime, env.Shards.at(0), env.TableId.PathId.OwnerId,
                env.Domain(), 10, TStage::DRAIN_CDC);
            UNIT_ASSERT_VALUES_EQUAL_C(current.GetStatus(), ui32(Ydb::StatusIds::SUCCESS), current.ShortDebugString());
            UNIT_ASSERT(current.GetReady());
        }

        // OPEN must be persisted explicitly and survive recovery from storage.
        env.Thaw(11);
        RebootTablet(env.Runtime, env.Shards.at(0), env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(env.Write(2, 20).GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
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
