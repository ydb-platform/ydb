#include "datashard_impl.h"
#include "datashard_ut_common_kqp.h"

#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/ut_common/database_freeze.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <google/protobuf/descriptor.h>

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
