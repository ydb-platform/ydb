#include "defs.h"
#include "datashard_ut_common_kqp.h"
#include "datashard_ut_read_table.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;
using namespace NDataShardReadTableTest;
using namespace NWilson;

Y_UNIT_TEST_SUITE(TDataShardTrace) {
    void ExecSQL(Tests::TServer::TPtr server,
             TActorId sender,
             const TString &sql,
             Ydb::StatusIds::StatusCode code,
             NWilson::TTraceId traceId = {})
    {
        google::protobuf::Arena arena;
        auto &runtime = *server->GetRuntime();
        TAutoPtr<IEventHandle> handle;

        THolder<NKqp::TEvKqp::TEvQueryRequest> request;
        if (traceId) {
            struct RequestCtx : NGRpcService::IRequestCtxMtSafe {
                RequestCtx(NWilson::TTraceId &&traceId) : TraceId(std::move(traceId)) {}

                NWilson::TTraceId GetWilsonTraceId() const override {
                    return TraceId.Clone();
                }

                TMaybe<TString> GetTraceId() const override {
                    return Nothing();
                }

                const TMaybe<TString> GetPeerMetaValues(const TString&) const override {
                    return Nothing();
                }
 
                TString GetPeerName() const override {
                    return {};
                }

                const TString& GetRequestName() const override {
                    static TString empty;
                    return empty;
                }

                TMaybe<NRpcService::TRlPath> GetRlPath() const override {
                    return Nothing();
                }

                TInstant GetDeadline() const override {
                    return TInstant::Max();
                }

                const TMaybe<TString> GetDatabaseName() const override {
                    return "";
                }

                const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
                    return Ptr;
                }

                const TString& GetSerializedToken() const override {
                    return Token;
                }

                bool IsClientLost() const override {
                    return false;
                };

                virtual const google::protobuf::Message* GetRequest() const override {
                    return nullptr;
                };

                const TMaybe<TString> GetRequestType() const override {
                    return "_document_api_request";
                };

                void SetFinishAction(std::function<void()>&& cb) override {
                    Y_UNUSED(cb);
                };

                google::protobuf::Arena* GetArena() override {
                    return nullptr;
                };

                TIntrusiveConstPtr<NACLib::TUserToken> Ptr;
                TString Token;
                NWilson::TTraceId TraceId;
            };
            
            auto *txControl = google::protobuf::Arena::CreateMessage<Ydb::Table::TransactionControl>(&arena);
            txControl->mutable_begin_tx()->mutable_serializable_read_write();
            txControl->set_commit_tx(true);

            auto ptr = std::make_shared<RequestCtx>(std::move(traceId));
            request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>(
                NKikimrKqp::QUERY_ACTION_EXECUTE,
                NKikimrKqp::QUERY_TYPE_SQL_DML,
                TActorId(),
                ptr,
                TString(), //sessionId
                TString(sql),
                TString(), //queryId
                txControl, //tx_control
                nullptr, //ydbParameters
                Ydb::Table::QueryStatsCollection::STATS_COLLECTION_UNSPECIFIED, //collectStats
                nullptr, // query_cache_policy
                nullptr //operationParams
            );
        } else {
            request = MakeSQLRequest(sql, true);
        }
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release(), 0, 0, nullptr));
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetRef().GetYdbStatus(), code);
    }

    void SplitTable(TTestActorRuntime &runtime, Tests::TServer::TPtr server, ui64 splitKey) {
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);
        auto senderSplit = runtime.AllocateEdgeActor();
        auto tablets = GetTableShards(server, senderSplit, "/Root/table-1");
        UNIT_ASSERT(tablets.size() == 1);
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", tablets.at(0), splitKey);
        WaitTxNotification(server, senderSplit, txId);
        tablets = GetTableShards(server, senderSplit, "/Root/table-1");
        UNIT_ASSERT(tablets.size() == 2);
    }

    std::tuple<TTestActorRuntime&, Tests::TServer::TPtr, TActorId> TestCreateServer() {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);

        return {runtime, server, sender};
    }

    void CheckTxHasWriteLog(std::reference_wrapper<TFakeWilsonUploader::Span> txSpan) {
        auto writeLogSpan = txSpan.get().FindOne("Tablet.WriteLog");
        UNIT_ASSERT(writeLogSpan);
        auto writeLogEntrySpan = writeLogSpan->get().FindOne("Tablet.WriteLog.LogEntry");
        UNIT_ASSERT(writeLogEntrySpan);
    }

    void CheckTxHasDatashardUnits(std::reference_wrapper<TFakeWilsonUploader::Span> txSpan, ui8 count) {
        auto executeSpan = txSpan.get().FindOne("Tablet.Transaction.Execute");
        UNIT_ASSERT(executeSpan);
        auto unitSpans = executeSpan->get().FindAll("Datashard.Unit");
        UNIT_ASSERT_VALUES_EQUAL(count, unitSpans.size());
    }

    void CheckExecuteHasDatashardUnits(std::reference_wrapper<TFakeWilsonUploader::Span> executeSpan, ui8 count) {
        auto unitSpans = executeSpan.get().FindAll("Datashard.Unit");
        UNIT_ASSERT_VALUES_EQUAL(count, unitSpans.size());
    }

    Y_UNIT_TEST(TestTraceDistributedUpsert) {
        auto [runtime, server, sender] = TestCreateServer();

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
    
        TFakeWilsonUploader *uploader = new TFakeWilsonUploader();
        TActorId uploaderId = runtime.Register(uploader, 0);
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), uploaderId, 0); 
        runtime.SimulateSleep(TDuration::Seconds(10));

        const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

        SplitTable(runtime, server, 5);

        NWilson::TTraceId traceId = NWilson::TTraceId::NewTraceId(15, 4095);
        ExecSQL(
            server,
            sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500), (7, 700), (9, 900);",
            Ydb::StatusIds::SUCCESS,
            std::move(traceId)
        );

        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT_EQUAL(1, uploader->Traces.size());

        TFakeWilsonUploader::Trace &trace = uploader->Traces.begin()->second;

        auto deSpan = trace.Root.BFSFindOne("DataExecuter");
        UNIT_ASSERT(deSpan);

        auto dsTxSpans = deSpan->get().FindAll("Datashard.Transaction");
        UNIT_ASSERT_EQUAL(2, dsTxSpans.size()); // Two shards, each executes a user transaction.

        for (auto dsTxSpan : dsTxSpans) {
            auto tabletTxs = dsTxSpan.get().FindAll("Tablet.Transaction");
            UNIT_ASSERT_EQUAL(2, tabletTxs.size()); // Each shard executes a proposal tablet tx and a progress tablet tx.

            auto propose = tabletTxs[0];
            // Note: when volatile transactions are enabled propose doesn't persist anything
            if (!usesVolatileTxs) {
                CheckTxHasWriteLog(propose);
            }
            CheckTxHasDatashardUnits(propose, 3);

            auto progress = tabletTxs[1];
            CheckTxHasWriteLog(progress); 
            CheckTxHasDatashardUnits(progress, usesVolatileTxs ? 6 : 11);
        }

        std::string canon;
        if (usesVolatileTxs) {
            canon = "(Session.query.QUERY_ACTION_EXECUTE -> [(CompileService -> [(CompileActor)]) , "
                "(LiteralExecuter) , (DataExecuter -> [(WaitForTableResolve) , (RunTasks) , (Datashard.Transaction -> "
                "[(Tablet.Transaction -> [(Tablet.Transaction.Execute -> [(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)])"
                "]) , (Tablet.Transaction -> [(Tablet.Transaction.Execute -> "
                "[(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)"
                "]) , (Tablet.WriteLog -> "
                "[(Tablet.WriteLog.LogEntry)])])]) , (Datashard.Transaction -> [(Tablet.Transaction -> [(Tablet.Transaction.Execute -> "
                "[(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)])]) , "
                "(Tablet.Transaction -> [(Tablet.Transaction.Execute -> [(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , "
                "(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)"
                "]) , (Tablet.WriteLog -> [(Tablet.WriteLog.LogEntry)])])])])])";
        } else {
            canon = "(Session.query.QUERY_ACTION_EXECUTE -> [(CompileService -> [(CompileActor)]) , "
                "(LiteralExecuter) , (DataExecuter -> [(WaitForTableResolve) , (RunTasks) , (Datashard.Transaction -> "
                "[(Tablet.Transaction -> [(Tablet.Transaction.Execute -> [(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , "
                "(Tablet.WriteLog -> [(Tablet.WriteLog.LogEntry)])]) , (Tablet.Transaction -> [(Tablet.Transaction.Execute -> "
                "[(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , "
                "(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.WriteLog -> "
                "[(Tablet.WriteLog.LogEntry)])])]) , (Datashard.Transaction -> [(Tablet.Transaction -> [(Tablet.Transaction.Execute -> "
                "[(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.WriteLog -> [(Tablet.WriteLog.LogEntry)])]) , "
                "(Tablet.Transaction -> [(Tablet.Transaction.Execute -> [(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , "
                "(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , "
                "(Datashard.Unit) , (Datashard.Unit)]) , (Tablet.WriteLog -> [(Tablet.WriteLog.LogEntry)])])])])])";
        }

        UNIT_ASSERT_VALUES_EQUAL(canon, trace.ToString());
    }

    Y_UNIT_TEST(TestTraceDistributedSelect) {
        auto [runtime, server, sender] = TestCreateServer();

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
    
        TFakeWilsonUploader *uploader = new TFakeWilsonUploader();
        TActorId uploaderId = runtime.Register(uploader, 0);
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), uploaderId, 0); 
        runtime.SimulateSleep(TDuration::Seconds(10));

        SplitTable(runtime, server, 5);

        ExecSQL(
            server,
            sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500), (7, 700), (9, 900);",
            Ydb::StatusIds::SUCCESS
        );

        ExecSQL(
            server,
            sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 100), (4, 300), (6, 500), (8, 700), (10, 900);",
            Ydb::StatusIds::SUCCESS
        );

        {
            // Compact and restart, so that upon SELECT we will go and load data from BS.
            auto senderCompact = runtime.AllocateEdgeActor();
            auto shards = GetTableShards(server, senderCompact, "/Root/table-1");
            for (auto shard: shards) {
                auto [tables, ownerId] = GetTables(server, shard);
                auto compactionResult = CompactTable(runtime, shard, TTableId(ownerId, tables["table-1"].GetPathId()), true);
                UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
            }

            for (auto shard: shards) {
                TActorId sender = runtime.AllocateEdgeActor();
                GracefulRestartTablet(runtime, shard, sender);
            }
        }

        NWilson::TTraceId traceId = NWilson::TTraceId::NewTraceId(15, 4095);

        ExecSQL(
            server,
            sender,
            "SELECT * FROM `/Root/table-1` WHERE key = 1 OR key = 3 OR key = 5 OR key = 7 OR key = 9;",
            Ydb::StatusIds::SUCCESS,
            std::move(traceId)
        );

        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT_EQUAL(1, uploader->Traces.size());

        TFakeWilsonUploader::Trace &trace = uploader->Traces.begin()->second;

        std::string canon;
        if (server->GetSettings().AppConfig->GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup() || server->GetSettings().AppConfig->GetTableServiceConfig().GetPredicateExtract20()) {
            auto readActorSpan = trace.Root.BFSFindOne("ReadActor");
            UNIT_ASSERT(readActorSpan);

            auto dsReads = readActorSpan->get().FindAll("Datashard.Read"); // Read actor sends EvRead to each shard.
            UNIT_ASSERT_EQUAL(dsReads.size(), 2);

            canon = "(Session.query.QUERY_ACTION_EXECUTE -> [(CompileService -> [(CompileActor)]) , (LiteralExecuter) "
                ", (DataExecuter -> [(WaitForTableResolve) , (WaitForShardsResolve) , (WaitForSnapshot) , (ComputeActor) "
                ", (RunTasks) , (KqpNode.SendTasks) , (ComputeActor -> [(ReadActor -> [(WaitForShardsResolve) , (Datashard.Read "
                "-> [(Tablet.Transaction -> [(Tablet.Transaction.Execute -> [(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) "
                ", (Tablet.Transaction.Wait) , (Tablet.Transaction.Enqueued) , (Tablet.Transaction.Execute -> [(Datashard.Unit)]) "
                ", (Tablet.Transaction.Wait) , (Tablet.Transaction.Enqueued) , (Tablet.Transaction.Execute -> [(Datashard.Unit) "
                ", (Datashard.Unit)]) , (Tablet.WriteLog -> [(Tablet.WriteLog.LogEntry)])])]) , (Datashard.Read -> [(Tablet.Transaction "
                "-> [(Tablet.Transaction.Execute -> [(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.Transaction.Wait) "
                ", (Tablet.Transaction.Enqueued) , (Tablet.Transaction.Execute -> [(Datashard.Unit)]) , (Tablet.Transaction.Wait) "
                ", (Tablet.Transaction.Enqueued) , (Tablet.Transaction.Execute -> [(Datashard.Unit) , (Datashard.Unit)]) , (Tablet.WriteLog "
                "-> [(Tablet.WriteLog.LogEntry)])])])])])])])";
        } else {
            auto deSpan = trace.Root.BFSFindOne("DataExecuter");
            UNIT_ASSERT(deSpan);

            auto dsTxSpans = deSpan->get().FindAll("Datashard.Transaction");
            UNIT_ASSERT_EQUAL(2, dsTxSpans.size()); // Two shards, each executes a user transaction.

            for (auto dsTxSpan : dsTxSpans) {
                auto tabletTxs = dsTxSpan.get().FindAll("Tablet.Transaction");
                UNIT_ASSERT_EQUAL(1, tabletTxs.size());

                auto propose = tabletTxs[0];
                CheckTxHasWriteLog(propose);

                // Blobs are loaded from BS.
                UNIT_ASSERT_EQUAL(2, propose.get().FindAll("Tablet.Transaction.Wait").size());
                UNIT_ASSERT_EQUAL(2, propose.get().FindAll("Tablet.Transaction.Enqueued").size());

                // We execute tx multiple times, because we have to load data for it to execute.
                auto executeSpans = propose.get().FindAll("Tablet.Transaction.Execute");
                UNIT_ASSERT_EQUAL(3, executeSpans.size());

                CheckExecuteHasDatashardUnits(executeSpans[0], 3);
                CheckExecuteHasDatashardUnits(executeSpans[1], 1);
                CheckExecuteHasDatashardUnits(executeSpans[2], 3);
            }

            canon = "(Session.query.QUERY_ACTION_EXECUTE -> [(CompileService -> [(CompileActor)]) "
                ", (LiteralExecuter) , (DataExecuter -> [(WaitForTableResolve) , (WaitForSnapshot) , (ComputeActor) , (RunTasks) , "
                "(Datashard.Transaction -> [(Tablet.Transaction -> [(Tablet.Transaction.Execute -> [(Datashard.Unit) , "
                "(Datashard.Unit) , (Datashard.Unit)]) , (Tablet.Transaction.Wait) , (Tablet.Transaction.Enqueued) , "
                "(Tablet.Transaction.Execute -> [(Datashard.Unit)]) , (Tablet.Transaction.Wait) , (Tablet.Transaction.Enqueued) , "
                "(Tablet.Transaction.Execute -> [(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.WriteLog -> "
                "[(Tablet.WriteLog.LogEntry)])])]) , (Datashard.Transaction -> [(Tablet.Transaction -> [(Tablet.Transaction.Execute -> "
                "[(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.Transaction.Wait) , (Tablet.Transaction.Enqueued) , "
                "(Tablet.Transaction.Execute -> [(Datashard.Unit)]) , (Tablet.Transaction.Wait) , (Tablet.Transaction.Enqueued) , "
                "(Tablet.Transaction.Execute -> [(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.WriteLog -> "
                "[(Tablet.WriteLog.LogEntry)])])])])])";
        }
        
        
        UNIT_ASSERT_VALUES_EQUAL(canon, trace.ToString());
    }

    Y_UNIT_TEST(TestTraceDistributedSelectViaReadActors) {
        auto [runtime, server, sender] = TestCreateServer();

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);

        TFakeWilsonUploader* uploader = new TFakeWilsonUploader();
        TActorId uploaderId = runtime.Register(uploader, 0);
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), uploaderId, 0);
        runtime.SimulateSleep(TDuration::Seconds(10));

        SplitTable(runtime, server, 5);

        ExecSQL(
            server,
            sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500), (7, 700), (9, 900);",
            Ydb::StatusIds::SUCCESS
        );

        ExecSQL(
            server,
            sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 100), (4, 300), (6, 500), (8, 700), (10, 900);",
            Ydb::StatusIds::SUCCESS
        );

        NWilson::TTraceId traceId = NWilson::TTraceId::NewTraceId(15, 4095);

        ExecSQL(
            server,
            sender,
            "SELECT * FROM `/Root/table-1`;",
            Ydb::StatusIds::SUCCESS,
            std::move(traceId)
        );

        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT_EQUAL(1, uploader->Traces.size());

        TFakeWilsonUploader::Trace& trace = uploader->Traces.begin()->second;

        auto readActorSpan = trace.Root.BFSFindOne("ReadActor");
        UNIT_ASSERT(readActorSpan);

        auto dsReads = readActorSpan->get().FindAll("Datashard.Read"); // Read actor sends EvRead to each shard.
        UNIT_ASSERT_EQUAL(dsReads.size(), 2);

        std::string canon = "(Session.query.QUERY_ACTION_EXECUTE -> [(CompileService -> [(CompileActor)]) , "
            "(DataExecuter -> [(WaitForTableResolve) , (WaitForShardsResolve) , (WaitForSnapshot) , (ComputeActor) , "
            "(RunTasks) , (KqpNode.SendTasks) , (ComputeActor -> [(ReadActor -> [(WaitForShardsResolve) , "
            "(Datashard.Read -> [(Tablet.Transaction -> [(Tablet.Transaction.Execute -> [(Datashard.Unit) , "
            "(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.WriteLog -> [(Tablet.WriteLog.LogEntry)])])"
            "]) , (Datashard.Read -> [(Tablet.Transaction -> [(Tablet.Transaction.Execute -> "
            "[(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.WriteLog -> "
            "[(Tablet.WriteLog.LogEntry)])])])])])])])";
        UNIT_ASSERT_VALUES_EQUAL(canon, trace.ToString());
    }

    Y_UNIT_TEST(TestTraceWriteImmediateOnShard) {
        auto [runtime, server, sender] = TestCreateServer();

        auto opts = TShardedTableOptions().Columns({{"key", "Uint32", true, false}, {"value", "Uint32", false, false}});
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        TFakeWilsonUploader *uploader = new TFakeWilsonUploader();
        TActorId uploaderId = runtime.Register(uploader, 0);
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), uploaderId, 0); 
        runtime.SimulateSleep(TDuration::Seconds(10));

        NWilson::TTraceId traceId = NWilson::TTraceId::NewTraceId(15, 4095);
        const ui32 rowCount = 3;
        ui64 txId = 100;
        Write(runtime, sender, shards[0], tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED, std::move(traceId));

        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT_EQUAL(1, uploader->Traces.size());

        TFakeWilsonUploader::Trace &trace = uploader->Traces.begin()->second;
        
        auto wtSpan = trace.Root.BFSFindOne("Datashard.WriteTransaction");
        UNIT_ASSERT(wtSpan);
        
        auto tabletTxs = wtSpan->get().FindAll("Tablet.Transaction");
        UNIT_ASSERT_EQUAL(1, tabletTxs.size());
        auto writeTx = tabletTxs[0];

        CheckTxHasWriteLog(writeTx); 
        CheckTxHasDatashardUnits(writeTx, 5); 

        std::string canon = "(Datashard.WriteTransaction -> [(Tablet.Transaction -> [(Tablet.Transaction.Execute -> "
        "[(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.WriteLog -> "
        "[(Tablet.WriteLog.LogEntry)])])])";
        UNIT_ASSERT_VALUES_EQUAL(canon, trace.ToString());
    }
}

} // namespace NKikimr
