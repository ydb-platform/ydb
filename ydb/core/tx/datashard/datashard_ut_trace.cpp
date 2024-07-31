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

        THolder<NKqp::TEvKqp::TEvQueryRequest> request = MakeSQLRequest(sql, true);
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release(), 0, 0, nullptr, std::move(traceId)));
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

    template<class TValue>
    struct TRepeat {
        TValue Value;
        size_t Count;
    };

    template<class TValue>
    TRepeat<std::decay_t<TValue>> Repeat(TValue&& value, size_t count) {
        return TRepeat<std::decay_t<TValue>>{ std::forward<TValue>(value), count };
    }

    template<class TValue>
    struct TConditional {
        bool Condition;
        TValue Value;
    };

    template<class TValue>
    TConditional<std::decay_t<TValue>> Conditional(bool condition, TValue&& value) {
        return TConditional<std::decay_t<TValue>>{ condition, std::forward<TValue>(value) };
    }

    struct TExpectedSpan {
        std::string_view Name;
        std::vector<TExpectedSpan> Children;

        explicit TExpectedSpan(std::string_view name)
            : Name(name)
        {}

        void AddChild(std::string_view name) {
            Children.emplace_back(name);
        }

        void AddChild(const TExpectedSpan& span) {
            Children.push_back(span);
        }

        void AddChild(TExpectedSpan&& span) {
            Children.push_back(std::move(span));
        }

        void AddChild(const std::vector<TExpectedSpan>& children) {
            for (auto& child : children) {
                Children.push_back(child);
            }
        }

        void AddChild(std::vector<TExpectedSpan>&& children) {
            for (auto& child : children) {
                Children.push_back(std::move(child));
            }
        }

        template<class TValue>
        void AddChild(const TRepeat<TValue>& repeat) {
            for (size_t i = 0; i < repeat.Count; ++i) {
                AddChild(repeat.Value);
            }
        }

        template<class TValue>
        void AddChild(const TConditional<TValue>& conditional) {
            if (conditional.Condition) {
                AddChild(conditional.Value);
            }
        }

        template<class TValue>
        void AddChild(TConditional<TValue>&& conditional) {
            if (conditional.Condition) {
                AddChild(std::move(conditional.Value));
            }
        }

        void ToString(std::string& out) const {
            out.append("(");
            out.append(Name);
            if (!Children.empty()) {
                out.append(" -> [");
                bool first = true;
                for (auto& child : Children) {
                    if (first) {
                        first = false;
                    } else {
                        out.append(" , ");
                    }
                    child.ToString(out);
                }
                out.append("]");
            }
            out.append(")");
        }

        std::string ToString() const {
            std::string out;
            ToString(out);
            return out;
        }
    };

    template<class... TArgs>
    TExpectedSpan ExpectedSpan(std::string_view name, TArgs&&... args) {
        TExpectedSpan span(name);
        (span.AddChild(std::forward<TArgs>(args)), ...);
        return span;
    }

    template<class... TArgs>
    std::vector<TExpectedSpan> ExpectedSpanVec(TArgs&&... args) {
        std::vector<TExpectedSpan> vec;
        (vec.emplace_back(std::forward<TArgs>(args)), ...);
        return vec;
    }

    template<class... TArgs>
    TConditional<std::vector<TExpectedSpan>> ConditionalSpanVec(bool condition, TArgs&&... args) {
        return Conditional(condition, ExpectedSpanVec(std::forward<TArgs>(args)...));
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
        UNIT_ASSERT_VALUES_EQUAL(1, uploader->Traces.size());

        TFakeWilsonUploader::Trace &trace = uploader->Traces.begin()->second;

        auto deSpan = trace.Root.BFSFindOne("DataExecuter");
        UNIT_ASSERT(deSpan);

        auto dsTxSpans = deSpan->get().FindAll("Datashard.Transaction");
        UNIT_ASSERT_VALUES_EQUAL(2, dsTxSpans.size()); // Two shards, each executes a user transaction.

        for (auto dsTxSpan : dsTxSpans) {
            auto tabletTxs = dsTxSpan.get().FindAll("Tablet.Transaction");
            UNIT_ASSERT_VALUES_EQUAL(2, tabletTxs.size()); // Each shard executes a proposal tablet tx and a progress tablet tx.

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

        std::string canon = ExpectedSpan("Session.query.QUERY_ACTION_EXECUTE",
            ExpectedSpan("CompileService", "CompileActor"),
            "LiteralExecuter",
            ExpectedSpan("DataExecuter",
                "WaitForTableResolve",
                "RunTasks",
                Repeat(
                    ExpectedSpan("Datashard.Transaction",
                        ExpectedSpan("Tablet.Transaction",
                            ExpectedSpan("Tablet.Transaction.Execute",
                                Repeat("Datashard.Unit", 3)),
                            Conditional(!usesVolatileTxs,
                                ExpectedSpan("Tablet.WriteLog", "Tablet.WriteLog.LogEntry")),
                            "Tablet.Transaction.Complete"),
                        Conditional(usesVolatileTxs, "Datashard.SendWithConfirmedReadOnlyLease"),
                        ExpectedSpan("Tablet.Transaction",
                            ExpectedSpan("Tablet.Transaction.Execute",
                                Repeat("Datashard.Unit", usesVolatileTxs ? 6 : 11)),
                            ExpectedSpan("Tablet.WriteLog",
                                "Tablet.WriteLog.LogEntry"),
                            "Tablet.Transaction.Complete"),
                        "Datashard.SendResult"),
                    2)))
            .ToString();

        UNIT_ASSERT_VALUES_EQUAL(trace.ToString(), canon);
    }

    Y_UNIT_TEST(TestTraceDistributedSelect) {
        auto [runtime, server, sender] = TestCreateServer();
        bool bTreeIndex = runtime.GetAppData().FeatureFlags.GetEnableLocalDBBtreeIndex();

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
        UNIT_ASSERT_VALUES_EQUAL(1, uploader->Traces.size());

        TFakeWilsonUploader::Trace &trace = uploader->Traces.begin()->second;

        std::string canon;
        auto readActorSpan = trace.Root.BFSFindOne("ReadActor");
        UNIT_ASSERT(readActorSpan);

        auto dsReads = readActorSpan->get().FindAll("Datashard.Read"); // Read actor sends EvRead to each shard.
        UNIT_ASSERT_VALUES_EQUAL(dsReads.size(), 2);

        canon = ExpectedSpan("Session.query.QUERY_ACTION_EXECUTE",
            ExpectedSpan("CompileService", "CompileActor"),
            "LiteralExecuter",
            ExpectedSpan("DataExecuter",
                "WaitForTableResolve",
                "WaitForShardsResolve",
                "WaitForSnapshot",
                ExpectedSpan("ComputeActor",
                    ExpectedSpan("ReadActor",
                        "WaitForShardsResolve",
                        Repeat(
                            ExpectedSpan("Datashard.Read",
                                ExpectedSpan("Tablet.Transaction",
                                    ExpectedSpan("Tablet.Transaction.Execute",
                                        Repeat("Datashard.Unit", 3)),
                                    // No extra page fault with btree index (root is in meta)
                                    ConditionalSpanVec(!bTreeIndex,
                                        "Tablet.Transaction.Wait",
                                        "Tablet.Transaction.Enqueued",
                                        ExpectedSpan("Tablet.Transaction.Execute",
                                            "Datashard.Unit")),
                                    "Tablet.Transaction.Wait",
                                    "Tablet.Transaction.Enqueued",
                                    ExpectedSpan("Tablet.Transaction.Execute",
                                        Repeat("Datashard.Unit", 2)),
                                    ExpectedSpan("Tablet.WriteLog", "Tablet.WriteLog.LogEntry"),
                                    "Tablet.Transaction.Complete"),
                                "Datashard.SendWithConfirmedReadOnlyLease"),
                            2))),
                "ComputeActor",
                "RunTasks"))
            .ToString();

        UNIT_ASSERT_VALUES_EQUAL(trace.ToString(), canon);
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
        UNIT_ASSERT_VALUES_EQUAL(1, uploader->Traces.size());

        TFakeWilsonUploader::Trace& trace = uploader->Traces.begin()->second;

        auto readActorSpan = trace.Root.BFSFindOne("ReadActor");
        UNIT_ASSERT(readActorSpan);

        auto dsReads = readActorSpan->get().FindAll("Datashard.Read"); // Read actor sends EvRead to each shard.
        UNIT_ASSERT_VALUES_EQUAL(dsReads.size(), 2);

        std::string canon = ExpectedSpan("Session.query.QUERY_ACTION_EXECUTE",
            ExpectedSpan("CompileService", "CompileActor"),
            ExpectedSpan("DataExecuter",
                "WaitForTableResolve",
                "WaitForShardsResolve",
                "WaitForSnapshot",
                ExpectedSpan("ComputeActor",
                    ExpectedSpan("ReadActor",
                        "WaitForShardsResolve",
                        Repeat(
                            ExpectedSpan("Datashard.Read",
                                ExpectedSpan("Tablet.Transaction",
                                    ExpectedSpan("Tablet.Transaction.Execute",
                                        Repeat("Datashard.Unit", 4)),
                                    ExpectedSpan("Tablet.WriteLog", "Tablet.WriteLog.LogEntry"),
                                    "Tablet.Transaction.Complete"),
                                "Datashard.SendWithConfirmedReadOnlyLease"),
                            2))),
                "ComputeActor",
                "RunTasks"))
            .ToString();

        UNIT_ASSERT_VALUES_EQUAL(trace.ToString(), canon);
    }

    Y_UNIT_TEST(TestTraceWriteImmediateOnShard) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        TFakeWilsonUploader *uploader = new TFakeWilsonUploader();
        TActorId uploaderId = runtime.Register(uploader, 0);
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), uploaderId, 0);
        runtime.SimulateSleep(TDuration::Seconds(10));

        NWilson::TTraceId traceId = NWilson::TTraceId::NewTraceId(15, 4095);
        const ui32 rowCount = 3;
        ui64 txId = 100;
        auto request = MakeWriteRequest(txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, opts.Columns_, rowCount);
        runtime.SendToPipe(shards[0], sender, request.release(), 0, GetPipeConfigWithRetries(), TActorId(), 0, std::move(traceId));

        auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(sender);
        auto resultRecord = ev->Get()->Record;
        UNIT_ASSERT_C(resultRecord.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED, "Status: " << resultRecord.GetStatus() << " Issues: " << resultRecord.GetIssues());

        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT_VALUES_EQUAL(1, uploader->Traces.size());

        TFakeWilsonUploader::Trace &trace = uploader->Traces.begin()->second;

        auto wtSpan = trace.Root.BFSFindOne("Datashard.WriteTransaction");
        UNIT_ASSERT(wtSpan);

        auto tabletTxs = wtSpan->get().FindAll("Tablet.Transaction");
        UNIT_ASSERT_VALUES_EQUAL(1, tabletTxs.size());
        auto writeTx = tabletTxs[0];

        CheckTxHasWriteLog(writeTx);
        CheckTxHasDatashardUnits(writeTx, 5);

        std::string canon = ExpectedSpan("Datashard.WriteTransaction",
            ExpectedSpan("Tablet.Transaction",
                ExpectedSpan("Tablet.Transaction.Execute",
                    Repeat("Datashard.Unit", 5)),
                ExpectedSpan("Tablet.WriteLog", "Tablet.WriteLog.LogEntry"),
                "Tablet.Transaction.Complete"),
            "Datashard.SendImmediateWriteResult")
            .ToString();

        UNIT_ASSERT_VALUES_EQUAL(trace.ToString(), canon);
    }
}

} // namespace NKikimr
