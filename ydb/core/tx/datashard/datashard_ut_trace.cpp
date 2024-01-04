#include "defs.h"
#include "datashard_ut_common_kqp.h"
#include "datashard_ut_read_table.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;
using namespace NDataShardReadTableTest;

Y_UNIT_TEST_SUITE(TDataShardTrace) {

    class FakeWilsonUploader : public TActorBootstrapped<FakeWilsonUploader> {
        public:
        class Span {
        public:
            Span(TString name, TString parentSpanId, ui64 startTime) : Name(name), ParentSpanId(parentSpanId), StartTime(startTime) {}

            std::optional<std::reference_wrapper<Span>> FindOne(TString targetName) {
                for (const auto childRef : Children) {
                    if (childRef.get().Name == targetName) {
                        return childRef;
                    }
                }

                return {};
            }

            std::vector<std::reference_wrapper<Span>> FindAll(TString targetName) {
                std::vector<std::reference_wrapper<Span>> res;

                for (const auto childRef : Children) {
                    if (childRef.get().Name == targetName) {
                        res.emplace_back(childRef);
                    }
                }

                return res;
            }

            std::optional<std::reference_wrapper<Span>> BFSFindOne(TString targetName) {
                std::queue<std::reference_wrapper<Span>> bfsQueue;
                bfsQueue.push(std::ref(*this));

                while (!bfsQueue.empty()) {
                    Span &currentSpan = bfsQueue.front().get();
                    bfsQueue.pop();

                    if (currentSpan.Name == targetName) {
                        return currentSpan;
                    }

                    for (const auto childRef : currentSpan.Children) {
                        bfsQueue.push(childRef);
                    }
                }

                return {};
            }

            static bool CompareByStartTime(const std::reference_wrapper<Span>& span1, const std::reference_wrapper<Span>& span2) {
                return span1.get().StartTime < span2.get().StartTime;
            }

            TString Name;
            TString ParentSpanId;
            ui64 StartTime;
            std::set<std::reference_wrapper<Span>, decltype(&CompareByStartTime)> Children{&CompareByStartTime};
        };

        class Trace {
        public:
            std::string ToString() const {
                std::string result;

                for (const auto& spanPair : Spans) {
                    const Span& span = spanPair.second;
                    if (span.ParentSpanId.empty()) {
                        result += ToStringHelper(span);
                    }
                }

                return result;
            }
        private:
            std::string ToStringHelper(const Span& span) const {
                std::string result = "(" + span.Name;

                if (!span.Children.empty()) {
                    result += " -> [";
                    auto it = span.Children.begin();
                    while (it != span.Children.end()) {
                        const Span& childSpan = it->get();
                        result += ToStringHelper(childSpan);
                        ++it;

                        if (it != span.Children.end()) {
                            result += " , ";
                        }
                    }
                    result += "]";
                }

                result += ")";

                return result;
            }
        public:
            std::unordered_map<TString, Span> Spans;

            Span Root{"Root", "", 0};
        };

    public:
        void Bootstrap() {
            Become(&TThis::StateFunc);
        }

        void Handle(NWilson::TEvWilson::TPtr ev) {
            auto& span = ev->Get()->Span;
            const TString &traceId = span.trace_id();
            const TString &spanId = span.span_id();
            const TString &parentSpanId = span.parent_span_id();
            const TString &spanName = span.name();
            ui64 startTime = span.start_time_unix_nano();

            Trace &trace = Traces[traceId];

            trace.Spans.try_emplace(spanId, spanName, parentSpanId, startTime);
        }

        void BuildTraceTrees() {
            for (auto& tracePair : Traces) {
                Trace& trace = tracePair.second;

                for (auto& spanPair : trace.Spans) {
                    Span& span = spanPair.second;

                    const TString& parentSpanId = span.ParentSpanId;

                    // Check if the span has a parent
                    if (!parentSpanId.empty()) {
                        auto parentSpanIt = trace.Spans.find(parentSpanId);
                        UNIT_ASSERT(parentSpanIt != trace.Spans.end());
                        parentSpanIt->second.Children.insert(std::ref(span));
                    } else {
                        trace.Root.Children.insert(std::ref(span));
                    }
                }
            }
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NWilson::TEvWilson, Handle);
        );

    public:
        std::unordered_map<TString, Trace> Traces;
    };

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

    void CheckTxHasWriteLog(std::reference_wrapper<FakeWilsonUploader::Span> txSpan) {
        auto writeLogSpan = txSpan.get().FindOne("Tablet.WriteLog");
        UNIT_ASSERT(writeLogSpan);
        auto writeLogEntrySpan = writeLogSpan->get().FindOne("Tablet.WriteLog.LogEntry");
        UNIT_ASSERT(writeLogEntrySpan);
    }

    void CheckTxHasDatashardUnits(std::reference_wrapper<FakeWilsonUploader::Span> txSpan, ui8 count) {
        auto executeSpan = txSpan.get().FindOne("Tablet.Transaction.Execute");
        UNIT_ASSERT(executeSpan);
        auto unitSpans = executeSpan->get().FindAll("Datashard.Unit");
        UNIT_ASSERT_EQUAL(count, unitSpans.size());
    }

    void CheckExecuteHasDatashardUnits(std::reference_wrapper<FakeWilsonUploader::Span> executeSpan, ui8 count) {
        auto unitSpans = executeSpan.get().FindAll("Datashard.Unit");
        UNIT_ASSERT_EQUAL(count, unitSpans.size());
    }

    Y_UNIT_TEST(TestTraceDistributedUpsert) {
        auto [runtime, server, sender] = TestCreateServer();

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
    
        FakeWilsonUploader *uploader = new FakeWilsonUploader();
        TActorId uploaderId = runtime.Register(uploader, 0);
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), uploaderId, 0); 
        runtime.SimulateSleep(TDuration::Seconds(10));

        SplitTable(runtime, server, 5);

        NWilson::TTraceId traceId = NWilson::TTraceId::NewTraceId(15, 4095);
        ExecSQL(
            server,
            sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500), (7, 700), (9, 900);",
            true,
            Ydb::StatusIds::SUCCESS,
            std::move(traceId)
        );

        uploader->BuildTraceTrees();

        UNIT_ASSERT_EQUAL(1, uploader->Traces.size());

        FakeWilsonUploader::Trace &trace = uploader->Traces.begin()->second;

        auto deSpan = trace.Root.BFSFindOne("DataExecuter");
        UNIT_ASSERT(deSpan);

        auto dsTxSpans = deSpan->get().FindAll("Datashard.Transaction");
        UNIT_ASSERT_EQUAL(2, dsTxSpans.size()); // Two shards, each executes a user transaction.

        for (auto dsTxSpan : dsTxSpans) {
            auto tabletTxs = dsTxSpan.get().FindAll("Tablet.Transaction");
            UNIT_ASSERT_EQUAL(2, tabletTxs.size()); // Each shard executes a proposal tablet tx and a progress tablet tx.

            auto propose = tabletTxs[0];
            CheckTxHasWriteLog(propose);
            CheckTxHasDatashardUnits(propose, 3);

            auto progress = tabletTxs[1];
            CheckTxHasWriteLog(progress); 
            CheckTxHasDatashardUnits(progress, 11); 
        }
        
        std::string canon = "(Session.query.QUERY_ACTION_EXECUTE -> [(CompileService -> [(CompileActor)]) , "
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
        UNIT_ASSERT_VALUES_EQUAL(canon, trace.ToString());
    }

    Y_UNIT_TEST(TestTraceDistributedSelect) {
        auto [runtime, server, sender] = TestCreateServer();

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
    
        FakeWilsonUploader *uploader = new FakeWilsonUploader();
        TActorId uploaderId = runtime.Register(uploader, 0);
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), uploaderId, 0); 
        runtime.SimulateSleep(TDuration::Seconds(10));

        SplitTable(runtime, server, 5);

        ExecSQL(
            server,
            sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500), (7, 700), (9, 900);",
            true,
            Ydb::StatusIds::SUCCESS
        );

        ExecSQL(
            server,
            sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 100), (4, 300), (6, 500), (8, 700), (10, 900);",
            true,
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
            true,
            Ydb::StatusIds::SUCCESS,
            std::move(traceId)
        );

        uploader->BuildTraceTrees();

        UNIT_ASSERT_EQUAL(1, uploader->Traces.size());

        FakeWilsonUploader::Trace &trace = uploader->Traces.begin()->second;

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
        
        std::string canon = "(Session.query.QUERY_ACTION_EXECUTE -> [(CompileService -> [(CompileActor)]) "
        ", (LiteralExecuter) , (DataExecuter -> [(WaitForTableResolve) , (WaitForSnapshot) , (RunTasks) , "
        "(Datashard.Transaction -> [(Tablet.Transaction -> [(Tablet.Transaction.Execute -> [(Datashard.Unit) , "
        "(Datashard.Unit) , (Datashard.Unit)]) , (Tablet.Transaction.Wait) , (Tablet.Transaction.Enqueued) , "
        "(Tablet.Transaction.Execute -> [(Datashard.Unit)]) , (Tablet.Transaction.Wait) , (Tablet.Transaction.Enqueued) , "
        "(Tablet.Transaction.Execute -> [(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.WriteLog -> "
        "[(Tablet.WriteLog.LogEntry)])])]) , (Datashard.Transaction -> [(Tablet.Transaction -> [(Tablet.Transaction.Execute -> "
        "[(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.Transaction.Wait) , (Tablet.Transaction.Enqueued) , "
        "(Tablet.Transaction.Execute -> [(Datashard.Unit)]) , (Tablet.Transaction.Wait) , (Tablet.Transaction.Enqueued) , "
        "(Tablet.Transaction.Execute -> [(Datashard.Unit) , (Datashard.Unit) , (Datashard.Unit)]) , (Tablet.WriteLog -> "
        "[(Tablet.WriteLog.LogEntry)])])])])])";
        UNIT_ASSERT_VALUES_EQUAL(canon, trace.ToString());
    }

    Y_UNIT_TEST(TestTraceWriteImmediateOnShard) {
        auto [runtime, server, sender] = TestCreateServer();

        auto opts = TShardedTableOptions().Columns({{"key", "Uint32", true, false}, {"value", "Uint32", false, false}});
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        FakeWilsonUploader *uploader = new FakeWilsonUploader();
        TActorId uploaderId = runtime.Register(uploader, 0);
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), uploaderId, 0); 
        runtime.SimulateSleep(TDuration::Seconds(10));

        NWilson::TTraceId traceId = NWilson::TTraceId::NewTraceId(15, 4095);
        const ui32 rowCount = 3;
        ui64 txId = 100;
        Write(runtime, sender, shards[0], tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED, std::move(traceId));

        uploader->BuildTraceTrees();

        UNIT_ASSERT_EQUAL(1, uploader->Traces.size());

        FakeWilsonUploader::Trace &trace = uploader->Traces.begin()->second;
        
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
