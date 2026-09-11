#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/tracing/test_util/kqp_trace_test_helpers.h>
#include <ydb/core/kqp/tracing/kqp_query_tracing.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/rm_service/kqp_snapshot_manager.h>
#include <ydb/core/grpc_services/cancelation/cancelation_event.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <optional>
#include <ranges>
#include <set>
#include <util/folder/dirut.h>

namespace NKikimr {
using namespace Tests;
using namespace NWilson;
using namespace NKqp::NTest;

Y_UNIT_TEST_SUITE(TKqpQueryTrace) {
    std::tuple<TTestActorRuntime&, TServer::TPtr, TActorId> CreateServer(ui32 nodes = 1,
            NKikimrConfig::TAppConfig config = {}) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root").SetUseRealThreads(false).SetNodeCount(nodes).SetAppConfig(config);
        if (config.GetTableServiceConfig().GetSpillingServiceConfig().GetLocalFileConfig().GetEnable()) {
            NKikimrKqp::TKqpSetting spilling;
            spilling.SetName("_KqpEnableSpilling");
            spilling.SetValue("true");
            settings.SetEnableKqpSpilling(true).SetKqpSettings({spilling});
        }
        TServer::TPtr server = new TServer(settings);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        return {runtime, server, sender};
    }

    TFakeWilsonUploader* RegisterUploader(TTestActorRuntime& runtime) {
        auto* uploader = new TFakeWilsonUploader();
        const auto id = runtime.Register(uploader, 0);
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.RegisterService(MakeWilsonUploaderId(), id, node);
        }
        if (!runtime.IsRealThreads()) {
            runtime.SimulateSleep(TDuration::Seconds(10));
        }
        return uploader;
    }

    TTraceSnapshot WaitForQueryTrace(TTestActorRuntime& runtime, TStringBuf sql,
            NKikimrKqp::EQueryType type, std::initializer_list<TStringBuf> requiredSpans) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        TTraceSnapshot snapshot;
        do {
            auto promise = NThreading::NewPromise<std::vector<TTraceSnapshot::TOtelSpan>>();
            runtime.GetActorSystem(0)->Send(MakeWilsonUploaderId(), new TFakeWilsonUploader::TEvGetSnapshot(promise));
            UNIT_ASSERT_C(promise.GetFuture().Wait(deadline - TInstant::Now()), "Uploader did not return a snapshot");
            const auto& spans = promise.GetFuture().GetValueSync();
            const auto query = std::ranges::find_if(spans, [&](const auto& span) {
                const auto* text = FindAttribute(span, "db.query.text");
                const auto* queryType = FindAttribute(span, "ydb.query.type");
                return span.name() == "Query" && text && text->value().string_value() == sql
                    && queryType && queryType->value().string_value() == NKikimrKqp::EQueryType_Name(type);
            });
            if (query != spans.end()) {
                snapshot = {};
                for (const auto& span : spans) {
                    if (span.trace_id() == query->trace_id()) {
                        snapshot.AddSpan(span);
                    }
                }
                if (std::ranges::all_of(requiredSpans, [&](TStringBuf name) { return FindSpan(snapshot, name); })
                        && snapshot.BuildTraceTrees()) {
                    return snapshot;
                }
            }
            Sleep(TDuration::MilliSeconds(10));
        } while (TInstant::Now() < deadline);
        UNIT_FAIL("Query trace did not complete: " << sql << "; " << snapshot.PrintTraces());
        return {};
    }

    NKikimrKqp::TEvQueryResponse ExecRequest(TTestActorRuntime& runtime, TActorId sender,
            THolder<NKqp::TEvKqp::TEvQueryRequest> request, ui8 level = 15,
            Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, ui32 proxyNode = 0) {
        ActorIdToProto(sender, request->Record.MutableRequestActorId());
        NWilson::TTraceId traceId;
        if (level) {
            traceId = NWilson::TTraceId::NewTraceId(level, 4095);
        }
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(proxyNode)), sender,
            request.Release(), 0, 0, nullptr, std::move(traceId)));
        while (true) {
            TAutoPtr<IEventHandle> handle;
            auto replies = runtime.GrabEdgeEventsRethrow<NKqp::TEvKqp::TEvQueryResponse,
                NKqp::TEvKqpExecuter::TEvStreamData>(handle);
            if (auto* response = std::get<NKqp::TEvKqp::TEvQueryResponse*>(replies)) {
                UNIT_ASSERT_VALUES_EQUAL_C(response->Record.GetYdbStatus(), status, response->Record.DebugString());
                auto result = std::move(response->Record);
                runtime.SimulateSleep(TDuration::Seconds(1));
                return result;
            }
            const auto& data = std::get<NKqp::TEvKqpExecuter::TEvStreamData*>(replies)->Record;
            auto ack = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(data.GetSeqNo(), data.GetChannelId());
            ack->Record.SetFreeSpace(1 << 20);
            runtime.Send(new IEventHandle(handle->Sender, sender, ack.Release()));
        }
    }

    void ExecSQL(TTestActorRuntime& runtime, TActorId sender, const TString& sql,
            ui8 level = 15, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
            const TString& session = {}, ui32 proxyNode = 0,
            NKikimrKqp::EQueryType type = NKikimrKqp::QUERY_TYPE_SQL_DML, bool keepInCache = false) {
        auto request = MakeSQLRequest(sql, type != NKikimrKqp::QUERY_TYPE_SQL_DDL);
        request->Record.MutableRequest()->SetType(type);
        if (type == NKikimrKqp::QUERY_TYPE_SQL_SCAN) {
            request->Record.MutableRequest()->ClearTxControl();
        }
        if (session) {
            request->Record.MutableRequest()->SetSessionId(session);
        }
        request->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(keepInCache);
        ExecRequest(runtime, sender, std::move(request), level, status, proxyNode);
    }

    TString CreateSession(TTestActorRuntime& runtime, TActorId sender, NKikimrKqp::EQueryType type) {
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender,
            new NKqp::TEvKqp::TEvCreateSessionRequest()));
        auto created = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvCreateSessionResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(created->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        const auto sessionId = created->Get()->Record.GetResponse().GetSessionId();
        if (type == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY) {
            auto attach = MakeHolder<NKqp::TEvKqp::TEvPingSessionRequest>();
            auto& request = *attach->Record.MutableRequest();
            request.SetSessionId(sessionId);
            ActorIdToProto(sender, request.MutableExtSessionCtrlActorId());
            runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender,
                attach.Release()));
            auto attached = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvPingSessionResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(attached->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        }
        return sessionId;
    }

    Y_UNIT_TEST(CommonTreeIncludesKqpAndDatashard) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        ExecSQL(runtime, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100);", 0);
        auto* uploader = RegisterUploader(runtime);
        ExecSQL(runtime, sender, "SELECT SUM(value) FROM `/Root/table-1` WHERE key > 0u;");
        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT_VALUES_EQUAL(uploader->Traces.size(), 1u);
        AssertDescendant(*uploader, "Query", "KQP request");
        AssertDescendant(*uploader, "Compile query", "Get query plan");
        AssertDescendant(*uploader, "Load metadata", "Compile query");
        AssertDescendant(*uploader, "Task: ", "Execute plan");
        AssertDescendant(*uploader, "Task: ", "Stage: ");
        AssertDescendant(*uploader, "Stage: ", "Run tasks");
        AssertDescendant(*uploader, "Run tasks", "Execute plan");
        AssertDescendant(*uploader, "Datashard.Read", "Read shard");
        AssertDescendant(*uploader, "Read shard", "Read table");
        AssertDescendant(*uploader, "Datashard.Unit", "Datashard.Read");
        AssertDescendant(*uploader, "Read table", "Task: ");
        AssertStatus(*uploader, "Query", NTraceProto::Status::STATUS_CODE_OK);
        const auto* query = FindSpan(*uploader, "Query");
        UNIT_ASSERT_VALUES_EQUAL_C(FindAttribute(*query, "db.operation.name")->value().string_value(), "SELECT", query->DebugString());
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*query, "db.namespace")->value().string_value(), "/Root");
        UNIT_ASSERT(FindAttribute(*query, "ydb.cpu_us"));
        UNIT_ASSERT(FindAttribute(*query, "ydb.wait_us"));
        UNIT_ASSERT(FindAttribute(*query, "ydb.spilled_bytes"));
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*query, "ydb.code.component")->value().string_value(), "KQP");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*FindSpan(*uploader, "KQP request"), "ydb.code.component")->value().string_value(), "KQP");
        const auto* run = FindSpan(*uploader, "Run tasks");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*run, "ydb.phase")->value().string_value(), "RunTasks");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*run, "ydb.code.component")->value().string_value(), "DqExecution");
        const auto* resolve = FindSpan(*uploader, "Resolve tables");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*resolve, "ydb.phase")->value().string_value(), "ResolveTables");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*resolve, "ydb.actor.type")->value().string_value(), "TKqpTableResolver");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*resolve, "ydb.code.component")->value().string_value(), "KqpExecuter.Prepare");
        const auto* partitioning = FindSpan(*uploader, "Partitioning");
        UNIT_ASSERT(partitioning);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*partitioning, "ydb.phase")->value().string_value(), "ResolvePartitioning");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*partitioning, "ydb.peer.actor.type")->value().string_value(), "SchemeCache");
        const auto* read = FindSpan(*uploader, "Read shard");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*read, "ydb.code.component")->value().string_value(), "KqpShardRead");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*read, "ydb.peer.actor.type")->value().string_value(), "DataShard");
        const auto* shard = FindSpan(*uploader, "Datashard.Read");
        UNIT_ASSERT(FindAttribute(*shard, "Shard"));
        UNIT_ASSERT(!std::ranges::empty(StageSpans(*uploader)));
        UNIT_ASSERT(FindAttribute(*FindSpan(*uploader, "Task: "), "ydb.task_id"));
        UNIT_ASSERT(!FindSpan(*uploader, "Session.query.QUERY_ACTION_EXECUTE"));
    }

    Y_UNIT_TEST(CommonLevelsAndDisabledTracing) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        auto* uploader = RegisterUploader(runtime);
        const TString sql = "SELECT * FROM `/Root/table-1` WHERE key = 1u;";
        ExecSQL(runtime, sender, sql, 0);
        UNIT_ASSERT(uploader->Spans.empty());
        ExecSQL(runtime, sender, sql, TComponentTracingLevels::TQueryProcessor::TopLevel);
        const auto* query = FindSpan(*uploader, "Query");
        UNIT_ASSERT(query);
        UNIT_ASSERT(!FindAttribute(*query, "ydb.wait_us"));
        UNIT_ASSERT(!FindAttribute(*query, "ydb.spilled_bytes"));
        ClearUploader(*uploader);
        ExecSQL(runtime, sender, sql, TComponentTracingLevels::TQueryProcessor::Basic);
        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT(FindSpan(*uploader, "Execute plan"));
        UNIT_ASSERT(!FindSpan(*uploader, "Task: "));
        UNIT_ASSERT(!FindSpan(*uploader, "Load metadata"));
        ClearUploader(*uploader);
        ExecSQL(runtime, sender, sql, TComponentTracingLevels::TQueryProcessor::Detailed);
        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT(FindSpan(*uploader, "Task: "));
        UNIT_ASSERT(FindSpan(*uploader, "Resolve tables"));
    }

    Y_UNIT_TEST(TracingKeepsBasicExecutionStats) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        auto* uploader = RegisterUploader(runtime);
        ui32 requests = 0;
        const auto observer = runtime.AddObserver<NKqp::TEvKqpNode::TEvStartKqpTasksRequest>(
            [&](NKqp::TEvKqpNode::TEvStartKqpTasksRequest::TPtr& ev) {
                ++requests;
                UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(ev->Get()->Record.GetRuntimeSettings().GetStatsMode()),
                    static_cast<int>(NYql::NDqProto::DQ_STATS_MODE_BASIC));
            });
        auto request = MakeSQLRequest("SELECT SUM(value) FROM `/Root/table-1`;", true);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCAN);
        request->Record.MutableRequest()->ClearTxControl();
        ActorIdToProto(sender, request->Record.MutableRequestActorId());
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender,
            request.Release(), 0, 0, nullptr, NWilson::TTraceId::NewTraceId(15, 4095)));
        const auto response = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
            response->Get()->Record.DebugString());
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(requests);
        UNIT_ASSERT(FindSpan(*uploader, "Task: "));
    }

    Y_UNIT_TEST(QueryCpuIncludesExecutionWithoutClientStats) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 2, false);
        ExecSQL(runtime, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1u, 10u), (4000000000u, 20u);", 0);
        auto* uploader = RegisterUploader(runtime);
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            for (const auto mode : {Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE,
                    Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC,
                    Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL}) {
                for (const ui8 level : {0, 1, 15}) {
                    ClearUploader(*uploader);
                    std::map<TActorId, ui64> taskCpu;
                    const auto observer = runtime.AddObserver<NYql::NDq::TEvDqCompute::TEvState>(
                        [&](NYql::NDq::TEvDqCompute::TEvState::TPtr& ev) {
                            auto& state = ev->Get()->Record;
                            if (state.GetState() == NYql::NDqProto::COMPUTE_STATE_FINISHED && state.GetStats().TasksSize() == 1) {
                                auto& task = *state.MutableStats()->MutableTasks(0);
                                task.SetCpuTimeUs(1000 + task.GetTaskId());
                                taskCpu[ev->Sender] = task.GetCpuTimeUs();
                            }
                        });
                    const auto responses = runtime.AddObserver<NKqp::TEvKqpExecuter::TEvTxResponse>(
                        [&](NKqp::TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
                            if (!level) {
                                NKqpProto::TKqpExecutionExtraStats extra;
                                ev->Get()->Record.GetResponse().GetResult().GetStats().GetExtra().UnpackTo(&extra);
                                UNIT_ASSERT(!extra.HasCpuTimeUs());
                                UNIT_ASSERT(!extra.GetWaitTimeUs());
                                UNIT_ASSERT(!extra.GetSpilledBytes());
                                UNIT_ASSERT(!extra.GetMaxTaskSkew());
                            }
                        });
                    auto request = MakeSQLRequest("SELECT SUM(value) FROM `/Root/table-1` WHERE key > 0u;");
                    request->Record.MutableRequest()->SetType(type);
                    request->Record.MutableRequest()->SetCollectStats(mode);
                    ExecRequest(runtime, sender, std::move(request), level);
                    UNIT_ASSERT(!taskCpu.empty());
                    if (!level) {
                        UNIT_ASSERT(uploader->Spans.empty());
                        continue;
                    }
                    ui64 cpu = 0;
                    for (const auto& [actor, value] : taskCpu) {
                        cpu += value;
                    }
                    const auto* query = FindSpan(*uploader, "Query");
                    UNIT_ASSERT(query);
                    UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*query, "ydb.cpu_us")->value().int_value(), cpu);
                    UNIT_ASSERT(FindAttribute(*query, "ydb.compile.cpu_us"));
                    UNIT_ASSERT(FindAttribute(*query, "ydb.session.cpu_us"));
                    if (level == 15) {
                        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*FindSpan(*uploader, "Execute plan"), "ydb.cpu_us")->value().int_value(), cpu);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(TableFreeCpuIsIncludedWithoutClientStats) {
        auto [runtime, server, sender] = CreateServer();
        auto* uploader = RegisterUploader(runtime);
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            ClearUploader(*uploader);
            auto request = MakeSQLRequest("SELECT ListSum(ListFromRange(0u, 100000u));");
            request->Record.MutableRequest()->SetType(type);
            request->Record.MutableRequest()->SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE);
            ExecRequest(runtime, sender, std::move(request));
            const auto* execution = FindSpan(*uploader, "Execute plan");
            UNIT_ASSERT_C(execution, uploader->PrintTraces());
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*execution, "ydb.actor.type")->value().string_value(),
                type == NKikimrKqp::QUERY_TYPE_SQL_DML ? "TKqpLiteralExecuter" : "DataExecuter");
            const auto cpu = FindAttribute(*execution, "ydb.cpu_us")->value().int_value();
            UNIT_ASSERT_C(cpu > 0, execution->DebugString());
            const auto* query = FindSpan(*uploader, "Query");
            UNIT_ASSERT(query);
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*query, "ydb.cpu_us")->value().int_value(), cpu);
        }
    }

    Y_UNIT_TEST(BatchUpdateAndDeletePreserveStatsWithOptionalTracing) {
        NKikimrConfig::TAppConfig config;
        auto* batchSettings = config.MutableTableServiceConfig()->MutableBatchOperationSettings();
        batchSettings->SetMaxBatchSize(2);
        batchSettings->SetPartitionExecutionLimit(1);
        auto [runtime, server, sender] = CreateServer(1, config);
        CreateShardedTable(server, sender, "/Root", "table-1", 2, false);
        auto* uploader = RegisterUploader(runtime);
        TStringBuilder fill;
        TStringBuilder updated;
        fill << "UPSERT INTO `/Root/table-1` (key, value) VALUES ";
        for (ui64 i = 0; i < 10; ++i) {
            const ui64 key = i < 5 ? i + 1 : 4000000000 + i - 5;
            fill << (i ? ", " : "") << "(" << key << "u, 0u)";
            updated << "key = " << key << ", value = 42\n";
        }
        fill << ";";
        for (const bool tracing : {false, true}) {
            for (const auto mode : {Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE,
                    Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC}) {
                ExecSQL(runtime, sender, fill, 0);
                for (const bool erase : {false, true}) {
                    ClearUploader(*uploader);
                    std::set<TActorId> partitionedExecuters;
                    ui64 batches = 0;
                    ui64 cpuUs = 0;
                    ui64 waitUs = 0;
                    ui64 spilledBytes = 0;
                    ui64 changedRows = 0;
                    double maxSkew = 0;
                    bool incomplete = false;
                    std::optional<NYql::NDqProto::TDqExecutionStats> batchResult;
                    const auto responses = runtime.AddObserver<NKqp::TEvKqpExecuter::TEvTxResponse>(
                        [&](NKqp::TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
                            const auto& response = ev->Get()->Record.GetResponse();
                            if (runtime.FindActorName(ev->GetRecipientRewrite()) == "KQP_PARTITIONED_EXECUTER"
                                    && response.GetStatus() == Ydb::StatusIds::SUCCESS) {
                                partitionedExecuters.insert(ev->GetRecipientRewrite());
                                ++batches;
                                const auto& stats = response.GetResult().GetStats();
                                NKqpProto::TKqpExecutionExtraStats extra;
                                stats.GetExtra().UnpackTo(&extra);
                                cpuUs += NKqp::GetExecutionTraceCpuTimeUs(stats);
                                waitUs += extra.GetWaitTimeUs();
                                spilledBytes += extra.GetSpilledBytes();
                                maxSkew = std::max(maxSkew, extra.GetMaxTaskSkew());
                                incomplete |= extra.GetTaskStatsIncomplete();
                                for (const auto& table : stats.GetTables()) {
                                    changedRows += table.GetWriteRows() + table.GetEraseRows();
                                }
                                if (!tracing) {
                                    UNIT_ASSERT(!extra.HasCpuTimeUs());
                                }
                            } else if (partitionedExecuters.contains(ev->Sender)) {
                                batchResult = response.GetResult().GetStats();
                            }
                        });
                    auto request = MakeSQLRequest(erase ? "BATCH DELETE FROM `/Root/table-1`;"
                        : "BATCH UPDATE `/Root/table-1` SET value = 42u;");
                    request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
                    request->Record.MutableRequest()->ClearTxControl();
                    request->Record.MutableRequest()->SetCollectStats(mode);
                    ExecRequest(runtime, sender, std::move(request), tracing ? 15 : 0);
                    UNIT_ASSERT_VALUES_EQUAL(partitionedExecuters.size(), 1);
                    UNIT_ASSERT_C(batches >= 6, batches);
                    UNIT_ASSERT_VALUES_EQUAL(changedRows, 10);
                    UNIT_ASSERT(batchResult);
                    UNIT_ASSERT_VALUES_EQUAL(batchResult->HasExtra(), tracing);
                    ui64 exportedRows = 0;
                    for (const auto& table : batchResult->GetTables()) {
                        exportedRows += table.GetWriteRows() + table.GetEraseRows();
                    }
                    UNIT_ASSERT_VALUES_EQUAL(exportedRows, changedRows);
                    UNIT_ASSERT_VALUES_EQUAL(batchResult->GetCpuTimeUs(),
                        mode == Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC ? cpuUs : 0);
                    if (tracing) {
                        NKqpProto::TKqpExecutionExtraStats extra;
                        UNIT_ASSERT(batchResult->GetExtra().UnpackTo(&extra));
                        UNIT_ASSERT(cpuUs > 0);
                        UNIT_ASSERT_VALUES_EQUAL(extra.GetCpuTimeUs(), cpuUs);
                        UNIT_ASSERT_VALUES_EQUAL(extra.GetWaitTimeUs(), waitUs);
                        UNIT_ASSERT_VALUES_EQUAL(extra.GetSpilledBytes(), spilledBytes);
                        UNIT_ASSERT_VALUES_EQUAL(extra.GetMaxTaskSkew(), maxSkew);
                        UNIT_ASSERT_VALUES_EQUAL(extra.GetTaskStatsIncomplete(), incomplete);
                        const auto* query = FindSpan(*uploader, "Query");
                        UNIT_ASSERT(query);
                        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*query, "ydb.rows_written")->value().int_value(), changedRows);
                        UNIT_ASSERT(FindAttribute(*query, "ydb.cpu_us")->value().int_value() >= static_cast<i64>(cpuUs));
                    } else {
                        UNIT_ASSERT(uploader->Spans.empty());
                    }
                    UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(runtime, "/Root/table-1"), erase ? TString() : TString(updated));
                }
            }
        }
    }

    Y_UNIT_TEST(ScriptQueryUsesWorkerStatistics) {
        NKqp::TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        auto* sampling = settings.AppConfig.MutableTracingConfig()->AddSampling();
        sampling->SetFraction(1.0);
        sampling->SetLevel(15);
        sampling->SetMaxTracesPerMinute(1'000'000);
        sampling->SetMaxTracesBurst(1'000'000);
        NKqp::TKikimrRunner kikimr(settings);
        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "table-1"
            Columns { Name: "key", Type: "Uint64" }
            Columns { Name: "value", Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto write = session.ExecuteDataQuery(
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1u, 10u), (2u, 20u);",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        RegisterUploader(runtime);
        NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());
        for (const bool streaming : {false, true}) {
            for (const auto mode : {NYdb::NTable::ECollectQueryStatsMode::None,
                    NYdb::NTable::ECollectQueryStatsMode::Basic}) {
                const auto requestSettings = NYdb::NScripting::TExecuteYqlRequestSettings()
                    .CollectQueryStats(mode).ReportCostInfo(true);
                const TString sql = TStringBuilder() << "SELECT SUM(value) FROM `/Root/table-1`; -- stats " << static_cast<int>(mode);
                float consumedRu = 0;
                bool hasStats = false;
                if (streaming) {
                    auto iterator = client.StreamExecuteYqlScript(sql, requestSettings).GetValueSync();
                    UNIT_ASSERT_C(iterator.IsSuccess(), iterator.GetIssues().ToString());
                    while (true) {
                        auto part = iterator.ReadNext().GetValueSync();
                        consumedRu += part.GetConsumedRu();
                        hasStats |= part.HasQueryStats();
                        if (!part.IsSuccess()) {
                            UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                            break;
                        }
                    }
                } else {
                    const auto result = client.ExecuteYqlScript(sql, requestSettings).GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    consumedRu = result.GetConsumedRu();
                    hasStats = result.GetStats().has_value();
                }
                const auto type = streaming ? NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING : NKikimrKqp::QUERY_TYPE_SQL_SCRIPT;
                const auto snapshot = WaitForQueryTrace(runtime, sql, type, {"Query", "KQP request", "Execute plan"});
                const auto query = std::ranges::find_if(snapshot.Spans, [&](const auto& span) {
                    const auto* queryType = FindAttribute(span, "ydb.query.type");
                    return span.name() == "Query" && queryType
                        && queryType->value().string_value() == NKikimrKqp::EQueryType_Name(type);
                });
                UNIT_ASSERT_C(query != snapshot.Spans.end(), snapshot.PrintTraces());
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*query, "ydb.rows_read")->value().int_value(), 2);
                UNIT_ASSERT(FindAttribute(*query, "ydb.bytes_read")->value().int_value() > 0);
                UNIT_ASSERT(FindAttribute(*query, "ydb.cpu_us")->value().int_value() > 0);
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*query, "ydb.consumed_ru")->value().int_value(), consumedRu);
                UNIT_ASSERT_VALUES_EQUAL(hasStats, mode == NYdb::NTable::ECollectQueryStatsMode::Basic);
            }
        }
    }

    Y_UNIT_TEST(RecompileRefreshesRequestedTableDiagnostics) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        const auto session = CreateSession(runtime, sender, NKikimrKqp::QUERY_TYPE_SQL_DML);
        auto request = MakeSQLRequest("SELECT * FROM `/Root/table-1`;");
        request->Record.MutableRequest()->SetSessionId(session);
        request->Record.MutableRequest()->SetCollectDiagnostics(true);
        request->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);
        const auto prepared = ExecRequest(runtime, sender, std::move(request), 0);
        UNIT_ASSERT(prepared.GetResponse().GetPreparedQuery());
        UNIT_ASSERT(prepared.GetResponse().GetQueryDiagnostics().Contains("table_metadata"));
        UNIT_ASSERT(!prepared.GetResponse().GetQueryDiagnostics().Contains("AddedForRecompile"));

        ExecSQL(runtime, sender, "ALTER TABLE `/Root/table-1` ADD COLUMN AddedForRecompile Uint64;",
            0, Ydb::StatusIds::SUCCESS, {}, 0, NKikimrKqp::QUERY_TYPE_SQL_DDL);
        ui32 recompilations = 0;
        const auto observer = runtime.AddObserver<NKqp::TEvKqp::TEvRecompileRequest>(
            [&](NKqp::TEvKqp::TEvRecompileRequest::TPtr& ev) {
                ++recompilations;
                UNIT_ASSERT(ev->Get()->CollectDiagnostics);
            });
        request = MakeSQLRequest("");
        request->Record.MutableRequest()->SetSessionId(session);
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_PREPARED_DML);
        request->Record.MutableRequest()->SetPreparedQuery(prepared.GetResponse().GetPreparedQuery());
        request->Record.MutableRequest()->SetCollectDiagnostics(true);
        const auto recompiled = ExecRequest(runtime, sender, std::move(request), 0);
        UNIT_ASSERT_VALUES_EQUAL(recompilations, 1);
        UNIT_ASSERT_C(recompiled.GetResponse().GetQueryDiagnostics().Contains("AddedForRecompile"),
            recompiled.GetResponse().GetQueryDiagnostics());
    }

    Y_UNIT_TEST(CommonConfigSamplesSdkReadPaths) {
        NKqp::TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        settings.FeatureFlags.SetEnableFulltextIndex(true);
        settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(
            NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
        auto* sampling = settings.AppConfig.MutableTracingConfig()->AddSampling();
        sampling->SetFraction(1.0);
        sampling->SetLevel(15);
        sampling->SetMaxTracesPerMinute(1'000'000);
        sampling->SetMaxTracesBurst(1'000'000);
        NKqp::TKikimrRunner kikimr(settings);
        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "table-1"
            Columns { Name: "key", Type: "Uint64" }
            Columns { Name: "value", Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        RegisterUploader(runtime);
        TTraceSnapshot snapshot;
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteDataQuery("SELECT * FROM `/Root/table-1`;",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        snapshot = WaitForQueryTrace(runtime, "SELECT * FROM `/Root/table-1`;", NKikimrKqp::QUERY_TYPE_SQL_DML,
            {"Query", "KQP request", "Datashard.Read", "Read table"});
        AssertDescendant(snapshot, "Query", "KQP request");
        AssertDescendant(snapshot, "Datashard.Read", "Read table");

        auto iterator = tableClient.StreamExecuteScanQuery("SELECT * FROM `/Root/table-1`;").GetValueSync();
        UNIT_ASSERT_C(iterator.IsSuccess(), iterator.GetIssues().ToString());
        while (true) {
            auto part = iterator.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }
        }
        snapshot = WaitForQueryTrace(runtime, "SELECT * FROM `/Root/table-1`;", NKikimrKqp::QUERY_TYPE_SQL_SCAN,
            {"Query", "Task: ", "Execute plan"});
        AssertDescendant(snapshot, "Task: ", "Execute plan");
        AssertStatus(snapshot, "Query", NTraceProto::Status::STATUS_CODE_OK);

        auto db = kikimr.GetQueryClient();
        auto queryResult = db.ExecuteQuery("SELECT * FROM `/Root/table-1`;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        snapshot = WaitForQueryTrace(runtime, "SELECT * FROM `/Root/table-1`;", NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY,
            {"Query", "KQP request", "Datashard.Read", "Read table"});
        AssertDescendant(snapshot, "Query", "KQP request");
        AssertDescendant(snapshot, "Datashard.Read", "Read table");

        queryResult = db.ExecuteQuery(R"(
            CREATE TABLE `/Root/Texts` (
                Key Uint64,
                Text String,
                PRIMARY KEY (Key),
                INDEX fulltext_idx GLOBAL USING fulltext_plain ON (Text)
                    WITH (tokenizer=standard, use_filter_lowercase=true)
            );
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        queryResult = db.ExecuteQuery(R"(
            UPSERT INTO `/Root/Texts` (Key, Text) VALUES
                (1, "Cats love cats"), (2, "Dogs love foxes");
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        const TString sql = R"(
            SELECT Key FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "cats");
        )";
        queryResult = db.ExecuteQuery(sql, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        snapshot = WaitForQueryTrace(runtime, sql, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY,
            {"Query", "Datashard.Read", "Full-text search"});
        AssertDescendant(snapshot, "Datashard.Read", "Full-text search");
    }

    Y_UNIT_TEST(CompileFailureAndCacheHit) {
        auto [runtime, server, sender] = CreateServer();
        auto* uploader = RegisterUploader(runtime);
        ExecSQL(runtime, sender, "SELECT * FROM `/Root/missing_table`;", 15, Ydb::StatusIds::SCHEME_ERROR);
        AssertStatus(*uploader, "Get query plan", NTraceProto::Status::STATUS_CODE_ERROR);
        AssertStatus(*uploader, "Compile query", NTraceProto::Status::STATUS_CODE_ERROR);
        AssertStatus(*uploader, "Load metadata", NTraceProto::Status::STATUS_CODE_ERROR);
        ClearUploader(*uploader);
        const TString sql = "SELECT 12345;";
        ExecSQL(runtime, sender, sql, 15, Ydb::StatusIds::SUCCESS, {}, 0, NKikimrKqp::QUERY_TYPE_SQL_DML, true);
        ClearUploader(*uploader);
        ExecSQL(runtime, sender, sql, 15, Ydb::StatusIds::SUCCESS, {}, 0, NKikimrKqp::QUERY_TYPE_SQL_DML, true);
        const auto* query = FindSpan(*uploader, "Query");
        UNIT_ASSERT(query);
        const auto* hit = FindAttribute(*query, "ydb.compile.cache_hit");
        UNIT_ASSERT(hit && hit->value().bool_value());
        UNIT_ASSERT(!FindSpan(*uploader, "Compile query"));
    }

    Y_UNIT_TEST(SharedCompilationKeepsWaiterCoverageAndLink) {
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            for (const bool traceFirst : {false, true}) {
                auto [runtime, server, sender] = CreateServer();
                CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
                auto* uploader = RegisterUploader(runtime);
                const auto waiter = runtime.AllocateEdgeActor();
                TAutoPtr<IEventHandle> metadata;
                size_t compileRequests = 0;
                size_t metadataRequests = 0;
                const auto observer = runtime.AddObserver<NKqp::TEvKqp::TEvCompileRequest>(
                    [&](NKqp::TEvKqp::TEvCompileRequest::TPtr&) { ++compileRequests; });
                auto previous = runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                    if (ev->GetTypeRewrite() == TEvTxProxySchemeCache::TEvNavigateKeySetResult::EventType) {
                        const auto& result = static_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult*>(ev->GetBase())->Request->ResultSet;
                        if (std::ranges::any_of(result, [](const auto& entry) {
                                return CanonizePath(entry.Path) == "/Root/table-1";
                            })) {
                            ++metadataRequests;
                            if (!metadata) {
                                metadata = ev.Release();
                                return true;
                            }
                        }
                    }
                    return false;
                });
                auto send = [&](TActorId replyTo, bool traced) {
                    auto request = MakeSQLRequest("SELECT * FROM `/Root/table-1` WHERE key = 123u;", true);
                    ActorIdToProto(replyTo, request->Record.MutableRequestActorId());
                    request->Record.MutableRequest()->SetType(type);
                    NWilson::TTraceId traceId;
                    if (traced) {
                        traceId = NWilson::TTraceId::NewTraceId(15, 4095);
                    }
                    runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), replyTo,
                        request.Release(), 0, 0, nullptr, std::move(traceId)));
                };
                send(sender, traceFirst);
                TDispatchOptions blocked;
                blocked.FinalEvents.emplace_back([&](IEventHandle&) { return bool(metadata); });
                runtime.DispatchEvents(blocked);
                const size_t beforeWaiter = compileRequests;
                send(waiter, true);
                TDispatchOptions joined;
                joined.FinalEvents.emplace_back([&](IEventHandle&) { return compileRequests > beforeWaiter; });
                runtime.DispatchEvents(joined);
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
                runtime.SetEventFilter(std::move(previous));
                runtime.Send(metadata.Release());
                size_t responses = 0;
                while (responses < 2) {
                    TAutoPtr<IEventHandle> handle;
                    auto replies = runtime.GrabEdgeEventsRethrow<NKqp::TEvKqp::TEvQueryResponse,
                        NKqp::TEvKqpExecuter::TEvStreamData>(handle);
                    if (auto* response = std::get<NKqp::TEvKqp::TEvQueryResponse*>(replies)) {
                        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
                        ++responses;
                        continue;
                    }
                    const auto& data = std::get<NKqp::TEvKqpExecuter::TEvStreamData*>(replies)->Record;
                    auto ack = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(data.GetSeqNo(), data.GetChannelId());
                    ack->Record.SetFreeSpace(1 << 20);
                    runtime.Send(new IEventHandle(handle->Sender, handle->Recipient, ack.Release()));
                }
                runtime.SimulateSleep(TDuration::Seconds(1));
                UNIT_ASSERT_VALUES_EQUAL(metadataRequests, 1u);
                UNIT_ASSERT(uploader->BuildTraceTrees());
                const TFakeWilsonUploader::TOtelSpan* shared = nullptr;
                const TFakeWilsonUploader::TOtelSpan* owner = nullptr;
                for (const auto& span : uploader->Spans) {
                    if (span.name() != "Get query plan") {
                        continue;
                    }
                    if (const auto* coverage = FindAttribute(span, "ydb.trace.coverage")) {
                        UNIT_ASSERT_VALUES_EQUAL(coverage->value().string_value(), "joined_in_progress");
                        shared = &span;
                    } else {
                        owner = &span;
                    }
                }
                UNIT_ASSERT(shared);
                UNIT_ASSERT_VALUES_EQUAL(shared->links_size(), traceFirst ? 1 : 0);
                if (traceFirst) {
                    UNIT_ASSERT(owner);
                    UNIT_ASSERT_VALUES_EQUAL(shared->links(0).trace_id(), owner->trace_id());
                    UNIT_ASSERT_VALUES_EQUAL(shared->links(0).span_id(), owner->span_id());
                }
                for (const auto& span : uploader->Spans) {
                    if (span.trace_id() == shared->trace_id()) {
                        UNIT_ASSERT(span.name() != "Compile query" && span.name() != "Load metadata");
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(ForwardingAndEarlyRejection) {
        auto [runtime, server, sender] = CreateServer(2);
        auto* uploader = RegisterUploader(runtime);
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            const auto session = CreateSession(runtime, sender, type);
            for (const ui8 level : {1, 15}) {
                ClearUploader(*uploader);
                ExecSQL(runtime, sender, "SELECT 1;", level, Ydb::StatusIds::SUCCESS, session, 1, type);
                UNIT_ASSERT(uploader->BuildTraceTrees());
                UNIT_ASSERT_VALUES_EQUAL(uploader->Traces.size(), 1u);
                const TFakeWilsonUploader::TOtelSpan* forwarded = nullptr;
                const TFakeWilsonUploader::TOtelSpan* local = nullptr;
                size_t hops = 0;
                for (const auto& span : uploader->Spans) {
                    if (span.name() != "KQP request") {
                        continue;
                    }
                    ++hops;
                    UNIT_ASSERT(!FindAttribute(span, "ydb.rejected"));
                    UNIT_ASSERT(!FindAttribute(span, "ydb.trace.coverage"));
                    UNIT_ASSERT_VALUES_EQUAL(FindAttribute(span, "ydb.target_node_id")->value().int_value(),
                        runtime.GetNodeId(0));
                    if (FindAttribute(span, "ydb.forwarded")->value().bool_value()) {
                        forwarded = &span;
                    } else {
                        local = &span;
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(hops, 2u);
                UNIT_ASSERT(forwarded && local);
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*forwarded, "node_id")->value().int_value(), runtime.GetNodeId(1));
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*local, "node_id")->value().int_value(), runtime.GetNodeId(0));
                UNIT_ASSERT_VALUES_EQUAL(local->parent_span_id(), forwarded->span_id());
                UNIT_ASSERT_VALUES_EQUAL(FindSpan(*uploader, "Query")->parent_span_id(), local->span_id());
            }
        }
        ClearUploader(*uploader);
        ExecSQL(runtime, sender, "SELECT 1;", 15, Ydb::StatusIds::BAD_SESSION,
            "ydb://session/3?node_id=1&id=missing");
        AssertStatus(*uploader, "KQP request", NTraceProto::Status::STATUS_CODE_ERROR);
        UNIT_ASSERT(!FindAttribute(*FindSpan(*uploader, "KQP request"), "ydb.rejected"));

        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            ClearUploader(*uploader);
            ExecSQL(runtime, sender, "SELECT 1;", 15, Ydb::StatusIds::BAD_SESSION,
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId(0) << "&id=missing", 1, type);
            UNIT_ASSERT(uploader->BuildTraceTrees());
            UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 2);
            for (const auto& span : uploader->Spans) {
                UNIT_ASSERT_VALUES_EQUAL(span.name(), "KQP request");
                UNIT_ASSERT(FindAttribute(span, "ydb.rejected")->value().bool_value());
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(span, "ydb.trace.coverage")->value().string_value(), "proxy_only");
            }
        }
    }

    Y_UNIT_TEST(SessionRejectionIsPropagatedAcrossProxies) {
        auto [runtime, server, sender] = CreateServer(2);
        auto* uploader = RegisterUploader(runtime);
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            const auto session = CreateSession(runtime, sender, type);
            const auto firstSender = runtime.AllocateEdgeActor();
            TAutoPtr<IEventHandle> compile;
            auto previous = runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == NKqp::TEvKqp::TEvCompileRequest::EventType && !compile) {
                    compile = ev;
                    return true;
                }
                return false;
            });
            auto request = MakeSQLRequest("SELECT 987654;");
            request->Record.MutableRequest()->SetType(type);
            request->Record.MutableRequest()->SetSessionId(session);
            ActorIdToProto(firstSender, request->Record.MutableRequestActorId());
            runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(0)), firstSender, request.Release()));
            TDispatchOptions blocked;
            blocked.FinalEvents.emplace_back([&](IEventHandle&) { return bool(compile); });
            runtime.DispatchEvents(blocked);

            ClearUploader(*uploader);
            ExecSQL(runtime, sender, "SELECT 1;", 15, Ydb::StatusIds::SESSION_BUSY, session, 1, type);
            UNIT_ASSERT(uploader->BuildTraceTrees());
            UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 2);
            for (const auto& span : uploader->Spans) {
                UNIT_ASSERT_VALUES_EQUAL(span.name(), "KQP request");
                UNIT_ASSERT(FindAttribute(span, "ydb.rejected")->value().bool_value());
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(span, "ydb.trace.coverage")->value().string_value(),
                    "rejected_before_query_state");
            }
            runtime.SetEventFilter(std::move(previous));
            runtime.Send(new IEventHandle(compile->Sender, firstSender, new NGRpcService::TEvClientLost()), 0, true);
            const auto response = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(firstSender);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetYdbStatus(), Ydb::StatusIds::CANCELLED);
        }
    }

    Y_UNIT_TEST(ForwardedTimeoutIsNotAnEarlyRejection) {
        auto [runtime, server, sender] = CreateServer(2);
        auto* uploader = RegisterUploader(runtime);
        const auto proxyId = runtime.GetLocalServiceId(NKqp::MakeKqpProxyID(runtime.GetNodeId(0)), 0);
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            const auto session = CreateSession(runtime, sender, type);
            TAutoPtr<IEventHandle> forwarded;
            auto previous = runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == NKqp::TEvKqp::TEvQueryRequest::EventType
                        && ev->GetRecipientRewrite() == proxyId) {
                    forwarded = ev;
                    return true;
                }
                return false;
            });
            ClearUploader(*uploader);
            auto request = MakeSQLRequest("SELECT 1;");
            request->Record.MutableRequest()->SetType(type);
            request->Record.MutableRequest()->SetSessionId(session);
            request->Record.MutableRequest()->SetTimeoutMs(100);
            ExecRequest(runtime, sender, std::move(request), 15, Ydb::StatusIds::TIMEOUT, 1);
            runtime.SetEventFilter(std::move(previous));
            UNIT_ASSERT(forwarded);
            AssertStatus(*uploader, "KQP request", NTraceProto::Status::STATUS_CODE_ERROR);
            const auto* proxy = FindSpan(*uploader, "KQP request");
            UNIT_ASSERT(!FindAttribute(*proxy, "ydb.rejected"));
            UNIT_ASSERT(!FindAttribute(*proxy, "ydb.trace.coverage"));
        }
    }

    Y_UNIT_TEST(DistributedCommitPhaseOutcomes) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 2, false);
        auto* uploader = RegisterUploader(runtime);
        const TString sql = "UPSERT INTO `/Root/table-1` (key, value) VALUES (1u, 10u), (4000000000u, 20u);";
        ExecSQL(runtime, sender, sql);
        AssertStatus(*uploader, "Prepare shards", NTraceProto::Status::STATUS_CODE_OK);
        AssertStatus(*uploader, "Coordinator", NTraceProto::Status::STATUS_CODE_OK);
        AssertStatus(*uploader, "Apply commit", NTraceProto::Status::STATUS_CODE_OK);
        AssertDescendant(*uploader, "Prepare shards", "Commit");
        const auto* prepare = FindSpan(*uploader, "Prepare shards");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*prepare, "ydb.phase")->value().string_value(), "CommitPrepareShards");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*prepare, "ydb.peer.actor.type")->value().string_value(), "DataShard");
        const auto* commit = FindSpan(*uploader, "Commit");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*commit, "ydb.actor.type")->value().string_value(), "TKqpBufferWriteActor");
        for (const auto* name : {"Prepare shards", "Apply commit"}) {
            const auto* phase = FindSpan(*uploader, name);
            UNIT_ASSERT_VALUES_EQUAL(phase->events_size(), 2);
            UNIT_ASSERT(FindAttribute(phase->events(1), "ydb.last_shard")->value().bool_value());
        }
        ClearUploader(*uploader);
        ExecSQL(runtime, sender,
            "INSERT INTO `/Root/table-1` (key, value) VALUES (2u, 10u), (4000000001u, 20u);");
        AssertDescendant(*uploader, "Flush effects", "Commit");
        AssertStatus(*uploader, "Prepare shards", NTraceProto::Status::STATUS_CODE_OK);
        AssertStatus(*uploader, "Apply commit", NTraceProto::Status::STATUS_CODE_OK);
        ClearUploader(*uploader);
        bool injected = false;
        TTestActorRuntimeBase::TEventFilter previous;
        previous = runtime.SetEventFilter([&](TTestActorRuntimeBase& rt, TAutoPtr<IEventHandle>& ev) {
            if (!injected && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
                const auto* result = ev->Get<NEvents::TDataEvents::TEvWriteResult>();
                auto error = NEvents::TDataEvents::TEvWriteResult::BuildError(result->Record.GetOrigin(),
                    result->Record.GetTxId(), NKikimrDataEvents::TEvWriteResult::STATUS_DISK_GROUP_OUT_OF_SPACE,
                    "injected commit failure");
                runtime.Send(ev->Recipient, ev->Sender, error.release());
                injected = true;
                return true;
            }
            return previous ? previous(rt, ev) : false;
        });
        ExecSQL(runtime, sender, sql, 15, Ydb::StatusIds::UNAVAILABLE);
        runtime.SetEventFilter(std::move(previous));
        UNIT_ASSERT(injected);
        AssertStatus(*uploader, "Commit", NTraceProto::Status::STATUS_CODE_ERROR);
        AssertStatus(*uploader, "Prepare shards", NTraceProto::Status::STATUS_CODE_ERROR);
        UNIT_ASSERT(!FindSpan(*uploader, "Apply commit"));
    }

    Y_UNIT_TEST(ColumnScanUsesCommonTrace) {
        auto [runtime, server, sender] = CreateServer();
        ExecSQL(runtime, sender, R"(
            CREATE TABLE `/Root/ColumnTable` (
                Key Uint64 NOT NULL,
                Value Uint64,
                PRIMARY KEY (Key)
            ) PARTITION BY HASH(Key) WITH (STORE = COLUMN);
        )", 0, Ydb::StatusIds::SUCCESS, {}, 0, NKikimrKqp::QUERY_TYPE_SQL_DDL);
        ExecSQL(runtime, sender, "UPSERT INTO `/Root/ColumnTable` (Key, Value) VALUES (1u, 10u);",
            0, Ydb::StatusIds::SUCCESS, {}, 0, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        auto* uploader = RegisterUploader(runtime);
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_SCAN, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            ClearUploader(*uploader);
            ExecSQL(runtime, sender, "SELECT SUM(Value) FROM `/Root/ColumnTable`;",
                15, Ydb::StatusIds::SUCCESS, {}, 0, type);
            UNIT_ASSERT(uploader->BuildTraceTrees());
            UNIT_ASSERT_VALUES_EQUAL(uploader->Traces.size(), 1u);
            AssertDescendant(*uploader, "Scan shard", "Scan table");
            AssertDescendant(*uploader, "Scan table", "Execute plan");
            AssertDescendant(*uploader, "Scan table", "Stage: ");
            AssertDescendant(*uploader, "Task: ", "Stage: ");
            AssertStatus(*uploader, "Scan shard", NTraceProto::Status::STATUS_CODE_OK);
            AssertStatus(*uploader, "Scan table", NTraceProto::Status::STATUS_CODE_OK);
            const auto* metadata = FindSpan(*uploader, "Metadata");
            UNIT_ASSERT(metadata);
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*metadata, "ydb.phase")->value().string_value(), "ResolveMetadata");
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*metadata, "ydb.peer.actor.type")->value().string_value(), "SchemeCache");
            const auto* shard = FindSpan(*uploader, "Scan shard");
            UNIT_ASSERT(FindAttribute(*shard, "ydb.shard_id"));
            UNIT_ASSERT(FindAttribute(*shard, "ydb.node_id"));
            UNIT_ASSERT(FindAttribute(*shard, "ydb.cpu_us"));
            UNIT_ASSERT(FindAttribute(*shard, "ydb.wait_us"));
            UNIT_ASSERT(FindAttribute(*shard, "ydb.finished")->value().bool_value());
        }
    }

    Y_UNIT_TEST(JoinAndAggregateDiagnosticsMatchTaskReports) {
        auto [runtime, server, sender] = CreateServer(2);
        CreateShardedTable(server, sender, "/Root", "table-1", 2, false);
        ExecSQL(runtime, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1u, 10u), (2u, 20u), (4000000000u, 10u);", 0);
        auto* uploader = RegisterUploader(runtime);
        const TString sql = R"(
            PRAGMA ydb.CostBasedOptimizationLevel='0';
            PRAGMA ydb.HashJoinMode='graceandself';
            SELECT a.value AS v, COUNT(*) AS n FROM `/Root/table-1` AS a
            FULL JOIN `/Root/table-1` AS b ON a.value = b.value GROUP BY a.value;
        )";
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            for (const ui8 level : {6, 10}) {
                ClearUploader(*uploader);
                std::map<TActorId, NYql::NDqProto::TDqTaskStats> tasks;
                bool taskStatsIncomplete = false;
                const auto observer = runtime.AddObserver<NYql::NDq::TEvDqCompute::TEvState>(
                    [&](NYql::NDq::TEvDqCompute::TEvState::TPtr& ev) {
                        const auto& state = ev->Get()->Record;
                        if (state.GetState() == NYql::NDqProto::COMPUTE_STATE_FINISHED && state.GetStats().TasksSize() == 1) {
                            tasks[ev->Sender] = state.GetStats().GetTasks(0);
                            const auto& task = tasks.at(ev->Sender);
                            taskStatsIncomplete |= !state.GetStats().GetDurationUs()
                                && (!task.GetStartTimeMs() || task.GetFinishTimeMs() < task.GetStartTimeMs());
                        }
                    });
                ExecSQL(runtime, sender, sql, level, Ydb::StatusIds::SUCCESS, {}, 0, type);
                UNIT_ASSERT(uploader->BuildTraceTrees());
                UNIT_ASSERT(!tasks.empty());
                ui64 cpu = 0, input = 0, output = 0, wait = 0;
                std::map<ui32, std::map<ui32, ui64>> nodesByStage;
                for (const auto& [actor, task] : tasks) {
                    ++nodesByStage[task.GetStageId()][actor.NodeId()];
                    cpu += task.GetCpuTimeUs();
                    input += task.GetInputRows();
                    output += task.GetOutputRows();
                    wait += task.GetWaitInputTimeUs() + task.GetWaitOutputTimeUs();
                }
                const auto* query = FindSpan(*uploader, "Query");
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*query, "ydb.wait_us")->value().int_value(), wait);
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*query, "ydb.task_stats_incomplete")->value().bool_value(),
                    taskStatsIncomplete);
                ui64 reported = 0, stageCpu = 0, stageInput = 0, stageOutput = 0;
                bool hasJoin = false, hasAggregate = false;
                for (const auto& event : StageSpans(*uploader)) {
                    reported += FindAttribute(event, "ydb.reported_tasks")->value().int_value();
                    stageCpu += FindAttribute(event, "ydb.cpu_us")->value().int_value();
                    stageInput += FindAttribute(event, "ydb.input_rows")->value().int_value();
                    stageOutput += FindAttribute(event, "ydb.output_rows")->value().int_value();
                    for (const auto& operation : FindAttribute(event, "ydb.stage.operations")->value().array_value().values()) {
                        hasJoin |= operation.string_value() == "Join";
                        hasAggregate |= operation.string_value() == "Aggregate";
                    }
                    TStringBuilder nodes;
                    const auto stageId = FindAttribute(event, "ydb.stage_id")->value().int_value();
                    for (const auto& taskSpan : uploader->Spans) {
                        const auto* taskStage = FindAttribute(taskSpan, "ydb.stage_id");
                        if (taskSpan.name().StartsWith("Task: ") && taskStage
                                && taskStage->value().int_value() == stageId) {
                            const auto* operations = FindAttribute(taskSpan, "ydb.task.operations");
                            UNIT_ASSERT(operations);
                            UNIT_ASSERT_VALUES_EQUAL(taskSpan.parent_span_id(), event.span_id());
                            UNIT_ASSERT_VALUES_EQUAL(taskSpan.trace_id(), event.trace_id());
                            UNIT_ASSERT(taskSpan.start_time_unix_nano() >= event.start_time_unix_nano());
                            UNIT_ASSERT(taskSpan.end_time_unix_nano() <= event.end_time_unix_nano());
                            UNIT_ASSERT_VALUES_EQUAL(operations->value().SerializeAsString(),
                                FindAttribute(event, "ydb.stage.operations")->value().SerializeAsString());
                        }
                    }
                    for (const auto& [node, count] : nodesByStage.at(stageId)) {
                        if (nodes) {
                            nodes << ",";
                        }
                        nodes << node << ":" << count;
                    }
                    UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.tasks_by_node")->value().string_value(), nodes);
                    UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.tasks")->value().int_value(),
                        FindAttribute(event, "ydb.reported_tasks")->value().int_value());
                }
                if (level == TComponentTracingLevels::TQueryProcessor::Basic) {
                    UNIT_ASSERT_VALUES_EQUAL(reported, 0);
                } else {
                    UNIT_ASSERT_C(hasJoin && hasAggregate, uploader->PrintTraces());
                    UNIT_ASSERT_VALUES_EQUAL(reported, tasks.size());
                    UNIT_ASSERT_VALUES_EQUAL(stageCpu, cpu);
                    UNIT_ASSERT_VALUES_EQUAL(stageInput, input);
                    UNIT_ASSERT_VALUES_EQUAL(stageOutput, output);
                }
            }
        }
    }

    Y_UNIT_TEST(SortedAndConstantTasksPreserveAvailableTiming) {
        auto [runtime, server, sender] = CreateServer(2);
        CreateShardedTable(server, sender, "/Root", "table-1", 2, false);
        ExecSQL(runtime, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1u, 10u), (4000000000u, 20u);", 0);
        auto* uploader = RegisterUploader(runtime);
        const std::vector<TString> queries = {
            "SELECT key, value FROM `/Root/table-1` ORDER BY value;",
            "SELECT key, value FROM `/Root/table-1` WHERE value = 123u ORDER BY value;",
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (2u, 30u), (4000000001u, 40u);",
        };
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            for (const ui8 level : {6, 10}) {
                for (const auto& sql : queries) {
                    ClearUploader(*uploader);
                    size_t finishedTasks = 0;
                    size_t timedTasks = 0;
                    const auto observer = runtime.AddObserver<NYql::NDq::TEvDqCompute::TEvState>(
                        [&](NYql::NDq::TEvDqCompute::TEvState::TPtr& ev) {
                            const auto& state = ev->Get()->Record;
                            if (state.GetState() != NYql::NDqProto::COMPUTE_STATE_FINISHED) {
                                return;
                            }
                            UNIT_ASSERT_VALUES_EQUAL_C(state.GetStats().TasksSize(), 1, sql);
                            const auto& task = state.GetStats().GetTasks(0);
                            timedTasks += state.GetStats().GetDurationUs()
                                || (task.GetStartTimeMs() && task.GetFinishTimeMs() >= task.GetStartTimeMs());
                            ++finishedTasks;
                        });
                    ExecSQL(runtime, sender, sql, level, Ydb::StatusIds::SUCCESS, {}, 0, type);
                    UNIT_ASSERT_C(finishedTasks, sql);
                    const auto* query = FindSpan(*uploader, "Query");
                    UNIT_ASSERT(query);
                    UNIT_ASSERT_VALUES_EQUAL_C(FindAttribute(*query, "ydb.task_stats_incomplete")->value().bool_value(),
                        timedTasks != finishedTasks, sql);
                    size_t reported = 0;
                    size_t timed = 0;
                    for (const auto& event : StageSpans(*uploader)) {
                        reported += FindAttribute(event, "ydb.reported_tasks")->value().int_value();
                        timed += FindAttribute(event, "ydb.timed_tasks")->value().int_value();
                    }
                    if (level >= TComponentTracingLevels::TQueryProcessor::Detailed) {
                        UNIT_ASSERT_VALUES_EQUAL_C(reported, finishedTasks, sql);
                        UNIT_ASSERT_VALUES_EQUAL_C(timed, timedTasks, sql);
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(reported, 0);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(RetriedReadAppearsInTaskDiagnostics, Lookup) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(true);
        auto [runtime, server, sender] = CreateServer(1, config);
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        ExecSQL(runtime, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1u, 10u);", 0);
        auto* uploader = RegisterUploader(runtime);
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            ClearUploader(*uploader);
            bool interrupted = false;
            TTestActorRuntimeBase::TEventFilter previous;
            previous = runtime.SetEventFilter([&](TTestActorRuntimeBase& rt, TAutoPtr<IEventHandle>& ev) {
                if (!interrupted && ev->TraceId && ev->GetTypeRewrite() == TEvPipeCache::TEvForward::EventType) {
                    const auto* forward = ev->Get<TEvPipeCache::TEvForward>();
                    if (forward->Ev->Type() == TEvDataShard::TEvRead::EventType) {
                        runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                            new TEvPipeCache::TEvDeliveryProblem(forward->TabletId, true)), 0, true);
                        interrupted = true;
                        return true;
                    }
                }
                return previous ? previous(rt, ev) : false;
            });
            ExecSQL(runtime, sender, Lookup ? R"(
                $keys = AsList(AsStruct(1u AS key), AsStruct(4000000000u AS key));
                SELECT b.value FROM AS_TABLE($keys) AS a JOIN `/Root/table-1` AS b ON a.key = b.key;
            )" : "SELECT SUM(value) FROM `/Root/table-1`;",
                15, Ydb::StatusIds::SUCCESS, {}, 0, type);
            runtime.SetEventFilter(std::move(previous));
            UNIT_ASSERT(interrupted);
            UNIT_ASSERT(uploader->BuildTraceTrees());
            const auto* read = FindSpan(*uploader, Lookup ? "Lookup rows" : "Read table");
            UNIT_ASSERT(read);
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*read, "ydb.read_retries")->value().int_value(), 1);
            AssertDescendant(*uploader, "Datashard.Read", "Read shard");
            bool shardRetried = false;
            for (const auto& event : read->events()) {
                shardRetried |= event.name() == "Shard read statistics"
                    && FindAttribute(event, "ydb.read_retries")->value().int_value() == 1
                    && FindAttribute(event, "ydb.reads")->value().int_value() == 2;
            }
            UNIT_ASSERT(shardRetried);
            bool taskRetried = false;
            for (const auto& event : StageSpans(*uploader)) {
                for (const auto& task : FindAttribute(event, "ydb.interesting_tasks")->value().array_value().values()) {
                    for (const auto& attr : task.kvlist_value().values()) {
                        taskRetried |= attr.key() == "ydb.read_retries" && attr.value().int_value() == 1;
                    }
                }
            }
            UNIT_ASSERT(taskRetried);
        }
    }

    Y_UNIT_TEST(FailedTaskMarksDiagnosticsIncomplete) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 2, false);
        ExecSQL(runtime, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1u, 10u), (4000000000u, 20u);", 0);
        auto* uploader = RegisterUploader(runtime);
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            ClearUploader(*uploader);
            bool failed = false;
            const auto observer = runtime.AddObserver<NYql::NDq::TEvDqCompute::TEvState>(
                [&](NYql::NDq::TEvDqCompute::TEvState::TPtr& ev) {
                    failed |= ev->Get()->Record.GetState() == NYql::NDqProto::COMPUTE_STATE_FAILURE;
                });
            ExecSQL(runtime, sender,
                "SELECT Ensure(value, value = 0u, 'execution failed') FROM `/Root/table-1`;",
                15, Ydb::StatusIds::PRECONDITION_FAILED, {}, 0, type);
            UNIT_ASSERT(failed);
            AssertStatus(*uploader, "Query", NTraceProto::Status::STATUS_CODE_ERROR);
            const auto* execution = FindSpan(*uploader, "Execute plan");
            const auto* query = FindSpan(*uploader, "Query");
            UNIT_ASSERT(execution && query);
            UNIT_ASSERT(FindAttribute(*execution, "ydb.task_stats_incomplete")->value().bool_value());
            UNIT_ASSERT(FindAttribute(*query, "ydb.task_stats_incomplete")->value().bool_value());
            UNIT_ASSERT(std::ranges::any_of(StageSpans(*uploader), [](const auto& event) {
                return FindAttribute(event, "ydb.failed_tasks")->value().int_value() > 0;
            }));
        }
    }

    Y_UNIT_TEST(SpillingReportsFollowRequestedStatsMode) {
        NKikimrConfig::TAppConfig config;
        auto& tableService = *config.MutableTableServiceConfig();
        tableService.SetEnableQueryServiceSpilling(true);
        tableService.SetEnableSpillingInHashJoinShuffleConnections(false);
        auto& memory = *tableService.MutableResourceManager();
        memory.SetMkqlLightProgramMemoryLimit(100);
        memory.SetMkqlHeavyProgramMemoryLimit(300);
        memory.SetSpillingPercent(0.01);
        auto& spilling = *tableService.MutableSpillingServiceConfig()->MutableLocalFileConfig();
        spilling.SetEnable(true);
        spilling.SetRoot("./spilling/");
        MakeDirIfNotExist("./spilling");
        auto [runtime, server, sender] = CreateServer(1, config);
        ExecSQL(runtime, sender, "CREATE TABLE `/Root/SpillData` (Key Uint64, Value String, PRIMARY KEY (Key));",
            0, Ydb::StatusIds::SUCCESS, {}, 0, NKikimrKqp::QUERY_TYPE_SQL_DDL);
        for (ui32 i = 0; i < 128; ++i) {
            ExecSQL(runtime, sender, TStringBuilder() << "UPSERT INTO `/Root/SpillData` (Key, Value) VALUES ("
                << i << "u, '" << TString(200000 + i, 'a' + i % 26) << "');", 0);
        }
        auto* uploader = RegisterUploader(runtime);
        NKqp::TKqpCounters counters(runtime.GetAppData().Counters);
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_SCAN, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            for (const bool full : {false, true}) {
                ClearUploader(*uploader);
                std::map<TActorId, ui64> taskSpills;
                const auto writesBefore = counters.ComputeSpilling.WriteBlobs->Val();
                const auto readsBefore = counters.ComputeSpilling.ReadBlobs->Val();
                const auto tasks = runtime.AddObserver<NKqp::TEvKqpNode::TEvStartKqpTasksRequest>(
                    [&](NKqp::TEvKqpNode::TEvStartKqpTasksRequest::TPtr& ev) {
                        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(ev->Get()->Record.GetRuntimeSettings().GetStatsMode()),
                            static_cast<int>(full ? NYql::NDqProto::DQ_STATS_MODE_FULL : NYql::NDqProto::DQ_STATS_MODE_BASIC));
                    });
                const auto states = runtime.AddObserver<NYql::NDq::TEvDqCompute::TEvState>(
                    [&](NYql::NDq::TEvDqCompute::TEvState::TPtr& ev) {
                        const auto& state = ev->Get()->Record;
                        if (state.GetState() == NYql::NDqProto::COMPUTE_STATE_FINISHED) {
                            ui64 bytes = 0;
                            for (const auto& task : state.GetStats().GetTasks()) {
                                bytes += task.GetSpillingComputeWriteBytes() + task.GetSpillingChannelWriteBytes();
                            }
                            taskSpills[ev->Sender] = bytes;
                        }
                    });
                auto request = MakeSQLRequest(R"(
                    PRAGMA ydb.EnableSpillingNodes='GraceJoin';
                    PRAGMA ydb.CostBasedOptimizationLevel='0';
                    PRAGMA ydb.HashJoinMode='graceandself';
                    SELECT COUNT(*) FROM `/Root/SpillData` AS a
                    FULL JOIN `/Root/SpillData` AS b ON a.Value = b.Value;
                )");
                request->Record.MutableRequest()->SetType(type);
                request->Record.MutableRequest()->SetCollectStats(full
                    ? Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL
                    : Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE);
                if (type == NKikimrKqp::QUERY_TYPE_SQL_SCAN) {
                    request->Record.MutableRequest()->ClearTxControl();
                }
                ExecRequest(runtime, sender, std::move(request));
                UNIT_ASSERT(counters.ComputeSpilling.WriteBlobs->Val() > writesBefore);
                UNIT_ASSERT(counters.ComputeSpilling.ReadBlobs->Val() > readsBefore);
                UNIT_ASSERT(!taskSpills.empty());
                ui64 spilledBytes = 0;
                for (const auto& [actor, bytes] : taskSpills) {
                    spilledBytes += bytes;
                }
                UNIT_ASSERT_VALUES_EQUAL(spilledBytes > 0, full);
                UNIT_ASSERT(uploader->BuildTraceTrees());
                const auto* query = FindSpan(*uploader, "Query");
                UNIT_ASSERT(query);
                UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*query, "ydb.spilled_bytes")->value().int_value(), spilledBytes);
            }
        }
    }

    Y_UNIT_TEST(IndexMetadataPurposeAndBufferReads) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
        auto [runtime, server, sender] = CreateServer(1, config);
        ExecSQL(runtime, sender, R"(
            CREATE TABLE `/Root/UniqueValues` (
                Key Uint32,
                Value Uint32 NOT NULL,
                PRIMARY KEY (Key),
                INDEX ValueIndex GLOBAL UNIQUE SYNC ON (Value)
            );
        )", 0, Ydb::StatusIds::SUCCESS, {}, 0, NKikimrKqp::QUERY_TYPE_SQL_DDL);
        auto* uploader = RegisterUploader(runtime);
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            const auto sessionId = CreateSession(runtime, sender, type);
            TString txId;
            for (ui32 step = 0; step < 3; ++step) {
                const ui8 level = step == 1 ? 0 : 15;
                ClearUploader(*uploader);
                auto request = MakeSQLRequest(TStringBuilder()
                    << "UPSERT INTO `/Root/UniqueValues` (Key, Value) VALUES (1u, " << (10 + step) << "u); "
                    << "SELECT * FROM `/Root/UniqueValues` WHERE Key = 1u;");
                auto& query = *request->Record.MutableRequest();
                query.SetType(type);
                query.SetSessionId(sessionId);
                auto& tx = *query.MutableTxControl();
                if (txId) {
                    tx.clear_begin_tx();
                    tx.set_tx_id(txId);
                }
                tx.set_commit_tx(step == 2);
                auto response = ExecRequest(runtime, sender, std::move(request), level);
                txId = response.GetResponse().GetTxMeta().id();
                if (!level) {
                    UNIT_ASSERT(uploader->Spans.empty());
                    continue;
                }
                UNIT_ASSERT(uploader->BuildTraceTrees());
                UNIT_ASSERT(std::ranges::any_of(uploader->Spans, [](const auto& span) {
                    const auto* purpose = FindAttribute(span, "ydb.compile_dependency.purpose");
                    return purpose && purpose->value().string_value() == "index_implementation";
                }));
                const auto* lookup = FindSpan(*uploader, "Check rows");
                UNIT_ASSERT_C(lookup, uploader->PrintTraces());
                UNIT_ASSERT_C(std::ranges::any_of(uploader->Spans, [](const auto& span) {
                    return span.name() == "Check rows" && std::ranges::any_of(span.events(), [](const auto& event) {
                        return event.name() == "Shard read statistics";
                    });
                }), "type=" << static_cast<int>(type) << " step=" << step << " " << uploader->PrintTraces());
                UNIT_ASSERT_VALUES_EQUAL(uploader->Traces.size(), 1);
                AssertDescendant(*uploader, "Check rows", "Query");
                AssertStatus(*uploader, "Buffer rows", NTraceProto::Status::STATUS_CODE_OK);
            }
        }
    }

    Y_UNIT_TEST(SnapshotTraceEndsWithCancelledQuery) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        auto* uploader = RegisterUploader(runtime);
        for (const auto type : {NKikimrKqp::QUERY_TYPE_SQL_DML, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}) {
            const auto sessionId = CreateSession(runtime, sender, type);
            auto makeRequest = [&] {
                auto request = MakeSQLRequest("SELECT * FROM `/Root/table-1`;");
                auto& query = *request->Record.MutableRequest();
                query.SetType(type);
                query.SetSessionId(sessionId);
                query.MutableTxControl()->set_commit_tx(false);
                ActorIdToProto(sender, request->Record.MutableRequestActorId());
                return request;
            };

            ClearUploader(*uploader);
            TAutoPtr<IEventHandle> blockedSnapshot;
            TTestActorRuntimeBase::TEventFilter previous;
            previous = runtime.SetEventFilter([&](TTestActorRuntimeBase& rt, TAutoPtr<IEventHandle>& ev) {
                if (!blockedSnapshot && ev->GetTypeRewrite() == NKqp::TEvKqpSnapshot::TEvCreateSnapshotResponse::EventType) {
                    blockedSnapshot = ev.Release();
                    return true;
                }
                return previous ? previous(rt, ev) : false;
            });
            runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender,
                makeRequest().Release(), 0, 0, nullptr, NWilson::TTraceId::NewTraceId(15, 4095)));
            TDispatchOptions blocked;
            blocked.FinalEvents.emplace_back([&](IEventHandle&) { return bool(blockedSnapshot); });
            runtime.DispatchEvents(blocked);
            runtime.SetEventFilter(std::move(previous));
            runtime.Send(new IEventHandle(blockedSnapshot->Recipient, sender, new NGRpcService::TEvClientLost()));
            const auto cancelled = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(cancelled->Get()->Record.GetYdbStatus(), Ydb::StatusIds::CANCELLED);
            runtime.SimulateSleep(TDuration::Seconds(1));
            UNIT_ASSERT(uploader->BuildTraceTrees());
            AssertStatus(*uploader, "Acquire snapshot", NTraceProto::Status::STATUS_CODE_ERROR);
            const auto* snapshot = FindSpan(*uploader, "Acquire snapshot");
            const auto* query = FindSpan(*uploader, "Query");
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*snapshot, "ydb.status_code")->value().string_value(), "CANCELLED");
            UNIT_ASSERT(snapshot->end_time_unix_nano() <= query->end_time_unix_nano());

            ClearUploader(*uploader);
            runtime.Send(blockedSnapshot.Release());
            runtime.SimulateSleep(TDuration::Seconds(1));
            UNIT_ASSERT(uploader->Spans.empty());
            ExecRequest(runtime, sender, makeRequest());
            UNIT_ASSERT(uploader->BuildTraceTrees());
            UNIT_ASSERT_VALUES_EQUAL(uploader->Traces.size(), 1);
            AssertStatus(*uploader, "Acquire snapshot", NTraceProto::Status::STATUS_CODE_OK);
            AssertStatus(*uploader, "Query", NTraceProto::Status::STATUS_CODE_OK);
        }
    }

    TString CheckClientLost(TTestActorRuntime& runtime, TActorId sender,
            TFakeWilsonUploader* uploader,
            bool tracing, ui32 requestNode) {
        ClearUploader(*uploader);

        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(0)), sender,
            new NKqp::TEvKqp::TEvCreateSessionRequest()));
        auto createSession = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvCreateSessionResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(createSession->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);

        TAutoPtr<IEventHandle> blockedCompile;
        TActorId sessionActor;
        TTestActorRuntimeBase::TEventFilter previousFilter;
        auto filter = [&](TTestActorRuntimeBase& runtimeBase, TAutoPtr<IEventHandle>& ev) {
            if (!blockedCompile
                    && ev->GetTypeRewrite() == NKqp::TEvKqp::TEvCompileRequest::EventType) {
                sessionActor = ev->Sender;
                blockedCompile = ev.Release();
                return true;
            }
            return previousFilter ? previousFilter(runtimeBase, ev) : false;
        };
        previousFilter = runtime.SetEventFilter(filter);

        auto request = MakeSQLRequest("SELECT 1;", true);
        request->Record.MutableRequest()->SetSessionId(
            createSession->Get()->Record.GetResponse().GetSessionId());
        request->Record.MutableRequest()->SetKeepSession(true);
        NWilson::TTraceId traceId;
        if (tracing) {
            traceId = NWilson::TTraceId::NewTraceId(15, 4095);
        }
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(requestNode)), sender,
            request.Release(), 0, 0, nullptr, std::move(traceId)));
        TDispatchOptions compileBlocked;
        compileBlocked.FinalEvents.emplace_back([&](IEventHandle&) { return bool(blockedCompile); });
        runtime.DispatchEvents(compileBlocked);
        runtime.SetEventFilter(std::move(previousFilter));

        runtime.Send(new IEventHandle(sessionActor, sender,
            new NGRpcService::TEvClientLost()));
        runtime.SimulateSleep(TDuration::Seconds(1));

        const auto cancelled = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(cancelled->Get()->Record.GetYdbStatus(), Ydb::StatusIds::CANCELLED);
        if (tracing) {
            UNIT_ASSERT(uploader->BuildTraceTrees());
            UNIT_ASSERT_VALUES_EQUAL(uploader->Traces.size(), 1u);
            AssertStatus(*uploader, "Query", NTraceProto::Status::STATUS_CODE_ERROR);
        } else {
            UNIT_ASSERT(uploader->Spans.empty());
        }

        auto list = MakeHolder<NKqp::TEvKqp::TEvListSessionsRequest>();
        list->Record.SetFreeSpace(1000000);
        list->Record.AddColumns(1);
        list->Record.AddColumns(3);
        list->Record.AddColumns(4);
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(0)), sender,
            list.Release()));
        const auto sessions = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvListSessionsResponse>(sender);
        const TString sessionId = createSession->Get()->Record.GetResponse().GetSessionId();
        const auto found = std::ranges::find_if(sessions->Get()->Record.GetSessions(),
            [&](const auto& item) { return item.GetSessionId() == sessionId; });
        UNIT_ASSERT_C(found != sessions->Get()->Record.GetSessions().end(),
            sessions->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(found->GetState(), "IDLE", "cancelled query kept as active");
        UNIT_ASSERT_C(found->GetQuery().empty(), "cancelled query text remained attached to session");
        return sessionId;
    }

    Y_UNIT_TEST(ClientLostRestoresSessionIdleTimeout) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetSessionIdleDurationSeconds(5);
        auto [runtime, server, sender] = CreateServer(2, std::move(config));
        Y_UNUSED(server);
        auto* uploader = RegisterUploader(runtime);
        for (const bool tracing : {false, true}) {
            for (const ui32 requestNode : {0u, 1u}) {
                const TString sessionId = CheckClientLost(runtime, sender, uploader, tracing, requestNode);
                runtime.SimulateSleep(TDuration::Seconds(10));

                auto list = MakeHolder<NKqp::TEvKqp::TEvListSessionsRequest>();
                list->Record.SetFreeSpace(1000000);
                list->Record.AddColumns(1);
                runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(0)),
                    sender, list.Release()));
                const auto sessions = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvListSessionsResponse>(sender);
                UNIT_ASSERT_C(std::ranges::none_of(sessions->Get()->Record.GetSessions(),
                    [&](const auto& item) { return item.GetSessionId() == sessionId; }),
                    "cancelled session survived its idle timeout: " << sessions->Get()->Record.DebugString());
            }
        }
    }

}
} // namespace NKikimr
