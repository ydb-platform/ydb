#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/tracing/kqp_query_tracing.h>
#include <ydb/core/kqp/tracing/kqp_execution_tracing.h>
#include <ydb/core/kqp/tracing/kqp_scan_tracing.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/core/kqp/tracing/kqp_trace_settings.h>
#include <ydb/core/kqp/common/simple/query_stats.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_tracing.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/grpc_services/cancelation/cancelation_event.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr {
using namespace Tests;
using namespace NWilson;

Y_UNIT_TEST_SUITE(TKqpQueryTrace) {
    std::tuple<TTestActorRuntime&, TServer::TPtr, TActorId> CreateServer(ui32 nodes = 1,
            NKikimrConfig::TAppConfig config = {}) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root").SetUseRealThreads(false).SetNodeCount(nodes).SetAppConfig(config);
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

    void ClearUploader(TFakeWilsonUploader& uploader) {
        uploader.Spans.clear();
        uploader.Traces.clear();
    }

    const TFakeWilsonUploader::TOtelSpan* FindSpan(const TFakeWilsonUploader& uploader, TStringBuf name) {
        for (const auto& span : uploader.Spans) {
            if (span.name() == name) {
                return &span;
            }
        }
        return nullptr;
    }

    template<class T>
    const opentelemetry::proto::common::v1::KeyValue* FindAttribute(
            const T& span, TStringBuf name) {
        for (const auto& attr : span.attributes()) {
            if (attr.key() == name) {
                return &attr;
            }
        }
        return nullptr;
    }

    void AssertStatus(const TFakeWilsonUploader& uploader, TStringBuf name,
            NTraceProto::Status::StatusCode status) {
        const auto* span = FindSpan(uploader, name);
        UNIT_ASSERT_C(span, "missing span " << name << ": " << uploader.PrintTraces());
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(span->status().code()), static_cast<int>(status), span->DebugString());
    }

    void AssertDescendant(const TFakeWilsonUploader& uploader, TStringBuf childName, TStringBuf parentName) {
        const auto* child = FindSpan(uploader, childName);
        UNIT_ASSERT_C(child, uploader.PrintTraces());
        TString parentId = child->parent_span_id();
        for (size_t hop = 0; hop < uploader.Spans.size(); ++hop) {
            const auto it = std::ranges::find_if(uploader.Spans, [&](const auto& span) {
                return span.trace_id() == child->trace_id() && span.span_id() == parentId;
            });
            if (it == uploader.Spans.end()) {
                break;
            }
            if (it->name() == parentName) {
                return;
            }
            parentId = it->parent_span_id();
        }
        UNIT_FAIL(childName << " is not under " << parentName << ": " << uploader.PrintTraces());
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

    Y_UNIT_TEST(CommonTreeIncludesKqpAndDatashard) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        ExecSQL(runtime, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100);", 0);
        auto* uploader = RegisterUploader(runtime);
        ExecSQL(runtime, sender, "SELECT SUM(value) FROM `/Root/table-1` WHERE key > 0u;");
        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT_VALUES_EQUAL(uploader->Traces.size(), 1u);
        AssertDescendant(*uploader, "Execute query", "KQP request");
        AssertDescendant(*uploader, "Compile query", "Compile");
        AssertDescendant(*uploader, "Load metadata", "Compile query");
        AssertDescendant(*uploader, "Compute task", "Execute");
        AssertDescendant(*uploader, "Datashard.Read", "Read table");
        AssertDescendant(*uploader, "Read table", "Compute task");
        AssertStatus(*uploader, "Execute query", NTraceProto::Status::STATUS_CODE_OK);
        const auto* query = FindSpan(*uploader, "Execute query");
        UNIT_ASSERT_VALUES_EQUAL_C(FindAttribute(*query, "db.operation.name")->value().string_value(), "SELECT", query->DebugString());
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*query, "db.namespace")->value().string_value(), "/Root");
        UNIT_ASSERT(FindAttribute(*query, "ydb.cpu_us"));
        UNIT_ASSERT(FindAttribute(*query, "ydb.wait_us"));
        UNIT_ASSERT(FindAttribute(*query, "ydb.spilled_bytes"));
        const auto* shard = FindSpan(*uploader, "Datashard.Read");
        UNIT_ASSERT(FindAttribute(*shard, "ydb.shard_id"));
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*shard, "ydb.rows")->value().int_value(), 1);
        UNIT_ASSERT(std::ranges::any_of(uploader->Spans, [](const auto& span) {
            return std::ranges::any_of(span.events(), [](const auto& event) {
                return event.name() == "Stage statistics";
            });
        }));
        UNIT_ASSERT(FindAttribute(*FindSpan(*uploader, "Compute task"), "ydb.task_id"));
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
        const auto* query = FindSpan(*uploader, "Execute query");
        UNIT_ASSERT(query);
        UNIT_ASSERT(!FindAttribute(*query, "ydb.wait_us"));
        UNIT_ASSERT(!FindAttribute(*query, "ydb.spilled_bytes"));
        ClearUploader(*uploader);
        ExecSQL(runtime, sender, sql, TComponentTracingLevels::TQueryProcessor::Basic);
        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT(FindSpan(*uploader, "Execute"));
        UNIT_ASSERT(!FindSpan(*uploader, "Compute task"));
        UNIT_ASSERT(!FindSpan(*uploader, "Load metadata"));
        ClearUploader(*uploader);
        ExecSQL(runtime, sender, sql, TComponentTracingLevels::TQueryProcessor::Detailed);
        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT(FindSpan(*uploader, "Compute task"));
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
        UNIT_ASSERT(FindSpan(*uploader, "Compute task"));
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
        auto* uploader = RegisterUploader(*kikimr.GetTestServer().GetRuntime());
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteDataQuery("SELECT * FROM `/Root/table-1`;",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        Sleep(TDuration::Seconds(1));
        UNIT_ASSERT(uploader->BuildTraceTrees());
        AssertDescendant(*uploader, "Execute query", "KQP request");
        AssertDescendant(*uploader, "Datashard.Read", "Read table");

        ClearUploader(*uploader);
        auto iterator = tableClient.StreamExecuteScanQuery("SELECT * FROM `/Root/table-1`;").GetValueSync();
        UNIT_ASSERT_C(iterator.IsSuccess(), iterator.GetIssues().ToString());
        while (true) {
            auto part = iterator.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }
        }
        Sleep(TDuration::Seconds(1));
        UNIT_ASSERT(uploader->BuildTraceTrees());
        AssertDescendant(*uploader, "Compute task", "Execute");
        AssertStatus(*uploader, "Execute query", NTraceProto::Status::STATUS_CODE_OK);

        auto db = kikimr.GetQueryClient();
        ClearUploader(*uploader);
        auto queryResult = db.ExecuteQuery("SELECT * FROM `/Root/table-1`;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        Sleep(TDuration::Seconds(1));
        UNIT_ASSERT(uploader->BuildTraceTrees());
        AssertDescendant(*uploader, "Execute query", "KQP request");
        AssertDescendant(*uploader, "Datashard.Read", "Read table");

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
        Sleep(TDuration::Seconds(1));
        ClearUploader(*uploader);
        queryResult = db.ExecuteQuery(R"(
            SELECT Key FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "cats");
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        Sleep(TDuration::Seconds(1));
        UNIT_ASSERT(uploader->BuildTraceTrees());
        AssertDescendant(*uploader, "Datashard.Read", "Full-text search");
    }

    Y_UNIT_TEST(CompileFailureAndCacheHit) {
        auto [runtime, server, sender] = CreateServer();
        auto* uploader = RegisterUploader(runtime);
        ExecSQL(runtime, sender, "SELECT * FROM `/Root/missing_table`;", 15, Ydb::StatusIds::SCHEME_ERROR);
        AssertStatus(*uploader, "Compile", NTraceProto::Status::STATUS_CODE_ERROR);
        AssertStatus(*uploader, "Compile query", NTraceProto::Status::STATUS_CODE_ERROR);
        AssertStatus(*uploader, "Load metadata", NTraceProto::Status::STATUS_CODE_ERROR);
        ClearUploader(*uploader);
        const TString sql = "SELECT 12345;";
        ExecSQL(runtime, sender, sql, 15, Ydb::StatusIds::SUCCESS, {}, 0, NKikimrKqp::QUERY_TYPE_SQL_DML, true);
        ClearUploader(*uploader);
        ExecSQL(runtime, sender, sql, 15, Ydb::StatusIds::SUCCESS, {}, 0, NKikimrKqp::QUERY_TYPE_SQL_DML, true);
        const auto* query = FindSpan(*uploader, "Execute query");
        UNIT_ASSERT(query);
        const auto* hit = FindAttribute(*query, "ydb.compile.cache_hit");
        UNIT_ASSERT(hit && hit->value().bool_value());
        UNIT_ASSERT(!FindSpan(*uploader, "Compile query"));
    }

    Y_UNIT_TEST(ForwardingAndEarlyRejection) {
        auto [runtime, server, sender] = CreateServer(2);
        auto* uploader = RegisterUploader(runtime);
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(0)), sender,
            new NKqp::TEvKqp::TEvCreateSessionRequest()));
        const auto created = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvCreateSessionResponse>(sender);
        ExecSQL(runtime, sender, "SELECT 1;", 15, Ydb::StatusIds::SUCCESS,
            created->Get()->Record.GetResponse().GetSessionId(), 1);
        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT_VALUES_EQUAL(uploader->Traces.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(std::ranges::count_if(uploader->Spans,
            [](const auto& span) { return span.name() == "KQP request"; }), 2);
        ClearUploader(*uploader);
        ExecSQL(runtime, sender, "SELECT 1;", 15, Ydb::StatusIds::BAD_SESSION,
            "ydb://session/3?node_id=1&id=missing");
        AssertStatus(*uploader, "KQP request", NTraceProto::Status::STATUS_CODE_ERROR);
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

    Y_UNIT_TEST(BasicTaskTraceUsesExistingTimingAndSpillCounters) {
        auto [runtime, server, sender] = CreateServer();
        auto* uploader = RegisterUploader(runtime);
        NYql::NDq::TDqTaskRunnerStats source{};
        source.StartTs = TInstant::MilliSeconds(120);
        source.SpillingComputeWriteBytes = 30;
        source.SpillingChannelWriteBytes = 70;
        NYql::NDqProto::TDqComputeActorStats stats;
        auto& task = *stats.AddTasks();
        task.SetCreateTimeMs(100);
        NYql::NDq::FillComputeTraceStats(source, task);
        UNIT_ASSERT_VALUES_EQUAL(task.GetStartTimeMs(), 120);
        NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Detailed,
            NWilson::TTraceId::NewTraceId(15, 4095), "Compute task", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        NYql::NDq::AddComputeTraceAttributes(span, stats);
        span.EndOk();
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto* compute = FindSpan(*uploader, "Compute task");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*compute, "ydb.queue_delay_us")->value().int_value(), 20000);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*compute, "ydb.spilled_bytes")->value().int_value(), 100);
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
            AssertDescendant(*uploader, "Scan table", "Execute");
            AssertStatus(*uploader, "Scan shard", NTraceProto::Status::STATUS_CODE_OK);
            AssertStatus(*uploader, "Scan table", NTraceProto::Status::STATUS_CODE_OK);
            const auto* shard = FindSpan(*uploader, "Scan shard");
            UNIT_ASSERT(FindAttribute(*shard, "ydb.shard_id"));
            UNIT_ASSERT(FindAttribute(*shard, "ydb.node_id"));
            UNIT_ASSERT(FindAttribute(*shard, "ydb.cpu_us"));
            UNIT_ASSERT(FindAttribute(*shard, "ydb.wait_us"));
            UNIT_ASSERT(FindAttribute(*shard, "ydb.finished")->value().bool_value());
        }
    }

    Y_UNIT_TEST(StageDiagnosticsPreserveAnomaliesAndTotals) {
        auto [runtime, server, sender] = CreateServer();
        auto* uploader = RegisterUploader(runtime);
        NKqp::TExecutionTraceStats diagnostics;
        NKqpProto::TKqpPhyStage stage;
        stage.SetProgramAst("Aggregate");
        for (ui64 id = 1; id <= 10; ++id) {
            NYql::NDqProto::TDqTaskStats task;
            task.SetTaskId(id);
            task.SetStageId(0);
            task.SetCpuTimeUs(5);
            task.SetWaitInputTimeUs(10);
            task.SetSpillingComputeWriteBytes(id == 2 ? 200 : 0);
            diagnostics.AddTask(0, stage, 10, task, id * 100, id <= 5 ? 1 : 2, id == 1);
        }
        NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Basic,
            NWilson::TTraceId::NewTraceId(15, 4095), "Execute", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        NYql::NDqProto::TDqExecutionStats stats;
        diagnostics.Finish(span, stats, Ydb::StatusIds::ABORTED);
        span.EndError("task failed");
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto* execution = FindSpan(*uploader, "Execute");
        UNIT_ASSERT(execution);
        UNIT_ASSERT_VALUES_EQUAL(execution->events_size(), 1);
        const auto& event = execution->events(0);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.reported_tasks")->value().int_value(), 10);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.failed_tasks")->value().int_value(), 1);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.task_duration_min_us")->value().int_value(), 100);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.task_duration_avg_us")->value().int_value(), 550);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.task_duration_max_us")->value().int_value(), 1000);
        UNIT_ASSERT_DOUBLES_EQUAL(FindAttribute(event, "ydb.task_skew")->value().double_value(), 1000.0 / 550, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.tasks_by_node")->value().string_value(), "1:5,2:5");
        const auto& tasks = FindAttribute(event, "ydb.interesting_tasks")->value().array_value();
        UNIT_ASSERT_VALUES_EQUAL(tasks.values_size(), NKqp::NQueryTraceSettings::MaxTasksPerStage);
        bool hasFailed = false;
        bool hasSpill = false;
        for (const auto& task : tasks.values()) {
            for (const auto& attr : task.kvlist_value().values()) {
                hasFailed |= attr.key() == "ydb.failed" && attr.value().bool_value();
                hasSpill |= attr.key() == "ydb.spilled_bytes" && attr.value().int_value() == 200;
            }
        }
        UNIT_ASSERT(hasFailed && hasSpill);
        NKqpProto::TKqpExecutionExtraStats extra;
        UNIT_ASSERT(stats.GetExtra().UnpackTo(&extra));
        UNIT_ASSERT(extra.GetTaskStatsIncomplete());
        UNIT_ASSERT_VALUES_EQUAL(extra.GetWaitTimeUs(), 100);
        UNIT_ASSERT_VALUES_EQUAL(extra.GetSpilledBytes(), 200);

        NKqp::TKqpQueryStats queryStats;
        queryStats.Executions.push_back(stats);
        queryStats.LocksBrokenAsVictim = 3;
        NWilson::TSpan query(TComponentTracingLevels::TQueryProcessor::TopLevel,
            NWilson::TTraceId::NewTraceId(15, 4095), "Query", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        NKqp::AddQueryResultAttributes(query, {"SELECT", "SELECT"}, queryStats, 1, Ydb::StatusIds::ABORTED);
        query.EndError("task failed");
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto* result = FindSpan(*uploader, "Query");
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*result, "ydb.spilled_bytes")->value().int_value(), 200);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*result, "ydb.wait_us")->value().int_value(), 100);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*result, "ydb.locks_broken_as_victim")->value().int_value(), 3);
    }

    Y_UNIT_TEST(StageLimitsKeepTotalsAndTransactionIdentity) {
        auto [runtime, server, sender] = CreateServer();
        auto* uploader = RegisterUploader(runtime);
        NKqp::TExecutionTraceStats diagnostics;
        NKqpProto::TKqpPhyStage stage;
        NYql::NDqProto::TDqTaskStats task;
        task.SetStageId(0);
        task.SetWaitOutputTimeUs(1);
        task.SetSpillingChannelWriteBytes(2);
        for (ui64 tx = 0; tx < NKqp::NQueryTraceSettings::MaxStages + 3; ++tx) {
            task.SetTaskId(tx + 1);
            diagnostics.AddTask(tx, stage, 2, task, 0, 1, true);
        }
        NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Basic,
            NWilson::TTraceId::NewTraceId(15, 4095), "Execute", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        NYql::NDqProto::TDqExecutionStats stats;
        diagnostics.Finish(span, stats, Ydb::StatusIds::ABORTED);
        span.EndError("incomplete execution");
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto* execution = FindSpan(*uploader, "Execute");
        UNIT_ASSERT_VALUES_EQUAL(execution->events_size(), NKqp::NQueryTraceSettings::MaxStages);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*execution, "ydb.wait_us")->value().int_value(), NKqp::NQueryTraceSettings::MaxStages + 3);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*execution, "ydb.spilled_bytes")->value().int_value(), 2 * (NKqp::NQueryTraceSettings::MaxStages + 3));
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*execution, "ydb.tasks_without_stage_details")->value().int_value(), 3);
        for (const auto& event : execution->events()) {
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.reported_tasks")->value().int_value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.tasks")->value().int_value(), 2);
            UNIT_ASSERT_VALUES_EQUAL(FindAttribute(event, "ydb.timed_tasks")->value().int_value(), 0);
        }
    }

    Y_UNIT_TEST(ShardEventLimitRetainsLastAcknowledgement) {
        auto [runtime, server, sender] = CreateServer();
        auto* uploader = RegisterUploader(runtime);
        NKqp::TShardTraceEvents events;
        NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Detailed,
            NWilson::TTraceId::NewTraceId(15, 4095), "Prepare shards", NWilson::EFlags::NONE, runtime.GetActorSystem(0));
        for (ui64 shard = 1; shard <= 100; ++shard) {
            events.Acknowledge(span, shard, shard == 100);
        }
        events.Finish(span);
        span.EndOk();
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto* phase = FindSpan(*uploader, "Prepare shards");
        UNIT_ASSERT_VALUES_EQUAL(phase->events_size(), NKqp::NQueryTraceSettings::MaxShardEvents);
        const auto& last = phase->events(phase->events_size() - 1);
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(last, "ydb.shard_id")->value().int_value(), 100);
        UNIT_ASSERT(FindAttribute(last, "ydb.last_shard")->value().bool_value());
        UNIT_ASSERT_VALUES_EQUAL(FindAttribute(*phase, "ydb.shard_events_dropped")->value().int_value(), 100 - NKqp::NQueryTraceSettings::MaxShardEvents);
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
            runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender,
                new NKqp::TEvKqp::TEvCreateSessionRequest()));
            auto created = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvCreateSessionResponse>(sender);
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
                        return event.name() == "Shard read result";
                    });
                }), "type=" << static_cast<int>(type) << " step=" << step << " " << uploader->PrintTraces());
                UNIT_ASSERT_VALUES_EQUAL(uploader->Traces.size(), 1);
                AssertDescendant(*uploader, "Check rows", "Execute query");
            }
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
            AssertStatus(*uploader, "Execute query", NTraceProto::Status::STATUS_CODE_ERROR);
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
