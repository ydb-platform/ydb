#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/compilation/compile_diagnostics.h>
#include <ydb/core/kqp/session_actor/kqp_user_facing_tracing.h>
#include <ydb/core/grpc_services/cancelation/cancelation_event.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <atomic>
#include <unordered_map>

namespace NKikimr {

using namespace Tests;
using namespace NWilson;

Y_UNIT_TEST_SUITE(TKqpUserFacingTrace) {

    void EnableUserTracing(NKqp::TKikimrSettings& settings) {
        auto* samplingRule = settings.AppConfig.MutableUserFacingTracingConfig()->AddSampling();
        samplingRule->SetFraction(1.0);
        samplingRule->SetLevel(15);
        samplingRule->SetMaxTracesPerMinute(1'000'000);
        samplingRule->SetMaxTracesBurst(1'000'000);
    }

    std::tuple<TTestActorRuntime&, TServer::TPtr, TActorId> CreateServer(ui32 nodeCount = 1,
            NKikimrConfig::TAppConfig appConfig = {}) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(nodeCount)
            .SetAppConfig(appConfig);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        return {runtime, server, sender};
    }

    std::pair<TFakeWilsonUploader*, TFakeWilsonUploader*> RegisterUploaders(
            TTestActorRuntime& runtime, ui32 nodeIndex = 0) {
        auto* devUploader = new TFakeWilsonUploader();
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), runtime.Register(devUploader, nodeIndex), nodeIndex);
        auto* userUploader = new TFakeWilsonUploader();
        runtime.RegisterService(NWilson::MakeUserFacingWilsonUploaderId(),
            runtime.Register(userUploader, nodeIndex), nodeIndex);
        if (!runtime.IsRealThreads()) {
            runtime.SimulateSleep(TDuration::Seconds(10));
        }
        return {devUploader, userUploader};
    }

    void ExecSQL(TTestActorRuntime& runtime, TActorId sender, const TString& sql,
            bool devTracing, bool userTracing,
            Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS,
            const TString& sessionId = {}, ui32 proxyNodeIndex = 0, bool dml = true,
            bool keepInCache = false, ui8 userVerbosity = 15, bool implicitTx = false) {
        THolder<NKqp::TEvKqp::TEvQueryRequest> request = MakeSQLRequest(sql, dml);
        if (implicitTx) {
            request->Record.MutableRequest()->ClearTxControl();
            request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        }
        if (keepInCache) {
            request->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);
        }
        if (sessionId) {
            request->Record.MutableRequest()->SetSessionId(sessionId);
        }
        if (userTracing) {
            NWilson::TTraceId::NewTraceId(userVerbosity, 4095).Serialize(request->Record.MutableUserFacingTraceId());
        }
        NWilson::TTraceId devTrace;
        if (devTracing) {
            devTrace = NWilson::TTraceId::NewTraceId(15, 4095);
        }
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(proxyNodeIndex)), sender,
            request.Release(), 0, 0, nullptr, std::move(devTrace)));
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetYdbStatus(), code);
        if (runtime.IsRealThreads()) {
            Sleep(TDuration::Seconds(1));
        } else {
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
    }

    const NWilson::TFakeWilsonUploader::TOtelSpan* FindSpan(
            const TFakeWilsonUploader& uploader, TStringBuf name) {
        for (const auto& span : uploader.Spans) {
            if (span.name() == name) {
                return &span;
            }
        }
        return nullptr;
    }

    const opentelemetry::proto::common::v1::KeyValue* FindAttribute(
            const NWilson::TFakeWilsonUploader::TOtelSpan& span, TStringBuf key) {
        for (const auto& attr : span.attributes()) {
            if (attr.key() == key) {
                return &attr;
            }
        }
        return nullptr;
    }

    const NWilson::TFakeWilsonUploader::TOtelSpan* FindSpanWithAttribute(
            const TFakeWilsonUploader& uploader, TStringBuf key) {
        for (const auto& span : uploader.Spans) {
            if (FindAttribute(span, key)) {
                return &span;
            }
        }
        return nullptr;
    }

    const NWilson::TFakeWilsonUploader::TOtelSpan* FindSpanById(
            const TFakeWilsonUploader& uploader, const TString& spanId) {
        for (const auto& span : uploader.Spans) {
            if (span.span_id() == spanId) {
                return &span;
            }
        }
        return nullptr;
    }

    const NWilson::TFakeWilsonUploader::TOtelSpan* FindReadShardSpan(
            const TFakeWilsonUploader& uploader, TStringBuf timingBoundary = {}) {
        for (const auto& span : uploader.Spans) {
            if (!TStringBuf(span.name()).StartsWith("Read from shard ")
                    || !FindAttribute(span, "ydb.shard_id")) {
                continue;
            }
            const auto* boundary = FindAttribute(span, "ydb.timing_boundary");
            if (timingBoundary.empty()
                    || (boundary && boundary->value().string_value() == timingBoundary)) {
                return &span;
            }
        }
        return nullptr;
    }

    void AssertChildSpansAreWithinParents(const TFakeWilsonUploader& uploader) {
        std::unordered_map<TString, const TFakeWilsonUploader::TOtelSpan*> spansById;
        for (const auto& span : uploader.Spans) {
            spansById.emplace(span.span_id(), &span);
            UNIT_ASSERT_C(span.end_time_unix_nano() >= span.start_time_unix_nano(),
                "negative span duration: " << span.name());
        }
        for (const auto& span : uploader.Spans) {
            const auto parentIt = spansById.find(span.parent_span_id());
            if (parentIt == spansById.end()) {
                continue;
            }
            const auto& parent = *parentIt->second;
            UNIT_ASSERT_C(span.start_time_unix_nano() >= parent.start_time_unix_nano(),
                "child starts before parent: " << span.name() << " / " << parent.name());
            UNIT_ASSERT_C(span.end_time_unix_nano() <= parent.end_time_unix_nano(),
                "child ends after parent: " << span.name() << " / " << parent.name());
        }
    }

    TFakeWilsonUploader::Span* FindRootChild(TFakeWilsonUploader& up, const TString& name) {
        for (auto& tracePair : up.Traces) {
            if (auto s = tracePair.second.Root.FindOne(name)) {
                return &s->get();
            }
        }
        return nullptr;
    }

    void AssertSpanStatus(const TFakeWilsonUploader::TOtelSpan* span,
            NWilson::NTraceProto::Status::StatusCode status, TStringBuf message) {
        UNIT_ASSERT_C(span, message);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(span->status().code()),
            static_cast<int>(status), message);
    }

    void ClearUploader(TFakeWilsonUploader& uploader) {
        uploader.Spans.clear();
        uploader.Traces.clear();
    }

    Y_UNIT_TEST(ForwardedRequestKeepsProxySnapshot) {
        auto [runtime, server, sender] = CreateServer();
        Y_UNUSED(server);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender, R"(
            CREATE TABLE `/Root/ForwardedTrace` (
                Key Uint64,
                PRIMARY KEY (Key)
            );
        )",
            /*devTracing*/ false, /*userTracing*/ true,
            Ydb::StatusIds::SUCCESS, {}, 0, /*dml*/ false);

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_C(FindSpan(*userUploader, "KQP proxy"), "forwarded script lost proxy snapshot");
        const auto* root = FindSpan(*userUploader, "DDL");
        UNIT_ASSERT_C(root, "forwarded DDL root span missing");
        const auto* coverage = FindAttribute(*root, "ydb.trace.coverage");
        UNIT_ASSERT_C(coverage, "forwarded DDL does not declare partial trace coverage");
        UNIT_ASSERT_VALUES_EQUAL(coverage->value().string_value(), "routing_session_only");
    }

    Y_UNIT_TEST(SessionBusyFinishesRejectedTrace) {
        auto [runtime, server, sender] = CreateServer();
        Y_UNUSED(server);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(0)), sender,
            new NKqp::TEvKqp::TEvCreateSessionRequest()));
        auto createSession = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvCreateSessionResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(createSession->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        const TString sessionId = createSession->Get()->Record.GetResponse().GetSessionId();

        TAutoPtr<IEventHandle> blockedCompile;
        TTestActorRuntimeBase::TEventFilter previousFilter;
        auto filter = [&](TTestActorRuntimeBase& runtimeBase, TAutoPtr<IEventHandle>& ev) {
            if (!blockedCompile
                    && ev->GetTypeRewrite() == NKqp::TEvKqp::TEvCompileRequest::EventType) {
                blockedCompile = ev.Release();
                return true;
            }
            return previousFilter ? previousFilter(runtimeBase, ev) : false;
        };
        previousFilter = runtime.SetEventFilter(filter);

        auto active = MakeSQLRequest("SELECT 1;", true);
        active->Record.MutableRequest()->SetSessionId(sessionId);
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(0)), sender,
            active.Release()));
        TDispatchOptions compileBlocked;
        compileBlocked.FinalEvents.emplace_back([&](IEventHandle&) { return bool(blockedCompile); });
        runtime.DispatchEvents(compileBlocked);

        const TActorId rejectedSender = runtime.AllocateEdgeActor();
        auto rejected = MakeSQLRequest("SELECT 2;", true);
        rejected->Record.MutableRequest()->SetSessionId(sessionId);
        NWilson::TTraceId::NewTraceId(15, 4095).Serialize(
            rejected->Record.MutableUserFacingTraceId());
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(0)), rejectedSender,
            rejected.Release()));
        auto rejectedResponse = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(rejectedSender);
        UNIT_ASSERT_VALUES_EQUAL(rejectedResponse->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SESSION_BUSY);
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        AssertSpanStatus(FindSpan(*userUploader, "EXECUTE"),
            NWilson::NTraceProto::Status::STATUS_CODE_ERROR,
            "session-busy request did not finish its user trace");
        const auto* root = FindSpan(*userUploader, "EXECUTE");
        UNIT_ASSERT_VALUES_EQUAL(
            FindAttribute(*root, "ydb.trace.coverage")->value().string_value(),
            "rejected_before_query_state");

        runtime.SetEventFilter(std::move(previousFilter));
        runtime.Send(blockedCompile.Release());
        auto activeResponse = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(activeResponse->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ChannelMatrixTreeShapeAndImmediateCommit) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        ExecSQL(runtime, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);",
            /*devTracing*/ false, /*userTracing*/ false);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender,
            "SELECT SUM(value) AS total FROM `/Root/table-1` WHERE key > 0u;",
            /*devTracing*/ true, /*userTracing*/ true);

        UNIT_ASSERT(devUploader->BuildTraceTrees());
        UNIT_ASSERT(userUploader->BuildTraceTrees());

        UNIT_ASSERT_VALUES_EQUAL(1, devUploader->Traces.size());
        UNIT_ASSERT_VALUES_EQUAL(1, userUploader->Traces.size());

        auto* userRoot = FindRootChild(*userUploader, "SELECT /Root/table-1");
        UNIT_ASSERT_C(userRoot, "user-facing root span missing, traces: " << userUploader->PrintTraces());
        UNIT_ASSERT_C(FindRootChild(*devUploader, "Session.query.QUERY_ACTION_EXECUTE"), "dev root span missing");
        UNIT_ASSERT_C(!FindRootChild(*devUploader, "SELECT /Root/table-1"), "user tree leaked into dev uploader");
        UNIT_ASSERT_C(!FindRootChild(*userUploader, "Session.query.QUERY_ACTION_EXECUTE"),
            "dev tree leaked into user uploader");
        UNIT_ASSERT_C(userRoot->FindOne("KQP proxy"), "KQP proxy phase missing");
        auto session = userRoot->FindOne("Session");
        UNIT_ASSERT_C(session, "KQP session actor span missing");
        const auto* sessionSpan = FindSpan(*userUploader, "Session");
        UNIT_ASSERT(sessionSpan);
        UNIT_ASSERT(FindAttribute(*sessionSpan, "ydb.actor.type"));

        auto execute = session->get().BFSFindOne("Execute");
        UNIT_ASSERT_C(execute, "user Execute phase missing (executer live span)");
        const auto* executeSpan = FindSpan(*userUploader, "Execute");
        UNIT_ASSERT(executeSpan);
        UNIT_ASSERT(FindAttribute(*executeSpan, "ydb.actor.type"));
        UNIT_ASSERT_C(execute->get().BFSFindOne("Run"), "user Run phase missing");
        auto prepare = execute->get().FindOne("Prepare");
        UNIT_ASSERT_C(prepare, "user Prepare group missing");
        auto resolveTables = prepare->get().BFSFindOne("Resolve tables");
        UNIT_ASSERT_C(resolveTables, "Resolve tables not under Prepare");
        UNIT_ASSERT_C(resolveTables->get().FindOne("Partitioning"), "Partitioning not under Resolve tables");
        const auto* resolveTablesSpan = FindSpan(*userUploader, "Resolve tables");
        UNIT_ASSERT(resolveTablesSpan);
        UNIT_ASSERT_VALUES_EQUAL(
            FindAttribute(*resolveTablesSpan, "ydb.phase")->value().string_value(),
            "ResolveTables");

        auto compile = session->get().FindOne("Compile");
        UNIT_ASSERT_C(compile, "user Compile phase missing");
        auto compileQuery = compile->get().FindOne("Compile query");
        UNIT_ASSERT_C(compileQuery, "compile actor span missing");
        UNIT_ASSERT_C(compileQuery->get().BFSFindOne("Load metadata /Root/table-1"),
            "metadata request missing under Compile");
        UNIT_ASSERT_C(compileQuery->get().BFSFindOne("Load statistics /Root/table-1"),
            "statistics request missing under Compile");
        const auto* compileSpan = FindSpan(*userUploader, "Compile");
        const auto* compileQuerySpan = FindSpan(*userUploader, "Compile query");
        UNIT_ASSERT(compileSpan);
        UNIT_ASSERT(compileQuerySpan);
        UNIT_ASSERT(FindAttribute(*compileSpan, "ydb.actor.type"));
        UNIT_ASSERT(FindAttribute(*compileQuerySpan, "ydb.actor.type"));

        auto run = execute->get().BFSFindOne("Run");
        UNIT_ASSERT(run);
        const auto* task = FindSpanWithAttribute(*userUploader, "ydb.task_id");
        UNIT_ASSERT_C(task, "per-task span missing, traces: " << userUploader->PrintTraces());
        const auto* stage = FindSpanById(*userUploader, task->parent_span_id());
        UNIT_ASSERT_C(stage && FindAttribute(*stage, "ydb.stage_id"),
            "task parent is not a stage span, traces: " << userUploader->PrintTraces());
        const auto* runSpan = FindSpan(*userUploader, "Run");
        UNIT_ASSERT(runSpan);
        UNIT_ASSERT_VALUES_EQUAL(stage->parent_span_id(), runSpan->span_id());
        const auto* taskActor = FindAttribute(*task, "ydb.actor.type");
        UNIT_ASSERT(taskActor);

        UNIT_ASSERT_C(!userRoot->BFSFindOne("ComputeActor"), "user tree leaked engine internals");

        bool queryTextChecked = false;
        for (const auto& span : userUploader->Spans) {
            for (const auto& attr : span.attributes()) {
                if (attr.key() == "db.query.text") {
                    const TString& text = attr.value().string_value();
                    UNIT_ASSERT_C(!text.Contains("100"), "literal leaked into db.query.text: " << text);
                    UNIT_ASSERT_C(!text.Contains("table-1"), "identifier leaked into db.query.text: " << text);
                    UNIT_ASSERT_C(text.Contains("SELECT"), "db.query.text lost query shape: " << text);
                    queryTextChecked = true;
                }
            }
        }
        UNIT_ASSERT_C(queryTextChecked, "db.query.text attribute missing");
        AssertChildSpansAreWithinParents(*userUploader);

        ClearUploader(*devUploader);
        ClearUploader(*userUploader);
        ExecSQL(runtime, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 200);",
            /*devTracing*/ true, /*userTracing*/ false);
        UNIT_ASSERT(devUploader->BuildTraceTrees());
        UNIT_ASSERT_VALUES_EQUAL(devUploader->Traces.size(), 1u);
        UNIT_ASSERT(userUploader->Spans.empty());
        UNIT_ASSERT_C(FindRootChild(*devUploader, "Session.query.QUERY_ACTION_EXECUTE"),
            "dev tree missing when user tracing is off");

        ClearUploader(*devUploader);
        ClearUploader(*userUploader);
        ExecSQL(runtime, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 400);",
            /*devTracing*/ false, /*userTracing*/ true);
        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_VALUES_EQUAL(userUploader->Traces.size(), 1u);
        UNIT_ASSERT_C(FindRootChild(*userUploader, "UPSERT /Root/table-1"),
            "user tree missing when dev tracing is off");
        UNIT_ASSERT_C(FindSpan(*userUploader, "Apply commit"),
            "immediate commit Apply commit span missing");
        const auto* commitShard = FindSpanWithAttribute(*userUploader, "ydb.shard_id");
        UNIT_ASSERT_C(commitShard && TStringBuf(commitShard->name()).StartsWith("Commit shard "),
            "immediate commit DataShard span missing");
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(CompileAndRuntimeErrorsHaveErrorStatus) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
        auto [runtime, server, sender] = CreateServer(1, std::move(appConfig));
        Y_UNUSED(server);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender, "SELECT * FRM `/Root/missing`;",
            /*devTracing*/ false, /*userTracing*/ true, Ydb::StatusIds::GENERIC_ERROR);

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        AssertSpanStatus(FindSpan(*userUploader, "Compile"),
            NWilson::NTraceProto::Status::STATUS_CODE_ERROR,
            "compile error was exported as successful");
        auto* compileRoot = FindRootChild(*userUploader, "EXECUTE");
        UNIT_ASSERT_C(compileRoot, "compile error root span missing");
        AssertSpanStatus(FindSpan(*userUploader, "EXECUTE"),
            NWilson::NTraceProto::Status::STATUS_CODE_ERROR,
            "compile error root span was exported as successful");

        userUploader->Spans.clear();
        userUploader->Traces.clear();

        ExecSQL(runtime, sender, R"(
            CREATE TABLE `/Root/UniqueValues` (
                Key Uint32,
                Value Uint32 NOT NULL,
                PRIMARY KEY (Key),
                INDEX ValueIndex GLOBAL UNIQUE SYNC ON (Value)
            );
        )", /*devTracing*/ false, /*userTracing*/ false,
            Ydb::StatusIds::SUCCESS, {}, 0, /*dml*/ false);
        ExecSQL(runtime, sender,
            "UPSERT INTO `/Root/UniqueValues` (Key, Value) VALUES (1u, 10u);",
            /*devTracing*/ false, /*userTracing*/ false);
        ExecSQL(runtime, sender,
            "UPSERT INTO `/Root/UniqueValues` (Key, Value) VALUES (2u, 10u);",
            /*devTracing*/ false, /*userTracing*/ true,
            Ydb::StatusIds::PRECONDITION_FAILED);

        UNIT_ASSERT(userUploader->BuildTraceTrees());
        AssertSpanStatus(FindSpan(*userUploader, "Execute"),
            NWilson::NTraceProto::Status::STATUS_CODE_ERROR,
            "runtime error Execute span was exported as successful");
        auto* runtimeRoot = FindRootChild(*userUploader, "UPSERT /Root/UniqueValues");
        UNIT_ASSERT_C(runtimeRoot, "runtime error root span missing");
        AssertSpanStatus(FindSpan(*userUploader, "UPSERT /Root/UniqueValues"),
            NWilson::NTraceProto::Status::STATUS_CODE_ERROR,
            "runtime error root span was exported as successful");
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(ClientLostFinishesTraceWithoutResponse) {
        auto [runtime, server, sender] = CreateServer();
        Y_UNUSED(server);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

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
        NWilson::TTraceId::NewTraceId(15, 4095).Serialize(
            request->Record.MutableUserFacingTraceId());
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(0)), sender,
            request.Release()));
        TDispatchOptions compileBlocked;
        compileBlocked.FinalEvents.emplace_back([&](IEventHandle&) { return bool(blockedCompile); });
        runtime.DispatchEvents(compileBlocked);
        runtime.SetEventFilter(std::move(previousFilter));

        runtime.Send(new IEventHandle(sessionActor, sender,
            new NGRpcService::TEvClientLost()));
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_VALUES_EQUAL(userUploader->Traces.size(), 1u);
        AssertSpanStatus(FindSpan(*userUploader, "EXECUTE"),
            NWilson::NTraceProto::Status::STATUS_CODE_ERROR,
            "client-lost request did not finish its user trace");
    }

    Y_UNIT_TEST(WideReadRetainsBoundedDiagnostics) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 16, false);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender, "SELECT * FROM `/Root/table-1`;",
            /*devTracing*/ false, /*userTracing*/ true);

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());

        std::unordered_map<TString, size_t> tasksByStage;
        std::unordered_map<TString, size_t> shardsByTask;
        bool diagnosticsTruncated = false;
        for (const auto& span : userUploader->Spans) {
            if (FindAttribute(span, "ydb.task_id")) {
                ++tasksByStage[span.parent_span_id()];
            }
            if (TStringBuf(span.name()).StartsWith("Read from shard ")) {
                ++shardsByTask[span.parent_span_id()];
            }
            diagnosticsTruncated = diagnosticsTruncated
                || FindAttribute(span, "ydb.tasks_truncated")
                || FindAttribute(span, "ydb.shards_truncated");
        }
        UNIT_ASSERT_C(diagnosticsTruncated, "wide read did not report truncated diagnostics");
        for (const auto& [_, count] : tasksByStage) {
            UNIT_ASSERT_C(count <= NKqp::MaxInterestingTasksPerStage,
                "stage exported too many task diagnostics: " << count);
        }
        for (const auto& [_, count] : shardsByTask) {
            UNIT_ASSERT_C(count <= NKqp::MaxInterestingShardsPerTask,
                "task exported too many shard diagnostics: " << count);
        }
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(CompileCacheHitAndStaleRecompile) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        const TString query = "SELECT * FROM `/Root/table-1` WHERE key = 1u;";
        ExecSQL(runtime, sender, query, /*devTracing*/ false, /*userTracing*/ false,
            Ydb::StatusIds::SUCCESS, {}, 0, true, /*keepInCache*/ true);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender, query, /*devTracing*/ false, /*userTracing*/ true,
            Ydb::StatusIds::SUCCESS, {}, 0, true, /*keepInCache*/ true);

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        const auto* querySpan = FindSpanWithAttribute(*userUploader, "ydb.compile.cache_hit");
        UNIT_ASSERT_C(querySpan, "compile cache hit is not recorded on the query span");
        const auto* cacheHit = FindAttribute(*querySpan, "ydb.compile.cache_hit");
        UNIT_ASSERT(cacheHit);
        UNIT_ASSERT(cacheHit->value().bool_value());
        UNIT_ASSERT_C(!FindSpan(*userUploader, "Compile"),
            "compile service span emitted for a local cache hit");
        UNIT_ASSERT_C(!FindSpan(*userUploader, "Compile query"),
            "compile actor span emitted for a cache hit");
        AssertChildSpansAreWithinParents(*userUploader);

        ClearUploader(*userUploader);
        ExecSQL(runtime, sender, "ALTER TABLE `/Root/table-1` ADD COLUMN extra Uint64;",
            /*devTracing*/ false, /*userTracing*/ false, Ydb::StatusIds::SUCCESS,
            {}, 0, /*dml*/ false);

        std::atomic<size_t> recompileRequests = 0;
        TTestActorRuntimeBase::TEventFilter previousFilter;
        auto filter = [&](TTestActorRuntimeBase& runtimeBase, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKqp::TEvKqp::TEvRecompileRequest::EventType) {
                ++recompileRequests;
            }
            return previousFilter ? previousFilter(runtimeBase, ev) : false;
        };
        previousFilter = runtime.SetEventFilter(filter);

        ExecSQL(runtime, sender, query, /*devTracing*/ false, /*userTracing*/ true,
            Ydb::StatusIds::SUCCESS, {}, 0, true, /*keepInCache*/ true);
        runtime.SetEventFilter(std::move(previousFilter));

        UNIT_ASSERT_VALUES_EQUAL(recompileRequests.load(), 1u);
        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_C(FindSpan(*userUploader, "Compile"),
            "stale cache recompilation is missing from Compile");
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(MultiStatementQueryUsesScriptRootName) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        CreateShardedTable(server, sender, "/Root", "table-2", 1, false);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender, R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10);
            DELETE FROM `/Root/table-2` WHERE key = 2u;
        )", /*devTracing*/ false, /*userTracing*/ true);

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_C(FindRootChild(*userUploader, "EXECUTE SCRIPT"),
            "multi-statement query kept the first operation as its root name");
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(CoalescedCompileDoesNotDependOnFirstRequestSampling) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);
        const TActorId tracedSender = runtime.AllocateEdgeActor();
        const TString query = "SELECT * FROM `/Root/table-1` WHERE key = 123u;";

        std::vector<TAutoPtr<IEventHandle>> captured;
        size_t compileRequests = 0;
        TTestActorRuntimeBase::TEventFilter previousFilter;
        auto filter = [&](TTestActorRuntimeBase& runtimeBase, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKqp::TEvKqp::TEvCompileRequest::EventType) {
                ++compileRequests;
            }
            if (captured.empty()
                    && ev->GetTypeRewrite() == TEvTxProxySchemeCache::TEvNavigateKeySetResult::EventType) {
                captured.push_back(ev.Release());
                return true;
            }
            return previousFilter ? previousFilter(runtimeBase, ev) : false;
        };
        previousFilter = runtime.SetEventFilter(filter);

        auto send = [&](TActorId replyTo, bool userTracing) {
            auto request = MakeSQLRequest(query, true);
            if (userTracing) {
                NWilson::TTraceId::NewTraceId(15, 4095).Serialize(
                    request->Record.MutableUserFacingTraceId());
            }
            runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(0)), replyTo,
                request.Release()));
        };

        send(sender, /*userTracing*/ false);
        TDispatchOptions firstBlocked;
        firstBlocked.FinalEvents.emplace_back([&](IEventHandle&) { return !captured.empty(); });
        runtime.DispatchEvents(firstBlocked);

        send(tracedSender, /*userTracing*/ true);
        TDispatchOptions secondQueued;
        secondQueued.FinalEvents.emplace_back([&](IEventHandle&) { return compileRequests >= 2; });
        runtime.DispatchEvents(secondQueued);

        runtime.SetEventFilter(std::move(previousFilter));
        for (auto& event : captured) {
            runtime.Send(event.Release());
        }

        auto first = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
        auto second = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(tracedSender);
        UNIT_ASSERT_VALUES_EQUAL(first->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(second->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        const auto* compileActor = FindSpan(*userUploader, "Compile query");
        UNIT_ASSERT_C(compileActor,
            "sampled coalesced waiter did not receive compile actor snapshot");
        UNIT_ASSERT_C(FindSpan(*userUploader, "Load metadata /Root/table-1"),
            "sampled coalesced waiter did not receive dependency snapshot");
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(DistributedCommitExportsEveryPhaseAndShard) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 2, false);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender, R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES
                (1u, 10u), (4000000000u, 20u);
        )", /*devTracing*/ false, /*userTracing*/ true);

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_C(FindSpan(*userUploader, "Prepare shards"),
            "distributed commit prepare phase missing");
        UNIT_ASSERT_C(FindSpan(*userUploader, "Coordinator"),
            "distributed commit coordinator phase missing");
        UNIT_ASSERT_C(FindSpan(*userUploader, "Apply commit"),
            "distributed commit apply phase missing");
        size_t commitShards = 0;
        for (const auto& span : userUploader->Spans) {
            commitShards += TStringBuf(span.name()).StartsWith("Commit shard ");
        }
        UNIT_ASSERT_VALUES_EQUAL(commitShards, 2u);
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(BasicVerbosityOmitsDetailedSpans) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender, "SELECT * FROM `/Root/table-1`;",
            /*devTracing*/ false, /*userTracing*/ true, Ydb::StatusIds::SUCCESS,
            {}, 0, true, false, TComponentTracingLevels::TQueryProcessor::Basic);

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT(FindSpan(*userUploader, "KQP proxy"));
        UNIT_ASSERT(FindSpan(*userUploader, "Session"));
        UNIT_ASSERT(FindSpan(*userUploader, "Execute"));
        UNIT_ASSERT_C(!FindSpanWithAttribute(*userUploader, "ydb.stage_id"),
            "stage span ignored Basic verbosity");
        UNIT_ASSERT_C(!FindSpanWithAttribute(*userUploader, "ydb.task_id"),
            "task span ignored Basic verbosity");
        UNIT_ASSERT_C(!FindReadShardSpan(*userUploader),
            "shard span ignored Basic verbosity");
        UNIT_ASSERT_C(!FindSpan(*userUploader, "Load metadata /Root/table-1"),
            "compile dependency ignored Basic verbosity");
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(SpanBudgetAndClockCorrection) {
        NKqp::TUserFacingSpanBudget budget(/*verbosity*/ 15, /*limit*/ 5, /*reserved*/ 2);
        UNIT_ASSERT(budget.Admit(TComponentTracingLevels::TQueryProcessor::Basic));
        UNIT_ASSERT(budget.Admit(TComponentTracingLevels::TQueryProcessor::Detailed));
        UNIT_ASSERT(budget.Admit(TComponentTracingLevels::TQueryProcessor::Diagnostic));
        UNIT_ASSERT(!budget.Admit(TComponentTracingLevels::TQueryProcessor::Basic));
        UNIT_ASSERT_VALUES_EQUAL(budget.Dropped(), 1u);

        NKqp::TUserFacingSpanBudget lowBudget(
            TComponentTracingLevels::TQueryProcessor::Basic, /*limit*/ 5, /*reserved*/ 2);
        UNIT_ASSERT(!lowBudget.Admit(TComponentTracingLevels::TQueryProcessor::Detailed));
        UNIT_ASSERT_VALUES_EQUAL(lowBudget.Dropped(), 0u);

        const NKqp::TTimeWindow parent{
            TInstant::Seconds(100), TInstant::Seconds(110)};
        const auto corrected = NKqp::FitUserFacingRemoteWindow({
            TInstant::Seconds(3700), TInstant::Seconds(3702)}, parent);
        UNIT_ASSERT(corrected);
        UNIT_ASSERT_VALUES_EQUAL(corrected.End - corrected.Start, TDuration::Seconds(2));
        UNIT_ASSERT(corrected.Start >= parent.Start);
        UNIT_ASSERT(corrected.End <= parent.End);

        NKqp::TShardReadDiagnosticsCollector collector;
        for (ui64 shardId = 1; shardId <= NKqp::MaxShardReadDiagnostics; ++shardId) {
            collector.OnStart(shardId, TInstant::MilliSeconds(100));
            collector.OnFinish(shardId, 0, 0, 0, Ydb::StatusIds::SUCCESS, true,
                TInstant::MilliSeconds(101));
        }
        const ui64 slowShard = NKqp::MaxShardReadDiagnostics + 1;
        collector.OnStart(slowShard, TInstant::MilliSeconds(200));
        collector.OnFinish(slowShard, 0, 0, 0, Ydb::StatusIds::SUCCESS, true,
            TInstant::MilliSeconds(1200));
        const ui64 failedShard = slowShard + 1;
        collector.OnStart(failedShard, TInstant::MilliSeconds(1300));
        collector.OnFinish(failedShard, 0, 2, 7, Ydb::StatusIds::ABORTED, true,
            TInstant::MilliSeconds(1301));
        NKqpProto::TKqpTaskExtraStats stats;
        collector.Export(stats, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.ShardReadsSize(), NKqp::MaxShardReadDiagnostics);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetShardReadsTruncated(), 2u);
        bool slowFound = false;
        bool errorFound = false;
        for (const auto& shard : stats.GetShardReads()) {
            slowFound = slowFound || shard.GetShardId() == slowShard;
            if (shard.GetShardId() == failedShard) {
                UNIT_ASSERT_VALUES_EQUAL(shard.GetStatus(), Ydb::StatusIds::ABORTED);
                UNIT_ASSERT_VALUES_EQUAL(shard.GetRetries(), 2u);
                UNIT_ASSERT(shard.GetFinished());
                errorFound = true;
            }
        }
        UNIT_ASSERT_C(slowFound, "slow shard was not retained");
        UNIT_ASSERT(errorFound);

        NKqp::TShardReadDiagnosticsCollector concurrentCollector;
        for (ui64 shardId = 1; shardId <= NKqp::MaxShardReadDiagnostics; ++shardId) {
            concurrentCollector.OnStart(shardId, TInstant::MilliSeconds(100));
        }
        const ui64 lateSlowShard = NKqp::MaxShardReadDiagnostics + 1;
        const ui64 lateSlowStartMs = concurrentCollector.OnStart(
            lateSlowShard, TInstant::MilliSeconds(200));
        for (ui64 shardId = 1; shardId <= NKqp::MaxShardReadDiagnostics; ++shardId) {
            concurrentCollector.OnFinish(shardId, 0, 0, 0, Ydb::StatusIds::SUCCESS, true,
                TInstant::MilliSeconds(201), 100);
        }
        concurrentCollector.OnFinish(lateSlowShard, 0, 0, 0, Ydb::StatusIds::SUCCESS, true,
            TInstant::MilliSeconds(1200), lateSlowStartMs);
        NKqpProto::TKqpTaskExtraStats concurrentStats;
        concurrentCollector.Export(concurrentStats, 0);
        UNIT_ASSERT(std::any_of(concurrentStats.GetShardReads().begin(),
            concurrentStats.GetShardReads().end(), [&](const auto& shard) {
                return shard.GetShardId() == lateSlowShard;
            }));

        std::vector<NKqp::TExecutionTraceSnapshot> executionCandidates;
        for (size_t i = 1; i <= NKqp::MaxExecutionTraceSnapshots; ++i) {
            NKqp::TExecutionTraceSnapshot trace;
            trace.Status = Ydb::StatusIds::SUCCESS;
            trace.Timeline.Execute = {TInstant::MilliSeconds(1), TInstant::MilliSeconds(i + 1)};
            executionCandidates.push_back(std::move(trace));
        }
        NKqp::TExecutionTraceSnapshot failedExecution;
        failedExecution.Status = Ydb::StatusIds::ABORTED;
        failedExecution.Timeline.Execute = {TInstant::MilliSeconds(1), TInstant::MilliSeconds(2)};
        executionCandidates.push_back(std::move(failedExecution));
        std::vector<NKqp::TExecutionTraceSnapshot> retainedExecutions;
        size_t executionsDropped = 0;
        NKqp::AppendExecutionTraceSnapshots(retainedExecutions, executionsDropped,
            executionCandidates);
        UNIT_ASSERT_VALUES_EQUAL(retainedExecutions.size(), NKqp::MaxExecutionTraceSnapshots);
        UNIT_ASSERT_VALUES_EQUAL(executionsDropped, 1u);
        UNIT_ASSERT(std::any_of(retainedExecutions.begin(), retainedExecutions.end(),
            [](const auto& trace) { return trace.Status == Ydb::StatusIds::ABORTED; }));

        NKqp::TExecutionTraceSnapshot wideExecution;
        for (size_t i = 0; i <= NKqp::MaxStageTraceSnapshotsPerQuery; ++i) {
            NKqp::TStageTraceSnapshot stage;
            stage.StageId = i;
            stage.Durations.MaxUs = i;
            wideExecution.Stages.push_back(std::move(stage));
        }
        NKqp::TrimExecutionTraceSnapshot(wideExecution);
        UNIT_ASSERT_VALUES_EQUAL(wideExecution.Stages.size(),
            NKqp::MaxStageTraceSnapshotsPerQuery);
        UNIT_ASSERT_VALUES_EQUAL(wideExecution.StagesTruncated, 1u);
        UNIT_ASSERT(std::any_of(wideExecution.Stages.begin(), wideExecution.Stages.end(),
            [](const auto& stage) { return stage.StageId == NKqp::MaxStageTraceSnapshotsPerQuery; }));

        NKqp::TCompileDiagnosticsCollector compileCollector;
        compileCollector.Begin(NKqp::ECompileDependency::SchemeCache, "/Root/pending",
            TInstant::MilliSeconds(100));
        const auto compileSnapshot = compileCollector.Snapshot(TInstant::MilliSeconds(200));
        UNIT_ASSERT_VALUES_EQUAL(compileSnapshot->Dependencies.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(compileSnapshot->Dependencies.front().End,
            TInstant::MilliSeconds(200));
        UNIT_ASSERT_VALUES_EQUAL(compileSnapshot->Dependencies.front().Status,
            NKqp::ECompileDependencyStatus::Unknown);

        NKqp::TShardAckDiagnosticsCollector commitCollector;
        for (ui64 shardId = 1; shardId <= NKqp::MaxCommitShardDiagnostics + 2; ++shardId) {
            commitCollector.OnAck(shardId, TInstant::MilliSeconds(shardId));
        }
        UNIT_ASSERT_VALUES_EQUAL(commitCollector.Shards().size(),
            NKqp::MaxCommitShardDiagnostics);
        UNIT_ASSERT_VALUES_EQUAL(commitCollector.Dropped(), 2u);
        for (const auto& shard : commitCollector.Shards()) {
            UNIT_ASSERT_C(shard.ShardId > 2,
                "commit diagnostics retained an early acknowledgement instead of a straggler");
        }
    }

    Y_UNIT_TEST(SensitiveQueryTextIsHidden) {
        auto [runtime, server, sender] = CreateServer();
        Y_UNUSED(server);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender, "SELECT 1 -- password 'swordfish'",
            /*devTracing*/ false, /*userTracing*/ true);

        UNIT_ASSERT(devUploader->Spans.empty());
        const TFakeWilsonUploader::TOtelSpan* querySpan = nullptr;
        for (const auto& span : userUploader->Spans) {
            if (FindAttribute(span, "db.query.text")) {
                querySpan = &span;
                break;
            }
        }
        UNIT_ASSERT_C(querySpan, "query text attribute missing");
        const auto* queryText = FindAttribute(*querySpan, "db.query.text");
        UNIT_ASSERT_VALUES_EQUAL(queryText->value().string_value(),
            "Query text is hidden due to a sensitive marker: password");
        UNIT_ASSERT_C(!TStringBuf(querySpan->status().message()).Contains("swordfish"),
            "secret leaked through span status");
        const auto* execute = FindSpan(*userUploader, "Execute");
        UNIT_ASSERT_C(execute, "literal query has no Execute span");
        UNIT_ASSERT(FindAttribute(*execute, "ydb.actor.type"));
    }

    Y_UNIT_TEST(PartitionedBatchExportsEveryExecution) {
        NKikimrConfig::TAppConfig appConfig;
        auto* batch = appConfig.MutableTableServiceConfig()->MutableBatchOperationSettings();
        batch->SetMaxBatchSize(1);
        batch->SetPartitionExecutionLimit(2);
        auto [runtime, server, sender] = CreateServer(1, std::move(appConfig));
        CreateShardedTable(server, sender, "/Root", "table-1", 2, false);
        ExecSQL(runtime, sender, R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES
                (1, 10), (2, 20), (3, 30), (4, 40);
        )", /*devTracing*/ false, /*userTracing*/ false);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender, "BATCH UPDATE `/Root/table-1` SET value = 100;",
            /*devTracing*/ false, /*userTracing*/ true, Ydb::StatusIds::SUCCESS,
            {}, 0, true, false, 15, /*implicitTx*/ true);

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        size_t executions = 0;
        for (const auto& span : userUploader->Spans) {
            executions += span.name() == "Execute";
        }
        UNIT_ASSERT_C(executions > 1,
            "partitioned BATCH UPDATE exported only one or no child execution");
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(UserOnlyProductionConfigSamplesGrpcRequest) {
        NKqp::TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        EnableUserTracing(settings);

        NKqp::TKikimrRunner kikimr(settings);
        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "table-1"
            Columns { Name: "key", Type: "Uint64" }
            Columns { Name: "value", Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        NYdb::NTable::TTableClient tableClient(kikimr.GetDriver());
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteDataQuery(
            "SELECT * FROM `/Root/table-1`;",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        Sleep(TDuration::Seconds(1));

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_C(FindRootChild(*userUploader, "SELECT /Root/table-1"),
            "user-facing trace was not sampled from UserFacingTracingConfig");

        bool shardSeen = false;
        for (const auto& span : userUploader->Spans) {
            for (const auto& attr : span.attributes()) {
                shardSeen = shardSeen || attr.key() == "ydb.shard_id";
            }
        }
        UNIT_ASSERT_C(shardSeen, "DataShard information missing from SELECT trace");
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(ProxyForwardingAcrossNodes) {
        auto [runtime, server, sender] = CreateServer(2);
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        auto [devUploader, userUploader] = RegisterUploaders(runtime, 1);

        const TActorId remoteSender = runtime.AllocateEdgeActor(1);
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId(1)), remoteSender,
            new NKqp::TEvKqp::TEvCreateSessionRequest()));
        auto createSession = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvCreateSessionResponse>(remoteSender);
        UNIT_ASSERT_VALUES_EQUAL(createSession->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        const TString sessionId = createSession->Get()->Record.GetResponse().GetSessionId();

        ExecSQL(runtime, sender, "SELECT * FROM `/Root/table-1`;",
            /*devTracing*/ false, /*userTracing*/ true, Ydb::StatusIds::SUCCESS,
            sessionId, /*proxyNodeIndex*/ 0);
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        const auto* forwarding = FindSpan(*userUploader, "Forward to KQP proxy");
        UNIT_ASSERT_C(forwarding, "inter-node forwarding span missing");
        const auto* sourceNode = FindAttribute(*forwarding, "ydb.source_node_id");
        const auto* targetNode = FindAttribute(*forwarding, "ydb.target_node_id");
        UNIT_ASSERT(sourceNode);
        UNIT_ASSERT(targetNode);
        UNIT_ASSERT_VALUES_EQUAL(sourceNode->value().int_value(), runtime.GetNodeId(0));
        UNIT_ASSERT_VALUES_EQUAL(targetNode->value().int_value(), runtime.GetNodeId(1));
        UNIT_ASSERT_VALUES_EQUAL(forwarding->start_time_unix_nano(), forwarding->end_time_unix_nano());
        const auto* durationMeasured = FindAttribute(*forwarding, "ydb.duration.measured");
        UNIT_ASSERT(durationMeasured);
        UNIT_ASSERT(!durationMeasured->value().bool_value());

        size_t proxySpans = 0;
        for (const auto& span : userUploader->Spans) {
            proxySpans += span.name() == "KQP proxy";
        }
        UNIT_ASSERT_VALUES_EQUAL(proxySpans, 2u);
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(FullTextReadExportsDataShardSpan) {
        NKqp::TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        settings.FeatureFlags.SetEnableFulltextIndex(true);
        settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(
            NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
        EnableUserTracing(settings);

        NKqp::TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto result = db.ExecuteQuery(R"(
            CREATE TABLE `/Root/Texts` (
                Key Uint64,
                Text String,
                PRIMARY KEY (Key),
                INDEX fulltext_idx GLOBAL USING fulltext_plain ON (Text)
                    WITH (tokenizer=standard, use_filter_lowercase=true)
            );
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        result = db.ExecuteQuery(R"(
            UPSERT INTO `/Root/Texts` (Key, Text) VALUES
                (1, "Cats love cats"),
                (2, "Dogs love foxes");
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto [devUploader, userUploader] = RegisterUploaders(runtime);
        result = db.ExecuteQuery(R"(
            SELECT Key FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "cats");
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        Sleep(TDuration::Seconds(1));

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_C(FindReadShardSpan(*userUploader),
            "full-text source did not export a DataShard span");
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(BufferLookupExportsDataShardSpan) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
        auto [runtime, server, sender] = CreateServer(1, std::move(appConfig));
        Y_UNUSED(server);
        ExecSQL(runtime, sender, R"(
            CREATE TABLE `/Root/UniqueValues` (
                Key Uint32,
                Value Uint32 NOT NULL,
                PRIMARY KEY (Key),
                INDEX ValueIndex GLOBAL UNIQUE SYNC ON (Value)
            );
        )", /*devTracing*/ false, /*userTracing*/ false,
            Ydb::StatusIds::SUCCESS, {}, 0, /*dml*/ false);

        std::atomic<size_t> bufferReads = 0;
        TTestActorRuntimeBase::TEventFilter previousFilter;
        auto filter = [&](TTestActorRuntimeBase& runtimeBase, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType
                    && runtime.FindActorName(ev->Sender) == "KQP_BUFFER_LOOKUP_ACTOR") {
                ++bufferReads;
            }
            return previousFilter ? previousFilter(runtimeBase, ev) : false;
        };
        previousFilter = runtime.SetEventFilter(filter);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender,
            "UPSERT INTO `/Root/UniqueValues` (Key, Value) VALUES (1u, 10u);",
            /*devTracing*/ false, /*userTracing*/ true);
        runtime.SetEventFilter(std::move(previousFilter));

        UNIT_ASSERT_C(bufferReads.load() > 0, "query did not use KQP buffer lookup actor");
        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_C(FindReadShardSpan(*userUploader),
            "buffer lookup did not export a DataShard span");
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(StreamLookupExportsDataShardSpan) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(true);
        auto [runtime, server, sender] = CreateServer(1, std::move(appConfig));
        Y_UNUSED(server);
        ExecSQL(runtime, sender, R"(
            CREATE TABLE `/Root/LookupTable` (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key)
            ) WITH (
                UNIFORM_PARTITIONS = 64,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 64
            );
        )", /*devTracing*/ false, /*userTracing*/ false,
            Ydb::StatusIds::SUCCESS, {}, 0, /*dml*/ false);

        std::atomic<size_t> streamReads = 0;
        TTestActorRuntimeBase::TEventFilter previousFilter;
        auto filter = [&](TTestActorRuntimeBase& runtimeBase, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType
                    && runtime.FindActorName(ev->Sender) == "KQP_STREAM_LOOKUP_ACTOR") {
                ++streamReads;
            }
            return previousFilter ? previousFilter(runtimeBase, ev) : false;
        };
        previousFilter = runtime.SetEventFilter(filter);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender, R"(
            $data = AsList(
                AsStruct(1u AS Key),
                AsStruct(2147483648u AS Key));

            SELECT b.Value
            FROM AS_TABLE($data) AS a
            JOIN `/Root/LookupTable` AS b
            ON a.Key = b.Key;
        )", /*devTracing*/ false, /*userTracing*/ true);
        runtime.SetEventFilter(std::move(previousFilter));

        UNIT_ASSERT_C(streamReads.load() > 0, "query did not use KQP stream lookup actor");
        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_C(FindReadShardSpan(*userUploader),
            "stream lookup did not export a DataShard span");
        AssertChildSpansAreWithinParents(*userUploader);
    }

    Y_UNIT_TEST(ScanExportsFirstToLastMessageBoundary) {
        NKqp::TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        EnableUserTracing(settings);

        NKqp::TKikimrRunner kikimr(settings);
        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "ScanTable"
            Columns { Name: "Key", Type: "Uint64" }
            Columns { Name: "Value", Type: "Uint64" }
            KeyColumnNames: ["Key"]
        )");
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto upsert = session.ExecuteDataQuery(
            "UPSERT INTO `/Root/ScanTable` (Key, Value) VALUES (1, 10), (2, 20);",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto [devUploader, userUploader] = RegisterUploaders(runtime);
        auto iterator = tableClient.StreamExecuteScanQuery(
            "SELECT * FROM `/Root/ScanTable`;").GetValueSync();
        UNIT_ASSERT_C(iterator.IsSuccess(), iterator.GetIssues().ToString());
        while (true) {
            auto part = iterator.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }
        }
        Sleep(TDuration::Seconds(1));

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_C(FindReadShardSpan(*userUploader, "first_to_last_message"),
            "scan did not export its first-to-last-message boundary");
        const auto* executeSpan = FindSpan(*userUploader, "Execute");
        UNIT_ASSERT(executeSpan);
        UNIT_ASSERT(FindAttribute(*executeSpan, "ydb.actor.type"));
        const auto* taskSpan = FindSpanWithAttribute(*userUploader, "ydb.task_id");
        UNIT_ASSERT(taskSpan);
        UNIT_ASSERT(FindAttribute(*taskSpan, "ydb.actor.type"));
        AssertChildSpansAreWithinParents(*userUploader);
    }
}

} // namespace NKikimr
