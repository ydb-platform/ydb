#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>

#include <library/cpp/testing/unittest/registar.h>

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
            const TString& sessionId = {}, ui32 proxyNodeIndex = 0, bool dml = true) {
        THolder<NKqp::TEvKqp::TEvQueryRequest> request = MakeSQLRequest(sql, dml);
        if (sessionId) {
            request->Record.MutableRequest()->SetSessionId(sessionId);
        }
        if (userTracing) {
            NWilson::TTraceId::NewTraceId(15, 4095).Serialize(request->Record.MutableUserFacingTraceId());
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

    Y_UNIT_TEST(UserTreeShapeAndSeparation) {
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

        auto execute = userRoot->BFSFindOne("Execute");
        UNIT_ASSERT_C(execute, "user Execute phase missing (executer live span)");
        UNIT_ASSERT_C(execute->get().BFSFindOne("Run"), "user Run phase missing");
        auto prepare = execute->get().FindOne("Prepare");
        UNIT_ASSERT_C(prepare, "user Prepare group missing");
        auto resolveTables = prepare->get().BFSFindOne("ResolveTables");
        UNIT_ASSERT_C(resolveTables, "ResolveTables not under Prepare");
        UNIT_ASSERT_C(resolveTables->get().FindOne("Partitioning"), "Partitioning not under ResolveTables");

        auto compile = userRoot->FindOne("Compile");
        UNIT_ASSERT_C(compile, "user Compile phase missing");
        UNIT_ASSERT_C(compile->get().BFSFindOne("Load metadata /Root/table-1"),
            "metadata request missing under Compile");
        UNIT_ASSERT_C(compile->get().BFSFindOne("Load statistics /Root/table-1"),
            "statistics request missing under Compile");

        auto run = execute->get().BFSFindOne("Run");
        UNIT_ASSERT(run);
        const auto* stage = FindSpanWithAttribute(*userUploader, "ydb.stage_id");
        UNIT_ASSERT_C(stage, "stage span missing, traces: " << userUploader->PrintTraces());
        const auto* task = FindSpanWithAttribute(*userUploader, "ydb.task_id");
        UNIT_ASSERT_C(task, "per-task span missing, traces: " << userUploader->PrintTraces());
        UNIT_ASSERT_VALUES_EQUAL(task->parent_span_id(), stage->span_id());
        const auto* runSpan = FindSpan(*userUploader, "Run");
        UNIT_ASSERT(runSpan);
        UNIT_ASSERT_VALUES_EQUAL(stage->parent_span_id(), runSpan->span_id());

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
    }

    Y_UNIT_TEST(UserChannelOffProducesNoUserTree) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100);",
            /*devTracing*/ true, /*userTracing*/ false);

        UNIT_ASSERT(devUploader->BuildTraceTrees());
        UNIT_ASSERT_VALUES_EQUAL(1, devUploader->Traces.size());
        UNIT_ASSERT(userUploader->Spans.empty());
        UNIT_ASSERT_C(FindRootChild(*devUploader, "Session.query.QUERY_ACTION_EXECUTE"), "dev root span missing");
    }

    Y_UNIT_TEST(UserChannelWorksWithoutDevTracing) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(runtime, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100);",
            /*devTracing*/ false, /*userTracing*/ true);

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_VALUES_EQUAL(1, userUploader->Traces.size());
        UNIT_ASSERT_C(FindRootChild(*userUploader, "UPSERT /Root/table-1"),
            "user tree missing when dev tracing is off");
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
        AssertChildSpansAreWithinParents(*userUploader);
    }
}

} // namespace NKikimr
