#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>

#include <library/cpp/testing/unittest/registar.h>

// User-facing tracing: the user channel emits its own curated tree (separate trace-id) with
// user-language phases (Execute -> Prepare/Run). The whole tree is rendered post-hoc by the
// session at reply time from engine-recorded timings (single renderer, kqp_user_facing_tracing.cpp);
// the dev Wilson tree must stay unaffected, and nothing is emitted when the user channel is off.
namespace NKikimr {

using namespace Tests;
using namespace NWilson;

Y_UNIT_TEST_SUITE(TKqpUserFacingTrace) {

    std::tuple<TTestActorRuntime&, TServer::TPtr, TActorId> CreateServer() {
        TPortManager pm;
        NKikimrConfig::TAppConfig appConfig;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(appConfig);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        return {runtime, server, sender};
    }

    std::pair<TFakeWilsonUploader*, TFakeWilsonUploader*> RegisterUploaders(TTestActorRuntime& runtime) {
        auto* devUploader = new TFakeWilsonUploader();
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), runtime.Register(devUploader, 0), 0);
        auto* userUploader = new TFakeWilsonUploader();
        runtime.RegisterService(NWilson::MakeUserFacingWilsonUploaderId(), runtime.Register(userUploader, 0), 0);
        runtime.SimulateSleep(TDuration::Seconds(10));
        return {devUploader, userUploader};
    }

    // Seeds a dev trace via the event's TraceId and, when requested, a user-facing trace via the
    // serialized Record field (the path a cross-node request takes). The proxy is bypassed here.
    void ExecSQL(TServer::TPtr server, TActorId sender, const TString& sql,
            bool devTracing, bool userTracing,
            Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS) {
        auto& runtime = *server->GetRuntime();
        THolder<NKqp::TEvKqp::TEvQueryRequest> request = MakeSQLRequest(sql, true);
        if (userTracing) {
            NWilson::TTraceId::NewTraceId(15, 4095).Serialize(request->Record.MutableUserFacingTraceId());
        }
        NWilson::TTraceId devTrace;
        if (devTracing) {
            devTrace = NWilson::TTraceId::NewTraceId(15, 4095);
        }
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender,
            request.Release(), 0, 0, nullptr, std::move(devTrace)));
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetYdbStatus(), code);
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
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(server, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);",
            /*devTracing*/ true, /*userTracing*/ true);

        UNIT_ASSERT(devUploader->BuildTraceTrees());
        UNIT_ASSERT(userUploader->BuildTraceTrees());

        // Dev and user channels are independent trees in independent uploaders.
        UNIT_ASSERT_VALUES_EQUAL(1, devUploader->Traces.size());
        UNIT_ASSERT_VALUES_EQUAL(1, userUploader->Traces.size());

        auto* userRoot = FindRootChild(*userUploader, "UPSERT /Root/table-1");
        UNIT_ASSERT_C(userRoot, "user-facing root span missing, traces: " << userUploader->PrintTraces());
        UNIT_ASSERT_C(FindRootChild(*devUploader, "Session.query.QUERY_ACTION_EXECUTE"), "dev root span missing");
        UNIT_ASSERT_C(!FindRootChild(*devUploader, "UPSERT /Root/table-1"), "user tree leaked into dev uploader");
        UNIT_ASSERT_C(!FindRootChild(*userUploader, "Session.query.QUERY_ACTION_EXECUTE"),
            "dev tree leaked into user uploader");

        // Live executer phase in the user tree: Execute groups a Prepare (resolve) node and a Run node.
        auto execute = userRoot->BFSFindOne("Execute");
        UNIT_ASSERT_C(execute, "user Execute phase missing (executer live span)");
        UNIT_ASSERT_C(execute->get().BFSFindOne("Run"), "user Run phase missing");
        auto prepare = execute->get().FindOne("Prepare");
        UNIT_ASSERT_C(prepare, "user Prepare group missing");
        auto resolveTables = prepare->get().BFSFindOne("ResolveTables");
        UNIT_ASSERT_C(resolveTables, "ResolveTables not under Prepare");
        // Scheme-cache round-trips are nested under ResolveTables. (Metadata/navigate may be
        // skipped when table names were already resolved at compile time; key-range resolve
        // always runs.)
        UNIT_ASSERT_C(resolveTables->get().FindOne("Partitioning"), "Partitioning not under ResolveTables");

        // Compile is derived post-hoc as a top-level phase under the user root.
        UNIT_ASSERT_C(userRoot->FindOne("Compile"), "user Compile phase missing");

        // Effects commit (buffer actor round-trip) is a phase of Execute.
        UNIT_ASSERT_C(execute->get().FindOne("Commit"), "user Commit phase missing");

        // query -> stage -> task: at least one task span nests under a stage.
        auto run = execute->get().BFSFindOne("Run");
        UNIT_ASSERT(run);
        UNIT_ASSERT_C(run->get().BFSFindOne("Write task 1"), "per-task span missing under stage");
        UNIT_ASSERT_C(run->get().BFSFindOne("Write /Root/table-1"), "write stage not named by table");

        // The user tree is pruned: engine internals (ComputeActor) live in the dev tree only.
        UNIT_ASSERT_C(!userRoot->BFSFindOne("ComputeActor"), "user tree leaked engine internals");

        // db.query.text is sanitized: literals become '?' placeholders, no user data leaks.
        bool queryTextChecked = false;
        for (const auto& span : userUploader->Spans) {
            for (const auto& attr : span.attributes()) {
                if (attr.key() == "db.query.text") {
                    const TString& text = attr.value().string_value();
                    UNIT_ASSERT_C(!text.Contains("100"), "literal leaked into db.query.text: " << text);
                    UNIT_ASSERT_C(text.Contains("?"), "db.query.text not parameterized: " << text);
                    queryTextChecked = true;
                }
            }
        }
        UNIT_ASSERT_C(queryTextChecked, "db.query.text attribute missing");
    }

    Y_UNIT_TEST(UserChannelOffProducesNoUserTree) {
        auto [runtime, server, sender] = CreateServer();
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        auto [devUploader, userUploader] = RegisterUploaders(runtime);

        ExecSQL(server, sender,
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

        ExecSQL(server, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100);",
            /*devTracing*/ false, /*userTracing*/ true);

        UNIT_ASSERT(devUploader->Spans.empty());
        UNIT_ASSERT(userUploader->BuildTraceTrees());
        UNIT_ASSERT_VALUES_EQUAL(1, userUploader->Traces.size());
        UNIT_ASSERT_C(FindRootChild(*userUploader, "UPSERT /Root/table-1"),
            "user tree missing when dev tracing is off");
    }
}

} // namespace NKikimr
