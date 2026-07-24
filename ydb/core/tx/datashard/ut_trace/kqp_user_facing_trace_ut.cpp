#include "defs.h"
#include "datashard_ut_common_kqp.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

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

    TFakeWilsonUploader* RegisterUploader(TTestActorRuntime& runtime) {
        auto* uploader = new TFakeWilsonUploader();
        TActorId id = runtime.Register(uploader, 0);
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), id, 0);
        runtime.SimulateSleep(TDuration::Seconds(10));
        return uploader;
    }

    // Seeds a dev trace via the event's TraceId and, when requested, a user-facing trace via the
    // serialized Record field (the path a cross-node request takes). The proxy is bypassed here.
    void ExecSQL(TServer::TPtr server, TActorId sender, const TString& sql, bool userTracing,
            Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS) {
        auto& runtime = *server->GetRuntime();
        THolder<NKqp::TEvKqp::TEvQueryRequest> request = MakeSQLRequest(sql, true);
        if (userTracing) {
            NWilson::TTraceId::NewTraceId(15, 4095).Serialize(request->Record.MutableUserFacingTraceId());
        }
        NWilson::TTraceId devTrace = NWilson::TTraceId::NewTraceId(15, 4095);
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
        auto* uploader = RegisterUploader(runtime);

        ExecSQL(server, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);",
            /*userTracing*/ true);

        UNIT_ASSERT(uploader->BuildTraceTrees());

        // Dev and user channels are independent trees (distinct trace-ids).
        UNIT_ASSERT_VALUES_EQUAL(2, uploader->Traces.size());

        auto* userRoot = FindRootChild(*uploader, "UPSERT /Root/table-1");
        UNIT_ASSERT_C(userRoot, "user-facing root span missing, traces: " << uploader->PrintTraces());
        UNIT_ASSERT_C(FindRootChild(*uploader, "Session.query.QUERY_ACTION_EXECUTE"), "dev root span missing");

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
        for (const auto& span : uploader->Spans) {
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
        auto* uploader = RegisterUploader(runtime);

        ExecSQL(server, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100);",
            /*userTracing*/ false);

        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT_VALUES_EQUAL(1, uploader->Traces.size()); // dev only
        UNIT_ASSERT_C(!FindRootChild(*uploader, "UPSERT /Root/table-1"), "user tree emitted with channel off");
        UNIT_ASSERT_C(FindRootChild(*uploader, "Session.query.QUERY_ACTION_EXECUTE"), "dev root span missing");
    }
}

} // namespace NKikimr
