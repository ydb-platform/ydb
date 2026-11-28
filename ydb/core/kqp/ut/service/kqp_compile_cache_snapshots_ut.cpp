#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/compile_cache/types.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/ut/common/kqp_compile_cache_helpers.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/public/langver/yql_langver.h>

namespace NKikimr::NKqp {

namespace {

const TString Query = "SELECT id, name FROM `/Root/foo` WHERE name = '3_name';";

TKikimrSettings MakeSettings(bool compileCacheV2, bool readCommitted, ui32 cacheSize) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableFeatureFlags()->SetEnableCompileCacheV2(compileCacheV2);
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetEnableReadCommittedIsolation(readCommitted);
    appConfig.MutableTableServiceConfig()->SetCompileQueryCacheSize(cacheSize);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    return TKikimrSettings(appConfig).SetUseRealThreads(false).SetWithSampleTables(false);
}

// Talks to the compile service directly. The optimizer does not build snapshots yet, so the observer
// stands in for it: every compile actor response carries `Output` as the optimizer's snapshot.
struct TSnapshotFixture {
    explicit TSnapshotFixture(bool compileCacheV2, bool readCommitted = true, ui32 cacheSize = 100)
        : Kikimr(MakeSettings(compileCacheV2, readCommitted, cacheSize))
        , Runtime(*Kikimr.GetTestServer().GetRuntime())
    {
        UNIT_ASSERT(Kikimr.RunCall([&] {
            auto session = Kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            return session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/foo` (id Int64 NOT NULL, name String, PRIMARY KEY (id));
            )").GetValueSync().IsSuccess();
        }));

        Edge = Runtime.AllocateEdgeActor();
        // Requests go to the actor itself, not the service id: a service id takes an extra hop, and a
        // held response released later would overtake them.
        ServiceActor = Runtime.GetLocalServiceId(MakeKqpCompileServiceID(Runtime.GetNodeId(0)));
        Observer = Runtime.AddObserver<TEvKqp::TEvCompileResponse>([this](TEvKqp::TEvCompileResponse::TPtr& ev) {
            if (ev->Recipient != ServiceActor) {
                return;
            }
            ev->Get()->CacheOutput.CacheSnapshot = Output;
            if (HoldNext) {
                HoldNext = false;
                Held = ev.Release();
            }
        });
    }

    void Send(const TString& user = "root@builtin", TKqpTempTablesState::TConstPtr tempTables = nullptr,
        const TString& text = Query) {
        TMaybe<TKqpQueryId> query = TKqpQueryId(TString(DefaultKikimrPublicClusterName), "/Root", "", user, text,
            TKqpQuerySettings(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY), nullptr, TGUCSettings{});
        Runtime.Send(new IEventHandle(ServiceActor, Edge, new TEvKqp::TEvCompileRequest(Token(user), "", Nothing(),
            std::move(query), true, false, TInstant::Max(), nullptr, std::make_shared<TGUCSettings>(), Nothing(),
            std::make_shared<std::atomic<bool>>(true), Context(), {}, std::move(tempTables))));
    }

    void SendByUid(const TString& uid, const TString& user = "root@builtin", bool usePessimisticLocks = false,
        bool warmup = false, TInstant deadline = TInstant::Max(),
        TGUCSettings::TPtr guc = std::make_shared<TGUCSettings>()) {
        auto request = MakeHolder<TEvKqp::TEvCompileRequest>(Token(user), "", uid,
            Nothing(), true, false, deadline, nullptr, std::move(guc), Nothing(),
            std::make_shared<std::atomic<bool>>(true), Context());
        request->UsePessimisticLocks = usePessimisticLocks;
        request->IsWarmupCompilation = warmup;
        Runtime.Send(new IEventHandle(ServiceActor, Edge, request.Release()));
    }

    void SendRecompile(const TString& uid, const TString& user = "root@builtin") {
        Runtime.Send(new IEventHandle(ServiceActor, Edge, new TEvKqp::TEvRecompileRequest(Token(user), "", uid,
            Nothing(), false, TInstant::Max(), nullptr, std::make_shared<TGUCSettings>(), Nothing(),
            std::make_shared<std::atomic<bool>>(true), Context())));
    }

    TKqpCompileResult::TConstPtr Receive(bool fromCache) {
        auto response = Runtime.GrabEdgeEvent<TEvKqp::TEvCompileResponse>(Edge, TDuration::Seconds(30));
        UNIT_ASSERT(response && response->Get()->CompileResult);
        auto result = response->Get()->CompileResult;
        UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Stats.FromCache, fromCache);
        return result;
    }

    Ydb::StatusIds::StatusCode ReceiveStatus() {
        auto response = Runtime.GrabEdgeEvent<TEvKqp::TEvCompileResponse>(Edge, TDuration::Seconds(30));
        UNIT_ASSERT(response && response->Get()->CompileResult);
        return response->Get()->CompileResult->Status;
    }

    // Captures the next compile actor response; Release() delivers it.
    void Hold() {
        HoldNext = true;
        TDispatchOptions opts;
        opts.CustomFinalCondition = [&] { return bool(Held); };
        Runtime.DispatchEvents(opts, TDuration::Seconds(30));
        UNIT_ASSERT_C(Held, "Compile response should have been captured");
    }

    void Release() {
        Runtime.Send(Held.Release());
    }

    // The sys view row of `uid`.
    NKikimrKqp::TCompileCacheQueryInfo Row(const TString& uid) {
        auto response = NTest::ListQueryCache(Runtime, 0, "/Root");
        for (const auto& row : response->Get()->Record.GetCacheCacheQueries()) {
            if (row.GetQueryId() == uid) {
                return row;
            }
        }
        UNIT_FAIL("No cache row for " << uid);
        return {};
    }

    static TIntrusiveConstPtr<NACLib::TUserToken> Token(const TString& user) {
        return new NACLib::TUserToken(user, {});
    }

    static TIntrusivePtr<TUserRequestContext> Context() {
        return MakeIntrusive<TUserRequestContext>("snapshots", "/Root", "snapshots");
    }

    TKikimrRunner Kikimr;
    TTestActorRuntime& Runtime;
    TActorId Edge;
    TActorId ServiceActor;
    TPreCboSnapshotPtr Output;
    bool HoldNext = false;
    TAutoPtr<IEventHandle> Held;
    TTestActorRuntimeBase::TEventObserverHolder Observer;
};

} // namespace

Y_UNIT_TEST_SUITE(KqpCompileCacheSnapshots) {

    // The optimizer's answer decides the record: the same snapshot keeps it, a new one replaces it,
    // none stores the plan. V1 ignores snapshots.
    Y_UNIT_TEST_TWIN(PublishRules, CompileCacheV2) {
        TSnapshotFixture f(CompileCacheV2);

        f.Output = std::make_shared<NTest::TFakeSnapshot>();
        f.Send();
        const auto cold = f.Receive(false);
        const TString uid = cold->Uid;

        if (!CompileCacheV2) {
            f.Send();
            UNIT_ASSERT_VALUES_EQUAL(f.Receive(true)->Uid, uid);
            return;
        }

        // A snapshot record is not a plan: the next request compiles from it under the same uid.
        f.Send();
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(false)->Uid, uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetCompiledQueryAt(), cold->CompiledAt.MicroSeconds());

        f.Output = std::make_shared<NTest::TFakeSnapshot>();
        f.Send();
        const auto replaced = f.Receive(false);
        UNIT_ASSERT_VALUES_EQUAL(replaced->Uid, uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetCompiledQueryAt(), replaced->CompiledAt.MicroSeconds());

        f.Output = nullptr;
        f.Send();
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(false)->Uid, uid);
        f.Send();
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(true)->Uid, uid);

        // A snapshot record counts its uses like a plan: one per served request.
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 5);
        f.SendRecompile(uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(false)->Uid, uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 5);
    }

    // EXECUTE_PREPARED sends only the uid: a snapshot record is compiled under its uid and query, and
    // only for its own user.
    Y_UNIT_TEST_TWIN(SnapshotByUid, CompileCacheV2) {
        TSnapshotFixture f(CompileCacheV2);

        f.Output = std::make_shared<NTest::TFakeSnapshot>();
        f.Send();
        const TString uid = f.Receive(false)->Uid;

        f.SendByUid(uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(!CompileCacheV2)->Uid, uid);
        f.SendRecompile(uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(false)->Uid, uid);

        if (CompileCacheV2) {
            UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 2);
            f.SendByUid(uid, "other@builtin");
            UNIT_ASSERT_VALUES_EQUAL(f.ReceiveStatus(), Ydb::StatusIds::NOT_FOUND);
            f.SendRecompile(uid, "other@builtin");
            UNIT_ASSERT_VALUES_EQUAL(f.ReceiveStatus(), Ydb::StatusIds::NOT_FOUND);
        }
    }

    Y_UNIT_TEST(SnapshotSurvivesCompileErrors) {
        TSnapshotFixture f(true, /*readCommitted=*/false);
        f.Output = std::make_shared<NTest::TFakeSnapshot>();
        f.Send();
        const TString uid = f.Receive(false)->Uid;
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 1);

        f.SendByUid(uid, "root@builtin", /*usePessimisticLocks=*/true);
        UNIT_ASSERT_VALUES_EQUAL(f.ReceiveStatus(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 1);
        f.SendByUid(uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(false)->Uid, uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 2);

        f.SendByUid(uid, "root@builtin", false, false, TInstant::MicroSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(f.ReceiveStatus(), Ydb::StatusIds::TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 2);
        f.SendByUid(uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(false)->Uid, uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 3);
    }

    Y_UNIT_TEST(DifferentExecutionSettingsPreservePreparedSnapshot) {
        TSnapshotFixture f(true, /*readCommitted=*/true);
        f.Output = std::make_shared<NTest::TFakeSnapshot>();
        f.Send();
        const auto prepared = f.Receive(false);
        const TString uid = prepared->Uid;
        f.Output = nullptr;

        f.SendByUid(uid, "root@builtin", /*usePessimisticLocks=*/true);
        const auto rc = f.Receive(false);
        UNIT_ASSERT_VALUES_EQUAL(rc->Uid, uid);
        UNIT_ASSERT(rc->Query->Settings().UsePessimisticLocks);

        auto guc = std::make_shared<TGUCSettings>();
        guc->Set("search_path", "custom");
        f.SendByUid(uid, "root@builtin", false, false, TInstant::Max(), guc);
        const auto custom = f.Receive(false);
        UNIT_ASSERT_VALUES_EQUAL(custom->Uid, uid);
        UNIT_ASSERT(custom->Query->DecodeGUCSettings() == *guc);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetCompiledQueryAt(), prepared->CompiledAt.MicroSeconds());
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 3);

        f.SendByUid(uid);
        const auto normal = f.Receive(false);
        UNIT_ASSERT_VALUES_EQUAL(normal->Uid, uid);
        UNIT_ASSERT(!normal->Query->Settings().UsePessimisticLocks);
        UNIT_ASSERT(normal->Query->DecodeGUCSettings() == prepared->Query->DecodeGUCSettings());
        f.Send();
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(true)->Uid, uid);
    }

    Y_UNIT_TEST(PgModeChangeRecompilesPreparedSnapshot) {
        TSnapshotFixture f(true);
        f.Output = std::make_shared<NTest::TFakeSnapshot>();
        f.Send();
        const auto prepared = f.Receive(false);
        auto& appData = f.Runtime.GetAppData();
        NKikimrConfig::TFeatureFlags flags = appData.FeatureFlags;
        flags.SetDisablePgCompatibility(!prepared->Query->DisablePgCompatibility());
        appData.UpdateRuntimeFlags(flags);

        f.Output = nullptr;
        f.SendByUid(prepared->Uid, "root@builtin", /*usePessimisticLocks=*/true);
        const auto privatePlan = f.Receive(false);
        UNIT_ASSERT_VALUES_EQUAL(privatePlan->Query->DisablePgCompatibility(), flags.GetDisablePgCompatibility());
        UNIT_ASSERT(privatePlan->Query->Settings().UsePessimisticLocks);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(prepared->Uid).GetCompiledQueryAt(), prepared->CompiledAt.MicroSeconds());

        f.SendByUid(prepared->Uid);
        const auto replacement = f.Receive(false);
        UNIT_ASSERT_VALUES_EQUAL(replacement->Uid, prepared->Uid);
        UNIT_ASSERT_VALUES_EQUAL(replacement->Query->DisablePgCompatibility(), flags.GetDisablePgCompatibility());
        UNIT_ASSERT(!replacement->Query->Settings().UsePessimisticLocks);
        f.Send();
        UNIT_ASSERT_EQUAL(f.Receive(true), replacement);
    }

    Y_UNIT_TEST(CountsClientsSharingSnapshotCompilation) {
        TSnapshotFixture f(true);
        f.Output = std::make_shared<NTest::TFakeSnapshot>();
        f.Send();
        const TString uid = f.Receive(false)->Uid;

        f.SendByUid(uid, "root@builtin", /*usePessimisticLocks=*/false, /*warmup=*/true);
        f.Hold();
        f.SendByUid(uid);
        f.SendByUid(uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 1);
        f.Release();

        const auto plan = f.Receive(false);
        UNIT_ASSERT_VALUES_EQUAL(plan->Uid, uid);
        UNIT_ASSERT_EQUAL(f.Receive(false), plan);
        UNIT_ASSERT_EQUAL(f.Receive(false), plan);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 3);
    }

    // A session with temp tables neither takes nor stores a snapshot: it compiles from scratch.
    Y_UNIT_TEST(TempTablesPreservePreparedSnapshot) {
        TSnapshotFixture f(true);

        f.Output = std::make_shared<NTest::TFakeSnapshot>();
        f.Send();
        const TString uid = f.Receive(false)->Uid;

        f.Output = nullptr;
        f.Send("root@builtin", NTest::SomeTempTables());
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(false)->Uid, uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 2);

        f.SendByUid(uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(false)->Uid, uid);
        f.Send();
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(true)->Uid, uid);
    }

    // A session takes only plans from the cache: with a snapshot record the query is compiled again.
    Y_UNIT_TEST_TWIN(SessionSkipsSnapshot, CompileCacheV2) {
        TSnapshotFixture f(CompileCacheV2);
        f.Output = std::make_shared<NTest::TFakeSnapshot>();

        auto fromCache = [&] {
            auto result = f.Kikimr.RunCall([&] {
                auto settings = NYdb::NQuery::TExecuteQuerySettings().StatsMode(NYdb::NQuery::EStatsMode::Basic);
                return f.Kikimr.GetQueryClient().ExecuteQuery(Query, NYdb::NQuery::TTxControl::NoTx(), settings)
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result.GetStats()->GetCompilation()->IsFromCache();
        };
        UNIT_ASSERT(!fromCache());
        UNIT_ASSERT_VALUES_EQUAL(fromCache(), !CompileCacheV2);
    }

    Y_UNIT_TEST(EvictedSnapshotDoesNotServeWaitingTextRequest) {
        TSnapshotFixture f(true, /*readCommitted=*/true, /*cacheSize=*/1);
        f.Output = std::make_shared<NTest::TFakeSnapshot>();
        f.Send();
        const TString uid = f.Receive(false)->Uid;

        f.SendByUid(uid);
        f.Hold();
        f.Send("root@builtin", nullptr, "SELECT 43 AS evict_snapshot;");
        f.Receive(false);
        f.SendByUid(uid);
        UNIT_ASSERT_VALUES_EQUAL(f.ReceiveStatus(), Ydb::StatusIds::NOT_FOUND);
        f.Send();
        f.Release();
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(false)->Uid, uid);
        UNIT_ASSERT_VALUES_UNEQUAL(f.Receive(false)->Uid, uid);
    }

    Y_UNIT_TEST(DisabledV2CacheIsolatesTempTableRequests) {
        TSnapshotFixture f(true, /*readCommitted=*/true, /*cacheSize=*/0);
        f.Send("root@builtin", NTest::SomeTempTables());
        f.Hold();
        f.Send();
        f.Release();
        const TString first = f.Receive(false)->Uid;
        UNIT_ASSERT_VALUES_UNEQUAL(first, f.Receive(false)->Uid);
        f.Send();
        UNIT_ASSERT_VALUES_UNEQUAL(first, f.Receive(false)->Uid);
    }

    // Waiting requests join a compilation only in the same context: with temp tables the same name can
    // resolve to another table. V1 joins them regardless.
    Y_UNIT_TEST_TWIN(NoJoinAcrossTempTables, CompileCacheV2) {
        TSnapshotFixture f(CompileCacheV2);

        f.Send("root@builtin", NTest::SomeTempTables());
        f.Hold();
        f.Send();
        f.Release();
        const TString first = f.Receive(false)->Uid;
        const TString second = f.Receive(false)->Uid;
        UNIT_ASSERT_VALUES_EQUAL(first == second, !CompileCacheV2);
    }
}

} // namespace NKikimr::NKqp
