#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/compile_cache/types.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/public/langver/yql_langver.h>

namespace NKikimr::NKqp {

namespace {

const TString Query = "SELECT id, name FROM `/Root/foo` WHERE name = '3_name';";

struct TFakeSnapshot : TPreCboSnapshot {
    ui64 ByteSize() const override {
        return 1024;
    }
};

TKqpTempTablesState::TConstPtr SomeTempTables() {
    auto state = std::make_shared<TKqpTempTablesState>();
    state->TempTables["/Root/tmp"] = {};
    return state;
}

TKikimrSettings MakeSettings(bool compileCacheV2) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableFeatureFlags()->SetEnableCompileCacheV2(compileCacheV2);
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetEnableReadCommittedIsolation(false);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    return TKikimrSettings(appConfig).SetUseRealThreads(false).SetWithSampleTables(false);
}

// Talks to the compile service directly. The optimizer does not build snapshots yet, so the observer
// stands in for it: every compile actor response carries `Output` as the optimizer's snapshot.
struct TSnapshotFixture {
    explicit TSnapshotFixture(bool compileCacheV2)
        : Kikimr(MakeSettings(compileCacheV2))
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

    void Send(const TString& user = "root@builtin", TKqpTempTablesState::TConstPtr tempTables = nullptr) {
        TMaybe<TKqpQueryId> query = TKqpQueryId(TString(DefaultKikimrPublicClusterName), "/Root", "", user, Query,
            TKqpQuerySettings(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY), nullptr, TGUCSettings{});
        Runtime.Send(new IEventHandle(ServiceActor, Edge, new TEvKqp::TEvCompileRequest(Token(user), "", Nothing(),
            std::move(query), true, false, TInstant::Max(), nullptr, std::make_shared<TGUCSettings>(), Nothing(),
            std::make_shared<std::atomic<bool>>(true), Context(), {}, std::move(tempTables))));
    }

    void SendByUid(const TString& uid, const TString& user = "root@builtin", bool usePessimisticLocks = false,
        bool warmup = false) {
        auto request = MakeHolder<TEvKqp::TEvCompileRequest>(Token(user), "", uid,
            Nothing(), true, false, TInstant::Max(), nullptr, std::make_shared<TGUCSettings>(), Nothing(),
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
        auto* request = new TEvKqp::TEvListQueryCacheQueriesRequest();
        request->Record.SetTenantName("/Root");
        request->Record.SetFreeSpace(1024 * 1024);
        Runtime.Send(new IEventHandle(ServiceActor, Edge, request));
        auto response = Runtime.GrabEdgeEvent<TEvKqp::TEvListQueryCacheQueriesResponse>(Edge, TDuration::Seconds(5));
        UNIT_ASSERT(response);
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

        f.Output = std::make_shared<TFakeSnapshot>();
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

        f.Output = std::make_shared<TFakeSnapshot>();
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

        f.Output = std::make_shared<TFakeSnapshot>();
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
        TSnapshotFixture f(true);
        f.Output = std::make_shared<TFakeSnapshot>();
        f.Send();
        const TString uid = f.Receive(false)->Uid;
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 1);

        f.SendByUid(uid, "root@builtin", /*usePessimisticLocks=*/true);
        UNIT_ASSERT_VALUES_EQUAL(f.ReceiveStatus(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 1);
        f.SendByUid(uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(false)->Uid, uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 2);

        {
            // Deliver the compile actor's timeout once it has started processing this request.
            const auto timeout = f.Runtime.AddObserver<TEvKqp::TEvContinueProcess>(
                [&](TEvKqp::TEvContinueProcess::TPtr& ev) {
                    if (!ev->Get()->QueryId) {
                        f.Runtime.SendAsync(new IEventHandle(ev->Recipient, ev->Recipient, new TEvents::TEvWakeup()));
                        ev.Reset();
                    }
                });
            f.SendByUid(uid);
            UNIT_ASSERT_VALUES_EQUAL(f.ReceiveStatus(), Ydb::StatusIds::TIMEOUT);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 2);
        f.SendByUid(uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Receive(false)->Uid, uid);
        UNIT_ASSERT_VALUES_EQUAL(f.Row(uid).GetAccessCount(), 3);
    }

    Y_UNIT_TEST(CountsClientsSharingSnapshotCompilation) {
        TSnapshotFixture f(true);
        f.Output = std::make_shared<TFakeSnapshot>();
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

        f.Output = std::make_shared<TFakeSnapshot>();
        f.Send();
        const TString uid = f.Receive(false)->Uid;

        f.Output = nullptr;
        f.Send("root@builtin", SomeTempTables());
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
        f.Output = std::make_shared<TFakeSnapshot>();

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

    // Waiting requests join a compilation only in the same context: with temp tables the same name can
    // resolve to another table. V1 joins them regardless.
    Y_UNIT_TEST_TWIN(NoJoinAcrossTempTables, CompileCacheV2) {
        TSnapshotFixture f(CompileCacheV2);

        f.Send("root@builtin", SomeTempTables());
        f.Hold();
        f.Send();
        f.Release();
        const TString first = f.Receive(false)->Uid;
        const TString second = f.Receive(false)->Uid;
        UNIT_ASSERT_VALUES_EQUAL(first == second, !CompileCacheV2);
    }
}

} // namespace NKikimr::NKqp
