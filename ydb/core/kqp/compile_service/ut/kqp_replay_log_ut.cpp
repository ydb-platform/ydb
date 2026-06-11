#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/aclib/aclib.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

class TTestQueryReplayBackend : public IQueryReplayBackend {
public:
    explicit TTestQueryReplayBackend(std::shared_ptr<TVector<TString>> collected)
        : Collected(std::move(collected))
    {}

    void Collect(const TString& queryData) override {
        Collected->push_back(queryData);
    }

    void UpdateConfig(const NKikimrConfig::TTableServiceConfig&) override {
    }

    bool IsNull() override {
        return false;
    }

private:
    std::shared_ptr<TVector<TString>> Collected;
};

class TTestQueryReplayBackendFactory : public IQueryReplayBackendFactory {
public:
    explicit TTestQueryReplayBackendFactory(std::shared_ptr<TVector<TString>> collected)
        : Collected(std::move(collected))
    {}

    IQueryReplayBackend* Create(
        const NKikimrConfig::TTableServiceConfig&,
        TIntrusivePtr<TKqpCounters>) override
    {
        return new TTestQueryReplayBackend(Collected);
    }

private:
    std::shared_ptr<TVector<TString>> Collected;
};

bool ReplayLogContainsSubstring(const TVector<TString>& collected, const TString& substring) {
    for (const auto& message : collected) {
        if (message.Contains(substring)) {
            return true;
        }
    }
    return false;
}

void CompileQuery(TTestActorRuntime& runtime, const TString& sql) {
    const ui32 nodeIdx = 0;
    auto edgeActor = runtime.AllocateEdgeActor(nodeIdx);
    auto compileService = MakeKqpCompileServiceID(runtime.GetNodeId(nodeIdx));

    TKqpQuerySettings querySettings(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
    TKqpQueryId queryId(
        TString(DefaultKikimrPublicClusterName),
        "/Root",
        /*databaseId*/ "",
        /*userSid*/ "root@builtin",
        sql,
        querySettings,
        /*paramTypes*/ nullptr,
        TGUCSettings{});

    TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("root@builtin", {});

    runtime.Send(new IEventHandle(
        compileService,
        edgeActor,
        new TEvKqp::TEvCompileRequest(
            userToken,
            /*clientAddress*/ "",
            /*uid*/ Nothing(),
            TMaybe<TKqpQueryId>(std::move(queryId)),
            /*keepInCache*/ false,
            /*isQueryActionPrepare*/ false,
            /*perStatementResult*/ false,
            /*deadline*/ TInstant::Max(),
            /*dbCounters*/ nullptr,
            /*gUCSettings*/ std::make_shared<TGUCSettings>(),
            /*applicationName*/ Nothing(),
            /*intrestedInResult*/ std::make_shared<std::atomic<bool>>(true),
            MakeIntrusive<TUserRequestContext>("replay-log-ut", "/Root", "replay-log-ut-session"))));

    // Huge timeout below is set just in case – no such waiting is expected
    const auto ev = runtime.GrabEdgeEvent<TEvKqp::TEvCompileResponse>(edgeActor, TDuration::Seconds(30));
    UNIT_ASSERT_C(ev, "Compile request timed out: " + sql);
    UNIT_ASSERT_C(ev->Get()->CompileResult, "CompileResult is null: " + sql);
}

void ExecuteSchemeQuery(TKikimrRunner& kikimr, const TString& query) {
    kikimr.RunCall([&] {
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return true;
    });
}

void PrepareDataQuery(TKikimrRunner& kikimr, const TString& query) {
    kikimr.RunCall([&] {
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.PrepareDataQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return true;
    });
}

} // namespace

Y_UNIT_TEST_SUITE(KqpReplayLog) {
    Y_UNIT_TEST(SensitiveQueriesAreNotWrittenToReplayLog) {
        auto collected = std::make_shared<TVector<TString>>();
        auto replayFactory = std::make_shared<TTestQueryReplayBackendFactory>(collected);

        const TString secretValue = "secret-value-rand-value-mf83ezDr7";

        const TString createTableQuery = "CREATE TABLE t (id Int32, PRIMARY KEY(id))";
        const TString createSecretQuery = TStringBuilder()
            << "CREATE SECRET s WITH (VALUE = '" << secretValue << "')";
        const TString alterSecretQuery = TStringBuilder()
            << "ALTER SECRET s WITH (VALUE = '" << secretValue << "')";
        const TString selectQuery = "SELECT id FROM t";

        {
            TKikimrSettings settings;
            settings.SetUseRealThreads(false);
            settings.SetQueryReplayBackendFactory(replayFactory);

            TKikimrRunner kikimr(settings);
            auto& runtime = *kikimr.GetTestServer().GetRuntime();

            ExecuteSchemeQuery(kikimr, createTableQuery);
            CompileQuery(runtime, createTableQuery);
            PrepareDataQuery(kikimr, selectQuery);

            CompileQuery(runtime, createSecretQuery);
            ExecuteSchemeQuery(kikimr, createSecretQuery);
            CompileQuery(runtime, alterSecretQuery);
            ExecuteSchemeQuery(kikimr, alterSecretQuery);
        }

        UNIT_ASSERT_C(
            ReplayLogContainsSubstring(*collected, createTableQuery),
            "Expected replay log message for scheme query: " + createTableQuery);
        UNIT_ASSERT_C(
            ReplayLogContainsSubstring(*collected, selectQuery),
            "Expected replay log message for data query: " + selectQuery);
        UNIT_ASSERT_C(
            !ReplayLogContainsSubstring(*collected, secretValue),
            "Sensitive queries must not be written to replay log");
    }
}

} // namespace NKikimr::NKqp
