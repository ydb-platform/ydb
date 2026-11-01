#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NKqp::NGeneratedColumnTest {

using namespace NYdb;
using namespace NYdb::NQuery;

inline NKikimrConfig::TAppConfig GeneratedColumnsAppConfig(bool enableIndexStreamWrite = true) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableFeatureFlags()->SetEnableGeneratedStored(true);
    appConfig.MutableFeatureFlags()->SetEnableGeneratedVirtual(true);
    appConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(enableIndexStreamWrite);
    return appConfig;
}

inline std::string GetShowCreateTable(NYdb::NQuery::TSession& session, const std::string& path) {
    const std::string query = "SHOW CREATE TABLE `" + path + "`;";
    auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    TResultSetParser parser(result.GetResultSet(0));
    UNIT_ASSERT(parser.TryNextRow());
    auto ddl = parser.ColumnParser("CreateQuery").GetOptionalUtf8();
    UNIT_ASSERT_C(ddl.has_value(), "SHOW CREATE TABLE returned an empty CreateQuery");
    return *ddl;
}

class TTestFixture {
public:
    explicit TTestFixture(const std::string& createTable, const std::string& seed = "",
        bool enableIndexStreamWrite = true)
        : TTestFixture(createTable, seed, GeneratedColumnsAppConfig(enableIndexStreamWrite))
    {}

    TTestFixture(const std::string& createTable, const std::string& seed,
        const NKikimrConfig::TAppConfig& appConfig)
        : Kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false))
        , Db(Kikimr.GetQueryClient())
        , Session(Db.GetSession().GetValueSync().GetSession())
    {
        Exec(createTable);
        if (!seed.empty()) {
            Exec(seed);
        }
    }

    void Exec(const std::string& query) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
    }

    void Rejects(const std::string& query, const TString& expectedError) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "expected the query to be rejected: " << query);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), expectedError);
    }

    void Check(const std::string& query, const TString& expected) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
    }

    TString QueryYson(const std::string& query) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        return FormatResultSetYson(result.GetResultSet(0));
    }

    void CheckStaleEventually(const std::string& query, const TString& expected) {
        const TString normalizedExpected = ReformatYson(expected);
        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        TString actual;

        do {
            auto result = Session.ExecuteQuery(
                                     query, TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx())
                              .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n"
                                                               << result.GetIssues().ToString());
            actual = FormatResultSetYson(result.GetResultSet(0));
            if (ReformatYson(actual) == normalizedExpected) {
                return;
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (TInstant::Now() < deadline);

        CompareYson(expected, actual, TStringBuilder() << "eventual result mismatch for: " << query);
    }

    void CheckUnordered(const std::string& query, const TString& expected) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        CompareYsonUnordered(expected, FormatResultSetYson(result.GetResultSet(0)),
            TStringBuilder() << "unexpected rows for: " << query);
    }

    void CheckReturning(const std::string& update, const std::string& selectBack, const TString& expected) {
        auto result = Session.ExecuteQuery(update, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << update << "\n" << result.GetIssues().ToString());
        const TString returned = FormatResultSetYson(result.GetResultSet(0));

        CompareYsonUnordered(expected, returned, TStringBuilder() << "unexpected RETURNING rows for: " << update);

        auto after = Session.ExecuteQuery(selectBack, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(after.IsSuccess(), "query failed: " << selectBack << "\n" << after.GetIssues().ToString());

        CompareYsonUnordered(returned, FormatResultSetYson(after.GetResultSet(0)),
            TStringBuilder() << "RETURNING disagrees with a subsequent SELECT for: " << update);
    }

    TString ExplainAst(const std::string& query) {
        auto settings = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "explain failed: " << query << "\n" << result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats().has_value(), "no stats for: " << query);
        const auto ast = result.GetStats()->GetAst();
        UNIT_ASSERT_C(ast.has_value(), "no AST for: " << query);
        return TString(*ast);
    }

    NJson::TJsonValue ExplainPlan(const std::string& query) {
        auto settings = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "explain failed: " << query << "\n" << result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats().has_value(), "no stats for: " << query);
        const auto serializedPlan = result.GetStats()->GetPlan();
        UNIT_ASSERT_C(serializedPlan.has_value(), "no plan for: " << query);

        NJson::TJsonValue plan;
        UNIT_ASSERT_C(NJson::ReadJsonTree(*serializedPlan, &plan, true),
            "invalid JSON plan for: " << query << "\n" << *serializedPlan);
        return plan;
    }

    void CheckStreamLookup(const std::string& query, bool expected) {
        const TString ast = ExplainAst(query);
        const bool has = ast.Contains("KqpCnStreamLookup");
        UNIT_ASSERT_C(has == expected,
            "stream lookup expectation mismatch for: " << query
                << "\n  expected stream lookup: " << (expected ? "yes" : "no")
                << ", found: " << (has ? "yes" : "no")
                << "\nAST:\n" << ast);
    }

    void RestartSchemeShard(const std::string& tablePath) {
        auto& runtime = *Kikimr.GetTestServer().GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();

        auto connect = [&](TDuration timeout) {
            const auto pipe = runtime.Register(
                NTabletPipe::CreateClient(sender, Tests::SchemeRoot, pipeConfig));
            auto connected = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(
                sender, timeout);
            UNIT_ASSERT_C(connected, "timed out connecting to SchemeShard");
            UNIT_ASSERT_VALUES_EQUAL(connected->Get()->TabletId, Tests::SchemeRoot);
            UNIT_ASSERT_VALUES_EQUAL(connected->Get()->ClientId, pipe);
            UNIT_ASSERT_VALUES_EQUAL(connected->Get()->Status, NKikimrProto::OK);
            return std::pair(pipe, connected->Get()->Generation);
        };

        const auto [oldPipe, oldGeneration] = connect(TDuration::Seconds(30));
        runtime.Send(MakePipePerNodeCacheID(false), sender,
            new TEvPipeCache::TEvForward(
                new TEvents::TEvPoisonPill(), Tests::SchemeRoot, false));

        auto destroyed = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientDestroyed>(
            sender, TDuration::Seconds(30));
        UNIT_ASSERT_C(destroyed, "timed out waiting for SchemeShard shutdown");
        UNIT_ASSERT_VALUES_EQUAL(destroyed->Get()->TabletId, Tests::SchemeRoot);
        UNIT_ASSERT_VALUES_EQUAL(destroyed->Get()->ClientId, oldPipe);

        const auto [newPipe, newGeneration] = connect(TDuration::Seconds(30));
        UNIT_ASSERT_C(newGeneration > oldGeneration,
            "SchemeShard generation did not change after restart: " << oldGeneration);
        runtime.Send(new IEventHandle(newPipe, sender, new TEvents::TEvPoisonPill()));

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(TString(tablePath));
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.SyncVersion = true;
        runtime.Send(MakeSchemeCacheID(), sender,
            new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));

        auto response = runtime.GrabEdgeEventRethrow<
            TEvTxProxySchemeCache::TEvNavigateKeySetResult>(sender, TDuration::Seconds(30));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Request->ResultSet.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Request->ResultSet.front().Status,
            NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);
    }

    std::string ShowCreateTable(const std::string& tablePath) {
        return GetShowCreateTable(Session, tablePath);
    }

    NYdb::NQuery::TSession& QuerySession() {
        return Session;
    }

    TKikimrRunner& Runner() {
        return Kikimr;
    }

private:
    TKikimrRunner Kikimr;
    NYdb::NQuery::TQueryClient Db;
    NYdb::NQuery::TSession Session;
};


} // namespace NKikimr::NKqp::NGeneratedColumnTest
