#pragma once

#include <ydb/core/kqp/ut/rbo/effects/kqp_effects_helpers.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>

namespace NKikimr::NKqp::NGeneratedEffects {

using namespace NYdb;
using namespace NYdb::NQuery;

inline NKikimrConfig::TAppConfig GeneratedColumnsAppConfig() {
    auto config = NewRboAppConfig();
    config.MutableTableServiceConfig()->SetEnableNewRBO(false);
    config.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
    config.MutableFeatureFlags()->SetEnableGeneratedStored(true);
    return config;
}

class TTestFixture {
public:
    TTestFixture(bool newRbo, const std::string& createTable, const std::string& seed = "",
        const NKikimrConfig::TAppConfig& appConfig = GeneratedColumnsAppConfig())
        : Kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false))
        , Db(Kikimr.GetQueryClient())
        , Session(Db.GetSession().GetValueSync().GetSession())
    {
        Exec(createTable);
        if (!seed.empty()) {
            Exec(seed);
        }
        if (newRbo) {
            // Set up the fixture first, then require native RBO for every query under test.
            EnableNewRbo(Kikimr);
            Session = Db.GetSession().GetValueSync().GetSession();
        }
    }

    void Exec(const std::string& query) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
    }

    TString QueryYson(const std::string& query) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        return FormatResultSetYson(result.GetResultSet(0));
    }

    void Check(const std::string& query, const TString& expected) {
        CompareYson(expected, QueryYson(query));
    }

    TSession& QuerySession() {
        return Session;
    }

    ui64 GetRboCompileFailures() {
        return NYqlTests::GetNewRBOCompileCounters(Kikimr).second;
    }

private:
    TKikimrRunner Kikimr;
    TQueryClient Db;
    TSession Session;
};

} // namespace NKikimr::NKqp::NGeneratedEffects
