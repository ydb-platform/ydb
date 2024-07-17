#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <util/string/printf.h>

#include <fstream>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

TString GetStatic(const TString& filePath) {
    TString fullPath = SRC_("data/" + filePath);

    std::ifstream file(fullPath);
    
    if (!file.is_open()) {
        throw std::runtime_error("can't open + " + fullPath + " " + std::filesystem::current_path());
    }
    
    std::stringstream buffer;
    buffer << file.rdbuf();

    return buffer.str();
}

/*
 * A basic join order test. We define 5 tables sharing the same
 * key attribute and construct various full clique join queries
 */
static void CreateSampleTable(TSession session) {
    UNIT_ASSERT(session.ExecuteSchemeQuery(GetStatic("schema/rstuv.sql")).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/R` (id, payload1, ts) VALUES (1, "blah", CAST("1998-12-01" AS Date) );
        REPLACE INTO `/Root/S` (id, payload2) VALUES (1, "blah");
        REPLACE INTO `/Root/T` (id, payload3) VALUES (1, "blah");
        REPLACE INTO `/Root/U` (id, payload4) VALUES (1, "blah");
        REPLACE INTO `/Root/V` (id, payload5) VALUES (1, "blah");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(GetStatic("schema/tpch.sql")).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(GetStatic("schema/tpcds.sql")).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(GetStatic("schema/tpcc.sql")).GetValueSync().IsSuccess());

}

static TKikimrRunner GetKikimrWithJoinSettings(bool useStreamLookupJoin = false, TString stats = ""){
    TVector<NKikimrKqp::TKqpSetting> settings;

    NKikimrKqp::TKqpSetting setting;

    if (stats != "") {
        setting.SetName("OverrideStatistics");
        setting.SetValue(stats);
        settings.push_back(setting);
    }

    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(useStreamLookupJoin);
    appConfig.MutableTableServiceConfig()->SetEnableConstantFolding(true);
    appConfig.MutableTableServiceConfig()->SetCompileTimeoutMs(TDuration::Minutes(10).MilliSeconds());

    auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
    serverSettings.SetKqpSettings(settings);
    return TKikimrRunner(serverSettings);
}

class TChainConstructor {
public:
    TChainConstructor(size_t chainSize)
        : Kikimr_(GetKikimrWithJoinSettings())
        , TableClient_(Kikimr_.GetTableClient())
        , Session_(TableClient_.CreateSession().GetValueSync().GetSession())
        , ChainSize_(chainSize)
    {}

    void CreateTables() {
        for (size_t i = 0; i < ChainSize_; ++i) {
            TString tableName;
            
            tableName
                .append("/Root/table_").append(ToString(i));;

            TString createTable;
            createTable
                += "CREATE TABLE `" +  tableName + "` (id"
                +  ToString(i) + " Int32, " 
                +  "PRIMARY KEY (id" + ToString(i) + "));";

            std::cout << createTable << std::endl;
            auto res = Session_.ExecuteSchemeQuery(createTable).GetValueSync();
            std::cout << res.GetIssues().ToString() << std::endl;
            UNIT_ASSERT(res.IsSuccess());
        }
    }

    void JoinTables() {
        TString joinRequest;

        joinRequest.append("SELECT * FROM `/Root/table_0` as t0 ");

        for (size_t i = 1; i < ChainSize_; ++i) {
            TString table = "/Root/table_" + ToString(i);

            TString prevAliasTable = "t" + ToString(i - 1);
            TString aliasTable = "t" + ToString(i);

            joinRequest
                += "INNER JOIN `" + table + "`" + " AS " + aliasTable + " ON "
                +  aliasTable + ".id" + ToString(i) + "=" + prevAliasTable + ".id" 
                +  ToString(i-1) + " ";
        }

        auto result = Session_.ExecuteDataQuery(joinRequest, TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        std::cout << result.GetIssues().ToString() << std::endl;
        std::cout << joinRequest << std::endl;
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

private:
    TKikimrRunner Kikimr_;
    NYdb::NTable::TTableClient TableClient_;
    TSession Session_;
    size_t ChainSize_; 
};

void ExplainJoinOrderTestDataQuery(const TString& queryPath, bool useStreamLookupJoin) {
    auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSampleTable(session);

    /* join with parameters */
    {
        const TString query = GetStatic(queryPath);
        
        auto result = session.ExplainDataQuery(query).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        Cout << result.GetPlan();
    }
}

void ExecuteJoinOrderTestDataQuery(const TString& queryPath, bool useStreamLookupJoin) {
    auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSampleTable(session);

    /* join with parameters */
    {
        const TString query = GetStatic(queryPath);

        auto result = session.ExecuteDataQuery(query,TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }
}

Y_UNIT_TEST_SUITE(KqpJoinOrder) {
    Y_UNIT_TEST(Chain65Nodes) {
        TChainConstructor chain(65);
        chain.CreateTables();
        chain.JoinTables();
    }

    Y_UNIT_TEST_TWIN(FiveWayJoin, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQuery("queries/five_way_join.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinStatsOverride, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQuery("queries/five_way_join_stats_override.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FourWayJoinLeftFirst, StreamLookupJoin) {
        ExecuteJoinOrderTestDataQuery("queries/four_way_join_left_first.sql", StreamLookupJoin);
    }

     Y_UNIT_TEST_TWIN(FiveWayJoinWithPreds, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/five_way_join_with_preds.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithComplexPreds, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/five_way_join_with_complex_preds.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithComplexPreds2, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/five_way_join_with_complex_preds2.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithPredsAndEquiv, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/four_way_join_with_preds_and_equiv.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FourWayJoinWithPredsAndEquivAndLeft, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/four_way_join_with_preds_and_equiv_and_left.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithConstantFold, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/five_way_join_with_constant_fold.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithConstantFoldOpt, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/five_way_join_with_constant_fold_opt.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(DatetimeConstantFold, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/datetime_constant_fold.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCH3, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpch3.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCH5, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpch5.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCH10, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpch10.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCH11, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpch11.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCH21, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpch21.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCDS16, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpcds16.sql", StreamLookupJoin);       
    }

    Y_UNIT_TEST_TWIN(TPCDS61, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpcds61.sql", StreamLookupJoin);       
    }

    Y_UNIT_TEST_TWIN(TPCDS88, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpcds88.sql", StreamLookupJoin); 
    }

    Y_UNIT_TEST_TWIN(TPCDS90, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpcds90.sql", StreamLookupJoin);  
    }
    
    Y_UNIT_TEST_TWIN(TPCDS92, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpcds92.sql", StreamLookupJoin);
    }

    Y_UNIT_TEST_TWIN(TPCDS94, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpcds94.sql", StreamLookupJoin); 
    }

    Y_UNIT_TEST_TWIN(TPCDS95, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpcds95.sql", StreamLookupJoin); 
    }

    Y_UNIT_TEST_TWIN(TPCDS96, StreamLookupJoin) {
        ExplainJoinOrderTestDataQuery("queries/tpcds96.sql", StreamLookupJoin);     
    }

    void JoinOrderTestWithOverridenStats(const TString& queryPath, const TString& statsPath, const TString& correctJoinOrderPath, bool useStreamLookupJoin) {
        auto kikimr = GetKikimrWithJoinSettings(useStreamLookupJoin, GetStatic(statsPath));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = GetStatic(queryPath);
        
            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            TString ref = GetStatic(correctJoinOrderPath);

            /* correct canonized join order in cout, change corresponding join_order/.json file */
            Cout << CanonizeJoinOrder(result.GetPlan()) << Endl;

            /* Only check the plans if stream join is enabled*/
            if (useStreamLookupJoin) {
                UNIT_ASSERT(JoinOrderAndAlgosMatch(result.GetPlan(), ref));
            }
        }
    }

    Y_UNIT_TEST_TWIN(OverrideStatsTPCH2, StreamLookupJoin) {
        JoinOrderTestWithOverridenStats(
            "queries/tpch2.sql", "stats/tpch1000s.json", "join_order/tpch2_1000s.json", StreamLookupJoin
        );
    }

    Y_UNIT_TEST_TWIN(OverrideStatsTPCH9, StreamLookupJoin) {
        JoinOrderTestWithOverridenStats(
            "queries/tpch9.sql", "stats/tpch1000s.json", "join_order/tpch9_1000s.json", StreamLookupJoin
        );
    }

    /*
    Y_UNIT_TEST_TWIN(OverrideStatsTPCDS64, StreamLookupJoin) {
        JoinOrderTestWithOverridenStats(
            "queries/tpcds64.sql", "stats/tpcds1000s.json", "join_order/tpcds64_1000s.json", StreamLookupJoin
        );
    }
    */
   
    Y_UNIT_TEST_TWIN(OverrideStatsTPCDS78, StreamLookupJoin) {
        JoinOrderTestWithOverridenStats(
            "queries/tpcds78.sql", "stats/tpcds1000s.json", "join_order/tpcds78_1000s.json", StreamLookupJoin
        );
    }

    Y_UNIT_TEST(TPCC) {
        JoinOrderTestWithOverridenStats(
            "queries/tpcc.sql", "stats/tpcc.json", "join_order/tpcc.json", false);
    }

}
}
}
