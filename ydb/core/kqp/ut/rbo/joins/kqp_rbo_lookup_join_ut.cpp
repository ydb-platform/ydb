#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/kqp/ut/olap/helpers/test_case.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/simple/settings.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_plan_conversion_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_trace_output.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/string/split.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <optional>
#include <regex>
#include <fstream>
#include <utility>

// These headers import NKikimr::NKqp into the global namespace, so include them last
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_aggregation_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_join_builder.h>

#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_plan_helpers.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>

namespace NKikimr::NKqp {
namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYql::NNodes;
using namespace NStat;
using namespace NKikimr::NKqp::NYqlTests;

// Return the lowest plan node with an operator of the given name.
const NJson::TJsonValue* FindLowestPlanNodeByOperatorName(const NJson::TJsonValue& planNode, const TString& name) {
    if (!planNode.IsMap()) {
        return nullptr;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (const auto& child : plans->second.GetArraySafe()) {
            if (const auto* node = FindLowestPlanNodeByOperatorName(child, name)) {
                return node;
            }
        }
    }

    if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
        for (const auto& opNode : operators->second.GetArraySafe()) {
            const auto& op = opNode.GetMapSafe();
            const auto field = op.find("Name");
            if (field != op.end() && field->second.IsString() && field->second.GetStringSafe() == name) {
                return &planNode;
            }
        }
    }

    return nullptr;
}

void BasicHashJoinTest(bool useBlockHashJoin) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetUseBlockHashJoin(useBlockHashJoin);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/t1` (
            a Int64	NOT NULL,
	            b Int64,
            primary key(a)
        ) WITH (Store = Column);

        CREATE TABLE `/Root/t2` (
            a Int64	NOT NULL,
	            b Int64,
            primary key(a)
        ) WITH (Store = Column);

        CREATE TABLE `/Root/t3` (
            a Int64	NOT NULL,
	            b Int64,
            primary key(a)
        ) WITH (Store = Column);
    )").GetValueSync();

    NYdb::TValueBuilder rowsTablet1;
    rowsTablet1.BeginList();
    for (size_t i = 0; i < 4; ++i) {
        rowsTablet1.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i + 1)
            .EndStruct();
    }
    rowsTablet1.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/t1", rowsTablet1.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    NYdb::TValueBuilder rowsTablet2;
    rowsTablet2.BeginList();
    for (size_t i = 0; i < 3; ++i) {
        rowsTablet2.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i + 1)
            .EndStruct();
    }
    rowsTablet2.EndList();

    resultUpsert = db.BulkUpsert("/Root/t2", rowsTablet2.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    NYdb::TValueBuilder rowsTablet3;
    rowsTablet3.BeginList();
    for (size_t i = 0; i < 5; ++i) {
        rowsTablet3.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i + 1)
            .EndStruct();
    }
    rowsTablet3.EndList();

    resultUpsert = db.BulkUpsert("/Root/t3", rowsTablet3.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    db = kikimr.GetTableClient();
    auto session2 = db.CreateSession().GetValueSync().GetSession();

    const std::string queryPrefix =
        R"(
            PRAGMA ydb.CostBasedOptimizationLevel='0';
            PRAGMA ydb.HashJoinMode='grace';
        )";

    std::vector<std::string> queries = {
        R"(
            SELECT t1.a, t2.a FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a order by t1.a;
        )",
        R"(
            SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a order by t1.a;
        )",
        R"(
            PRAGMA AnsiImplicitCrossJoin;
            SELECT t1.a, t2.a, t3.a FROM `/Root/t1` as t1, `/Root/t2` as t2, `/Root/t3` as t3 where t1.a = t2.a and t2.a = t3.a order by t1.a, t2.a, t3.a;
        )",
        R"(
            PRAGMA AnsiImplicitCrossJoin;
            SELECT t1.a, t2.a, t3.a FROM `/Root/t1` as t1, `/Root/t2` as t2, `/Root/t3` as t3 where t1.a = t2.a order by t1.a, t2.a, t3.a;
        )",
        R"(
            PRAGMA AnsiImplicitCrossJoin;
            SELECT t1.a, t2.a, t3.a FROM `/Root/t1` as t1, `/Root/t2` as t2, `/Root/t3` as t3 order by t1.a, t2.a, t3.a;
        )",
        R"(
            SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a and t2.b > 2 order by t1.a, t2.a;
        )",
        R"(
            SELECT t1.a, t2.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a and t2.b = 2 order by t1.a, t2.a;
        )",
        R"(
            SELECT t1.a, t2.a FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a and t1.b = 2 order by t1.a, t2.a;
        )",
        R"(
            SELECT t1.a, t2.a, t3.a FROM `/Root/t1` as t1 inner join `/Root/t2` as t2 on t1.a = t2.a inner join `/Root/t3` as t3 on t2.a = t3.a and t3.b = 2 order by t1.a, t2.a, t3.b;
        )",
        R"(
            SELECT t1.a, t2.a, t3.a FROM `/Root/t1` as t1 left join `/Root/t2` as t2 on t1.a = t2.a left join `/Root/t3` as t3 on t2.a = t3.a and t3.b = 2 order by t1.a, t2.a, t3.b;
        )",
    };

    std::vector<std::string> results = {
        R"([[0;0];[1;1];[2;2]])",
        R"([[0;[0]];[1;[1]];[2;[2]];[3;#]])",
        R"([[0;0;0];[1;1;1];[2;2;2]])",
        R"([[0;0;0];[0;0;1];[0;0;2];[0;0;3];[0;0;4];[1;1;0];[1;1;1];[1;1;2];[1;1;3];[1;1;4];[2;2;0];[2;2;1];[2;2;2];[2;2;3];[2;2;4]])",
        R"([[0;0;0];[0;0;1];[0;0;2];[0;0;3];[0;0;4];[0;1;0];[0;1;1];[0;1;2];[0;1;3];[0;1;4];[0;2;0];[0;2;1];[0;2;2];[0;2;3];[0;2;4];[1;0;0];[1;0;1];[1;0;2];[1;0;3];[1;0;4];[1;1;0];[1;1;1];[1;1;2];[1;1;3];[1;1;4];[1;2;0];[1;2;1];[1;2;2];[1;2;3];[1;2;4];[2;0;0];[2;0;1];[2;0;2];[2;0;3];[2;0;4];[2;1;0];[2;1;1];[2;1;2];[2;1;3];[2;1;4];[2;2;0];[2;2;1];[2;2;2];[2;2;3];[2;2;4];[3;0;0];[3;0;1];[3;0;2];[3;0;3];[3;0;4];[3;1;0];[3;1;1];[3;1;2];[3;1;3];[3;1;4];[3;2;0];[3;2;1];[3;2;2];[3;2;3];[3;2;4]])",
        R"([[0;#];[1;#];[2;[2]];[3;#]])",
        R"([[0;#];[1;[1]];[2;#];[3;#]])",
        R"([[1;1]])",
        R"([[1;1;1]])",
        R"([[0;[0];#];[1;[1];[1]];[2;[2];#];[3;#;#]])",
    };

    const std::string joinAlgo = useBlockHashJoin ? "BlockHashJoinCore" : "GraceJoinCore";
    auto queryClient = kikimr.GetQueryClient();
    for (ui32 i = 0; i < queries.size(); ++i) {
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        const auto query = queryPrefix + "\n" + queries[i];

        auto result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        auto ast = *result.GetStats()->GetAst();
        const auto isCrossJoin = query.find("AnsiImplicitCrossJoin") != std::string::npos;
        UNIT_ASSERT_C(isCrossJoin || ast.find(joinAlgo) != std::string::npos, TStringBuilder() << "Wrong join algo. Expected: " << joinAlgo);
        if (!isCrossJoin) {
            const auto plan = TString{*result.GetStats()->GetPlan()};
            const auto simplifiedPlan = GetSimplifiedPlan(plan);
            const TString explainJoinAlgo = useBlockHashJoin ? "BlockHash" : "Grace";
            const auto* joinOp = FindOperatorByStringField(simplifiedPlan, "JoinAlgo", explainJoinAlgo);
            UNIT_ASSERT_C(joinOp, plan);
            const TString explainJoinName = TStringBuilder() << "Join (" << explainJoinAlgo << ")";
            UNIT_ASSERT_C(GetStringField(*joinOp, "Name").Contains(explainJoinName), plan);
        }

        result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
    }
}

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYqlTests;

Y_UNIT_TEST_SUITE(KqpRboLookupJoins) {
    Y_UNIT_TEST(LookupJoins_oldRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        // appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Table` (
                Key Int32,
                SubKey1 Int32,
                SubKey2 String,
                Value1 String,
                Value2 String,
                PRIMARY KEY (Key, SubKey1, SubKey2),
                INDEX Index1_12 GLOBAL ON (SubKey1, SubKey2),
                INDEX Index1_21 GLOBAL ON (SubKey2, Value1),
                INDEX Index1_212 GLOBAL ON (SubKey2) COVER (Value2)
            );

            CREATE TABLE `/Root/Table2` (
                Key Int32,
                SubKey1 Int32,
                SubKey2 String,
                Value1 String,
                Value2 String,
                PRIMARY KEY (Key, SubKey1, SubKey2),
                INDEX Index2_12 GLOBAL ON (SubKey1, SubKey2),
                INDEX Index2_21 GLOBAL ON (SubKey2, Value1),
                INDEX Index2_212 GLOBAL ON (SubKey2) COVER (Value2)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/Table` (Key, SubKey1, SubKey2, Value1, Value2) VALUES
                (0, 0, "0", "1", "1"),
                (0, 0, "1", "2", "2"),
                (0, 1, "0", "3", "3"),
                (0, 1, "1", "4", "4"),
                (1, 0, "0", "5", "5"),
                (1, 0, "1", "6", "6"),
                (1, 1, "0", "7", "7"),
                (1, 1, "1", "8", "8");

            UPSERT INTO `/Root/Table2` (Key, SubKey1, SubKey2, Value1, Value2) VALUES
                (0, 0, "0", "1", "1"),
                (0, 0, "1", "2", "2"),
                (0, 1, "0", "3", "3"),
                (0, 1, "1", "4", "4"),
                (1, 0, "0", "15", "15"),
                (1, 0, "1", "16", "16"),
                (1, 1, "0", "17", "17"),
                (1, 1, "1", "18", "18");    
            )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        db = kikimr.GetTableClient();
        session = db.CreateSession().GetValueSync().GetSession();
        auto db2 = kikimr.GetQueryClient();
        auto session2 = db2.GetSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                -- MapJoin, PK left / PK right
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value1 = t2.Value1
                WHERE t1.Key = 0 AND t2.Key = 0
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- MapJoin, Index12 left side stream lookup for Value1+Value2 / PK right
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value1 = t2.Value1
                WHERE t1.SubKey1 = 0 AND t1.SubKey2 = "0" AND t2.Key = 0
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- MapJoin, PK left / Index21 right side stream lookup for Value2
                SELECT t1.Value2, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value2 = t2.Value2
                WHERE t1.Key = 0 AND t2.SubKey2 = "0"
                ORDER BY t1.Value2, t2.Value1;
            )",
            R"(
                -- MapJoin, Index212 left / Index212 right, no stream lookup needed
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value2 = t2.Value2
                WHERE t1.SubKey2 = "1" AND t2.SubKey2 = "1"
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- LookupJoin, PK left / PK right (probe t2 by Key)
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Key = t2.Key
                WHERE t1.Key >= 0
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, Index12 left / PK right (t1 via Index12 filter, probe t2 PK)
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Key = t2.Key
                WHERE t1.SubKey1 = 0 AND t1.SubKey2 = "0"
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, PK left / Index21 right (probe t2 by SubKey2 and need t2.Value1)
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.SubKey2 = t2.SubKey2
                WHERE t1.Key = 1
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, Index212 left / Index212 right
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.SubKey2 = t2.SubKey2
                WHERE t1.SubKey2 >= "0"
                ORDER BY t1.Value2, t2.Value2;
            )",
        };

        std::vector<std::string> results = {
            R"([[["1"];["1"]];[["2"];["2"]];[["3"];["3"]];[["4"];["4"]]])",
            R"([[["1"];["1"]]])",
            R"([[["1"];["1"]];[["3"];["3"]]])",
            R"([[["2"];["2"]];[["4"];["4"]]])",
            R"([[["1"];["1"]];[["1"];["2"]];[["1"];["3"]];[["1"];["4"]];[["2"];["1"]];[["2"];["2"]];[["2"];["3"]];[["2"];["4"]];[["3"];["1"]];[["3"];["2"]];[["3"];["3"]];[["3"];["4"]];[["4"];["1"]];[["4"];["2"]];[["4"];["3"]];[["4"];["4"]];[["5"];["15"]];[["5"];["16"]];[["5"];["17"]];[["5"];["18"]];[["6"];["15"]];[["6"];["16"]];[["6"];["17"]];[["6"];["18"]];[["7"];["15"]];[["7"];["16"]];[["7"];["17"]];[["7"];["18"]];[["8"];["15"]];[["8"];["16"]];[["8"];["17"]];[["8"];["18"]]])",
            R"([[["1"];["1"]];[["1"];["2"]];[["1"];["3"]];[["1"];["4"]];[["5"];["15"]];[["5"];["16"]];[["5"];["17"]];[["5"];["18"]]])",
            R"([[["5"];["1"]];[["5"];["15"]];[["5"];["17"]];[["5"];["3"]];[["6"];["16"]];[["6"];["18"]];[["6"];["2"]];[["6"];["4"]];[["7"];["1"]];[["7"];["15"]];[["7"];["17"]];[["7"];["3"]];[["8"];["16"]];[["8"];["18"]];[["8"];["2"]];[["8"];["4"]]])",
            R"([[["1"];["1"]];[["1"];["15"]];[["1"];["17"]];[["1"];["3"]];[["2"];["16"]];[["2"];["18"]];[["2"];["2"]];[["2"];["4"]];[["3"];["1"]];[["3"];["15"]];[["3"];["17"]];[["3"];["3"]];[["4"];["16"]];[["4"];["18"]];[["4"];["2"]];[["4"];["4"]];[["5"];["1"]];[["5"];["15"]];[["5"];["17"]];[["5"];["3"]];[["6"];["16"]];[["6"];["18"]];[["6"];["2"]];[["6"];["4"]];[["7"];["1"]];[["7"];["15"]];[["7"];["17"]];[["7"];["3"]];[["8"];["16"]];[["8"];["18"]];[["8"];["2"]];[["8"];["4"]]])",
        };

        struct TCase {
            bool Lookup;
            TVector<TString> Impls;
        };
        const std::vector<TCase> cases = {
            {false, {}},
            {false, {"Index1_12/indexImplTable"}},
            {false, {"Index2_21/indexImplTable"}},
            {false, {"Index1_212/indexImplTable", "Index2_212/indexImplTable"}},
            {true,  {}},
            {true,  {"Index1_12/indexImplTable"}},
            {true,  {"Index2_21/indexImplTable"}},
            {true,  {"Index1_212/indexImplTable", "Index2_212/indexImplTable"}},
        };

        const std::string header = "PRAGMA ydb.OptDisableAutoIndexSelection = \"false\";\n";
        for (ui32 i = 0; i < queries.size(); ++i) {
            const std::string query = header + queries[i];
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
            Cout << query << "\n";
            auto result2 = session2.ExecuteQuery(query,
                    NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
                ).ExtractValueSync();
            const auto plan = TString{*result2.GetStats()->GetPlan()};
            PrintPlan(plan, /*analyzeMode=*/false);
            const auto ast = TString{*result2.GetStats()->GetAst()};
            Cout << "Plan AST:\n" << ast;

            const bool lookupPlan = plan.Contains("InnerJoin (Lookup)") || plan.Contains("TableLookupJoin");
            if (cases[i].Lookup) {
                UNIT_ASSERT_C(lookupPlan, "query #" << i << " expected a lookup join, plan:\n" << plan);
            } else {
                UNIT_ASSERT_C(plan.Contains("InnerJoin (Map)"), "query #" << i << " expected a map join, plan:\n" << plan);
                UNIT_ASSERT_C(!lookupPlan, "query #" << i << " expected no lookup join, plan:\n" << plan);
            }

            if (cases[i].Impls.empty()) {
                UNIT_ASSERT_C(!ast.Contains("indexImplTable"), "query #" << i << " expected only main-table reads, ast:\n" << ast);
            } else {
                for (const auto& impl : cases[i].Impls) {
                    UNIT_ASSERT_C(ast.Contains(impl), "query #" << i << " expected " << impl << ", ast:\n" << ast);
                    UNIT_ASSERT_C(plan.Contains("indexImplTable"), "query #" << i << " expected an index read, plan:\n" << plan);
                }
            }
        }
    }

    Y_UNIT_TEST(LookupJoins_newRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetUseBlockHashJoin(true);
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(false);
        appConfig.MutableTableServiceConfig()->SetEnablePruneKeyColumns(true);
        appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Table` (
                Key Int32,
                SubKey1 Int32,
                SubKey2 String,
                Value1 String,
                Value2 String,
                PRIMARY KEY (Key, SubKey1, SubKey2)
            );

            CREATE TABLE `/Root/Table2` (
                Key Int32,
                SubKey1 Int32,
                SubKey2 String,
                Value1 String,
                Value2 String,
                PRIMARY KEY (Key, SubKey1, SubKey2)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto upsert = [&](const char* table, const std::vector<std::tuple<i32, i32, TString, TString, TString>>& data) {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [key, subKey1, subKey2, value1, value2] : data) {
                rows.AddListItem().BeginStruct()
                    .AddMember("Key").OptionalInt32(key)
                    .AddMember("SubKey1").OptionalInt32(subKey1)
                    .AddMember("SubKey2").OptionalString(subKey2)
                    .AddMember("Value1").OptionalString(value1)
                    .AddMember("Value2").OptionalString(value2)
                    .EndStruct();
            }
            rows.EndList();
            auto r = db.BulkUpsert(table, rows.Build()).GetValueSync();
            UNIT_ASSERT_C(r.IsSuccess(), r.GetIssues().ToString());
        };

        upsert("/Root/Table", {{0, 0, "0", "1", "1"}, {0, 0, "1", "2", "2"}, {0, 1, "0", "3", "3"}, {0, 1, "1", "4", "4"},
                               {1, 0, "0", "5", "5"}, {1, 0, "1", "6", "6"}, {1, 1, "0", "7", "7"}, {1, 1, "1", "8", "8"}});

        upsert("/Root/Table2", {{0, 0, "0", "1", "1"}, {0, 0, "1", "2", "2"}, {0, 1, "0", "3", "3"}, {0, 1, "1", "4", "4"},
                                {1, 0, "0", "15", "15"}, {1, 0, "1", "16", "16"}, {1, 1, "0", "17", "17"}, {1, 1, "1", "18", "18"}});

        for (const auto& addIndex : {
                 "ALTER TABLE `/Root/Table` ADD INDEX Index1_12 GLOBAL ON (SubKey1, SubKey2);",
                 "ALTER TABLE `/Root/Table` ADD INDEX Index1_21 GLOBAL ON (SubKey2, Value1);",
                 "ALTER TABLE `/Root/Table` ADD INDEX Index1_212 GLOBAL ON (SubKey2) COVER (Value2);",
                 "ALTER TABLE `/Root/Table2` ADD INDEX Index2_12 GLOBAL ON (SubKey1, SubKey2);",
                 "ALTER TABLE `/Root/Table2` ADD INDEX Index2_21 GLOBAL ON (SubKey2, Value1);",
                 "ALTER TABLE `/Root/Table2` ADD INDEX Index2_212 GLOBAL ON (SubKey2) COVER (Value2);",
             }) {
            result = session.ExecuteSchemeQuery(addIndex).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        db = kikimr.GetTableClient();
        session = db.CreateSession().GetValueSync().GetSession();
        auto db2 = kikimr.GetQueryClient();
        auto session2 = db2.GetSession().GetValueSync().GetSession();

        std::vector<std::string> queries = {
            R"(
                -- MapJoin, PK left / PK right
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value1 = t2.Value1
                WHERE t1.Key = 0 AND t2.Key = 0
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- MapJoin, Index12 left side stream lookup for Value1+Value2 / PK right
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value1 = t2.Value1
                WHERE t1.SubKey1 = 0 AND t1.SubKey2 = "0" AND t2.Key = 0
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- MapJoin, PK left / Index21 right side stream lookup for Value2
                SELECT t1.Value2, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value2 = t2.Value2
                WHERE t1.Key = 0 AND t2.SubKey2 = "0"
                ORDER BY t1.Value2, t2.Value1;
            )",
            R"(
                -- MapJoin, Index212 left / Index212 right, no stream lookup needed
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Value2 = t2.Value2
                WHERE t1.SubKey2 = "1" AND t2.SubKey2 = "1"
                ORDER BY t1.Value2, t2.Value2;
            )",
            R"(
                -- LookupJoin, PK left / PK right (probe t2 by Key)
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Key = t2.Key
                WHERE t1.Key >= 0
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, Index12 left / PK right (t1 via Index12 filter, probe t2 PK)              
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.Key = t2.Key
                WHERE t1.SubKey1 = 0 AND t1.SubKey2 = "0"
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, PK left / Index21 right (probe t2 by SubKey2 and need t2.Value1)           
                SELECT t1.Value1, t2.Value1
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.SubKey2 = t2.SubKey2
                WHERE t1.Key = 1
                ORDER BY t1.Value1, t2.Value1;
            )",
            R"(
                -- LookupJoin, Index212 left / Index212 right             
                SELECT t1.Value2, t2.Value2
                FROM `/Root/Table` AS t1 INNER JOIN `/Root/Table2` AS t2 ON t1.SubKey2 = t2.SubKey2
                WHERE t1.SubKey2 >= "0"
                ORDER BY t1.Value2, t2.Value2;
            )",
        };

        std::vector<std::string> results = {
            R"([[["1"];["1"]];[["2"];["2"]];[["3"];["3"]];[["4"];["4"]]])",
            R"([[["1"];["1"]]])",
            R"([[["1"];["1"]];[["3"];["3"]]])",
            R"([[["2"];["2"]];[["4"];["4"]]])",
            R"([[["1"];["1"]];[["1"];["2"]];[["1"];["3"]];[["1"];["4"]];[["2"];["1"]];[["2"];["2"]];[["2"];["3"]];[["2"];["4"]];[["3"];["1"]];[["3"];["2"]];[["3"];["3"]];[["3"];["4"]];[["4"];["1"]];[["4"];["2"]];[["4"];["3"]];[["4"];["4"]];[["5"];["15"]];[["5"];["16"]];[["5"];["17"]];[["5"];["18"]];[["6"];["15"]];[["6"];["16"]];[["6"];["17"]];[["6"];["18"]];[["7"];["15"]];[["7"];["16"]];[["7"];["17"]];[["7"];["18"]];[["8"];["15"]];[["8"];["16"]];[["8"];["17"]];[["8"];["18"]]])",
            R"([[["1"];["1"]];[["1"];["2"]];[["1"];["3"]];[["1"];["4"]];[["5"];["15"]];[["5"];["16"]];[["5"];["17"]];[["5"];["18"]]])",
            R"([[["5"];["1"]];[["5"];["15"]];[["5"];["17"]];[["5"];["3"]];[["6"];["16"]];[["6"];["18"]];[["6"];["2"]];[["6"];["4"]];[["7"];["1"]];[["7"];["15"]];[["7"];["17"]];[["7"];["3"]];[["8"];["16"]];[["8"];["18"]];[["8"];["2"]];[["8"];["4"]]])",
            R"([[["1"];["1"]];[["1"];["15"]];[["1"];["17"]];[["1"];["3"]];[["2"];["16"]];[["2"];["18"]];[["2"];["2"]];[["2"];["4"]];[["3"];["1"]];[["3"];["15"]];[["3"];["17"]];[["3"];["3"]];[["4"];["16"]];[["4"];["18"]];[["4"];["2"]];[["4"];["4"]];[["5"];["1"]];[["5"];["15"]];[["5"];["17"]];[["5"];["3"]];[["6"];["16"]];[["6"];["18"]];[["6"];["2"]];[["6"];["4"]];[["7"];["1"]];[["7"];["15"]];[["7"];["17"]];[["7"];["3"]];[["8"];["16"]];[["8"];["18"]];[["8"];["2"]];[["8"];["4"]]])",
        };

        struct TCase {
            bool Lookup;
            TVector<TString> Impls;
        };
        const std::vector<TCase> cases = {
            {false, {}},
            {false, {"Index1_12/indexImplTable"}},
            {false, {"Index2_21/indexImplTable"}},
            {false, {"Index1_212/indexImplTable", "Index2_212/indexImplTable"}},
            {true,  {}},
            {true,  {"Index1_12/indexImplTable"}},
            {true,  {"Index2_21/indexImplTable"}},
            {true,  {"Index1_212/indexImplTable", "Index2_212/indexImplTable"}},
        };

        const std::string header = "PRAGMA ydb.OptDisableAutoIndexSelection = \"false\";\n";
        for (ui32 i = 0; i < queries.size(); ++i) {
            const std::string query = header + queries[i];
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
            Cout << query << "\n";
            auto result2 = session2.ExecuteQuery(query,
                    NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
                ).ExtractValueSync();
            const auto plan = TString{*result2.GetStats()->GetPlan()};
            PrintPlan(plan, /*analyzeMode=*/false);
            const auto ast = TString{*result2.GetStats()->GetAst()};
            Cout << "Plan AST:\n" << ast;

            const bool lookupPlan = plan.Contains("InnerJoin (Lookup)") || plan.Contains("TableLookupJoin");
            if (cases[i].Lookup) {
                UNIT_ASSERT_C(lookupPlan, "query #" << i << " expected a lookup join, plan:\n" << plan);
            } else {
                UNIT_ASSERT_C(plan.Contains("InnerJoin (BlockHash)"),
                              "query #" << i << " expected a map join, plan:\n" << plan);
                UNIT_ASSERT_C(!lookupPlan, "query #" << i << " expected no lookup join, plan:\n" << plan);
            }

            if (cases[i].Impls.empty()) {
                UNIT_ASSERT_C(!ast.Contains("indexImplTable"), "query #" << i << " expected only main-table reads, ast:\n" << ast);
            } else {
                for (const auto& impl : cases[i].Impls) {
                    UNIT_ASSERT_C(ast.Contains(impl), "query #" << i << " expected " << impl << ", ast:\n" << ast);
                    UNIT_ASSERT_C(plan.Contains("indexImplTable"), "query #" << i << " expected an index read, plan:\n" << plan);
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(IndexLookupJoinChains, PhysicalStagePeephole) {
        const TString schema = R"(
            CREATE TABLE `/Root/t1` (
                a Int32,
                b Int32,
                c Int32,
                d String,
                e Int32,
                f Int64,
                PRIMARY KEY (a)
            );

            CREATE TABLE `/Root/t2` (
                a Int32,
                b String,
                c String,
                PRIMARY KEY (a)
            );

            CREATE TABLE `/Root/t3` (
                a Int32,
                b String,
                c String,
                d Int32,
                e Int32,
                PRIMARY KEY (a, b)
            );

            CREATE TABLE `/Root/t4` (
                a Int32,
                b String,
                PRIMARY KEY (a)
            );

            CREATE TABLE `/Root/t5` (
                a Int32,
                b Int32,
                c Int32,
                d String,
                PRIMARY KEY (a)
            );

            CREATE TABLE `/Root/t6` (
                a Int32 NOT NULL,
                b Int32 NOT NULL,
                PRIMARY KEY (a)
            );
        )";

        struct TCase {
            const char* Name;
            TString Query;
            // How many joins are expected to be executed as a stream lookup join.
            ui32 LookupJoins;
            // Set when the old optimizer cannot be used as a reference for the result.
            const char* ExpectedYson = nullptr;
        };

        const TVector<TCase> cases = {
            {"two lookups by primary key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t2.b AS t2b, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t2` AS t2 ON t1.b = t2.a
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a AND t1.d = t3.b
                ORDER BY a;
            )", 2},

            {"lookup probed by a column of a previous lookup", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c, t4.b AS t4b
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a AND t1.d = t3.b
                    INNER JOIN `/Root/t4` AS t4 ON t3.e = t4.a
                ORDER BY a;
            )", 2},

            {"three lookups, a filter and an aggregation", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t4.b AS t4b, COUNT(*) AS cnt, SUM(t1.e * t3.d) AS total
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t2` AS t2 ON t1.b = t2.a
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a AND t1.d = t3.b
                    INNER JOIN `/Root/t4` AS t4 ON t3.e = t4.a
                WHERE t2.c = "x"
                GROUP BY t4.b
                ORDER BY t4b;
            )", 3},

            {"left join chain keeps unmatched rows", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t2.b AS t2b, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    LEFT JOIN `/Root/t2` AS t2 ON t1.b = t2.a
                    LEFT JOIN `/Root/t3` AS t3 ON t1.c = t3.a AND t1.d = t3.b
                ORDER BY a;
            )", 2},

            {"aggregation over an unmatched left join", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT COUNT(*) AS total, COUNT(t2.b) AS matched, SUM(t1.e) AS e
                FROM `/Root/t1` AS t1
                    LEFT JOIN `/Root/t2` AS t2 ON t1.b = t2.a;
            )", 1},

            {"key prefix lookup with several matches per key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, COUNT(*) AS cnt, SUM(t3.d) AS total
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a
                GROUP BY t1.a
                ORDER BY a;
            )", 1},

            {"predicate on the probed side", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a
                WHERE t3.d >= 20
                ORDER BY a, t3c;
            )", 1},

            {"self join by primary key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT x.a AS a, y.e AS e
                FROM `/Root/t1` AS x
                    INNER JOIN `/Root/t1` AS y ON x.b = y.a
                ORDER BY a;
            )", 1},

            {"point predicate ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.d = t3.b
                WHERE t3.a = 1
                ORDER BY a, t3c;
            )", 1},

            {"inner join over a subquery with a point predicate", R"(
                PRAGMA ydb.CostBasedOptimizationLevel='0';
                SELECT t1.a AS a, t3.b AS t3b
                FROM `/Root/t1` AS t1
                    INNER JOIN (SELECT a, b FROM `/Root/t3` WHERE a = 1) AS t3 ON t1.d = t3.b
                ORDER BY a, t3b;
            )", 1},

            // Point predicate is ok with 2 points for inner join.
            {"several point predicates ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.d = t3.b
                WHERE t3.a IN (1, 2)
                ORDER BY a, t3c;
            )", 1},

            {"null point predicate ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.d = t3.b
                WHERE t3.a IS NULL
                ORDER BY a, t3c;
            )", 1},

            {"point predicate on a join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.c = t3.a AND t1.d = t3.b
                WHERE t3.a = 1
                ORDER BY a, t3c;
            )", 1},

            {"left join with a point predicate ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    LEFT JOIN (SELECT * FROM `/Root/t3` WHERE a = 2) AS t3 ON t1.d = t3.b
                ORDER BY a, t3c;
            )", 1},

            // Here is a bug for old optimizer, we cannot use stream lookup join for point predicates > 1 with left joins.
            {"left join with several point predicates ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    LEFT JOIN (SELECT * FROM `/Root/t3` WHERE a IN (1, 2)) AS t3 ON t1.d = t3.b
                ORDER BY a, t3c;
            )", 0,
             R"([[[1];["p1a"]];[[1];["p2a"]];[[2];["p1b"]];[[3];["p1a"]];[[3];["p2a"]];[[4];["p1a"]];)"
             R"([[4];["p2a"]];[[5];["p1a"]];[[5];["p2a"]];[[6];["p1a"]];[[6];["p2a"]];[[7];["p1a"]];[[7];["p2a"]]])"},

            {"semi join from an in subplan", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.c IN (SELECT a FROM `/Root/t3`)
                ORDER BY a;
            )", 1},

            {"left only join from subselect", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.a NOT IN (SELECT a FROM `/Root/t2`)
                ORDER BY a;
            )", 0},

            // No optional keys.
            {"left only join on not null columns", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t6 # 100) Bytes(t6 # 1000)';
                SELECT t6.a AS a
                FROM `/Root/t6` AS t6
                WHERE t6.a NOT IN (SELECT a FROM `/Root/t6` WHERE b = 2)
                ORDER BY a;
            )", 1},

            {"semi join with a filtered probed side", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.c IN (SELECT a FROM `/Root/t3` WHERE d >= 30)
                ORDER BY a;
            )", 1},

            {"left only join with a filtered probed side", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.a NOT IN (SELECT a FROM `/Root/t3` WHERE d >= 30 AND d <= 50)
                ORDER BY a;
            )", 0},

            {"semi join with a point predicate ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.d IN (SELECT b FROM `/Root/t3` WHERE a = 2)
                ORDER BY a;
            )", 1},

            {"left only join with a point predicate ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.d NOT IN (SELECT b FROM `/Root/t3` WHERE a = 2)
                ORDER BY a;
            )", 0},

            {"semi join with several point predicates ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.d IN (SELECT b FROM `/Root/t3` WHERE a IN (1, 2))
                ORDER BY a;
            )", 0,
             R"([[[1]];[[2]];[[3]];[[4]];[[5]];[[6]];[[7]]])"},

            {"left only join with several point predicates ahead of the join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a
                FROM `/Root/t1` AS t1
                WHERE t1.d NOT IN (SELECT b FROM `/Root/t3` WHERE a IN (1, 2))
                ORDER BY a;
            )", 0,
             R"([])"},

            {"join key is not a key prefix", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t3.c AS t3c
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t3` AS t3 ON t1.d = t3.b
                ORDER BY a, t3c;
            )", 0},

            // Need support for cast.
            {"join key types differ", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t2.b AS t2b
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t2` AS t2 ON t1.f = t2.a
                ORDER BY a;
            )", 0},

            {"inner join with a residual non-key join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t5.d AS t5d
                FROM `/Root/t1` AS t1
                    INNER JOIN `/Root/t5` AS t5 ON t1.b = t5.a AND t1.c = t5.b
                ORDER BY a, t5d;
            )", 1},

            {"left join with a residual non-key join key", R"(
                PRAGMA ydb.OptimizerHints = 'Rows(t1 # 100) Bytes(t1 # 1000) Rows(t2 # 100) Bytes(t2 # 1000) Rows(t3 # 100) Bytes(t3 # 1000) Rows(t4 # 100) Bytes(t4 # 1000) Rows(t5 # 100) Bytes(t5 # 1000)';
                SELECT t1.a AS a, t5.d AS t5d
                FROM `/Root/t1` AS t1
                    LEFT JOIN `/Root/t5` AS t5 ON t1.b = t5.a AND t1.c = t5.b
                ORDER BY a, t5d;
            )", 1},
        };

        struct TQueryResult {
            TString Yson;
            TString Ast;
            TString Plan;
        };

        auto runQueries = [&](bool newRbo) {
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
            if (!PhysicalStagePeephole) {
                appConfig.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(false);
            }
            appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
            appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
            appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(false);
            appConfig.MutableTableServiceConfig()->SetEnablePruneKeyColumns(true);
            appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
            appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

            TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            auto schemeResult = session.ExecuteSchemeQuery(schema).GetValueSync();
            UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

            auto bulkUpsert = [&](const char* table, NYdb::TValueBuilder& rows) {
                auto result = db.BulkUpsert(table, rows.Build()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            };
            // For debugging.
            const bool enableAstDump = false;

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b, c] : TVector<std::tuple<i32, TString, TString>>{
                         {1, "n1", "x"}, {2, "n2", "y"}, {3, "n3", "x"}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").OptionalInt32(a)
                        .AddMember("b").OptionalString(b)
                        .AddMember("c").OptionalString(c)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t2", rows);
            }

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b] : TVector<std::tuple<i32, TString>>{{10, "s1"}, {20, "s2"}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").OptionalInt32(a)
                        .AddMember("b").OptionalString(b)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t4", rows);
            }

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b, c, d] : TVector<std::tuple<i32, i32, i32, TString>>{
                         {1, 1, 10, "m1"}, {1, 2, 20, "m2"}, {2, 2, 30, "m3"}, {3, 4, 40, "m4"}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").OptionalInt32(a)
                        .AddMember("b").OptionalInt32(b)
                        .AddMember("c").OptionalInt32(c)
                        .AddMember("d").OptionalString(d)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t5", rows);
            }

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b] : TVector<std::tuple<i32, i32>>{{1, 1}, {2, 1}, {3, 2}, {4, 2}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").Int32(a)
                        .AddMember("b").Int32(b)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t6", rows);
            }

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b, c, d, e] : TVector<std::tuple<i32, TString, TString, i32, i32>>{
                         {1, "a", "p1a", 10, 10}, {1, "b", "p1b", 20, 20}, {2, "a", "p2a", 30, 10},
                         {3, "a", "p3a", 40, 20}, {4, "a", "p4a", 50, 99}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").OptionalInt32(a)
                        .AddMember("b").OptionalString(b)
                        .AddMember("c").OptionalString(c)
                        .AddMember("d").OptionalInt32(d)
                        .AddMember("e").OptionalInt32(e)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t3", rows);
            }

            {
                // A null in the first key column: a point predicate can select it, so the constant
                // cell of a lookup key prefix has to be allowed to hold a null.
                NYdb::TValueBuilder rows;
                rows.BeginList();
                rows.AddListItem().BeginStruct()
                    .AddMember("a").OptionalInt32(std::nullopt)
                    .AddMember("b").OptionalString("a")
                    .AddMember("c").OptionalString("pna")
                    .AddMember("d").OptionalInt32(60)
                    .AddMember("e").OptionalInt32(30)
                    .EndStruct();
                rows.EndList();
                bulkUpsert("/Root/t3", rows);
            }

            {
                NYdb::TValueBuilder rows;
                rows.BeginList();
                for (const auto& [a, b, c, d, e] :
                     TVector<std::tuple<i32, std::optional<i32>, std::optional<i32>, TString, i32>>{
                         {1, 1, 1, "a", 2},
                         {2, 1, 1, "b", 3},
                         {3, 2, 2, "a", 1},
                         {4, 3, 4, "a", 5},
                         {5, 9, 3, "a", 7},
                         {6, std::nullopt, 1, "a", 1},
                         {7, 2, std::nullopt, "a", 4}}) {
                    rows.AddListItem().BeginStruct()
                        .AddMember("a").OptionalInt32(a)
                        .AddMember("b").OptionalInt32(b)
                        .AddMember("c").OptionalInt32(c)
                        .AddMember("d").OptionalString(d)
                        .AddMember("e").OptionalInt32(e)
                        .AddMember("f").OptionalInt64(b ? std::optional<i64>(*b) : std::nullopt)
                        .EndStruct();
                }
                rows.EndList();
                bulkUpsert("/Root/t1", rows);
            }

            auto explainSession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();

            TVector<TQueryResult> results;
            for (const auto& testCase : cases) {
                auto result = session.ExecuteDataQuery(testCase.Query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), testCase.Name << " (new RBO: " << newRbo << "): " << result.GetIssues().ToString());

                auto explained = explainSession.ExecuteQuery(testCase.Query, NYdb::NQuery::TTxControl::NoTx(),
                        NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
                UNIT_ASSERT_C(explained.IsSuccess(), testCase.Name << ": " << explained.GetIssues().ToString());

                results.push_back({FormatResultSetYson(result.GetResultSet(0)), TString{*explained.GetStats()->GetAst()},
                                   TString{*explained.GetStats()->GetPlan()}});

                if (enableAstDump && newRbo && getenv("DUMP_AST") && TString(testCase.Name) == getenv("DUMP_AST")) {
                    Cout << "=== AST DUMP [" << testCase.Name << "] ===\n"
                         << *explained.GetStats()->GetAst() << "\n=== AST DUMP END ===\n";
                }
            }
            return results;
        };

        auto countOccurrences = [](const TString& text, TStringBuf needle) {
            ui32 count = 0;
            for (size_t pos = text.find(needle); pos != TString::npos; pos = text.find(needle, pos + needle.size())) {
                ++count;
            }
            return count;
        };

        const auto newRboResults = runQueries(/*newRbo=*/true);
        const auto yqlResults = runQueries(/*newRbo=*/false);
        UNIT_ASSERT_VALUES_EQUAL(newRboResults.size(), cases.size());
        UNIT_ASSERT_VALUES_EQUAL(yqlResults.size(), cases.size());

        for (size_t i = 0; i < cases.size(); ++i) {
            const auto& testCase = cases[i];
            const auto& newRbo = newRboResults[i];

            // Check that results are the same.
            if (testCase.ExpectedYson) {
                UNIT_ASSERT_VALUES_EQUAL_C(newRbo.Yson, TString(testCase.ExpectedYson), testCase.Name);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(newRbo.Yson, yqlResults[i].Yson, testCase.Name);
            }

            const auto lookupJoins = countOccurrences(newRbo.Ast, "KqpIndexLookupJoin");
            UNIT_ASSERT_VALUES_EQUAL_C(lookupJoins, testCase.LookupJoins, testCase.Name << ", ast:\n" << newRbo.Ast);
            UNIT_ASSERT_VALUES_EQUAL_C(newRbo.Plan.Contains("TableLookupJoin"), testCase.LookupJoins != 0,
                testCase.Name << ", plan:\n" << newRbo.Plan);
        }
    }
}

} // namespace NKikimr::NKqp
