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
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h>

namespace NKikimr::NKqp {
namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYql::NNodes;
using namespace NStat;
using namespace NKikimr::NKqp::NYqlTests;

// A partial aggregate must reduce rows in its input's stage, so no stage
// connection may separate it from its input. Returns the number of partials.
ui32 CountPartialAggregatesInInputStages(const NJson::TJsonValue& node, const TString& plan) {
    bool partial = false;
    if (node["Operators"].IsArray()) {
        for (const auto& op : node["Operators"].GetArraySafe()) {
            partial |= op["Phase"].IsString() && op["Phase"].GetStringSafe() == "Intermediate";
        }
    }
    ui32 partials = partial;
    if (node["Plans"].IsArray()) {
        for (const auto& child : node["Plans"].GetArraySafe()) {
            const auto& type = child["PlanNodeType"];
            UNIT_ASSERT_C(!partial || !type.IsString() || type.GetStringSafe() != "Connection",
                "Partial aggregate reads another stage:\n" << plan);
            partials += CountPartialAggregatesInInputStages(child, plan);
        }
    }
    return partials;
}

void CollectCallableNodes(const TExprNode::TPtr& node, TStringBuf callableName, TExprNode::TListType& result) {
    if (node->IsCallable(callableName)) {
        result.push_back(node);
    }

    for (const auto& child : node->ChildrenList()) {
        CollectCallableNodes(child, callableName, result);
    }
}

void TestMultiConsumer(bool columnTables) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto dbSession = db.CreateSession().GetValueSync().GetSession();

    TString schemaQ = R"(
        CREATE TABLE `/Root/t1` (
            a Int64 NOT NULL,
            b Int64 NOT NULL,
            c Int64,
            primary key(a, b)
        )
    )";

    if (columnTables) {
        schemaQ += R"(WITH (STORE = column))";
    }
    schemaQ += ";";

    auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
    UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 10; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(i)
            .AddMember("c").Int64(i)
            .EndStruct();
    }
    rows.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    std::vector<std::string> results = {
        R"([[1;1]])",
    };

    std::vector<std::string> queries = {
        R"(
            $subselect = (select a, b from `/Root/t1`);
            SELECT t1.a, t1.b FROM $subselect as t1 join $subselect as t2 on t1.a = t2.a WHERE t1.a == 1 and t1.b == 1 order by t1.a;
        )",
    };

    auto queryClient = kikimr.GetQueryClient();
    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto &query = queries[i];
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        auto result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                     .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
    }
}

// Creates `table` and runs `check` while it is empty and again after
// `addRows`, with and without the physical-stage peephole and block hash
// operators; `pragmas` selects the settings.
void RunReplicateAggregationQueries(bool columnTables, const TString& table, const TString& columns,
    const std::function<void(NYdb::TValueBuilder&)>& addRows,
    const std::function<void(NYdb::NQuery::TSession&, const TString& pragmas, bool populated, bool peephole)>& check)
{
    if (columnTables) {
        // Column-shard schema caches are process-wide and keyed by path ID,
        // which every new runner reuses; TTestHelper drops them likewise.
        NOlap::TSchemaCachesManager::DropCaches();
    }
    NKikimrConfig::TAppConfig appConfig;
    auto* config = appConfig.MutableTableServiceConfig();
    config->SetEnableNewRBO(true);
    config->SetEnableFallbackToYqlOptimizer(false);
    config->SetAllowOlapDataQuery(true);
    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto dbSession = db.CreateSession().GetValueSync().GetSession();
    const auto created = dbSession.ExecuteSchemeQuery(TStringBuilder() << "CREATE TABLE `" << table << "` ("
        << columns << ")" << (columnTables ? " WITH (STORE = column);" : ";")).GetValueSync();
    UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());

    auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
    for (const bool populated : {false, true}) {
        if (populated) {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            addRows(rows);
            const auto inserted = db.BulkUpsert(table, rows.EndList().Build()).GetValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
        }
        for (const bool peephole : {false, true}) {
            for (const bool blocks : {false, true}) {
                const TString pragmas = TStringBuilder() << SqlInPeepholePragma(peephole)
                    << "PRAGMA ydb.DqHashOperatorsUseBlocks = \"" << (blocks ? "true" : "false") << "\";\n";
                check(session, pragmas, populated, peephole);
            }
        }
    }
}

void TestReplicateBranchPushdown(bool columnTables) {
    struct TCase {
        TString Query;
        TString Expected;
        TString EmptyExpected = "[]";
    };
    const TVector<TCase> cases{
        {R"(
            SELECT k, SUM(b) AS v FROM $input GROUP BY k
            UNION ALL
            SELECT k, b AS v FROM $input
            ORDER BY k, v;
        )", R"([[0;20];[0;40];[0;60];[0;120];[1;10];[1;30];[1;50];[1;90]])"},
        {R"(
            $grouped = SELECT k, SUM(b) AS v FROM $input WHERE b >= 30 GROUP BY k;
            $scalar = SELECT SUM(b) AS total, COUNT(*) AS n FROM $input;
            SELECT g.k, g.v, s.total, s.n FROM $grouped AS g CROSS JOIN $scalar AS s ORDER BY g.k;
        )", R"([[0;100;[210];6u];[1;80;[210];6u]])"},
        {R"(
            $unique = SELECT DISTINCT k FROM $input;
            SELECT t.k, t.b FROM $input AS t INNER JOIN $unique AS u ON t.k = u.k ORDER BY t.k, t.b;
        )", R"([[0;20];[0;40];[0;60];[1;10];[1;30];[1;50]])"},
        {R"(
            SELECT COUNT(*) AS n FROM $input WHERE b >= 30
            UNION ALL SELECT COUNT(*) AS n FROM $input ORDER BY n;
        )", R"([[4u];[6u]])", R"([[0u];[0u]])"},
        {R"(
            $filtered = SELECT k, b + 1 AS b FROM $input WHERE b >= 30;
            $counts = SELECT k, COUNT(*) AS n FROM $filtered GROUP BY k;
            SELECT f.k AS k, f.b AS b, c.n AS n FROM $filtered AS f INNER JOIN $counts AS c ON f.k = c.k
            UNION ALL
            SELECT k, b, CAST(0 AS Uint64) AS n FROM $input WHERE b < 30
            ORDER BY k, b, n;
        )", R"([[0;20;[0u]];[0;41;[2u]];[0;61;[2u]];[1;10;[0u]];[1;31;[2u]];[1;51;[2u]]])"},
    };
    RunReplicateAggregationQueries(columnTables, "/Root/replicate", "a Int64 NOT NULL, b Int64 NOT NULL, PRIMARY KEY(a)",
        [](NYdb::TValueBuilder& rows) {
            for (i64 a = 1; a <= 6; ++a) {
                rows.AddListItem().BeginStruct().AddMember("a").Int64(a).AddMember("b").Int64(a * 10).EndStruct();
            }
        },
        [&](NYdb::NQuery::TSession& session, const TString& pragmas, bool populated, bool peephole) {
            for (const auto& testCase : cases) {
                const TString query = TStringBuilder() << pragmas
                    << "$input = SELECT COALESCE(a % 2, 0) AS k, b FROM `/Root/replicate`;\n" << testCase.Query;
                const auto explained = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
                UNIT_ASSERT_C(explained.IsSuccess(), explained.GetIssues().ToString() << "\n" << query);
                const auto ast = *explained.GetStats()->GetAst();
                UNIT_ASSERT_C(peephole || ast.find("Switch") != std::string::npos, ast);
                // Every case aggregates a Replicate port directly, so some
                // partial must run inside the shared stage's Switch branch.
                const TString plan{*explained.GetStats()->GetPlan()};
                UNIT_ASSERT_C(CountPartialAggregatesInInputStages(GetSimplifiedPlan(plan), plan) > 0, plan);
                const auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString() << "\n" << query);
                CompareYson(populated ? testCase.Expected : testCase.EmptyExpected,
                    FormatResultSetYson(result.GetResultSet(0)));
            }
        });
}

void TestMixedDistinctAggregationBeforeHashShuffle(bool columnTables) {
    RunReplicateAggregationQueries(columnTables, "/Root/partsupp",
        "ps_partkey Int64 NOT NULL, ps_suppkey Int64 NOT NULL, PRIMARY KEY(ps_partkey, ps_suppkey)",
        [](NYdb::TValueBuilder& rows) {
            for (i64 part = 1; part <= 6; ++part) {
                rows.AddListItem().BeginStruct()
                    .AddMember("ps_partkey").Int64(part)
                    .AddMember("ps_suppkey").Int64((part + 1) / 2)
                    .EndStruct();
            }
        },
        [](NYdb::NQuery::TSession& session, const TString& pragmas, bool populated, bool) {
            // https://github.com/ydb-platform/ydb/issues/53613
            const TString query = pragmas + "SELECT COUNT(DISTINCT ps_suppkey), COUNT(*) FROM partsupp;";
            const auto plan = ExecuteExplain(session, query);
            const auto simplifiedPlan = GetSimplifiedPlan(plan);
            const auto* shuffle = FindConnectionNode(simplifiedPlan, "HashShuffle");
            UNIT_ASSERT_C(shuffle, plan);
            // A partial DISTINCT must reduce this branch before rows
            // leave the shared scan stage through the hash shuffle.
            const auto* partial = FindOperatorByStringField(*shuffle, "Phase", "Intermediate");
            UNIT_ASSERT_C(partial, "Partial DISTINCT must precede HashShuffle:\n" << plan);
            const auto& groupBy = (*partial)["GroupBy"].GetArraySafe();
            UNIT_ASSERT_C(std::any_of(groupBy.begin(), groupBy.end(), [](const auto& column) {
                return column.GetStringSafe().Contains("ps_suppkey");
            }), plan);
            UNIT_ASSERT_C(FindOperatorByStringField(*shuffle, "Name", "TableFullScan"), plan);

            const auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString() << "\n" << query);
            // The COUNT(*) branch must keep duplicates and emit zero
            // on empty input, independently of DISTINCT's row reduction.
            CompareYson(populated ? "[[3u;6u]]" : "[[0u;0u]]", FormatResultSetYson(result.GetResultSet(0)));
        });
}

void TestBlockHashCombine(bool columnTables) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetDqHashOperatorsUseBlocks(true);

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto dbSession = db.CreateSession().GetValueSync().GetSession();

    TString schemaQ = R"(
        CREATE TABLE `/Root/t1` (
            a Int64 NOT NULL,
            b Int64,
            primary key(a)
        )
    )";
    if (columnTables) {
        schemaQ += R"(WITH (STORE = column))";
    }
    schemaQ += ";";

    auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
    UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (i64 i = 1; i <= 3; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").Int64(10)
            .EndStruct();
    }
    rows.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    const std::vector<std::string> queries = {
        R"(
            select sum(t1.a), t1.b from `/Root/t1` as t1 group by t1.b;
        )",
        R"(
            select min(t1.a), max(t1.a), count(t1.a), t1.b from `/Root/t1` as t1 group by t1.b;
        )",
        R"(
            select sum(t1.a), max(t1.b) from `/Root/t1` as t1;
        )",
    };

    const std::vector<std::string> results = {
        R"([[6;[10]]])",
        R"([[1;3;3u;[10]]])",
        R"([[[6];[10]]])",
    };

    auto queryClient = kikimr.GetQueryClient();
    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto& query = queries[i];
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        auto result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto ast = *result.GetStats()->GetAst();
        UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "DqPhyHashCombine"), 2, ast);
        // Row tables convert to blocks around each combine, column tables stay in blocks
        // from the block read up to the result stage.
        UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "WideToBlocks"), columnTables ? 0 : 2, ast);
        UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "WideFromBlocks"), columnTables ? 1 : 2, ast);

        result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
    }
}

void CollectHashShuffleFuncs(const NJson::TJsonValue& planNode, TVector<TString>& hashFuncs) {
    if (!planNode.IsMap()) {
        return;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (auto nodeType = planMap.find("Node Type");
            nodeType != planMap.end() && nodeType->second.GetStringSafe() == "HashShuffle") {
        hashFuncs.push_back(planMap.at("HashFunc").GetStringSafe());
    }

    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (const auto& child : plans->second.GetArraySafe()) {
            CollectHashShuffleFuncs(child, hashFuncs);
        }
    }
}

TVector<TString> CollectHashShuffleFuncs(const TString& plan) {
    NJson::TJsonValue planRoot;
    NJson::ReadJsonTree(plan, &planRoot, true);

    TVector<TString> hashFuncs;
    CollectHashShuffleFuncs(planRoot.GetMapSafe().at("SimplifiedPlan"), hashFuncs);
    return hashFuncs;
}

void CollectHashShuffleDescriptions(const NJson::TJsonValue& planNode, TVector<TString>& hashShuffles) {
    if (!planNode.IsMap()) {
        return;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (auto nodeType = planMap.find("Node Type");
            nodeType != planMap.end() && nodeType->second.GetStringSafe() == "HashShuffle") {
        TVector<TString> keyColumns;
        for (const auto& key : planMap.at("KeyColumns").GetArraySafe()) {
            keyColumns.push_back(key.GetStringSafe());
        }

        hashShuffles.push_back(TStringBuilder()
            << planMap.at("HashFunc").GetStringSafe()
            << "(" << JoinSeq(", ", keyColumns) << ")");
    }

    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (const auto& child : plans->second.GetArraySafe()) {
            CollectHashShuffleDescriptions(child, hashShuffles);
        }
    }
}

TVector<TString> CollectHashShuffleDescriptions(const TString& plan) {
    NJson::TJsonValue planRoot;
    NJson::ReadJsonTree(plan, &planRoot, true);

    TVector<TString> hashShuffles;
    CollectHashShuffleDescriptions(planRoot.GetMapSafe().at("SimplifiedPlan"), hashShuffles);
    return hashShuffles;
}

TVector<TString> SortDescriptions(TVector<TString> descriptions) {
    std::sort(descriptions.begin(), descriptions.end());
    return descriptions;
}

bool HasPhysicalHashShuffleWithHashFunc(const TString& ast, const TString& hashFunc) {
    size_t shufflePos = ast.find("DqCnHashShuffle");
    while (shufflePos != TString::npos) {
        const size_t nextShufflePos = ast.find("DqCnHashShuffle", shufflePos + 1);
        const size_t hashFuncPos = ast.find(hashFunc, shufflePos);
        if (hashFuncPos != TString::npos && (nextShufflePos == TString::npos || hashFuncPos < nextShufflePos)) {
            return true;
        }
        shufflePos = nextShufflePos;
    }

    return false;
}

bool IsHashShufflePlanNode(const NJson::TJsonValue& planNode) {
    if (!planNode.IsMap()) {
        return false;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (auto nodeType = planMap.find("Node Type"); nodeType != planMap.end()) {
        return nodeType->second.GetStringSafe() == "HashShuffle";
    }

    return false;
}

bool IsJoinPlanNode(const NJson::TJsonValue& planNode) {
    if (!planNode.IsMap()) {
        return false;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
        for (const auto& opNode : operators->second.GetArraySafe()) {
            const auto& op = opNode.GetMapSafe();
            if (op.contains("JoinKind")) {
                return true;
            }
        }
    }

    return false;
}

ui32 CountJoinPlanNodes(const NJson::TJsonValue& planNode) {
    if (!planNode.IsMap()) {
        return 0;
    }

    ui32 count = IsJoinPlanNode(planNode) ? 1 : 0;

    const auto& planMap = planNode.GetMapSafe();
    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (const auto& child : plans->second.GetArraySafe()) {
            count += CountJoinPlanNodes(child);
        }
    }

    return count;
}

ui32 CountJoinPlanNodes(const TString& plan) {
    NJson::TJsonValue planRoot;
    NJson::ReadJsonTree(plan, &planRoot, true);
    return CountJoinPlanNodes(planRoot.GetMapSafe().at("SimplifiedPlan"));
}

bool HasJoinWithBothInputsHashShuffled(const NJson::TJsonValue& planNode) {
    if (!planNode.IsMap()) {
        return false;
    }

    const auto& planMap = planNode.GetMapSafe();
    if (IsJoinPlanNode(planNode)) {
        if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
            const auto& children = plans->second.GetArraySafe();
            if (children.size() == 2 && IsHashShufflePlanNode(children[0]) && IsHashShufflePlanNode(children[1])) {
                return true;
            }
        }
    }

    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (const auto& child : plans->second.GetArraySafe()) {
            if (HasJoinWithBothInputsHashShuffled(child)) {
                return true;
            }
        }
    }

    return false;
}

bool HasJoinWithBothInputsHashShuffled(const TString& plan) {
    NJson::TJsonValue planRoot;
    NJson::ReadJsonTree(plan, &planRoot, true);
    return HasJoinWithBothInputsHashShuffled(planRoot.GetMapSafe().at("SimplifiedPlan"));
}

NKikimrKqp::TKqpSetting MakeHashCompatibilityStatsSetting(const TVector<TString>& tables) {
    TStringBuilder stats;
    stats << "{";
    for (size_t i = 0; i < tables.size(); ++i) {
        if (i) {
            stats << ",";
        }
        stats << "\"/Root/" << tables[i] << "\": {\"n_rows\": 1000000, \"byte_size\": 16000000}";
    }
    stats << "}";

    NKikimrKqp::TKqpSetting statsSetting;
    statsSetting.SetName("OptOverrideStatistics");
    statsSetting.SetValue(stats);
    return statsSetting;
}

void CreateHashCompatibilityTables(TSession& tableSession, const TVector<TString>& tables) {
    for (const auto& table : tables) {
        auto result = tableSession.ExecuteSchemeQuery(Sprintf(R"(
            CREATE TABLE `/Root/%s` (
                id Int32 NOT NULL,
                k Int32,
                payload Int32,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH (STORE = COLUMN, PARTITION_COUNT = 4);
        )", table.c_str())).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
}

TPreparedQueryHolder::TConstPtr CompilePreparedQuery(TKikimrRunner& kikimr, const TString& query) {
    // The KQP proxy registers the compile service during bootstrap; session creation is the readiness barrier.
    const auto sessionResult = kikimr.GetQueryClient().GetSession().GetValueSync();
    UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());

    const ui32 nodeIdx = 0;
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    const auto edgeActor = runtime.AllocateEdgeActor(nodeIdx);

    TKqpQuerySettings querySettings(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
    TKqpQueryId queryId(
        TString(DefaultKikimrPublicClusterName),
        "/Root",
        /*databaseId*/ "",
        /*userSid*/ "root@builtin",
        query,
        querySettings,
        /*paramTypes*/ nullptr,
        TGUCSettings{});

    TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken("root@builtin", {});
    runtime.Send(new IEventHandle(
        MakeKqpCompileServiceID(runtime.GetNodeId(nodeIdx)),
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
            std::make_shared<TGUCSettings>(),
            /*applicationName*/ Nothing(),
            std::make_shared<std::atomic<bool>>(true),
            MakeIntrusive<TUserRequestContext>("shuffle-elimination-ut", "/Root", "compile-session"))));

    const auto response = runtime.GrabEdgeEvent<TEvKqp::TEvCompileResponse>(edgeActor, TDuration::Seconds(30));
    UNIT_ASSERT_C(response, "Compile request timed out");
    UNIT_ASSERT_C(response->Get()->CompileResult, "Compile result is missing");
    UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS,
        response->Get()->CompileResult->Issues.ToString());
    UNIT_ASSERT_C(response->Get()->CompileResult->PreparedQuery, "Prepared query is missing");
    return response->Get()->CompileResult->PreparedQuery;
}

std::pair<TString, TString> ExplainHashCompatibilityQueryWithAst(const TVector<TString>& tables, const TString& query, bool blockChannelsAuto = false) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
    appConfig.MutableTableServiceConfig()->SetDefaultHashShuffleFuncType(
        NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2);
    if (blockChannelsAuto) {
        appConfig.MutableTableServiceConfig()->SetBlockChannelsMode(
            NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO);
    }

    auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
    settings.SetKqpSettings({MakeHashCompatibilityStatsSetting(tables)});
    TKikimrRunner kikimr(settings);

    auto tableClient = kikimr.GetTableClient();
    auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
    CreateHashCompatibilityTables(tableSession, tables);

    auto queryClient = kikimr.GetQueryClient();
    auto querySession = queryClient.GetSession().GetValueSync().GetSession();
    auto result = querySession.ExecuteQuery(query,
        NYdb::NQuery::TTxControl::NoTx(),
        NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
    ).ExtractValueSync();

    result.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    return {TString{*result.GetStats()->GetPlan()}, TString{*result.GetStats()->GetAst()}};
}

TString ExplainHashCompatibilityQuery(const TVector<TString>& tables, const TString& query) {
    return ExplainHashCompatibilityQueryWithAst(tables, query).first;
}

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYqlTests;

Y_UNIT_TEST_SUITE(KqpRboYql) {

    Y_UNIT_TEST_TWIN(MultiConsumer, ColumnStore) {
        TestMultiConsumer(ColumnStore);
    }

    Y_UNIT_TEST(MultiConsumerRowStorageSourceProducesDqPhyStage) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        auto schemaResult = dbSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/sales` (
                region_id Int64 NOT NULL,
                amount Int64 NOT NULL,
                primary key(region_id)
            );
            CREATE TABLE `/Root/quotas` (
                region_id Int64 NOT NULL,
                threshold Int64 NOT NULL,
                primary key(region_id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        // Insert test data.
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [regionId, amount] :
                 TVector<std::pair<i64, i64>>{{1, 100}, {2, 200}, {3, 300}, {4, 100}, {5, 500}}) {
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember("region_id").Int64(regionId)
                    .AddMember("amount").Int64(amount)
                    .EndStruct();
            }
            rows.EndList();
            auto resultUpsert = db.BulkUpsert("/Root/sales", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());
        }
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [regionId, threshold] :
                 TVector<std::pair<i64, i64>>{{3, 100}, {4, 500}, {10, 50}, {11, 150}}) {
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember("region_id").Int64(regionId)
                    .AddMember("threshold").Int64(threshold)
                    .EndStruct();
            }
            rows.EndList();
            auto resultUpsert = db.BulkUpsert("/Root/quotas", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());
        }

        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().GetValueSync().GetSession();

        // Find sales whose amount exceeds their region's quota. The equi key
        // (s.region_id = q.region_id) keeps the join keys non-empty, while the
        // residual comparison (s.amount > q.threshold) combined with LEFT-join
        // semantics forces the shared multi-consumer row-storage read.
        auto result = session.ExecuteQuery(
            R"(
                SELECT s.region_id, s.amount, q.threshold, q.region_id AS quota_region
                FROM `/Root/sales` AS s
                LEFT JOIN `/Root/quotas` AS q
                  ON s.region_id = q.region_id AND s.amount > q.threshold
                ORDER BY s.region_id;
            )",
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
        ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResultSetYson(result.GetResultSet(0)),
            R"([[1;100;#;#];[2;200;#;#];[3;300;[100];[3]];[4;100;#;#];[5;500;#;#]])");

       std::vector<std::pair<TString, TString>> residualQueries = {
            {R"(
                SELECT s.region_id, s.amount
                FROM `/Root/sales` AS s
                WHERE EXISTS (
                    SELECT 1 FROM `/Root/quotas` AS q
                    WHERE s.region_id = q.region_id AND s.amount > q.threshold
                )
                ORDER BY s.region_id;
             )",
             R"([[3;300]])"},
            {R"(
                SELECT s.region_id, s.amount
                FROM `/Root/sales` AS s
                WHERE NOT EXISTS (
                    SELECT 1 FROM `/Root/quotas` AS q
                    WHERE s.region_id = q.region_id AND s.amount > q.threshold
                )
                ORDER BY s.region_id;
             )",
             R"([[1;100];[2;200];[4;100];[5;500]])"},
            {R"(
                SELECT s.region_id, s.amount
                FROM `/Root/sales` AS s
                INNER JOIN `/Root/quotas` AS q
                  ON s.region_id = q.region_id AND s.amount > q.threshold
                ORDER BY s.region_id;
             )",
             R"([[3;300]])"},
        };

        for (const auto& [query, expected] : residualQueries) {
            result = session.ExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, query + ": " + result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, query);
        }
    }

    Y_UNIT_TEST(DistinctShuffleEliminationPreservesRenamedKeys) {
        NTests::TIdTestContext f;
        const auto id = f.Id(), k = f.Id(), intermediateId = f.Id(), intermediateK = f.Id();
        struct TCase {
            bool Enabled;
            TOrderedIUs<> ShuffledBy, Keys, Expected;
        };
        for (const auto& testCase : TVector<TCase>{
                {true, {id}, {id}, {intermediateId}},
                {true, {id}, {id, k}, {intermediateId}},
                {true, {k, id}, {id, k}, {intermediateK, intermediateId}},
                {true, {id, k}, {id}, {}},
                {true, {}, {id}, {}},
                {false, {id}, {id}, {}},
                {false, {k, id}, {id, k}, {}}}) {
            f.Props.ColumnLineage.Clear();
            f.Config->OptShuffleElimination = testCase.Enabled;
            auto read = f.Read({id, k});
            read->Props.Metadata.emplace();
            read->Props.Metadata->ShuffledByColumns = testCase.ShuffledBy;
            TAggregationIUs traits, finalTraits;
            TOrderedIUs<> finalKeys;
            for (const auto key : testCase.Keys.Items()) {
                const auto intermediate = key == id ? intermediateId : intermediateK;
                traits.Add(intermediate, TOpAggregationTraits{key, "distinct"});
                finalTraits.Add(key, TOpAggregationTraits{intermediate, "distinct"});
                finalKeys.Append(intermediate);
            }
            auto aggregate = MakeIntrusive<TOpAggregate>(std::move(read), std::move(traits), testCase.Keys,
                EOpPhase::Intermediate, true, f.Pos);
            aggregate->ComputeMetadata(f.RboCtx, f.Props);
            UNIT_ASSERT(aggregate->Props.Metadata->ShuffledByColumns == testCase.Expected);
            auto finalAggregate = MakeIntrusive<TOpAggregate>(std::move(aggregate), std::move(finalTraits),
                std::move(finalKeys), EOpPhase::Final, true, f.Pos);
            finalAggregate->ComputeMetadata(f.RboCtx, f.Props);
            const auto expectedFinal = testCase.Expected.Items().empty() ? testCase.Keys : testCase.ShuffledBy;
            UNIT_ASSERT(finalAggregate->Props.Metadata->ShuffledByColumns == expectedFinal);
        }
    }

    Y_UNIT_TEST(AggregateShuffleEliminationTracksCreatedPartitioning) {
        NTests::TIdTestContext f;
        const auto id = f.Id(), k = f.Id(), payload = f.Id(), result = f.Id();
        struct TCase {
            bool Enabled;
            EOpPhase Phase;
            TOrderedIUs<> ShuffledBy, Keys, Expected;
            bool EliminateShuffle;
        };
        for (const auto& testCase : TVector<TCase>{
                {true, EOpPhase::Undefined, {payload}, {k, id}, {k, id}, false},
                {true, EOpPhase::Final, {}, {k, id}, {k, id}, false},
                {true, EOpPhase::Intermediate, {payload}, {k, id}, {}, false},
                {true, EOpPhase::Undefined, {k}, {id, k}, {k}, true},
                {true, EOpPhase::Final, {k}, {id, k}, {k}, true},
                {true, EOpPhase::Intermediate, {k}, {id, k}, {k}, true},
                {true, EOpPhase::Final, {k}, {}, {}, false},
                {false, EOpPhase::Undefined, {}, {id}, {id}, false},
                {false, EOpPhase::Final, {k, id}, {id, k}, {id, k}, false}}) {
            f.Props.ColumnLineage.Clear();
            f.Config->OptShuffleElimination = testCase.Enabled;
            auto read = f.Read({id, k, payload});
            read->Props.Metadata.emplace();
            read->Props.Metadata->ShuffledByColumns = testCase.ShuffledBy;
            TAggregationIUs traits;
            traits.Add(result, TOpAggregationTraits{payload, "sum"});
            auto aggregate = MakeIntrusive<TOpAggregate>(std::move(read), std::move(traits), testCase.Keys,
                testCase.Phase, false, f.Pos);
            aggregate->ComputeMetadata(f.RboCtx, f.Props);
            UNIT_ASSERT(aggregate->Props.Metadata->ShuffledByColumns == testCase.Expected);
            // Advertising the result must not suppress the aggregation's own exchange.
            UNIT_ASSERT_VALUES_EQUAL(CanEliminateAggregateShuffle(*aggregate, f.RboCtx), testCase.EliminateShuffle);
        }
    }

    Y_UNIT_TEST(DistinctShuffleEliminationMatchesCreatedKeyOrder) {
        NTests::TIdTestContext f;
        f.Config->OptShuffleElimination = true;
        const auto id = f.Id(), k = f.Id(), outputK = f.Id(), outputId = f.Id();
        auto read = f.Read({id, k});
        read->StorageType = NYql::EStorageType::ColumnStorage;
        read->Props.Metadata.emplace();
        TAggregationIUs traits;
        traits.Add(outputK, TOpAggregationTraits{k, "distinct"});
        traits.Add(outputId, TOpAggregationTraits{id, "distinct"});
        auto aggregate = MakeIntrusive<TOpAggregate>(std::move(read), std::move(traits), TOrderedIUs<>{id, k},
            EOpPhase::Undefined, true, f.Pos);
        aggregate->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT(aggregate->Props.Metadata->ShuffledByColumns == (TOrderedIUs<>{outputK, outputId}));
        auto* op = aggregate.get();
        auto root = f.Root(std::move(aggregate), {{outputK, "k"}, {outputId, "id"}});
        TAssignStagesStage().RunStage(*root, f.RboCtx);
        const auto sourceStage = *op->GetInput()->Props.StageId;
        const auto aggregateStage = *op->Props.StageId;
        const auto& connections = root->PlanProps.StageGraph.EdgesBetween(root->PlanProps.StageGraph.GetStage(sourceStage), root->PlanProps.StageGraph.GetStage(aggregateStage));
        UNIT_ASSERT_VALUES_EQUAL(connections.size(), 1);
        UNIT_ASSERT(IsConnection<TShuffleConnection>(connections.front()->Connection()));
        UNIT_ASSERT(static_cast<const TShuffleConnection&>(connections.front()->Connection()).Keys == (TOrderedIUs<>{k, id}));

        TPropagateAggregateThroughStageRule rule;
        UNIT_ASSERT(rule.MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
        auto& final = CastOperator<TOpAggregate>(*root->GetInput());
        root->ComputeParents();
        UNIT_ASSERT(rule.MatchAndApply(final.MutableChild(0), f.RboCtx, root->PlanProps));
        const auto& partial = CastOperator<TOpAggregate>(*final.GetInput());
        const auto* shuffled = root->PlanProps.StageGraph.EdgesBetween(root->PlanProps.StageGraph.GetStage(sourceStage), root->PlanProps.StageGraph.GetStage(aggregateStage)).front();
        UNIT_ASSERT(IsConnection<TShuffleConnection>(shuffled->Connection()));
        UNIT_ASSERT(static_cast<const TShuffleConnection&>(shuffled->Connection()).Keys == final.GetKeyColumns());
        UNIT_ASSERT(partial.GetAggregationTraits().At(final.GetKeyColumns().Items()[0]).Input == k);
        UNIT_ASSERT(partial.GetAggregationTraits().At(final.GetKeyColumns().Items()[1]).Input == id);
    }

    Y_UNIT_TEST(DuplicateStageConnectionsKeepAllRequirementsLive) {
        NTests::TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), c = f.Id(), output = f.Id();
        auto hub = TReplicate::Create(f.Read({a, b, c}), f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        auto* leftPtr = left.get();
        auto* rightPtr = right.get();
        const auto rightA = *right->GetRebindings().Find(a), rightC = *right->GetRebindings().Find(c);
        TUnionAllIUs columns(TUnionInputPolicy{2});
        columns.Add(output, TUnionInputRow{{a, rightA}});
        auto merge = MakeIntrusive<TOpUnionAll>(std::move(left), std::move(right), f.Pos, std::move(columns));
        auto* mergePtr = merge.get();
        auto root = f.Root(std::move(merge), {{output, "a"}});
        auto& graph = root->PlanProps.StageGraph;
        const auto& producerStage = graph.AddStage();
        const auto& unionStage = graph.AddStage();
        hub->GetInput()->Props.StageId = producerStage.Id();
        leftPtr->Props.StageId = rightPtr->Props.StageId = producerStage.Id();
        mergePtr->Props.StageId = unionStage.Id();
        root->PlanProps.BindStageInput(*mergePtr, 0, graph.Connect(producerStage, unionStage, MakeIntrusive<TMergeConnection>(TSortIUs{{b, {true, true}}}, 0)));
        root->PlanProps.BindStageInput(*mergePtr, 1, graph.Connect(producerStage, unionStage, MakeIntrusive<TShuffleConnection>(TOrderedIUs<>{rightC}, 1)));
        ComputePlanLiveness(*root);
        UNIT_ASSERT(GetLiveOut(leftPtr) == (TUnorderedIUs{a, b}));
        UNIT_ASSERT(GetLiveOut(rightPtr) == (TUnorderedIUs{rightA, rightC}));
        UNIT_ASSERT(GetLiveOut(hub->GetInput().Get()) == (TUnorderedIUs{a, b, c}));
    }

    Y_UNIT_TEST(AggregateShuffleEliminationUsesAndPreservesMapConnection) {
        struct TCase {
            bool ShuffleEliminationEnabled;
            bool AggregateShuffleEliminationEnabled;
            bool TwoShuffleKeys;
            bool EliminateShuffle;
        };
        for (const auto& testCase : TVector<TCase>{{false, true, false, false},
                {true, false, false, true}, {true, true, true, false}}) {
            NTests::TIdTestContext f;
            f.Config->OptShuffleElimination = testCase.ShuffleEliminationEnabled;
            f.Config->OptShuffleEliminationForAggregation = testCase.AggregateShuffleEliminationEnabled;
            const auto id = f.Id(), k = f.Id(), payload = f.Id(), result = f.Id();
            auto read = f.Read({id, k, payload});
            const auto inputStage = f.Props.StageGraph.AddSourceStage(NYql::EStorageType::ColumnStorage).Id();
            read->Props.StageId = inputStage;
            read->StorageType = NYql::EStorageType::ColumnStorage;
            read->Props.Metadata.emplace();
            read->Props.Metadata->ShuffledByColumns = testCase.TwoShuffleKeys ? TOrderedIUs<>{id, k} : TOrderedIUs<>{id};
            TAggregationIUs traits;
            traits.Add(result, TOpAggregationTraits{payload, "sum"});
            auto aggregate = MakeIntrusive<TOpAggregate>(std::move(read), std::move(traits),
                testCase.TwoShuffleKeys ? TOrderedIUs<>{id} : TOrderedIUs<>{id, k}, EOpPhase::Intermediate, false, f.Pos);
            aggregate->ComputeMetadata(f.RboCtx, f.Props);
            auto* agg = aggregate.get();
            auto root = f.Root(std::move(aggregate), {{result, "sum"}});
            TAssignStagesStage().RunStage(*root, f.RboCtx);
            const auto aggregateStage = *agg->Props.StageId;
            const auto& connections = root->PlanProps.StageGraph.EdgesBetween(root->PlanProps.StageGraph.GetStage(inputStage), root->PlanProps.StageGraph.GetStage(aggregateStage));
            UNIT_ASSERT_VALUES_EQUAL(connections.size(), 1);
            if (testCase.EliminateShuffle) {
                UNIT_ASSERT(agg->Props.Metadata->ShuffledByColumns == TOrderedIUs<>{id});
                UNIT_ASSERT(IsConnection<TMapConnection>(connections.front()->Connection()));
                UNIT_ASSERT(TPropagateAggregateThroughStageRule().MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
                UNIT_ASSERT_VALUES_EQUAL(*root->GetInput()->Props.StageId, inputStage);
                const auto& after = root->PlanProps.StageGraph.EdgesBetween(root->PlanProps.StageGraph.GetStage(inputStage), root->PlanProps.StageGraph.GetStage(aggregateStage));
                UNIT_ASSERT_VALUES_EQUAL(after.size(), 1);
                UNIT_ASSERT(IsConnection<TMapConnection>(after.front()->Connection()));
            } else {
                UNIT_ASSERT(agg->Props.Metadata->ShuffledByColumns.Items().empty());
                UNIT_ASSERT(IsConnection<TShuffleConnection>(connections.front()->Connection()));
            }
        }
    }

    Y_UNIT_TEST(PhysicalSemiJoinUsesPerEdgeLiveIn) {
        NTests::TIdTestContext f;
        const auto a = f.Id("a"), b = f.Id("b");
        auto read = f.Read({a, b});
        f.SetType(*read);
        auto hub = TReplicate::Create(std::move(read), f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        const auto rightA = *right->GetRebindings().Find(a);
        f.SetType(*left);
        f.SetType(*right);
        auto join = MakeIntrusive<TOpJoin>(std::move(left), std::move(right), f.Pos, "LeftSemi", TPairedIUs{{a, rightA}});
        auto* joined = join.get();
        join->Props.JoinAlgo = EJoinAlgoType::GraceJoin;
        auto root = f.Root(std::move(join), {{a, "a"}, {b, "b"}});
        ComputePlanLiveness(*root);
        UNIT_ASSERT(GetLiveOut(hub->GetInput().Get()) == (TUnorderedIUs{a, b}));
        UNIT_ASSERT(GetLiveIn(joined, 0) == (TUnorderedIUs{a, b}));
        UNIT_ASSERT(GetLiveIn(joined, 1) == TUnorderedIUs{rightA});
        const TPhysicalNames names(root->PlanProps.InfoUnitRegistry);
        auto build = [&](bool blocks) {
            return TPhysicalJoinBuilder(*joined, f.ExprCtx, f.Pos, names).BuildPhysicalOp(
                f.ExprCtx.NewArgument(f.Pos, "left"), f.ExprCtx.NewArgument(f.Pos, "right"), blocks, f.TypeCtx).AsNarrow(f.ExprCtx);
        };
        auto physical = build(false);
        TExprNode::TListType cores;
        CollectCallableNodes(physical, "GraceJoinCore", cores);
        UNIT_ASSERT_VALUES_EQUAL(cores.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cores.front()->Child(6)->ChildrenSize(), 0);
        physical = build(true);
        cores.clear();
        CollectCallableNodes(physical, "BlockHashJoinCore", cores);
        UNIT_ASSERT_VALUES_EQUAL(cores.size(), 1);
        const auto blocks = cores.front()->ChildPtr(1);
        UNIT_ASSERT(blocks->IsCallable("WideToBlocks"));
        const auto flow = blocks->HeadPtr();
        UNIT_ASSERT(flow->IsCallable("FromFlow"));
        const auto expand = flow->HeadPtr();
        UNIT_ASSERT(expand->IsCallable("ExpandMap"));
        const auto lambda = expand->TailPtr();
        UNIT_ASSERT(lambda->IsLambda());
        UNIT_ASSERT_VALUES_EQUAL(lambda->ChildrenSize(), 2);
        UNIT_ASSERT(lambda->Tail().IsCallable("Member"));
        UNIT_ASSERT_VALUES_EQUAL(lambda->Tail().Tail().Content(), names.Get(rightA));
    }

    Y_UNIT_TEST(PhysicalAggregationDoesNotEmitDeadKeyColumns) {
        NTests::TIdTestContext f;
        const auto key = f.Id("key"), value = f.Id("value"), sum = f.Id("sum_value");
        auto read = f.Read({key, value});
        f.SetType(*read);
        TAggregationIUs traits;
        traits.Add(sum, TOpAggregationTraits{value, "sum"});
        auto aggregate = MakeIntrusive<TOpAggregate>(std::move(read), std::move(traits),
            TOrderedIUs<>{key}, EOpPhase::Final, false, f.Pos);
        f.SetType(*aggregate);
        auto* agg = aggregate.get();
        auto root = f.Root(std::move(aggregate), {{sum, "sum_value"}});
        ComputePlanLiveness(*root);
        UNIT_ASSERT(GetLiveOut(agg) == TUnorderedIUs{sum});
        const TPhysicalNames names(root->PlanProps.InfoUnitRegistry);
        auto physical = TPhysicalAggregationBuilder(*agg, f.ExprCtx, f.Pos, names, true)
            .BuildPhysicalOp(NPhysicalConvertionUtils::TStageBody::Narrow(f.ExprCtx.NewArgument(f.Pos, "input")), std::nullopt)
            .AsNarrow(f.ExprCtx);
        TExprNode::TListType narrowMaps;
        CollectCallableNodes(physical, "NarrowMap", narrowMaps);
        UNIT_ASSERT_VALUES_EQUAL(narrowMaps.size(), 1);
        const auto body = TCoLambda(narrowMaps.front()->ChildPtr(1)).Body().Ptr();
        UNIT_ASSERT(body->IsCallable("AsStruct"));
        UNIT_ASSERT_VALUES_EQUAL(body->ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(body->Head().Head().Content(), names.Get(sum));
    }

    Y_UNIT_TEST(PropagateLimitThroughStages) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
	            b Int64,
                primary key(a)
            ) WITH (STORE = column);

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
            ) WITH (STORE = column);
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t2.a from `/Root/t1` as t1 join `/Root/t2` as t2 on t1.a = t2.a where t1.b = 10 limit 1;
            )",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            // Any from Take -> WideTakeBlocks is also ok.
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Take"), 2);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // Push limit to CS.
        queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a from `/Root/t1` as t1 limit 1;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a from `/Root/t1` as t1 where t1.b = 10 limit 1;
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Take"), 1);
            // Pushed to cs.
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "ItemsLimit"), 1);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a from `/Root/t1` as t1 where t1.b = 10 order by t1.a limit 1 + 1;
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "TopSort"), 1);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(PropagateTopSortThroughStages) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
	            b Int64 NOT NULL,
                primary key(a, b)
            ) WITH (STORE = column);

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
            ) WITH (STORE = column);
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t2.a from `/Root/t1` as t1 join `/Root/t2` as t2 on t1.a = t2.a where t1.b = 10 order by t1.a limit 1;
            )",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            // TopSort -> Take(TopSort())
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "TopSort"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Take"), 1);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // Just propagate through stages, cannot push to cs, because t1.b is not a key.
        queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 order by t1.a asc, t1.b desc limit 1;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.a asc, t1.b desc limit 1;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 order by t1.a asc limit 1 + 1;
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Take"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "TopSort"), 1);

            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                         .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // Push to CS.
        queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 order by t1.a asc limit 1;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.a desc limit 1;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.a asc, t1.b asc limit 1;
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Take"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "ItemsLimit"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "TopSort"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "Sorted"), 1);

            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                         .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        queries = {
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 order by t1.a asc;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.a desc;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.b desc limit 1;
            )",
            R"(
                PRAGMA YqlSelect = "force";
                select t1.a, t1.b from `/Root/t1` as t1 where t1.b = 10 order by t1.a asc, t1.b desc limit 1;
            )",
        };

        queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            const TString explainQuery = TStringBuilder()
                << "PRAGMA ydb.EnableNewRBOPhysicalStagePeephole = \"true\";\n" << query;
            auto result =
                session.ExecuteQuery(explainQuery, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            // A full sort runs on blocks. A top-N stays narrow, because the block kernel reserves memory for its count up front.
            const bool isTopSort = query.find("limit") != std::string::npos;
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "SortBlocks"), isTopSort ? 0 : 1);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "TopSort"), isTopSort ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "DqCnMerge"), 1);

            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                         .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(LimitWithOffset) {
        // Column-shard schema caches are process-wide and keyed by path ID, which every new runner reuses,
        // and earlier tests have these columns in a different primary key.
        NOlap::TSchemaCachesManager::DropCaches();

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        const auto schemaResult = dbSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/olap` (
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                primary key(a)
            ) PARTITION BY HASH(a) WITH (STORE = column, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);

            CREATE TABLE `/Root/row` (
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                primary key(a)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        for (const auto* table : {"/Root/olap", "/Root/row"}) {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (i64 i = 0; i < 10; ++i) {
                rows.AddListItem().BeginStruct().AddMember("a").Int64(i).AddMember("b").Int64(9 - i).EndStruct();
            }
            rows.EndList();
            const auto upsertResult = db.BulkUpsert(table, rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        const auto explain = [&](const TString& query) {
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            const auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString() << " Query: " << query);
            return TString(*result.GetStats()->GetAst());
        };

        const auto execute = [&](const TString& query, const NYdb::TParams& params) {
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString() << " Query: " << query);
            return result.GetResultSet(0);
        };

        const auto noParams = NYdb::TParamsBuilder().Build();
        const auto makeParams = [](ui64 limit, ui64 offset) {
            NYdb::TParamsBuilder params;
            params.AddParam("$limit").Uint64(limit).Build();
            params.AddParam("$offset").Uint64(offset).Build();
            return params.Build();
        };

        // A sort with an offset must stay a top-N. The returned rows check that it keeps the rows the offset skips.
        const auto assertTopSort = [](const TString& ast, const TString& query) {
            UNIT_ASSERT_C(ast.Contains("TopSort"), "Expected top-N. Query: " << query << " AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("WideSortBlocks") && !ast.Contains("(WideSort ") && !ast.Contains("(Sort "),
                "Unexpected full sort. Query: " << query << " AST: " << ast);
            UNIT_ASSERT_C(!ast.Contains("WideTopSortBlocks"), "Unexpected block top-N. Query: " << query << " AST: " << ast);
        };

        struct TSortedCase {
            TString Query;
            TString Expected;
            // The ItemsLimit of the read, empty if the read has none.
            TString ReadLimit;
        };

        const std::vector<TSortedCase> sortedCases = {
            // Primary key order is pushed to the read, with the offset included in its limit.
            {"select a from `/Root/olap` order by a limit 3 offset 2", "[[2];[3];[4]]", "5"},
            {"select a from `/Root/olap` order by a desc limit 3 offset 2", "[[7];[6];[5]]", "5"},
            {"select a from `/Root/olap` where b > 2 order by a limit 2 offset 1", "[[1];[2]]", "3"},
            {"select a from `/Root/olap` order by a limit 3 offset 0", "[[0];[1];[2]]", "3"},
            {"select a from `/Root/olap` order by a limit 3 offset 100", "[]", "103"},
            {"select a from `/Root/olap` order by a limit 0 offset 2", "[]", "2"},
            // Other columns cannot use the read, but must stay a top-N.
            {"select a from `/Root/olap` order by b limit 2 offset 1", "[[8];[7]]", ""},
            {"select a, a + 1 as c from `/Root/olap` order by a + 1 limit 2 offset 1", "[[1;2];[2;3]]", ""},
            {"select b, count(*) as c from `/Root/olap` group by b order by b limit 2 offset 1", "[[1;1u];[2;1u]]", ""},
            // Row tables.
            {"select a from `/Root/row` order by b limit 2 offset 1", "[[8];[7]]", ""},
            // Nested offsets.
            {"select a from (select a from `/Root/olap` order by a limit 5 offset 1) order by a desc limit 2 offset 1", "[[4];[3]]", "6"},
        };

        for (const auto& testCase : sortedCases) {
            const TString query = TStringBuilder() << "PRAGMA YqlSelect = \"force\"; " << testCase.Query << ";";
            const auto ast = explain(query);
            assertTopSort(ast, query);
            if (testCase.ReadLimit.empty()) {
                UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "ItemsLimit"), 0, query << " AST: " << ast);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "ItemsLimit"), 1, query << " AST: " << ast);
                UNIT_ASSERT_C(ast.Contains(TStringBuilder() << "(Uint64 '\"" << testCase.ReadLimit << "\")"), query << " AST: " << ast);
            }
            CompareYson(testCase.Expected, FormatResultSetYson(execute(query, noParams)));
        }

        // The row order of an unsorted query is not defined, so only check how many rows it returns.
        struct TUnsortedCase {
            TString Query;
            size_t Rows;
            TString ReadLimit;
        };

        const std::vector<TUnsortedCase> unsortedCases = {
            {"select a from `/Root/olap` limit 3 offset 2", 3, "5"},
            {"select a from `/Root/olap` where b > 2 limit 2 offset 1", 2, "3"},
            {"select a from `/Root/olap` limit 3 offset 8", 2, "11"},
            {"select a from `/Root/olap` limit 3 offset 100", 0, "103"},
            {"select a, a + 1 as c from `/Root/olap` limit 3 offset 2", 3, "5"},
            // An inner offset is not moved past by an outer limit.
            {"select * from (select a from `/Root/olap` limit 5 offset 1) limit 2", 2, "6"},
            {"select a from `/Root/row` limit 3 offset 2", 3, ""},
        };

        for (const auto& testCase : unsortedCases) {
            const TString query = TStringBuilder() << "PRAGMA YqlSelect = \"force\"; " << testCase.Query << ";";
            const auto ast = explain(query);
            UNIT_ASSERT_C(ast.Contains("Skip"), query << " AST: " << ast);
            if (testCase.ReadLimit.empty()) {
                UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "ItemsLimit"), 0, query << " AST: " << ast);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "ItemsLimit"), 1, query << " AST: " << ast);
                UNIT_ASSERT_C(ast.Contains(TStringBuilder() << "(Uint64 '\"" << testCase.ReadLimit << "\")"), query << " AST: " << ast);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(execute(query, noParams).RowsCount(), testCase.Rows, query);
        }

        const auto assertReadLimit = [](const TString& ast, const TString& query, ui32 expected) {
            UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "ItemsLimit"), expected, query << " AST: " << ast);
        };

        constexpr ui64 maxLimit = std::numeric_limits<ui64>::max();
        const TString pragma = "PRAGMA YqlSelect = \"force\"; ";
        const TString declare = pragma + "declare $limit as Uint64; declare $offset as Uint64; ";
        for (const TString table : {"/Root/olap", "/Root/row"}) {
            const bool isColumnTable = table == "/Root/olap";
            const auto sortedQuery = [&](const TString& order, const TString& limit, const TString& offset) {
                return TString(TStringBuilder() << "select a from `" << table << "` order by " << order << " limit " << limit
                    << (offset.empty() ? "" : " offset " + offset) << ";");
            };

            // A limit of any size works, including the way "all rows after the offset" is written, which must not overflow.
            for (const TString limit : {TString("1000000"), TString("1000000000000"), ToString(maxLimit)}) {
                // The maximum limit keeps every row, so it needs no top-N.
                const auto assertSort = [&](const TString& ast, const TString& query) {
                    if (limit == ToString(maxLimit)) {
                        UNIT_ASSERT_C(!ast.Contains("TopSort"), "Unexpected top-N. Query: " << query << " AST: " << ast);
                    } else {
                        assertTopSort(ast, query);
                    }
                };

                const TString keyOrder = pragma + sortedQuery("a", limit, "8");
                const auto keyOrderAst = explain(keyOrder);
                assertSort(keyOrderAst, keyOrder);
                assertReadLimit(keyOrderAst, keyOrder, 1);
                CompareYson("[[8];[9]]", FormatResultSetYson(execute(keyOrder, noParams)));

                const TString otherOrder = pragma + sortedQuery("b", limit, "8");
                const auto otherOrderAst = explain(otherOrder);
                assertSort(otherOrderAst, otherOrder);
                assertReadLimit(otherOrderAst, otherOrder, 0);
                CompareYson("[[1];[0]]", FormatResultSetYson(execute(otherOrder, noParams)));

                const TString noOffset = pragma + sortedQuery("a", limit, "");
                UNIT_ASSERT_VALUES_EQUAL_C(execute(noOffset, noParams).RowsCount(), 10, noOffset);

                const TString unsorted = TStringBuilder() << pragma << "select a from `" << table << "` limit " << limit << " offset 8;";
                UNIT_ASSERT_VALUES_EQUAL_C(execute(unsorted, noParams).RowsCount(), 2, unsorted);
            }

            // A limit that is not a literal: column shards get it computed, and the sort stays a top-N.
            const TString expression = pragma + sortedQuery("a", "1 + 2", "1 + 1");
            const auto expressionAst = explain(expression);
            assertTopSort(expressionAst, expression);
            assertReadLimit(expressionAst, expression, isColumnTable ? 1 : 0);
            CompareYson("[[2];[3];[4]]", FormatResultSetYson(execute(expression, noParams)));

            const TString sortedParams = declare + sortedQuery("a", "$limit", "$offset");
            const auto sortedParamsAst = explain(sortedParams);
            assertTopSort(sortedParamsAst, sortedParams);
            assertReadLimit(sortedParamsAst, sortedParams, isColumnTable ? 1 : 0);
            CompareYson("[[2];[3];[4]]", FormatResultSetYson(execute(sortedParams, makeParams(3, 2))));
            CompareYson("[[8];[9]]", FormatResultSetYson(execute(sortedParams, makeParams(maxLimit, 8))));
            CompareYson("[]", FormatResultSetYson(execute(sortedParams, makeParams(0, 2))));

            const TString otherOrderParams = declare + sortedQuery("b", "$limit", "$offset");
            assertTopSort(explain(otherOrderParams), otherOrderParams);
            CompareYson("[[8];[7]]", FormatResultSetYson(execute(otherOrderParams, makeParams(2, 1))));
            CompareYson("[[1];[0]]", FormatResultSetYson(execute(otherOrderParams, makeParams(maxLimit, 8))));

            const TString unsortedParams = TStringBuilder() << declare << "select a from `" << table << "` limit $limit offset $offset;";
            assertReadLimit(explain(unsortedParams), unsortedParams, isColumnTable ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL_C(execute(unsortedParams, makeParams(3, 2)).RowsCount(), 3, unsortedParams);
            UNIT_ASSERT_VALUES_EQUAL_C(execute(unsortedParams, makeParams(maxLimit, 8)).RowsCount(), 2, unsortedParams);

            // A limit parameter without an offset.
            const TString limitParam = TStringBuilder() << pragma << "declare $limit as Uint64; select a from `" << table << "` order by a limit $limit;";
            assertReadLimit(explain(limitParam), limitParam, isColumnTable ? 1 : 0);
            NYdb::TParamsBuilder limitOnly;
            limitOnly.AddParam("$limit").Uint64(2).Build();
            CompareYson("[[0];[1]]", FormatResultSetYson(execute(limitParam, limitOnly.Build())));
        }
    }

    // The block top-N kernel reserves memory for twice its count up front, so only the force mode may use it.
    Y_UNIT_TEST(TopSortKernelFollowsBlockChannelsMode) {
        // Column-shard schema caches are process-wide and keyed by path ID, which every new runner reuses,
        // and earlier tests have these columns in a different primary key.
        NOlap::TSchemaCachesManager::DropCaches();

        for (const auto mode : {NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_SCALAR,
                                NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO,
                                NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE}) {
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
            appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
            appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
            appConfig.MutableTableServiceConfig()->SetBlockChannelsMode(mode);

            TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
            auto db = kikimr.GetTableClient();
            auto dbSession = db.CreateSession().GetValueSync().GetSession();

            const auto schemaResult = dbSession.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/olap` (
                    a Int64 NOT NULL,
                    b Int64 NOT NULL,
                    primary key(a)
                ) PARTITION BY HASH(a) WITH (STORE = column, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
            )").GetValueSync();
            UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (i64 i = 0; i < 10; ++i) {
                rows.AddListItem().BeginStruct().AddMember("a").Int64(i).AddMember("b").Int64(9 - i).EndStruct();
            }
            rows.EndList();
            const auto upsertResult = db.BulkUpsert("/Root/olap", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

            const auto run = [&](const TString& query, NYdb::NQuery::EExecMode execMode) {
                auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
                auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(execMode)).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString() << " Query: " << query);
                return result;
            };

            const TString query = "PRAGMA YqlSelect = \"force\"; select a from `/Root/olap` order by b limit 2 offset 1;";
            const TString ast(*run(query, NYdb::NQuery::EExecMode::Explain).GetStats()->GetAst());
            const bool force = mode == NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE;
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("WideTopSortBlocks"), force, ast);
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("(TopSort "), !force, ast);
            CompareYson("[[8];[7]]", FormatResultSetYson(run(query, NYdb::NQuery::EExecMode::Execute).GetResultSet(0)));

            // The maximum limit keeps every row, so it takes a full sort in every mode.
            const TString maxLimit = "PRAGMA YqlSelect = \"force\"; select a from `/Root/olap` order by b limit 18446744073709551615 offset 8;";
            CompareYson("[[1];[0]]", FormatResultSetYson(run(maxLimit, NYdb::NQuery::EExecMode::Execute).GetResultSet(0)));

            if (!force) {
                const TString hugeLimit = "PRAGMA YqlSelect = \"force\"; select a from `/Root/olap` order by b limit 1000000000000 offset 8;";
                CompareYson("[[1];[0]]", FormatResultSetYson(run(hugeLimit, NYdb::NQuery::EExecMode::Execute).GetResultSet(0)));
            }
        }
    }

    Y_UNIT_TEST(PushFilterThroughUnionAll) {
        // Column-shard schema caches are process-wide and keyed by path ID, which every new runner reuses,
        // and earlier tests have these columns in a different primary key.
        NOlap::TSchemaCachesManager::DropCaches();

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        const auto schemaResult = dbSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/olap1` (
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                primary key(a)
            ) WITH (STORE = column);

            CREATE TABLE `/Root/olap2` (
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                primary key(a)
            ) WITH (STORE = column);

            CREATE TABLE `/Root/nullable` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
            ) WITH (STORE = column);

            CREATE TABLE `/Root/row` (
                a Int64 NOT NULL,
                b Int64 NOT NULL,
                primary key(a)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        const auto upsert = [&](const TString& table, const std::vector<std::pair<i64, i64>>& data) {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [a, b] : data) {
                rows.AddListItem().BeginStruct().AddMember("a").Int64(a).AddMember("b").Int64(b).EndStruct();
            }
            rows.EndList();
            const auto result = db.BulkUpsert(table, rows.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        upsert("/Root/olap1", {{1, 10}, {2, 20}, {3, 30}});
        upsert("/Root/olap2", {{11, 10}, {12, 20}, {13, 40}});
        upsert("/Root/row", {{21, 10}, {22, 50}});

        NYdb::TValueBuilder nullableRows;
        nullableRows.BeginList();
        nullableRows.AddListItem().BeginStruct().AddMember("a").Int64(31).AddMember("b").OptionalInt64(std::nullopt).EndStruct();
        nullableRows.AddListItem().BeginStruct().AddMember("a").Int64(32).AddMember("b").OptionalInt64(10).EndStruct();
        nullableRows.EndList();
        const auto nullableResult = db.BulkUpsert("/Root/nullable", nullableRows.Build()).GetValueSync();
        UNIT_ASSERT_C(nullableResult.IsSuccess(), nullableResult.GetIssues().ToString());

        const auto run = [&](const TString& query, NYdb::NQuery::EExecMode execMode) {
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(execMode)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString() << " Query: " << query);
            return result;
        };

        struct TCase {
            TString Query;
            TString Expected;
            // How many reads must get the filter, unset if it does not reach a read in every case.
            std::optional<ui32> OlapFilters;
        };

        const TString olap12 = "select a, b from `/Root/olap1` union all select a, b from `/Root/olap2`";
        const std::vector<TCase> cases = {
            // Every read gets the filter.
            {"select a from (" + olap12 + ") where b = 10 order by a", "[[1];[11]]", 2},
            {"select a from (select a, b from `/Root/olap1` union all select a, b from `/Root/olap2` union all select a, b from `/Root/nullable`) where b = 10 order by a",
                "[[1];[11];[32]]", 3},
            // Only column tables have an olap filter, but the row table is still filtered.
            {"select a from (select a, b from `/Root/olap1` union all select a, b from `/Root/row`) where b = 10 order by a", "[[1];[21]]", 1},
            // Each input uses its own columns, even when the names cross over.
            {"select x from (select a as x, b as y from `/Root/olap1` union all select b as x, a as y from `/Root/olap2`) where y = 11", "[[10]]", std::nullopt},
            // The two conjuncts both go down.
            {"select a from (" + olap12 + ") where b = 10 and a > 1 order by a", "[[11]]", 2},
            // A conjunct with a subquery stays above the union.
            {"select a from (" + olap12 + ") where b = (select min(b) from `/Root/olap1`) order by a", "[[1];[11]]", std::nullopt},
            // A computed column cannot reach the read, but the rows stay correct.
            {"select c from (select a + 1 as c from `/Root/olap1` union all select a + 1 as c from `/Root/olap2`) where c = 2", "[[2]]", std::nullopt},
            // A limit inside an input still applies before the filter.
            {"select a from (select a, b from (select a, b from `/Root/olap1` order by a limit 1) union all select a, b from `/Root/olap2`) where b = 20 order by a",
                "[[12]]", std::nullopt},
            // Operators above the filter.
            {"select count(*) as c from (" + olap12 + ") where b = 10", "[[2u]]", 2},
            {"select a from (" + olap12 + ") where b = 10 order by a limit 1", "[[1]]", 2},
            // An aggregate in each input: the filter continues through it to the read.
            {"select c from (select b, count(*) as c from `/Root/olap1` group by b union all select b, count(*) as c from `/Root/olap2` group by b) where b = 10", "[[1u];[1u]]", 2},
            // Union with duplicate removal.
            {"select a from (" + olap12 + " union select a, b from `/Root/olap1`) where b = 10 order by a", "[[1];[11]]", std::nullopt},
            // Optional and non-optional columns in one union.
            {"select a from (select a, b from `/Root/nullable` union all select a, b from `/Root/olap1`) where b = 10 order by a", "[[1];[32]]", std::nullopt},
            {"select a from (select a, b from `/Root/olap1` union all select a, b from `/Root/nullable`) where b = 10 order by a", "[[1];[32]]", std::nullopt},
            {"select a from (select a, b from `/Root/nullable` union all select a, b from `/Root/olap1`) where b is null order by a", "[[31]]", std::nullopt},
            {"select a from (select a, b from `/Root/olap1` union all select a, b from `/Root/nullable`) where b is null order by a", "[[31]]", std::nullopt},
            {"select c from (select b, count(*) as c from `/Root/nullable` group by b union all select b, count(*) as c from `/Root/olap1` group by b) where b = 10", "[[1u];[1u]]", std::nullopt},
            {"select c from (select b, count(*) as c from `/Root/olap1` group by b union all select b, count(*) as c from `/Root/nullable` group by b) where b is null", "[[1u]]", std::nullopt},
        };

        for (const auto& testCase : cases) {
            const TString query = TStringBuilder() << "PRAGMA YqlSelect = \"force\"; " << testCase.Query << ";";
            const auto ast = *run(query, NYdb::NQuery::EExecMode::Explain).GetStats()->GetAst();
            if (testCase.OlapFilters) {
                UNIT_ASSERT_VALUES_EQUAL_C(CountNumberOfCallables(ast, "KqpOlapFilter"), *testCase.OlapFilters, query << " AST: " << ast);
            }
            CompareYson(testCase.Expected, FormatResultSetYson(run(query, NYdb::NQuery::EExecMode::Execute).GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(PropagateAggregateThroughStages) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
	            b Int64,
                c Int64,
                primary key(a)
            ) WITH (STORE = column);

            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                b Int64,
                c Int64,
                primary key(a)
            ) WITH (STORE = column);
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        const std::vector<std::string> queries = {
            R"(
                select avg(t1.b) from `/Root/t1` as t1 group by t1.a;
            )",
            R"(
                select avg(t1.a) from `/Root/t1` as t1 group by t1.b;
            )",
            R"(
                select avg(t1.a), avg(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select sum(t1.a), max(t1.a), t1.b from `/Root/t1` as t1 group by t1.b;
            )",
            R"(
                select sum(t1.a), min(t1.b), t1.b from `/Root/t1` as t1 group by t1.b;
            )",
            R"(
                select count(t1.a), t1.b from `/Root/t1` as t1 group by t1.b;
            )",
            R"(
                select sum(t1.a), max(t1.a) from `/Root/t1` as t1;
            )",
            R"(
                select sum(t1.a), min(t1.b) from `/Root/t1` as t1;
            )",
            R"(
                select count(t1.a) from `/Root/t1` as t1;
            )",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "DqPhyHashCombine"), 2);

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST_TWIN(ReplicateBranchPushdown, ColumnStore) {
        TestReplicateBranchPushdown(ColumnStore);
    }

    Y_UNIT_TEST_TWIN(MixedDistinctAggregationBeforeHashShuffle, ColumnStore) {
        TestMixedDistinctAggregationBeforeHashShuffle(ColumnStore);
    }

    Y_UNIT_TEST_TWIN(BlockHashCombine, ColumnStore) {
        TestBlockHashCombine(ColumnStore);
    }

    Y_UNIT_TEST_TWIN(AggregateShuffleEliminationReusesCreatedPartitioning, ColumnStore) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        auto* config = settings.AppConfig.MutableTableServiceConfig();
        config->SetEnableNewRBO(true);
        config->SetEnableFallbackToYqlOptimizer(false);
        config->SetAllowOlapDataQuery(true);
        config->SetDefaultHashShuffleFuncType(NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2);
        TKikimrRunner kikimr(settings);
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        const auto created = tableSession.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `/Root/aggregation` (
                id Int64 NOT NULL,
                k Int64 NOT NULL,
                payload Int64 NOT NULL,
                PRIMARY KEY (id)
            )
        )" << (ColumnStore ? "PARTITION BY HASH(id) WITH (STORE = COLUMN, PARTITION_COUNT = 4);"
                          : "WITH (PARTITION_AT_KEYS = (7, 13, 19));")).GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (i64 id = 1; id <= 24; ++id) {
            rows.AddListItem().BeginStruct()
                .AddMember("id").Int64(id)
                .AddMember("k").Int64(id % 2)
                .AddMember("payload").Int64(id % 3)
                .EndStruct();
        }
        const auto inserted = tableClient.BulkUpsert("/Root/aggregation", rows.EndList().Build()).GetValueSync();
        UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());

        struct TCase {
            TString Query;
            TString Expected;
            size_t EnabledShuffles;
            size_t DisabledShuffles;
        };
        const TVector<TCase> cases{
            {R"(
                $grouped = SELECT k, payload, SUM(id) AS total FROM `/Root/aggregation` GROUP BY k, payload;
                SELECT k, payload, SUM(total) AS total, COUNT(*) AS n FROM $grouped
                GROUP BY payload, k ORDER BY k, payload;
            )", "[[0;0;60;1u];[0;1;52;1u];[0;2;44;1u];[1;0;48;1u];[1;1;40;1u];[1;2;56;1u]]", 1, 2},
            {R"(
                $grouped = SELECT DISTINCT payload, k FROM `/Root/aggregation`;
                SELECT k, payload, COUNT(*) AS n FROM $grouped
                GROUP BY k, payload ORDER BY k, payload;
            )", "[[0;0;1u];[0;1;1u];[0;2;1u];[1;0;1u];[1;1;1u];[1;2;1u]]", 1, 2},
            // Dropping a partitioning key still requires the second shuffle.
            {R"(
                $grouped = SELECT k, payload, SUM(id) AS total FROM `/Root/aggregation` GROUP BY k, payload;
                SELECT k, SUM(total) AS total, COUNT(*) AS n FROM $grouped GROUP BY k ORDER BY k;
            )", "[[0;156;3u];[1;144;3u]]", 2, 2},
            {R"(
                $grouped = SELECT k, payload, SUM(id) AS total FROM `/Root/aggregation` GROUP BY k, payload;
                SELECT SUM(total) AS total, COUNT(*) AS n FROM $grouped;
            )", "[[[300];6u]]", 1, 1},
            // A window's exchange invalidates the first aggregation's distribution.
            {R"(
                $grouped = SELECT k, payload, SUM(id) AS total FROM `/Root/aggregation` GROUP BY k, payload;
                $ranked = SELECT k, payload, total, ROW_NUMBER() OVER (PARTITION BY total ORDER BY k) AS rn FROM $grouped;
                SELECT k, payload, SUM(total) AS total, MAX(rn) AS rn FROM $ranked
                GROUP BY k, payload ORDER BY k, payload;
            )", "[[0;0;60;1u];[0;1;52;1u];[0;2;44;1u];[1;0;48;1u];[1;1;40;1u];[1;2;56;1u]]", 3, 3},
            // Reuse the window's new distribution when it matches the outer grouping.
            {R"(
                $grouped = SELECT k, payload, SUM(id) AS total FROM `/Root/aggregation` GROUP BY k, payload;
                $ranked = SELECT k, payload, total, ROW_NUMBER() OVER (PARTITION BY k ORDER BY payload) AS rn FROM $grouped;
                SELECT k, SUM(total) AS total, MAX(rn) AS rn FROM $ranked
                GROUP BY k ORDER BY k;
            )", "[[0;156;3u];[1;144;3u]]", 2, 3},
        };
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        for (const bool enabled : {false, true}) {
            for (const auto& testCase : cases) {
                const TString query = TStringBuilder() << "PRAGMA YqlSelect = 'force';\n"
                    << "PRAGMA ydb.OptShuffleElimination = '" << (enabled ? "true" : "false") << "';\n"
                    << testCase.Query;
                const auto explained = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
                UNIT_ASSERT_C(explained.IsSuccess(), explained.GetIssues().ToString());
                const TString plan{*explained.GetStats()->GetPlan()};
                const auto hashes = CollectHashShuffleFuncs(plan);
                UNIT_ASSERT_VALUES_EQUAL_C(hashes.size(), enabled ? testCase.EnabledShuffles : testCase.DisabledShuffles, plan);
                UNIT_ASSERT_C(std::all_of(hashes.begin(), hashes.end(), [](const auto& hash) { return hash == "HashV2"; }), plan);
                const auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString() << "\n" << query);
                CompareYson(testCase.Expected, FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    }

    Y_UNIT_TEST(AggregationShuffleEliminationSettingDoesNotEnableTransactionLayout) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        const auto preparedQuery = CompilePreparedQuery(kikimr, R"(
            PRAGMA ydb.OptShuffleElimination = "false";
            PRAGMA ydb.OptShuffleEliminationForAggregation = "true";

            SELECT 1;
        )");

        const auto& transactions = preparedQuery->GetTransactions();
        UNIT_ASSERT(!transactions.empty());
        for (const auto& transaction : transactions) {
            UNIT_ASSERT(!transaction->EnableShuffleElimination());
        }
    }

    // A flat 3-way join on TPCH tables with overridden statistics and fixed
    // join order & type to only test SE, not anything around it
    Y_UNIT_TEST(ShuffleEliminationSimpleJoin) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/customer": {"n_rows":  150000, "byte_size":  15000000},
            "/Root/orders":   {"n_rows": 1500000, "byte_size": 150000000},
            "/Root/lineitem": {"n_rows": 6000000, "byte_size": 600000000}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(session, "schema/tpch.sql", /*useColumnStore*/ true);

        // Fix the order to only test shuffle elimination, not the join order.
        const TString query = R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = 'JoinOrder((l o) c)';

            SELECT c.c_custkey, o.o_orderkey, l.l_linenumber
            FROM `/Root/customer` c
            JOIN `/Root/orders` o ON c.c_custkey = o.o_custkey
            JOIN `/Root/lineitem` l ON o.o_orderkey = l.l_orderkey
        )";

        auto queryDb = kikimr.GetQueryClient();
        auto querySession = queryDb.GetSession().GetValueSync().GetSession();

        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const auto plan = TString{*result.GetStats()->GetPlan()};
        const auto hashShuffles = CollectHashShuffleDescriptions(plan);

        UNIT_ASSERT_VALUES_EQUAL_C(hashShuffles.size(), 2u, plan);
        const bool hasLineitemShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("(l.l_orderkey)");
            });
        const bool hasOrdersShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("(o.o_custkey)");
            });
        const bool hasCustomerShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("(c.c_custkey)");
            });

        UNIT_ASSERT_C(
            hasLineitemShuffle && hasOrdersShuffle && !hasCustomerShuffle,
            TStringBuilder() << "Expected only lineitem and orders-side shuffles, got: "
                             << JoinSeq(", ", hashShuffles) << "\n" << plan);
    }

    Y_UNIT_TEST(ShuffleEliminationTPCHQ5CompositeJoinKeys) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/customer": {"n_rows": 150000, "byte_size": 16117888},
            "/Root/orders": {"n_rows": 1500000, "byte_size": 92638032},
            "/Root/lineitem": {"n_rows": 6001215, "byte_size": 409400000},
            "/Root/supplier": {"n_rows": 10000, "byte_size": 1098296},
            "/Root/nation": {"n_rows": 25, "byte_size": 2424},
            "/Root/region": {"n_rows": 5, "byte_size": 1008}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(tableSession, "data/", "schema/tpch.sql", /*useColumnStore*/ true);

        TString query = GetFullPath("data/yql-tpch/q", "5.yql");
        const TString toDecimal = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
        const TString toDecimalMax = R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
        query = toDecimal + "\n" + toDecimalMax + "\n" +
            R"(PRAGMA ydb.CostBasedOptimizationLevel = "4";
PRAGMA ydb.OptShuffleElimination = "true";
PRAGMA ydb.OptimizerHints = '
    JoinType(region nation Shuffle)
    JoinType(region nation supplier Shuffle)
    JoinType(region nation supplier customer Shuffle)
    JoinType(region nation supplier customer orders Shuffle)
    JoinType(region nation supplier customer orders lineitem Shuffle)
    JoinOrder(((((region nation) supplier) customer) orders) lineitem)
';
)" + query;

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const auto plan = TString{*result.GetStats()->GetPlan()};
        NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(NYdb::NConsoleClient::EDataFormat::PrettyTable, false, Cout, 0);
        queryPlanPrinter.Print(plan);

        const auto hashShuffles = CollectHashShuffleDescriptions(plan);

        const bool hasCustomerCompositeShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("c_custkey") && desc.Contains("c_nationkey");
            }
        );

        const bool hasFinalRightShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("o_custkey") && !desc.Contains("c_nationkey") && !desc.Contains("s_nationkey");
            }
        );

        UNIT_ASSERT_C(
            !hasCustomerCompositeShuffle && hasFinalRightShuffle,
            TStringBuilder() << "Expected DPHyp shuffle requirements to be preserved: customer side eliminated, "
                             << "right side shuffled by the enumerated orders key. Got: "
                             << JoinSeq(", ", hashShuffles) << "\n" << plan);
    }

    Y_UNIT_TEST(ShuffleEliminationSimpleJoinKeysBothSides) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/customer": {"n_rows": 150000, "byte_size": 16117888},
            "/Root/orders": {"n_rows": 1500000, "byte_size": 92638032}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(tableSession, "data/", "schema/tpch.sql", /*useColumnStore*/ true);

        const TString query = R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(c o Shuffle)
                JoinOrder(c o)
            ';

            SELECT c.c_custkey, o.o_orderkey
            FROM `/Root/customer` AS c
            JOIN `/Root/orders` AS o
                ON c.c_nationkey = o.o_custkey
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const auto plan = TString{*result.GetStats()->GetPlan()};
        NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(NYdb::NConsoleClient::EDataFormat::PrettyTable, true, Cout, 0);
        queryPlanPrinter.Print(plan);

        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "JoinAlgo", "BlockHash"), plan);

        const auto hashShuffles = CollectHashShuffleDescriptions(plan);

        UNIT_ASSERT_VALUES_EQUAL_C(CountJoinPlanNodes(plan), 1u, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(hashShuffles.size(), 2u, plan);

        const bool hasCustomerShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("(c.c_nationkey)");
            });
        const bool hasOrdersShuffle = std::any_of(
            hashShuffles.begin(),
            hashShuffles.end(),
            [](const TString& desc) {
                return desc.Contains("(o.o_custkey)");
            });

        UNIT_ASSERT_C(
            HasJoinWithBothInputsHashShuffled(plan),
            TStringBuilder() << "Expected a join with HashShuffle on both inputs, got: "
                             << JoinSeq(", ", hashShuffles) << "\n" << plan);
        UNIT_ASSERT_C(
            hasCustomerShuffle && hasOrdersShuffle,
            TStringBuilder() << "Expected both simple join sides to be reshuffled, got: "
                             << JoinSeq(", ", hashShuffles) << "\n" << plan);
    }

    // Minimal HashV2 compatibility regression.
    // Regression test for hash-function compatibility across two GraceJoins:
    // the second join may reuse the first join's output shuffling, so the
    // remaining input must be shuffled with the same hash function.
    Y_UNIT_TEST(ShuffleEliminationTwoJoinsHashFuncCompatibility) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultHashShuffleFuncType(
            NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/a": {"n_rows": 1000000, "byte_size": 16000000},
            "/Root/b": {"n_rows": 1000000, "byte_size": 16000000},
            "/Root/c": {"n_rows": 1000000, "byte_size": 16000000}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        for (const TString& table : {"a", "b", "c"}) {
            auto result = tableSession.ExecuteSchemeQuery(Sprintf(R"(
                CREATE TABLE `/Root/%s` (
                    id Int32 NOT NULL,
                    k Int32,
                    payload Int32,
                    PRIMARY KEY (id)
                )
                PARTITION BY HASH(id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )", table.c_str())).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString query = R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinType(a b c Shuffle)
                JoinOrder((a b) c)
            ';

            SELECT a.k, b.payload, c.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.k = b.k
            JOIN `/Root/c` AS c ON a.k = c.k
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const auto plan = TString{*result.GetStats()->GetPlan()};
        const auto hashFuncs = CollectHashShuffleFuncs(plan);

        // First join shuffles both sides with the default hash. The second join
        // should reuse that shuffling on the left and shuffle the right side
        // with the same hash function, not ColumnShardHashV1.
        UNIT_ASSERT_VALUES_EQUAL_C(hashFuncs.size(), 3u, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(
            std::count(hashFuncs.begin(), hashFuncs.end(), TString("ColumnShardHashV1")),
            0,
            TStringBuilder() << "Hash shuffles use incompatible functions: "
                             << JoinSeq(", ", hashFuncs) << "\n" << plan);
    }

    // Minimal ColumnShardHashV1 preserved-partitioning case.
    Y_UNIT_TEST(ShuffleEliminationSingleJoinColumnShardHashCompatibility) {
        const auto plan = ExplainHashCompatibilityQuery({"a", "b"}, R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinOrder(a b)
            ';

            SELECT a.id, b.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.id = b.k
        )");

        const auto hashFuncs = CollectHashShuffleFuncs(plan);

        UNIT_ASSERT_VALUES_EQUAL_C(hashFuncs.size(), 1u, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(
            hashFuncs.front(),
            TString("ColumnShardHashV1"),
            TStringBuilder() << "The remaining shuffle must match the preserved source hash: "
                             << JoinSeq(", ", hashFuncs) << "\n" << plan);
    }

    Y_UNIT_TEST_TWIN(DistinctShuffleEliminationTPCHQ4, CompositePartitionKey) {
        NKikimrConfig::TAppConfig appConfig;
        auto* config = appConfig.MutableTableServiceConfig();
        config->SetEnableNewRBO(true);
        config->SetEnableFallbackToYqlOptimizer(false);
        config->SetAllowOlapDataQuery(true);
        config->SetDefaultLangVer(NYql::GetMaxLangVersion());
        config->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        config->SetDefaultCostBasedOptimizationLevel(4);
        config->SetDefaultEnableShuffleElimination(true);
        config->SetDefaultHashShuffleFuncType(NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2);

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({MakeTPCHStatsSetting()});
        TKikimrRunner kikimr(settings);
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        auto schema = tableSession.ExecuteSchemeQuery(Sprintf(R"(
            CREATE TABLE `/Root/lineitem` (
                l_orderkey Int64 NOT NULL,
                l_linenumber Int32 NOT NULL,
                l_commitdate Date NOT NULL,
                l_receiptdate Date NOT NULL,
                PRIMARY KEY (l_orderkey, l_linenumber)
            )
            PARTITION BY HASH(%s)
            WITH (STORE = COLUMN, PARTITION_COUNT = 8);

            CREATE TABLE `/Root/orders` (
                o_orderkey Int64 NOT NULL,
                o_orderpriority Utf8 NOT NULL,
                o_orderdate Date NOT NULL,
                PRIMARY KEY (o_orderkey)
            )
            PARTITION BY HASH(o_orderkey)
            WITH (STORE = COLUMN, PARTITION_COUNT = 4);
        )", CompositePartitionKey ? "l_orderkey, l_linenumber" : "l_orderkey")).GetValueSync();
        UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());

        const auto july = TInstant::ParseIso8601("1993-07-01T00:00:00Z");
        const auto october = TInstant::ParseIso8601("1993-10-01T00:00:00Z");
        NYdb::TValueBuilder orders, lineitems;
        orders.BeginList();
        lineitems.BeginList();
        for (i64 id = 1; id <= 32; ++id) {
            orders.AddListItem().BeginStruct()
                .AddMember("o_orderkey").Int64(id)
                .AddMember("o_orderpriority").Utf8(id % 2 ? "HIGH" : "LOW")
                .AddMember("o_orderdate").Date(id <= 16 ? july : october)
                .EndStruct();
            // Two qualifying line items per matching order must count only once.
            for (i32 line = 1; line <= 3; ++line) {
                lineitems.AddListItem().BeginStruct()
                    .AddMember("l_orderkey").Int64(id)
                    .AddMember("l_linenumber").Int32(line)
                    .AddMember("l_commitdate").Date(july)
                    .AddMember("l_receiptdate").Date(id % 4 && line < 3 ? october : july)
                    .EndStruct();
            }
        }
        orders.EndList();
        lineitems.EndList();
        for (auto& [table, rows] : TVector<std::pair<TString, NYdb::TValue>>{
                 {"/Root/orders", orders.Build()}, {"/Root/lineitem", lineitems.Build()}}) {
            const auto upsert = tableClient.BulkUpsert(table, std::move(rows)).GetValueSync();
            UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        }

        std::string query = NResource::Find("resfs/file/tpch/queries/yql/q4.sql");
        Replace(query, "{% include 'header.sql.jinja' %}", "PRAGMA YqlSelect = 'force';");
        Replace(query, "{{orders}}", "`/Root/orders`");
        Replace(query, "{{lineitem}}", "`/Root/lineitem`");
        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        const auto explain = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
        const auto plan = TString{*explain.GetStats()->GetPlan()};
        const auto shuffles = CollectHashShuffleDescriptions(plan);
        UNIT_ASSERT_VALUES_EQUAL_C(shuffles.size(), CompositePartitionKey ? 3 : 2, plan);
        // Identify the DISTINCT exchange by its key, the intermediate DISTINCT
        // result over l_orderkey, separately from the join and priority-aggregation exchanges.
        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto distinctShuffles = std::count_if(shuffles.begin(), shuffles.end(), [&](const TString& shuffle) {
            const TString key{TStringBuf(shuffle).After('(').RBefore(')')};
            const auto* aggregation = FindAggregation(simplifiedPlan, "distinct", key);
            return aggregation && StringArrayFieldContains(*aggregation, "Arguments", "/Root/lineitem.l_orderkey");
        });
        UNIT_ASSERT_VALUES_EQUAL_C(distinctShuffles, CompositePartitionKey ? 1 : 0, plan);

        const auto execute = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(execute.IsSuccess(), execute.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(execute.GetResultSet(0)), R"([["HIGH";8u];["LOW";4u]])");
    }

    Y_UNIT_TEST(ShuffleEliminationColumnShardHashPreservedInPhysicalAst) {
        const auto [plan, ast] = ExplainHashCompatibilityQueryWithAst({"a", "b"}, R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinOrder(a b)
            ';

            SELECT a.id, b.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.id = b.k
        )", /*blockChannelsAuto=*/true);

        UNIT_ASSERT_C(
            HasPhysicalHashShuffleWithHashFunc(ast, "ColumnShardHashV1"),
            TStringBuilder() << "Expected a physical hash shuffle to preserve ColumnShardHashV1\n"
                             << plan << "\n" << ast);
    }

    Y_UNIT_TEST(ShuffleEliminationCompositeSourceSubsetKeyExecute) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultHashShuffleFuncType(
            NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/a": {"n_rows": 1000000, "byte_size": 16000000},
            "/Root/b": {"n_rows": 1000000, "byte_size": 24000000}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto result = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/a` (
                id Int64 NOT NULL,
                payload Int64 NOT NULL,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH (STORE = COLUMN, PARTITION_COUNT = 4);
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/b` (
                id Int64 NOT NULL,
                k Int64 NOT NULL,
                payload Int64 NOT NULL,
                PRIMARY KEY (id, k)
            )
            PARTITION BY HASH(id, k)
            WITH (STORE = COLUMN, PARTITION_COUNT = 4);
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (ui64 id : {1, 2, 3}) {
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember("id").Int64(id)
                    .AddMember("payload").Int64(id * 10)
                    .EndStruct();
            }
            rows.EndList();

            auto upsert = tableClient.BulkUpsert("/Root/a", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        }

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(1)
                .AddMember("k").Int64(10)
                .AddMember("payload").Int64(101)
                .EndStruct();
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(1)
                .AddMember("k").Int64(11)
                .AddMember("payload").Int64(102)
                .EndStruct();
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(2)
                .AddMember("k").Int64(20)
                .AddMember("payload").Int64(200)
                .EndStruct();
            rows.AddListItem()
                .BeginStruct()
                .AddMember("id").Int64(4)
                .AddMember("k").Int64(40)
                .AddMember("payload").Int64(400)
                .EndStruct();
            rows.EndList();

            auto upsert = tableClient.BulkUpsert("/Root/b", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        }

        const TString query = R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(b a Shuffle)
                JoinOrder(b a)
            ';

            SELECT b.id, b.k, b.payload
            FROM `/Root/b` AS b
            JOIN `/Root/a` AS a ON b.id = a.id
            ORDER BY b.id, b.k
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        auto explain = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();
        UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());

        auto execute = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute)
        ).ExtractValueSync();

        UNIT_ASSERT_C(execute.IsSuccess(), execute.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(execute.GetResultSet(0)), R"([[1;10;101];[1;11;102];[2;20;200]])");
    }

    // All-ColumnShardHashV1 chain: no accidental HashV2 transition.
    Y_UNIT_TEST(ShuffleEliminationThreeJoinsColumnShardHashCompatibility) {
        const auto plan = ExplainHashCompatibilityQuery({"a", "b", "c", "d"}, R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinType(a b c Shuffle)
                JoinType(a b c d Shuffle)
                JoinOrder(((a b) c) d)
            ';

            SELECT a.id, b.payload, c.payload, d.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.id = b.k
            JOIN `/Root/c` AS c ON a.id = c.k
            JOIN `/Root/d` AS d ON a.id = d.k
        )");

        const auto hashFuncs = CollectHashShuffleFuncs(plan);

        UNIT_ASSERT_VALUES_EQUAL_C(hashFuncs.size(), 3u, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(
            std::count(hashFuncs.begin(), hashFuncs.end(), TString("ColumnShardHashV1")),
            3,
            TStringBuilder() << "All shuffles must match the preserved source hash: "
                             << JoinSeq(", ", hashFuncs) << "\n" << plan);
    }

    // All-HashV2 chain: no accidental ColumnShardHashV1 propagation.
    Y_UNIT_TEST(ShuffleEliminationThreeJoinsHashFuncCompatibility) {
        const auto plan = ExplainHashCompatibilityQuery({"a", "b", "c", "d"}, R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinType(a b c Shuffle)
                JoinType(a b c d Shuffle)
                JoinOrder(((a b) c) d)
            ';

            SELECT a.k, b.payload, c.payload, d.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.k = b.k
            JOIN `/Root/c` AS c ON a.k = c.k
            JOIN `/Root/d` AS d ON a.k = d.k
        )");

        const auto hashFuncs = CollectHashShuffleFuncs(plan);

        UNIT_ASSERT_VALUES_EQUAL_C(hashFuncs.size(), 4u, plan);
        UNIT_ASSERT_VALUES_EQUAL_C(
            std::count(hashFuncs.begin(), hashFuncs.end(), TString("HashV2")),
            4,
            TStringBuilder() << "All shuffles must stay compatible with the first join hash: "
                             << JoinSeq(", ", hashFuncs) << "\n" << plan);
    }

    // General mixed case: both hash domains and transitions in one plan.
    Y_UNIT_TEST(ShuffleEliminationMixedHashFuncCompatibility) {
        // When both final join inputs are already shuffled, DPHyp still
        // has to shuffle one due to runtime limitations and tie breaks
        // based on statistics - so we pin those too.
        const auto plan = ExplainHashCompatibilityQuery({"a", "b", "c", "i", "d", "e", "f", "g", "h"}, R"(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";
            PRAGMA ydb.OptimizerHints = '
                JoinType(a b Shuffle)
                JoinType(a b c Shuffle)
                JoinType(a b c i Shuffle)
                JoinType(a b c i d Shuffle)
                JoinType(e f Shuffle)
                JoinType(e f g Shuffle)
                JoinType(e f g h Shuffle)
                JoinType(a b c i d e f g h Shuffle)
                JoinOrder(((((a b) c) i) d) (((e f) g) h))
            ';

            SELECT a.id, b.payload, c.payload, i.payload, d.payload, e.id, f.payload, g.payload, h.payload
            FROM `/Root/a` AS a
            JOIN `/Root/b` AS b ON a.id = b.k
            JOIN `/Root/c` AS c ON a.id = c.k
            JOIN `/Root/i` AS i ON a.id = i.k
            JOIN `/Root/d` AS d ON a.k = d.k
            JOIN `/Root/e` AS e ON a.k = e.k
            JOIN `/Root/f` AS f ON e.id = f.k
            JOIN `/Root/g` AS g ON e.k = g.k
            JOIN `/Root/h` AS h ON e.k = h.k
        )");

        const auto hashShuffles = CollectHashShuffleDescriptions(plan);
        // Preorder traversal: the left subtree keeps ColumnShard-preserved
        // id=k joins for several levels, then switches to default HashV2 k=k joins.
        // The right subtree also switches to HashV2 on e.k. At the final join both
        // sides have compatible one-column HashV2 partitioning, so DPHyp must
        // redundantly reshuffle exactly one side. The left subtree (a,b,c,i,d) is
        // larger than the right (e,f,g,h), so CBO preserves the larger left subtree
        // and reshuffles the smaller right subtree on e.k.
        const TVector<TString> expectedHashShuffles = {
            "HashV2(a.k)",
            "ColumnShardHashV1(b.k)",
            "ColumnShardHashV1(c.k)",
            "ColumnShardHashV1(i.k)",
            "HashV2(d.k)",
            "HashV2(e.k)",
            "HashV2(e.k)",
            "ColumnShardHashV1(f.k)",
            "HashV2(g.k)",
            "HashV2(h.k)",
        };

        UNIT_ASSERT_VALUES_EQUAL_C(
            SortDescriptions(hashShuffles),
            SortDescriptions(expectedHashShuffles),
            TStringBuilder() << "Unexpected mixed hash propagation plan: "
                             << JoinSeq(", ", hashShuffles) << "\n" << plan);
    }

    Y_UNIT_TEST(ShuffleEliminationRightJoinAliasReproducer) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        NKikimrKqp::TKqpSetting statsSetting;
        statsSetting.SetName("OptOverrideStatistics");
        statsSetting.SetValue(R"({
            "/Root/_temp/_pool_249": {"n_rows": 12, "byte_size": 1024},
            "/Root/_temp/_pool_252": {"n_rows": 66464, "byte_size": 8517376},
            "/Root/_temp/_pool_256": {"n_rows": 36, "byte_size": 2048},
            "/Root/public/_accumrg28120": {"n_rows": 32601254, "byte_size": 4172960512}
        })");

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({statsSetting});
        TKikimrRunner kikimr(settings);

        auto schemeClient = kikimr.GetSchemeClient();
        auto mkDir = schemeClient.MakeDirectory("/Root/_temp").GetValueSync();
        UNIT_ASSERT_C(mkDir.IsSuccess(), mkDir.GetIssues().ToString());
        mkDir = schemeClient.MakeDirectory("/Root/public").GetValueSync();
        UNIT_ASSERT_C(mkDir.IsSuccess(), mkDir.GetIssues().ToString());

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/_temp/_pool_252` (
                `_q_000_f_000` Decimal(10, 0),
                `_q_000_f_001rref` String,
                `_q_000_f_002rref` String,
                `_q_000_f_003rref` String,
                `_q_000_f_004rref` String,
                `_q_000_f_005` Decimal(15, 3),
                `_q_000_f_006` Decimal(31, 18),
                `_ydb_pk` Int64 NOT NULL,
                PRIMARY KEY (`_ydb_pk`)
            );

            CREATE TABLE `/Root/_temp/_pool_256` (
                `_q_000_f_000rref` String,
                `_ydb_pk` Int64 NOT NULL,
                PRIMARY KEY (`_ydb_pk`)
            );

            CREATE TABLE `/Root/_temp/_pool_249` (
                `_q_000_f_000_type` String,
                `_q_000_f_000_rtref` String,
                `_q_000_f_000_rrref` String,
                `_ydb_pk` Int64 NOT NULL,
                PRIMARY KEY (`_ydb_pk`)
            );

            CREATE TABLE `/Root/public/_accumrg28120` (
                `_period` Timestamp64 NOT NULL,
                `_recordertref` String NOT NULL,
                `_recorderrref` String NOT NULL,
                `_lineno` Decimal(9, 0) NOT NULL,
                `_active` Bool NOT NULL,
                `_recordkind` Decimal(1, 0) NOT NULL,
                `_fld28121rref` String NOT NULL,
                `_fld28122rref` String NOT NULL,
                `_fld28123rref` String NOT NULL,
                `_fld28124rref` String NOT NULL,
                `_fld28125` Decimal(15, 3) NOT NULL,
                `_fld28126` Decimal(15, 2) NOT NULL,
                `_fld28127_type` String NOT NULL,
                `_fld28127_rtref` String NOT NULL,
                `_fld28127_rrref` String NOT NULL,
                `_fld28128_type` String NOT NULL,
                `_fld28128_rtref` String NOT NULL,
                `_fld28128_rrref` String NOT NULL,
                `_fld28129rref` String NOT NULL,
                `_fld28130rref` String NOT NULL,
                `_fld28131rref` String NOT NULL,
                `_ydb_pk` Int64 NOT NULL,
                PRIMARY KEY (`_ydb_pk`)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        const TString query = R"sql(
            PRAGMA ydb.CostBasedOptimizationLevel = "4";
            PRAGMA ydb.OptShuffleElimination = "true";

            DECLARE $const_0 AS String;
            DECLARE $const_1 AS String;
            DECLARE $const_2 AS Timestamp64;
            DECLARE $const_3 AS Timestamp64;
            DECLARE $const_4 AS Decimal(1, 0);

            SELECT
                `s2`.`__ydb_1_7`,
                `s2`.`__ydb_1_8`,
                `s2`.`__ydb_1_9`,
                `s2`.`__ydb_1_10`,
                `s2`.`__ydb_1_13`,
                `s2`.`__ydb_1_14`,
                `s2`.`__ydb_1_15`,
                `t4`.`_q_000_f_000`,
                `s2`.`__ydb_1_16`,
                `s2`.`__ydb_1_17`,
                `s2`.`__ydb_1_18`,
                `s2`.`__ydb_1_19`,
                `s2`.`__ydb_1_20`,
                `s2`.`__ydb_1_21`,
                `s2`.`__ydb_6_1`,
                `s2`.`__ydb_1_12`
            FROM `/Root/_temp/_pool_252` AS `t4`
            RIGHT JOIN (
                SELECT
                    `s1`.`__ydb_1_7` AS `__ydb_1_7`,
                    `s1`.`__ydb_1_8` AS `__ydb_1_8`,
                    `s1`.`__ydb_1_9` AS `__ydb_1_9`,
                    `s1`.`__ydb_1_10` AS `__ydb_1_10`,
                    `s1`.`__ydb_1_13` AS `__ydb_1_13`,
                    `s1`.`__ydb_1_14` AS `__ydb_1_14`,
                    `s1`.`__ydb_1_15` AS `__ydb_1_15`,
                    `s1`.`__ydb_1_16` AS `__ydb_1_16`,
                    `s1`.`__ydb_1_17` AS `__ydb_1_17`,
                    `s1`.`__ydb_1_18` AS `__ydb_1_18`,
                    `s1`.`__ydb_1_19` AS `__ydb_1_19`,
                    `s1`.`__ydb_1_20` AS `__ydb_1_20`,
                    `s1`.`__ydb_1_21` AS `__ydb_1_21`,
                    `s1`.`__ydb_1_12` AS `__ydb_1_12`,
                    `s1`.`__ydb_6_1` AS `__ydb_6_1`
                FROM `/Root/_temp/_pool_256` AS `t2`
                INNER JOIN (
                    SELECT
                        `t1`.`_fld28121rref` AS `__ydb_1_7`,
                        `t1`.`_fld28122rref` AS `__ydb_1_8`,
                        `t1`.`_fld28123rref` AS `__ydb_1_9`,
                        `t1`.`_fld28124rref` AS `__ydb_1_10`,
                        `t1`.`_fld28127_type` AS `__ydb_1_13`,
                        `t1`.`_fld28127_rtref` AS `__ydb_1_14`,
                        `t1`.`_fld28127_rrref` AS `__ydb_1_15`,
                        `t1`.`_fld28128_type` AS `__ydb_1_16`,
                        `t1`.`_fld28128_rtref` AS `__ydb_1_17`,
                        `t1`.`_fld28128_rrref` AS `__ydb_1_18`,
                        `t1`.`_fld28129rref` AS `__ydb_1_19`,
                        `t1`.`_fld28130rref` AS `__ydb_1_20`,
                        `t1`.`_fld28131rref` AS `__ydb_1_21`,
                        `t1`.`_fld28126` AS `__ydb_1_12`,
                        `t6`.`__ydb_6_1` AS `__ydb_6_1`
                    FROM `/Root/public/_accumrg28120` AS `t1`
                    LEFT JOIN (
                        SELECT
                            `t1`.`_ydb_pk` AS `__ydb_pk_0`,
                            `t6`.`_q_000_f_000` AS `__ydb_6_1`,
                            `t6`.`_q_000_f_001rref` AS `__ydb_6_2`,
                            `t6`.`_q_000_f_002rref` AS `__ydb_6_3`,
                            `t6`.`_q_000_f_003rref` AS `__ydb_6_4`,
                            `t6`.`_q_000_f_004rref` AS `__ydb_6_5`,
                            `t6`.`_ydb_pk` AS `_ydb_pk`
                        FROM `/Root/public/_accumrg28120` AS `t1`
                        INNER JOIN `/Root/_temp/_pool_252` AS `t6`
                            ON ((`t6`.`_q_000_f_001rref` = `t1`.`_fld28128_rrref`))
                            AND ((`t6`.`_q_000_f_002rref` = `t1`.`_fld28129rref`))
                            AND ((`t6`.`_q_000_f_003rref` = `t1`.`_fld28130rref`))
                            AND ((`t6`.`_q_000_f_004rref` = `t1`.`_fld28131rref`))
                        WHERE (($const_0 = `t1`.`_fld28128_type`))
                            AND (($const_1 = `t1`.`_fld28128_rtref`))
                    ) AS `t6`
                        ON (`t1`.`_ydb_pk` = `t6`.`__ydb_pk_0`)
                    LEFT ONLY JOIN `/Root/_temp/_pool_249` AS `t8`
                        ON ((`t1`.`_fld28127_type` = `t8`.`_q_000_f_000_type`))
                        AND ((`t1`.`_fld28127_rtref` = `t8`.`_q_000_f_000_rtref`))
                        AND ((`t1`.`_fld28127_rrref` = `t8`.`_q_000_f_000_rrref`))
                    WHERE ((`t1`.`_period` >= $const_2))
                        AND ((`t1`.`_period` <= $const_3))
                        AND (`t1`.`_active`)
                        AND ((`t1`.`_recordkind` = $const_4))
                ) AS `s1`
                    ON ((`s1`.`__ydb_1_7` = `t2`.`_q_000_f_000rref`))
            ) AS `s2`
                ON ((`t4`.`_q_000_f_001rref` = `s2`.`__ydb_1_7`))
                AND ((`t4`.`_q_000_f_002rref` = `s2`.`__ydb_1_8`))
                AND ((`t4`.`_q_000_f_003rref` = `s2`.`__ydb_1_9`))
                AND ((`t4`.`_q_000_f_004rref` = `s2`.`__ydb_1_10`))
        )sql";

        auto result = tableSession.ExplainDataQuery(query).GetValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        UNIT_ASSERT_C(!result.GetPlan().empty(), result.GetPlan());
    }

}

} // namespace NKikimr::NKqp
