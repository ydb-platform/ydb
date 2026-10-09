#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cctype>
#include <optional>
#include <tuple>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void PrintPlan(const TString& plan, bool analyzeMode) {
    NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(
        NYdb::NConsoleClient::EDataFormat::PrettyTable,
        analyzeMode, Cout, /*maxWidth=*/0
    );
    queryPlanPrinter.Print(plan);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpRboLookupJoin) {

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

    Y_UNIT_TEST_TWIN(LookupJoinByIndexWithConstPrefix, AllPointPrefixes) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(false);
        appConfig.MutableTableServiceConfig()->SetEnablePruneKeyColumns(true);
        appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
        appConfig.MutableTableServiceConfig()->SetEnableLookupJoinPointPrefixes(AllPointPrefixes);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64,
                PRIMARY KEY (a)
            );

            CREATE TABLE `/Root/t2` (
                a Int64,
                b Int64,
                c Int32,
                PRIMARY KEY (a)
            );

            CREATE TABLE `/Root/t3` (
                a Int64,
                b Int64,
                c Int32,
                PRIMARY KEY (a)
            );

            CREATE TABLE `/Root/t4` (
                a Int64,
                b Int64,
                c Int32,
                d Int32,
                PRIMARY KEY (a)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (i64 a : {1, 2, 3, 4}) {
                rows.AddListItem().BeginStruct()
                    .AddMember("a").OptionalInt64(a)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        for (const auto* table : {"/Root/t2", "/Root/t3"}) {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [a, b, c] : TVector<std::tuple<i64, i64, i32>>{
                     {10, 1, 0}, {11, 2, 1}, {12, 3, 0}, {13, 3, 0}, {14, 5, 0}}) {
                rows.AddListItem().BeginStruct()
                    .AddMember("a").OptionalInt64(a)
                    .AddMember("b").OptionalInt64(b)
                    .AddMember("c").OptionalInt32(c)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert(table, rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const auto& [a, b, c, d] : TVector<std::tuple<i64, i64, i32, i32>>{
                     {10, 1, 0, 7}, {11, 2, 1, 7}, {12, 3, 0, 7}, {13, 3, 0, 8}, {14, 5, 0, 7}}) {
                rows.AddListItem().BeginStruct()
                    .AddMember("a").OptionalInt64(a)
                    .AddMember("b").OptionalInt64(b)
                    .AddMember("c").OptionalInt32(c)
                    .AddMember("d").OptionalInt32(d)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t4", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        // BulkUpsert does not support tables with sync indexes, so indexes are built after loading.
        for (const auto* addIndex : {
                 "ALTER TABLE `/Root/t2` ADD INDEX idx_c_b GLOBAL ON (c, b);",
                 // Both indexes fit the predicate equally well, so idx_c wins by name. The lookup join has to redirect
                 // the read to idx_c_b, where the join key follows the point prefix.
                 "ALTER TABLE `/Root/t3` ADD INDEX idx_c GLOBAL ON (c) COVER (b);",
                 "ALTER TABLE `/Root/t3` ADD INDEX idx_c_b GLOBAL ON (c, b);",
                 // idx_c_d pins more key columns, so it wins for the predicate although it does not cover b.
                 // The lookup join has to probe the covering idx_c_b instead.
                 "ALTER TABLE `/Root/t4` ADD INDEX idx_c_d GLOBAL ON (c, d);",
                 "ALTER TABLE `/Root/t4` ADD INDEX idx_c_b GLOBAL ON (c, b) COVER (d);",
             }) {
            schemeResult = session.ExecuteSchemeQuery(addIndex).GetValueSync();
            UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());
        }

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto explain = [&](const TString& query) {
            auto explained = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_C(explained.IsSuccess(), explained.GetIssues().ToString());
            return std::make_pair(TString{*explained.GetStats()->GetAst()}, TString{*explained.GetStats()->GetPlan()});
        };

        for (const auto& [query, index] : TVector<std::pair<TString, TString>>{
                 {"SELECT a, b, c FROM `/Root/t3` WHERE c = 0;", "/Root/t3/idx_c/indexImplTable"},
                 {"SELECT a, b, c, d FROM `/Root/t4` WHERE c = 0 AND d = 7;", "/Root/t4/idx_c_d/indexImplTable"},
                 {"SELECT a, b, c FROM `/Root/t2` WHERE c = 0 AND b > 1;", "/Root/t2/idx_c_b/indexImplTable"},
                 {"SELECT a, b, c, d FROM `/Root/t4` WHERE c = 0 AND d > 7;", "/Root/t4/idx_c_d/indexImplTable"}}) {
            const auto [ast, plan] = explain(query);
            UNIT_ASSERT_C(ast.Contains(index), "expected a read of " << index << ", ast:\n" << ast);
        }

        struct TCase {
            TString Table;
            TString Predicate;
            TString Result;
            // The index chosen for the predicate, when it is not the one to lookup by.
            TString PredicateIndex;
        };

        const TString allRows = R"([[[1];[10];[1];[0]];[[3];[12];[3];[0]];[[3];[13];[3];[0]]])";
        const TVector<TCase> cases = {
            {"t2", "t.c = 0", allRows, ""},
            {"t3", "t.c = 0", allRows, "idx_c"},
            {"t4", "t.c = 0 AND t.d = 7", R"([[[1];[10];[1];[0]];[[3];[12];[3];[0]]])", "idx_c_d"},
            // The whole predicate is pushed into the ranges of the read, so only the point prefix c is left for the lookup.
            // The range b > 1 has to be applied to fetched rows: the lookup by b = 1 finds a row, which it filters out.
            {"t2", "t.c = 0 AND t.b > 1", R"([[[3];[12];[3];[0]];[[3];[13];[3];[0]]])", ""},
            // The range d > 7 is pushed into idx_c_d, the lookup by idx_c_b has to apply it to fetched rows.
            {"t4", "t.c = 0 AND t.d > 7", R"([[[3];[13];[3];[0]]])", "idx_c_d"},
        };

        for (const auto& [table, predicate, expected, predicateIndex] : cases) {
            // The lookup goes through idx_c_b: c is a constant point prefix and b is the lookup key.
            const TString query = Sprintf(R"(
                SELECT t1.a AS t1_a, t.a AS t_a, t.b AS t_b, t.c AS t_c
                FROM `/Root/t1` AS t1
                JOIN `/Root/%s` AS t
                  ON t1.a = t.b
                WHERE %s
                ORDER BY t_a;
            )", table.c_str(), predicate.c_str());

            auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), table << ": " << result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, table);

            const auto [ast, plan] = explain(query);
            if (!AllPointPrefixes && !predicateIndex.empty()) {
                // Only the point prefix of the index chosen for the predicate is known, and b does not follow it.
                UNIT_ASSERT_C(!ast.Contains("/Root/" + table + "/idx_c_b/indexImplTable"), table << ": expected no lookup by idx_c_b, ast:\n" << ast);
                continue;
            }

            UNIT_ASSERT_C(ast.Contains("KqpIndexLookupJoin"), table << ": expected a lookup join, ast:\n" << ast);
            UNIT_ASSERT_C(plan.Contains("TableLookupJoin"), table << ": expected a lookup join, plan:\n" << plan);
            UNIT_ASSERT_C(ast.Contains("/Root/" + table + "/idx_c_b/indexImplTable"), table << ": expected a lookup by idx_c_b, ast:\n" << ast);
            if (!predicateIndex.empty()) {
                const TString predicateIndexTable = "/Root/" + table + "/" + predicateIndex + "/indexImplTable";
                UNIT_ASSERT_C(!ast.Contains(predicateIndexTable), table << ": expected no read of " << predicateIndex << ", ast:\n" << ast);
            }
            UNIT_ASSERT_C(ast.Contains(R"('('"AllowNullKeysPrefixSize" '1))"), table << ": expected a lookup by a point prefix, ast:\n" << ast);
        }
    }

    // Returns stream lookups into the table. The AST binds the table and parts of the lookup settings
    // to variables, so the variables of a returned lookup are substituted with their values.
    TVector<TString> StreamLookupsInto(const TString& ast, const TString& table) {
        THashMap<TString, TString> values;
        TVector<TString> lookups;
        TStringBuf rest = ast;
        TString tableVar;
        for (TStringBuf line; rest.NextTok('\n', line);) {
            if (line.StartsWith("(let $") && line.EndsWith(")")) {
                const size_t nameEnd = line.find(' ', 5);
                const TString var(line.substr(5, nameEnd - 5));
                values[var] = TString(line.substr(nameEnd + 1, line.size() - nameEnd - 2));
                if (line.Contains("(KqpTable '\"" + table + "\"")) {
                    tableVar = " " + var + " ";
                }
            }
            if (!tableVar.empty() && line.Contains("(KqpCnStreamLookup ") && line.Contains(tableVar)) {
                lookups.emplace_back(line);
            }
        }

        auto substitute = [&](const TString& text) {
            TString result;
            size_t i = 0;
            while (i < text.size()) {
                if (text[i] == '$') {
                    size_t end = i + 1;
                    while (end < text.size() && std::isdigit(static_cast<unsigned char>(text[end]))) {
                        ++end;
                    }
                    if (const auto it = values.find(text.substr(i, end - i)); it != values.end()) {
                        result += it->second;
                        i = end;
                        continue;
                    }
                }
                result += text[i++];
            }
            return result;
        };

        for (auto& lookup : lookups) {
            for (int depth = 0; depth < 3; ++depth) {
                lookup = substitute(lookup);
            }
        }
        return lookups;
    }

    Y_UNIT_TEST_QUAD(LookupJoinByNonCoveringIndexWithConstPrefix, AllPointPrefixes, CoveringPredicateIndex) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(false);
        appConfig.MutableTableServiceConfig()->SetEnablePruneKeyColumns(true);
        appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
        appConfig.MutableTableServiceConfig()->SetEnableLookupJoinPointPrefixes(AllPointPrefixes);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                id Int64 NOT NULL,
                c  Int32,
                k  Int64,
                v  Int64,
                PRIMARY KEY (id)
            );

            CREATE TABLE `/Root/t2` (
                k Int64 NOT NULL,
                c Int32,
                PRIMARY KEY (k)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            TVector<std::tuple<i64, i32, i64, i64>> facts;
            for (i64 id = 0; id < 100; ++id) {
                facts.emplace_back(id, 0, id, id + 1000);
            }
            // A second match for k = 17, and a row which the point prefix c = 0 excludes.
            facts.emplace_back(100, 0, 17, 2000);
            facts.emplace_back(101, 1, 42, 3000);
            for (const auto& [id, c, k, v] : facts) {
                rows.AddListItem().BeginStruct()
                    .AddMember("id").Int64(id)
                    .AddMember("c").OptionalInt32(c)
                    .AddMember("k").OptionalInt64(k)
                    .AddMember("v").OptionalInt64(v)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            // k = 500 has no match in the index.
            for (const auto& [k, c] : TVector<std::pair<i64, i32>>{{17, 0}, {42, 0}, {50, 1}, {500, 0}}) {
                rows.AddListItem().BeginStruct()
                    .AddMember("k").Int64(k)
                    .AddMember("c").OptionalInt32(c)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t2", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        // BulkUpsert does not support tables with sync indexes, so the index is built after loading.
        // The index does not cover v.
        schemeResult = session.ExecuteSchemeQuery("ALTER TABLE `/Root/t1` ADD INDEX idx_c_k GLOBAL ON (c, k);").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());
        if (CoveringPredicateIndex) {
            // A covering index which fits the predicate as well: the read is redirected to it, and the lookup join
            // still has to reach the non-covering idx_c_k, whose key has the join key after the point prefix.
            schemeResult = session.ExecuteSchemeQuery("ALTER TABLE `/Root/t1` ADD INDEX idx_c GLOBAL ON (c) COVER (k, v);").GetValueSync();
            UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());
        }

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();

        // f is large and d is small, so d has to probe f: by the point prefix c and the join key k in idx_c_k,
        // and then by the primary key in t1 for v.
        const TString query = R"(
            PRAGMA ydb.OptimizerHints = 'Rows(f # 300000) Bytes(f # 10000000) Rows(d # 2) Bytes(d # 20)';
            SELECT f.id, f.v
            FROM `/Root/t1` AS f
            JOIN `/Root/t2` AS d
              ON f.k = d.k
            WHERE f.c = 0 AND d.c = 0
            ORDER BY f.id;
        )";

        auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[17;[1017]];[42;[1042]];[100;[2000]]])");

        auto explained = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(explained.IsSuccess(), explained.GetIssues().ToString());
        const auto ast = TString{*explained.GetStats()->GetAst()};

        auto streamLookupsInto = [&](const TString& table) { return StreamLookupsInto(ast, table); };

        const auto indexLookups = streamLookupsInto("/Root/t1/idx_c_k/indexImplTable");
        if (!AllPointPrefixes) {
            // The index is chosen for the predicate only, so the read cannot be replaced by a lookup.
            UNIT_ASSERT_C(indexLookups.empty(), "expected no lookup by idx_c_k, ast:\n" << ast);
            if (CoveringPredicateIndex) {
                UNIT_ASSERT_C(ast.Contains("/Root/t1/idx_c/indexImplTable"), "expected the read redirected to idx_c, ast:\n" << ast);
            }
            return;
        }

        UNIT_ASSERT_VALUES_EQUAL_C(indexLookups.size(), 1, ast);
        UNIT_ASSERT_C(indexLookups[0].Contains(R"('('"Strategy" '"LookupJoinRows") '('"AllowNullKeysPrefixSize" '1))"),
                      "expected a lookup join by the point prefix, ast:\n" << ast);

        // The primary key has no nulls, so the main table lookup does not allow null keys: the rows without a match
        // in the index come with a missing key, which is not looked up.
        const auto mainLookups = streamLookupsInto("/Root/t1");
        UNIT_ASSERT_VALUES_EQUAL_C(mainLookups.size(), 1, ast);
        UNIT_ASSERT_C(mainLookups[0].Contains(R"('('"Strategy" '"LookupJoinRows") '('"AllowNullKeysPrefixSize" '0))"),
                      "expected a lookup join by the primary key, ast:\n" << ast);

        // The index lookup feeds the main table lookup directly, so a single lookup join consumes the result.
        size_t lookupJoins = 0;
        for (size_t pos = ast.find("(KqpIndexLookupJoin "); pos != TString::npos; pos = ast.find("(KqpIndexLookupJoin ", pos + 1)) {
            ++lookupJoins;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(lookupJoins, 1, ast);
    }

    Y_UNIT_TEST_TWIN(LookupJoinBackToMainTable, AllPointPrefixes) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(false);
        appConfig.MutableTableServiceConfig()->SetEnablePruneKeyColumns(true);
        appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
        appConfig.MutableTableServiceConfig()->SetEnableLookupJoinPointPrefixes(AllPointPrefixes);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                id Int64 NOT NULL,
                c  Int32,
                x  Int32,
                v  Int64,
                PRIMARY KEY (id)
            );

            CREATE TABLE `/Root/t2` (
                id Int64 NOT NULL,
                PRIMARY KEY (id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (i64 id = 0; id <= 100; ++id) {
                // Only id = 100 is excluded by the predicate c = 0.
                rows.AddListItem().BeginStruct()
                    .AddMember("id").Int64(id)
                    .AddMember("c").OptionalInt32(id == 100 ? 1 : 0)
                    .AddMember("x").OptionalInt32(id % 5)
                    .AddMember("v").OptionalInt64(id + 1000)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            // id = 100 is excluded by the predicate, id = 500 has no match.
            for (i64 id : {17, 42, 100, 500}) {
                rows.AddListItem().BeginStruct()
                    .AddMember("id").Int64(id)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t2", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        // BulkUpsert does not support tables with sync indexes, so the index is built after loading. The index covers
        // the read and fits the predicate, so the read is redirected to it. Its key is (c, x, id): x separates the point
        // prefix c from the join key id, so the index cannot be probed by the join key, and the main table can.
        schemeResult = session.ExecuteSchemeQuery("ALTER TABLE `/Root/t1` ADD INDEX idx_c_x GLOBAL ON (c, x) COVER (v);").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        const TString query = R"(
            PRAGMA ydb.OptimizerHints = 'Rows(f # 300000) Bytes(f # 10000000) Rows(d # 2) Bytes(d # 20)';
            SELECT f.id, f.v
            FROM `/Root/t1` AS f
            JOIN `/Root/t2` AS d
              ON f.id = d.id
            WHERE f.c = 0
            ORDER BY f.id;
        )";

        auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[17;[1017]];[42;[1042]]])");

        auto explained = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(explained.IsSuccess(), explained.GetIssues().ToString());
        const auto ast = TString{*explained.GetStats()->GetAst()};

        const auto mainLookups = StreamLookupsInto(ast, "/Root/t1");
        if (!AllPointPrefixes) {
            // The read stays on the index chosen for the predicate, which cannot be probed by the join key.
            UNIT_ASSERT_C(ast.Contains("/Root/t1/idx_c_x/indexImplTable"), "expected the read redirected to idx_c_x, ast:\n" << ast);
            UNIT_ASSERT_C(mainLookups.empty(), "expected no lookup into t1, ast:\n" << ast);
            return;
        }

        UNIT_ASSERT_VALUES_EQUAL_C(mainLookups.size(), 1, ast);
        UNIT_ASSERT_C(mainLookups[0].Contains(R"('('"Strategy" '"LookupJoinRows"))"), "expected a lookup join into t1, ast:\n" << ast);
        UNIT_ASSERT_C(!ast.Contains("/Root/t1/idx_c_x/indexImplTable"), "expected no read of idx_c_x, ast:\n" << ast);
    }

    Y_UNIT_TEST(LookupJoinByNonCoveringIndexNullablePk) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(false);
        appConfig.MutableTableServiceConfig()->SetEnablePruneKeyColumns(true);
        appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
        appConfig.MutableTableServiceConfig()->SetEnableLookupJoinPointPrefixes(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // The primary key is nullable, so the main table lookup has to allow null keys.
        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                id Int64,
                c  Int32,
                k  Int64,
                v  Int64,
                PRIMARY KEY (id)
            );

            CREATE TABLE `/Root/t2` (
                k Int64 NOT NULL,
                c Int32,
                PRIMARY KEY (k)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            TVector<std::tuple<std::optional<i64>, i32, i64, i64>> facts;
            for (i64 id = 0; id < 100; ++id) {
                facts.emplace_back(id, 0, id, id + 1000);
            }
            // A second match for k = 17, a row which the point prefix c = 0 excludes, and a row with a null primary key.
            facts.emplace_back(100, 0, 17, 2000);
            facts.emplace_back(101, 1, 42, 3000);
            facts.emplace_back(std::nullopt, 0, 42, 4000);
            for (const auto& [id, c, k, v] : facts) {
                rows.AddListItem().BeginStruct()
                    .AddMember("id").OptionalInt64(id)
                    .AddMember("c").OptionalInt32(c)
                    .AddMember("k").OptionalInt64(k)
                    .AddMember("v").OptionalInt64(v)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            // k = 500 has no match in the index.
            for (const auto& [k, c] : TVector<std::pair<i64, i32>>{{17, 0}, {42, 0}, {50, 1}, {500, 0}}) {
                rows.AddListItem().BeginStruct()
                    .AddMember("k").Int64(k)
                    .AddMember("c").OptionalInt32(c)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t2", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        schemeResult = session.ExecuteSchemeQuery("ALTER TABLE `/Root/t1` ADD INDEX idx_c_k GLOBAL ON (c, k);").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        const TString query = R"(
            PRAGMA ydb.OptimizerHints = 'Rows(f # 300000) Bytes(f # 10000000) Rows(d # 2) Bytes(d # 20)';
            SELECT f.id, f.v
            FROM `/Root/t1` AS f
            JOIN `/Root/t2` AS d
              ON f.k = d.k
            WHERE f.c = 0 AND d.c = 0
            ORDER BY f.v;
        )";

        auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[[17];[1017]];[[42];[1042]];[[100];[2000]];[#;[4000]]])");

        auto explained = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(explained.IsSuccess(), explained.GetIssues().ToString());
        const auto ast = TString{*explained.GetStats()->GetAst()};

        const auto indexLookups = StreamLookupsInto(ast, "/Root/t1/idx_c_k/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL_C(indexLookups.size(), 1, ast);
        UNIT_ASSERT_C(indexLookups[0].Contains(R"('('"Strategy" '"LookupJoinRows") '('"AllowNullKeysPrefixSize" '1))"),
                      "expected a lookup join by the point prefix, ast:\n" << ast);

        // The main table lookup allows a null primary key.
        const auto mainLookups = StreamLookupsInto(ast, "/Root/t1");
        UNIT_ASSERT_VALUES_EQUAL_C(mainLookups.size(), 1, ast);
        UNIT_ASSERT_C(mainLookups[0].Contains(R"('('"Strategy" '"LookupJoinRows") '('"AllowNullKeysPrefixSize" '1))"),
                      "expected a lookup join by a nullable primary key, ast:\n" << ast);

        // A lookup join drops the rows without a match in the index before the main table lookup.
        size_t lookupJoins = 0;
        for (size_t pos = ast.find("(KqpIndexLookupJoin "); pos != TString::npos; pos = ast.find("(KqpIndexLookupJoin ", pos + 1)) {
            ++lookupJoins;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(lookupJoins, 2, ast);
    }

    // Review check: which table a lookup join probes when the join key is the whole primary key and a non-covering
    // index has a point prefix followed by the same key. Prints the plan shape, asserts only the result.
    Y_UNIT_TEST(ReviewLookupJoinIndexVsPrimaryKey) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(false);
        appConfig.MutableTableServiceConfig()->SetEnablePruneKeyColumns(true);
        appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
        appConfig.MutableTableServiceConfig()->SetEnableLookupJoinPointPrefixes(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                id Int64 NOT NULL,
                c  Int32,
                v  Int64,
                PRIMARY KEY (id)
            );

            CREATE TABLE `/Root/t2` (
                id Int64 NOT NULL,
                PRIMARY KEY (id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (i64 id = 0; id <= 100; ++id) {
                rows.AddListItem().BeginStruct()
                    .AddMember("id").Int64(id)
                    .AddMember("c").OptionalInt32(id == 100 ? 1 : 0)
                    .AddMember("v").OptionalInt64(id + 1000)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (i64 id : {17, 42, 100, 500}) {
                rows.AddListItem().BeginStruct()
                    .AddMember("id").Int64(id)
                    .EndStruct();
            }
            rows.EndList();
            auto upsertResult = db.BulkUpsert("/Root/t2", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        // Non-covering: v is not in the index.
        schemeResult = session.ExecuteSchemeQuery("ALTER TABLE `/Root/t1` ADD INDEX idx_c_id GLOBAL ON (c, id);").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        const TString query = R"(
            PRAGMA ydb.OptimizerHints = 'Rows(f # 300000) Bytes(f # 10000000) Rows(d # 2) Bytes(d # 20)';
            SELECT f.id, f.v
            FROM `/Root/t1` AS f
            JOIN `/Root/t2` AS d
              ON f.id = d.id
            WHERE f.c = 0
            ORDER BY f.id;
        )";

        auto result = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[17;[1017]];[42;[1042]]])");

        auto explained = querySession.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(explained.IsSuccess(), explained.GetIssues().ToString());
        const auto ast = TString{*explained.GetStats()->GetAst()};

        size_t lookupJoins = 0;
        for (size_t pos = ast.find("(KqpIndexLookupJoin "); pos != TString::npos; pos = ast.find("(KqpIndexLookupJoin ", pos + 1)) {
            ++lookupJoins;
        }
        Cerr << "REVIEW-CHECK index lookups: " << StreamLookupsInto(ast, "/Root/t1/idx_c_id/indexImplTable").size()
             << ", main table lookups: " << StreamLookupsInto(ast, "/Root/t1").size()
             << ", lookup joins: " << lookupJoins << Endl;
        Cerr << "REVIEW-CHECK ast:\n" << ast << Endl;
    }

    Y_UNIT_TEST(LookupJoins_newRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);
        appConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(false);
        appConfig.MutableTableServiceConfig()->SetEnablePruneKeyColumns(true);
        appConfig.MutableTableServiceConfig()->SetEnableAutoIndexSelectionForIndexLookupJoin(true);
        appConfig.MutableTableServiceConfig()->SetEnableLookupJoinPointPrefixes(true);
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
                -- LookupJoin, PK left / Index21 right by the point prefix SubKey2 and Value1, stream lookup for Value2
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
            {true,  {"Index1_21/indexImplTable"}},
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

} // namespace NKqp
} // namespace NKikimr
