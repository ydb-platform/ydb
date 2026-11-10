#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>
#include "kqp_workload_queries.h"

#include <library/cpp/resource/resource.h>
#include <yql/essentials/public/langver/yql_langver.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYqlTests;

struct TWorkloadQuery {
    ui32 Id;
    TString Text;
};

// Store the externally visible status and optimizer counter deltas from each
// compile. Frontend fallback may still use new RBO; an optimizer fallback would
// change these counters and violate the compatibility contract.
struct TCompileResult {
    EStatus Status;
    TString Issues;
    std::pair<ui32, ui32> OptimizerCounters;
};

// AST success, syntax diagnostics or fallback prove an attempted frontend parse.
// This deliberately does not establish native operator lowering: the separate
// direct and canonical suites require stronger evidence.
size_t CountFrontendAttempts(const TCapturedLog& logs) {
    const auto text = logs.Snapshot();
    size_t count = 0;
    for (const TStringBuf marker : {"New YQL parser fallback:", "New YQL AST construction succeeded",
        "New YQL parser reported syntax errors"}) {
        for (size_t pos = text.find(marker); pos != TString::npos; pos = text.find(marker, pos + marker.size())) {
            ++count;
        }
    }
    return count;
}

// Run each configuration in its own database fixture, using the original schema
// and query text. Explain exercises compilation without requiring benchmark
// data or comparing results from empty tables.
TVector<TCompileResult> CompileWorkload(const TVector<TWorkloadQuery>& queries, EBenchType type,
    bool enableNewParser, bool tpchPerf)
{
    NKikimrConfig::TAppConfig appConfig;
    auto* config = appConfig.MutableTableServiceConfig();
    config->SetEnableNewYqlParser(enableNewParser);
    config->SetEnableNewRBO(true);
    // A frontend fallback must not require optimizer error fallback.
    config->SetEnableFallbackToYqlOptimizer(false);
    config->SetEnableInlineJoinFiltersAfterCBO(false);
    config->SetEnableFallbackOnMultipleStatements(false);
    config->SetAllowOlapDataQuery(true);
    config->SetDefaultLangVer(NYql::GetMaxLangVersion());
    config->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TCapturedLog logs(true);
    auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false).SetLogCapture(logs);
    settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
    settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
    TVector<TCompileResult> results;
    {
        TKikimrRunner kikimr(settings);
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        if (tpchPerf) {
            CreateTablesFromPath(tableSession, "schema/tpch.sql", /*useColumnStore=*/true);
        } else {
            CreateTablesFromPath(tableSession, BenchmarkSchemaPathPrefix[type], BenchmarkSchemaPath[type], /*useColumnStore=*/true);
        }

        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        for (const auto& query : queries) {
            Cerr << "New parser=" << enableNewParser << ", query=" << query.Id << Endl;
            const auto before = GetNewRBOCompileCounters(kikimr);
            if (tpchPerf) {
                // Keep the same API as TPCH_YDB_PERF in the RBO workload suite.
                auto result = tableSession.ExplainDataQuery(query.Text).GetValueSync();
                UNIT_ASSERT_C(!result.IsSuccess() || !result.GetPlan().empty(), "Missing plan for query " << query.Id);
                results.push_back({result.GetStatus(), result.GetIssues().ToString(), {}});
            } else {
                auto result = session.ExecuteQuery(query.Text, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
                if (result.IsSuccess()) {
                    UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan()
                        && !result.GetStats()->GetPlan()->empty(), "Missing plan for query " << query.Id);
                }
                results.push_back({result.GetStatus(), result.GetIssues().ToString(), {}});
            }
            const auto after = GetNewRBOCompileCounters(kikimr);
            results.back().OptimizerCounters = {after.first - before.first, after.second - before.second};
        }

        if (enableNewParser) {
            // Log records are delivered asynchronously. Check that the feature
            // was exercised, rather than only checking legacy query outcomes.
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            while (CountFrontendAttempts(logs) < queries.size() && TInstant::Now() < deadline) {
                Sleep(TDuration::MilliSeconds(10));
            }
            UNIT_ASSERT_C(CountFrontendAttempts(logs) >= queries.size(), "Missing frontend records: " << logs.Snapshot());
        }
    }
    if (!enableNewParser) {
        UNIT_ASSERT_VALUES_EQUAL(CountFrontendAttempts(logs), 0);
    }
    return results;
}

// Compare enabled/disabled frontend configurations query by query. A workload
// may already be unsupported by the selected optimizer, so compare failure
// statuses too instead of declaring every successful fallback a native pass.
void CheckWorkload(const TVector<TWorkloadQuery>& queries, EBenchType type, bool tpchPerf = false) {
    const auto baseline = CompileWorkload(queries, type, /*enableNewParser=*/false, tpchPerf);
    const auto fallback = CompileWorkload(queries, type, /*enableNewParser=*/true, tpchPerf);
    size_t successes = 0;
    for (size_t i = 0; i < queries.size(); ++i) {
        const auto context = TStringBuilder() << "Query " << queries[i].Id
            << "\nOriginal pipeline: " << baseline[i].Issues << "\nParser fallback: " << fallback[i].Issues;
        // Include known unsupported workload queries too: their failures must
        // remain failures of the configured optimizer, with the same status.
        if (baseline[i].Status != EStatus::SUCCESS && fallback[i].Status == EStatus::SUCCESS
            && baseline[i].Issues.Contains("YqlSelect unsupported: GROUP BY aliases")) {
            UNIT_ASSERT_VALUES_EQUAL_C(fallback[i].OptimizerCounters, (std::pair<ui32, ui32>{1, 0}), context);
            ++successes;
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(fallback[i].Status, baseline[i].Status, context);
        UNIT_ASSERT_VALUES_EQUAL_C(fallback[i].OptimizerCounters, baseline[i].OptimizerCounters, context);
        successes += baseline[i].Status == EStatus::SUCCESS;
    }
    UNIT_ASSERT_C(successes > 0, "No workload query compiled successfully in the baseline");
}

// Chunk only the execution schedule; preserve query IDs and full original SQL
// so diagnostics still identify the source benchmark case.
void CheckYqlWorkload(EBenchType type, ui32 first, ui32 last) {
    TVector<TWorkloadQuery> queries;
    for (ui32 id = first; id <= last; ++id) {
        queries.push_back({id, LoadBenchmarkQuery(type, id)});
    }
    CheckWorkload(queries, type);
}

// Frontend and optimizer fallback are distinct mechanisms. These checks allow
// the former while rejecting the latter through counters and its issue marker.
bool HasOptimizerFallbackIssue(const NYdb::NIssue::TIssues& issues) {
    for (const auto& issue : issues) {
        if (issue.GetMessage().find("Compilation with the new RBO failed") != std::string::npos) {
            return true;
        }
    }
    return false;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlParserFallback) {
    Y_UNIT_TEST(SharedVolatileBindingsUseExplicitFrontendFallback) {
        NKikimrConfig::TAppConfig app;
        auto* config = app.MutableTableServiceConfig();
        config->SetEnableNewYqlParser(true);
        config->SetEnableNewRBO(true);
        config->SetEnableFallbackToYqlOptimizer(false);
        config->SetEnableFallbackOnMultipleStatements(false);
        config->SetEnableNewRBOPhysicalStagePeephole(false);
        TCapturedLog logs(true);
        auto settings = TKikimrSettings(app).SetWithSampleTables(false).SetLogCapture(logs);
        settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
        settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
        TKikimrRunner kikimr(settings);
        auto table = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto created = table.ExecuteSchemeQuery(
            "CREATE TABLE `/Root/t` (id Int64 NOT NULL, PRIMARY KEY(id));").GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        const auto inserted = session.ExecuteQuery("UPSERT INTO `/Root/t` (id) VALUES (1), (2);",
            NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
        for (const bool sharedQuery : {true, false}) {
            const auto before = GetNewRBOCompileCounters(kikimr);
            const auto logStart = logs.Snapshot().size();
            const TString sql = sharedQuery
                ? "$q = SELECT id, CAST(RandomUuid(id) AS Utf8) AS value FROM `/Root/t`; "
                    "$alias = $q; SELECT * FROM $q ORDER BY id; SELECT * FROM $alias ORDER BY id;"
                : "$x = RandomUuid(1); SELECT CAST($x AS Utf8) AS a, CAST($x AS Utf8) AS b;";
            const auto result = session.ExecuteQuery(sql, NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            if (sharedQuery) {
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 2);
                UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
            } else {
                TResultSetParser parser(result.GetResultSet(0));
                UNIT_ASSERT(parser.TryNextRow());
                const auto a = parser.ColumnParser("a").GetUtf8();
                const auto b = parser.ColumnParser("b").GetUtf8();
                UNIT_ASSERT_VALUES_EQUAL(a, b);
            }
            const auto after = GetNewRBOCompileCounters(kikimr);
            UNIT_ASSERT_VALUES_EQUAL(after.first, before.first + 1);
            UNIT_ASSERT_VALUES_EQUAL(after.second, before.second);
            const TString reason = sharedQuery ? "Native multi-result named query sharing" : "shared scalar value bindings";
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            while (!logs.Snapshot().substr(logStart).Contains(reason) && TInstant::Now() < deadline) {
                Sleep(TDuration::MilliSeconds(10));
            }
            const auto evidence = logs.Snapshot().substr(logStart);
            UNIT_ASSERT_C(evidence.Contains(reason), evidence);
            UNIT_ASSERT_C(evidence.Contains("Legacy YQL graph compilation started"), evidence);
            UNIT_ASSERT_C(!evidence.Contains("New YQL native execution contract built"), evidence);
        }
    }

    Y_UNIT_TEST_TWIN(NativeAggregatesAndComputedJoinKeysMatchLegacyOptimizer, PhysicalStagePeephole) {
        const TVector<TString> queries = {
            "SELECT MIN_BY(value, id), MAX_BY(value, id), MIN_BY(id, value), MAX_BY(id, value) FROM `/Root/t`;",
            "SELECT g, MIN_BY(value, id), MAX_BY(id, value) FROM `/Root/t` GROUP BY g ORDER BY g;",
            "SELECT MIN_BY(value, id), MAX_BY(id, value) FROM `/Root/t` WHERE id < 0;",
            "SELECT MAX_BY(value, id) FROM `/Root/t` WHERE id <= 2;",
            "SELECT MIN_BY(id, value), MAX_BY(id, value) FROM `/Root/t` WHERE value IS NULL;",
            "SELECT MIN_BY(value, id), MAX_BY(value, id) FROM `/Root/t` WHERE value IS NULL;",
            "SELECT MIN_BY(id, NULL), MAX_BY(NULL, id) FROM `/Root/t`;",
            "SELECT MIN_BY(AsList(id, id + 1), value), MAX_BY(AsList(id, id + 1), value) FROM `/Root/t`;",
            "SELECT MIN_BY(value, id), MAX_BY(id, value), COUNT(DISTINCT value) FROM `/Root/t`;",
            "SELECT g, MAX_BY(value, id), COUNT(DISTINCT value) FROM `/Root/t` GROUP BY g ORDER BY g;",
            "SELECT g, GROUPING(g) AS grouping, MIN_BY(value, id), MAX_BY(id, value) FROM `/Root/t` "
                "GROUP BY ROLLUP(g) ORDER BY grouping, g;",
            "SELECT l.id AS a, r.id AS b, s.id AS c FROM `/Root/t` AS l "
                "LEFT JOIN `/Root/t` AS r ON CAST(l.id AS Decimal(10, 0)) = CAST(r.id AS Decimal(22, 9)) "
                "LEFT JOIN `/Root/t` AS s ON CAST(l.id + 2 AS Decimal(10, 0)) = CAST(s.id AS Decimal(22, 9)) ORDER BY a;",
            "SELECT Ensure(id, id > 0, 'positive') FROM `/Root/t` ORDER BY id;",
            "SELECT id, [10, 20][id - 2], [10, 20][CAST(id - 2 AS Uint32?)], "
                "[10, 20][CAST(id AS Double) / 2] FROM `/Root/t` ORDER BY id;",
            // The established optimizer is an independent oracle for tuple
            // NULL semantics when focused lowering replaces full peephole.
            "SELECT id, (id, value) IN ((1, 'b'), (2, NULL)), "
                "(id, value) NOT IN ((3, 'a'), (4, NULL)), "
                "((id, value), id) IN (((1, 'b'), 1), ((2, NULL), 2)) "
                "FROM `/Root/t` ORDER BY id;",
        };
        TVector<TString> baseline;
        for (const bool native : {false, true}) {
            NKikimrConfig::TAppConfig app;
            auto* config = app.MutableTableServiceConfig();
            config->SetEnableNewYqlParser(native);
            // Legacy RBO does not import MIN_BY/MAX_BY; use the established
            // optimizer as a result oracle, and require direct RBO for native.
            config->SetEnableNewRBO(native);
            config->SetEnableNewRBOPhysicalStagePeephole(PhysicalStagePeephole);
            config->SetEnableFallbackToYqlOptimizer(false);
            TCapturedLog logs(true);
            auto settings = TKikimrSettings(app).SetWithSampleTables(false).SetLogCapture(logs);
            settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
            settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
            TKikimrRunner kikimr(settings);
            auto table = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto schema = table.ExecuteSchemeQuery("CREATE TABLE `/Root/t` "
                "(id Int64 NOT NULL, g Int32 NOT NULL, value String, PRIMARY KEY(id));").GetValueSync();
            UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
            auto rows = TValueBuilder().BeginList()
                .AddListItem().BeginStruct().AddMember("id").Int64(1).AddMember("g").Int32(1).AddMember("value").OptionalString("b").EndStruct()
                .AddListItem().BeginStruct().AddMember("id").Int64(2).AddMember("g").Int32(1).AddMember("value").EmptyOptional(EPrimitiveType::String).EndStruct()
                .AddListItem().BeginStruct().AddMember("id").Int64(3).AddMember("g").Int32(1).AddMember("value").OptionalString("a").EndStruct()
                .AddListItem().BeginStruct().AddMember("id").Int64(4).AddMember("g").Int32(2).AddMember("value").OptionalString("c").EndStruct()
                .EndList().Build();
            auto data = kikimr.GetTableClient().BulkUpsert("/Root/t", std::move(rows)).GetValueSync();
            UNIT_ASSERT_C(data.IsSuccess(), data.GetIssues().ToString());
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            const auto start = logs.Snapshot().size();
            for (size_t i = 0; i < queries.size(); ++i) {
                const auto result = session.ExecuteQuery(queries[i], NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), queries[i] << ": " << result.GetIssues().ToString());
                const auto actual = FormatResultSetYson(result.GetResultSet(0));
                if (!native) { baseline.push_back(actual); }
                else { UNIT_ASSERT_VALUES_EQUAL_C(actual, baseline[i], queries[i]); }
            }
            if (native) {
                const auto text = logs.Snapshot().substr(start);
                const TString marker = "New YQL native execution contract built";
                size_t count = 0;
                for (size_t pos = text.find(marker); pos != TString::npos; pos = text.find(marker, pos + marker.size())) { ++count; }
                UNIT_ASSERT_VALUES_EQUAL_C(count, queries.size(), text);
                UNIT_ASSERT_C(!text.Contains("New YQL parser fallback:"), text);
            }
        }
    }

    Y_UNIT_TEST(DeclaredParametersAndNativeWritesPreserveRowsAndIndexes) {
        TString groupedBaseline;
        for (const bool enable : {false, true}) {
            NKikimrConfig::TAppConfig app;
            auto* config = app.MutableTableServiceConfig();
            config->SetEnableNewYqlParser(enable);
            config->SetEnableNewRBO(true);
            config->SetEnableFallbackToYqlOptimizer(false);
            config->SetEnableFallbackOnMultipleStatements(false);
            config->SetEnableIndexStreamWrite(true);
            config->SetEnableAstCache(false);
            config->SetCompileQueryCacheSize(0);
            TCapturedLog logs(true);
            auto settings = TKikimrSettings(app).SetWithSampleTables(false).SetLogCapture(logs);
            settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
            settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
            TKikimrRunner kikimr(settings);
            auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto schema = tableSession.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/t` (id Int64 NOT NULL, value String, a Int32 NOT NULL, "
                "PRIMARY KEY(id), INDEX by_value GLOBAL ON (value));").GetValueSync();
            UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            const auto start = logs.Snapshot().size();
            auto params = TParamsBuilder().AddParam("$items").BeginList()
                .AddListItem().BeginStruct().AddMember("id").Int64(1).AddMember("value").OptionalString("a")
                    .AddMember("a").Int32(1).EndStruct()
                .AddListItem().BeginStruct().AddMember("id").Int64(2).AddMember("value").OptionalString("b")
                    .AddMember("a").Int32(2).EndStruct()
                .EndList().Build().Build();
            const auto insert = session.ExecuteQuery(
                "DECLARE $items AS List<Struct<'id':Int64,'value':String?,'a':Int32>>; "
                "INSERT INTO `/Root/t` SELECT * FROM AS_TABLE($items);",
                NQuery::TTxControl::BeginTx().CommitTx(), params).GetValueSync();
            UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
            UNIT_ASSERT(insert.GetResultSets().empty());
            const auto execute = [&](const TString& sql) {
                auto result = session.ExecuteQuery(sql, NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
                return result;
            };
            execute("INSERT INTO `/Root/t` (id, value, a) SELECT id + 10, value, a FROM `/Root/t`;");
            execute("UPDATE `/Root/t` SET value = 'changed', a = a + 10 WHERE id = 1;");
            execute("DELETE FROM `/Root/t` WHERE id = 2;");
            const auto rows = execute("SELECT id, value, a FROM `/Root/t` ORDER BY id;");
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(rows.GetResultSet(0)),
                "[[1;[\"changed\"];11];[11;[\"a\"];1];[12;[\"b\"];2]]");
            const auto uuids = execute("SELECT CAST(RandomUuid(id) AS Utf8) FROM `/Root/t`;");
            TResultSetParser uuidRows(uuids.GetResultSet(0));
            THashSet<TString> uniqueUuids;
            while (uuidRows.TryNextRow()) {
                const auto value = uuidRows.ColumnParser(0).GetUtf8();
                UNIT_ASSERT_VALUES_EQUAL(value.size(), 36);
                uniqueUuids.insert(TString(value));
            }
            UNIT_ASSERT_VALUES_EQUAL(uniqueUuids.size(), 3);
            const auto duplicate = session.ExecuteQuery(
                "INSERT INTO `/Root/t` SELECT 1 AS id, 'duplicate' AS value, 0 AS a;",
                NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!duplicate.IsSuccess());
            if (enable) {
                const TString marker = "New YQL native execution contract built";
                size_t count = 0;
                const auto deadline = TInstant::Now() + TDuration::Seconds(10);
                TString text;
                do {
                    text = logs.Snapshot().substr(start);
                    count = 0;
                    for (size_t pos = text.find(marker); pos != TString::npos; pos = text.find(marker, pos + marker.size())) { ++count; }
                    if (count < 7) { Sleep(TDuration::MilliSeconds(10)); }
                } while (count < 7 && TInstant::Now() < deadline);
                UNIT_ASSERT_VALUES_EQUAL_C(count, 7, text);
                UNIT_ASSERT_C(!text.Contains("New YQL parser fallback:"), text);
            }
            // Read the secondary index explicitly; VIEW itself can use the legacy frontend.
            const auto indexed = execute("SELECT id FROM `/Root/t` VIEW by_value WHERE value = 'changed';");
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(indexed.GetResultSet(0)), "[[1]]");
            for (const TString sql : {
                "DECLARE $_ AS Int64; SELECT 1;",
                "INSERT INTO `/Root/t` SELECT 'missing key' AS value, 1 AS a;",
                "INSERT INTO `/Root/t` SELECT 'wrong type' AS id, 1 AS a;",
                "UPDATE `/Root/t` SET id = id + 1;"}) {
                const auto invalid = session.ExecuteQuery(sql, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                UNIT_ASSERT_C(!invalid.IsSuccess(), sql);
            }
            execute("UPDATE `/Root/t` ON SELECT id, a + 1 AS a FROM `/Root/t` WHERE id = 1;");
            const auto updated = execute("SELECT a FROM `/Root/t` WHERE id = 1;");
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(updated.GetResultSet(0)), "[[12]]");
            auto groupParams = TParamsBuilder().AddParam("$x").Int64(5).Build().Build();
            const auto grouped = session.ExecuteQuery(
                "PRAGMA YqlSelectAllowUnnamedGroupByExpr; "
                "DECLARE $x AS Int64; SELECT $x AS k, SUM(id + $x) FROM `/Root/t` GROUP BY $x;",
                NQuery::TTxControl::BeginTx().CommitTx(), groupParams).GetValueSync();
            UNIT_ASSERT_C(grouped.IsSuccess(), grouped.GetIssues().ToString());
            if (!enable) { groupedBaseline = FormatResultSetYson(grouped.GetResultSet(0)); }
            else { UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(grouped.GetResultSet(0)), groupedBaseline); }

            const auto duplicateDeclare = session.ExecuteQuery(
                "DECLARE $x AS Int64; DECLARE $x AS String; SELECT $x + 1;",
                NQuery::TTxControl::BeginTx().CommitTx(), groupParams).GetValueSync();
            UNIT_ASSERT_C(duplicateDeclare.IsSuccess(), duplicateDeclare.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(duplicateDeclare.GetResultSet(0)), "[[6]]");

            const auto decimalSchema = tableSession.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/d` (k Decimal(7,0), id Int64 NOT NULL, PRIMARY KEY(k));").GetValueSync();
            UNIT_ASSERT_C(decimalSchema.IsSuccess(), decimalSchema.GetIssues().ToString());
            const auto decimalData = tableSession.ExecuteDataQuery(
                "UPSERT INTO `/Root/d` (k,id) VALUES (NULL,1),(Decimal('2',7,0),2);",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(decimalData.IsSuccess(), decimalData.GetIssues().ToString());
            const auto nullBound = execute("SELECT id FROM `/Root/d` WHERE k IS NULL;");
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(nullBound.GetResultSet(0)), "[[1]]");
        }
    }

    Y_UNIT_TEST(NativeValuesWriteModesAndReturning) {
        TVector<TString> baseline;
        for (const bool enable : {false, true}) {
            NKikimrConfig::TAppConfig app;
            auto* config = app.MutableTableServiceConfig();
            config->SetEnableNewYqlParser(enable);
            config->SetEnableNewRBO(true);
            config->SetEnableNewRBOPhysicalStagePeephole(false);
            config->SetEnableFallbackToYqlOptimizer(false);
            config->SetEnableIndexStreamWrite(true);
            config->SetEnableAstCache(false);
            config->SetCompileQueryCacheSize(0);
            TCapturedLog logs(true);
            auto settings = TKikimrSettings(app).SetWithSampleTables(false).SetLogCapture(logs);
            settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
            settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
            TKikimrRunner kikimr(settings);
            auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto schema = tableSession.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/w` (id Uint64 NOT NULL, value String, extra Int32, "
                "PRIMARY KEY(id), INDEX by_value GLOBAL ON(value));").GetValueSync();
            UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            const auto start = logs.Snapshot().size();
            const TVector<TString> queries = {
                "INSERT INTO w (id,value,extra) VALUES (1,'one',10),(2,'two',20);",
                "UPSERT INTO w (id,value) VALUES (1,'changed') RETURNING id,value,extra;",
                "REPLACE INTO w (id,value) SELECT 1u,'replaced' RETURNING *;",
                "UPDATE w ON (id,extra) VALUES (1,30),(99,99) RETURNING id,extra;",
                "DELETE FROM w ON (id) VALUES (2) RETURNING id,value,extra;",
                "INSERT OR REVERT INTO w (id,value) VALUES (3,'three');",
                "DELETE FROM w WHERE id=3 RETURNING id,value;",
                "UPDATE w SET extra=extra+1 WHERE id=1 RETURNING id,extra;",
                "SELECT id,value,extra FROM w ORDER BY id;",
            };
            size_t index = 0;
            for (const auto& sql : queries) {
                const auto result = session.ExecuteQuery(sql, NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
                TString actual;
                for (const auto& rows : result.GetResultSets()) { actual += FormatResultSetYson(rows); }
                if (!enable) { baseline.push_back(actual); }
                else { UNIT_ASSERT_VALUES_EQUAL_C(actual, baseline.at(index), sql); }
                if (index == 1) { UNIT_ASSERT_VALUES_EQUAL(actual, "[[1u;[\"changed\"];[10]]]"); }
                if (index == 8) { UNIT_ASSERT_VALUES_EQUAL(actual, "[[1u;[\"replaced\"];[31]]]"); }
                ++index;
            }
            if (enable) {
                const TString marker = "New YQL native execution contract built";
                const auto deadline = TInstant::Now() + TDuration::Seconds(10);
                TString text;
                size_t count = 0;
                do {
                    text = logs.Snapshot().substr(start);
                    count = 0;
                    for (size_t pos = text.find(marker); pos != TString::npos; pos = text.find(marker, pos + marker.size())) { ++count; }
                    if (count < queries.size()) { Sleep(TDuration::MilliSeconds(10)); }
                } while (count < queries.size() && TInstant::Now() < deadline);
                UNIT_ASSERT_VALUES_EQUAL_C(count, queries.size(), text);
                UNIT_ASSERT_C(!text.Contains("New YQL parser fallback:") && !text.Contains("New YQL frontend fallback:"), text);
            }
            const auto indexed = session.ExecuteQuery("SELECT id FROM w VIEW by_value WHERE value='replaced';",
                NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(indexed.IsSuccess(), indexed.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(indexed.GetResultSet(0)), "[[1u]]");
        }
    }

    // Use nonempty tables and compare actual result sets for both storage kinds.
    // This catches value/order/NULL mistakes that an explain-only plan check misses;
    // the native success log requirement additionally guards against silent fallback.
    Y_UNIT_TEST_QUAD(DirectRboResultsMatchLegacyFrontend, ColumnStore, PhysicalStagePeephole) {
        const TVector<TString> queries = {
            // Real rows, NULLs and duplicate groups are compared to the existing
            // frontend; the log assertion below requires every new path result.
            "SELECT value, SUM(id) AS s, COUNT(*), COUNT(DISTINCT id), AVG(id) FROM `/Root/t` GROUP BY value ORDER BY value;",
            "SELECT SUM(id), COUNT(*) FROM `/Root/t` WHERE id < 0;",
            "SELECT STDDEV_SAMP(id), STDDEV_SAMPLE(id) FROM `/Root/t`;",
            "SELECT STDDEV_SAMP(id) FROM `/Root/t` WHERE id = 1;",
            "SELECT STDDEV_SAMP(id) FROM `/Root/t` WHERE id < 0;",
            "SELECT id FROM `/Root/t` WHERE Re2::Match('a.*')(value) ORDER BY id;",
            "SELECT id FROM `/Root/t` WHERE Re2::Grep('a', NULL)(value) ORDER BY id;",
            "SELECT id AS n FROM `/Root/t` WHERE id > 2 UNION ALL SELECT id AS n FROM `/Root/t` WHERE id < 3 ORDER BY n DESC LIMIT 2 OFFSET 1;",
            "SELECT id FROM `/Root/t` UNION SELECT id FROM `/Root/t` ORDER BY id DESC LIMIT 1;",
            "SELECT id FROM `/Root/t` INTERSECT SELECT id FROM `/Root/t` WHERE id < 4 "
                "EXCEPT SELECT id FROM `/Root/t` WHERE id = 1 ORDER BY id DESC LIMIT 1;",
            "SELECT DISTINCT value AS z, id AS a FROM `/Root/t` UNION "
                "SELECT value AS z, id AS a FROM `/Root/t` ORDER BY a DESC, z LIMIT 2;",
            "SELECT value AS z, id AS a FROM `/Root/t` INTERSECT "
                "SELECT value AS z, id AS a FROM `/Root/t` ORDER BY a + 1 DESC, z LIMIT 2;",
            "SELECT value, GROUPING(value), SUM(id) FROM `/Root/t` GROUP BY ROLLUP(value) ORDER BY GROUPING(value), value;",
            "SELECT id, SUM(id) OVER (), RANK() OVER (ORDER BY id) FROM `/Root/t` ORDER BY id;",
            "SELECT id FROM (SELECT id FROM `/Root/t` UNION ALL SELECT id FROM `/Root/t` WHERE id <= 2) AS u ORDER BY id;",
            "SELECT l.id, (SELECT MAX(r.id) FROM `/Root/t` AS r WHERE r.id = l.id) FROM `/Root/t` AS l ORDER BY l.id;",
            "SELECT l.id FROM `/Root/t` AS l WHERE EXISTS (SELECT r.id FROM `/Root/t` AS r WHERE r.id = l.id) ORDER BY l.id;",
            "SELECT COUNT(*) FROM `/Root/t` AS l WHERE EXISTS (SELECT * FROM `/Root/t` AS r "
                "WHERE r.id = l.id AND r.value = l.value) AND NOT EXISTS "
                "(SELECT * FROM `/Root/t` AS r WHERE r.id > l.id);",
            "SELECT id FROM `/Root/t` WHERE id NOT IN (SELECT id FROM `/Root/t` WHERE id > 2) ORDER BY id;",
            "SELECT id FROM `/Root/t` WHERE id IN ((SELECT id FROM `/Root/t` WHERE id > 2)) ORDER BY id;",
            "SELECT id + 1 AS n, value FROM `/Root/t` WHERE id > 0 ORDER BY id DESC LIMIT 2 OFFSET 1;",
            "SELECT id FROM `/Root/t` WHERE value IS NULL ORDER BY id;",
            "SELECT id, id IN (1), id NOT IN (2), value IN ('one'), NULL IN (1), 1 IN (NULL), "
                "(1, 2) IN ((1, 2)) FROM `/Root/t` ORDER BY id;",
            "SELECT id, (id, value) IN ((1, 'a'), (3, NULL)), "
                "(id, value) NOT IN ((2, 'a'), (4, NULL)), "
                "((id, value), id) IN (((1, 'a'), 1), ((3, NULL), 3)) "
                "FROM `/Root/t` ORDER BY id;",
            "SELECT DISTINCT value AS v FROM `/Root/t` ORDER BY v;",
            "SELECT value, COUNT(DISTINCT id), SUM(id) FROM `/Root/t` GROUP BY value "
                "HAVING SUM(id) > 0 ORDER BY COUNT(DISTINCT id), value;",
            "SELECT id FROM `/Root/t` WHERE CAST(id AS Decimal(12, 2)) "
                "BETWEEN Decimal('1.00', 12, 2) AND Decimal('2.00', 12, 2) ORDER BY id;",
            // Chained DISTINCT set operations must preserve positional values
            // and NULL equality when their internal output IDs are reordered.
            "SELECT * FROM (SELECT value AS z, id AS a FROM `/Root/t` "
                "INTERSECT SELECT value AS z, id AS a FROM `/Root/t` WHERE id < 4 "
                "EXCEPT SELECT value AS z, id AS a FROM `/Root/t` WHERE id = 1) AS u ORDER BY a;",
            "SELECT d.k FROM (SELECT id + 2 AS k FROM `/Root/t`) AS d ORDER BY d.k;",
            "WITH c AS (SELECT id + 1 AS k FROM `/Root/t`) SELECT l.k AS a, r.k AS b "
                "FROM c AS l LEFT JOIN c AS r ON l.k = r.k ORDER BY a;",
            "SELECT l.id AS a, r.id AS b FROM `/Root/t` AS l LEFT JOIN `/Root/t` AS r ON l.id = r.id AND r.id > 2 ORDER BY a;",
            "SELECT * FROM `/Root/t` ORDER BY id;",
            "SELECT l.id, r.id FROM `/Root/t` AS l JOIN `/Root/t` AS r ON l.id = r.id ORDER BY l.id;",
            "SELECT [1, 2][0u] AS v, CAST(3 AS Int64) + 2 AS n;",
            "SELECT id, [10, 20][id - 2], [10, 20][CAST(id - 2 AS Uint32?)], "
                "[10, 20][CAST(id AS Double) / 2] FROM `/Root/t` ORDER BY id;",
            "SELECT 1, 2 AS column0, 3;",
            "SELECT 1 AS a; SELECT id AS b FROM `/Root/t` ORDER BY id LIMIT 2;",
            // Original workload helper declarations now pass through every
            // native phase, even when only one helper is used by the query.
            "$to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); }; "
                "$to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); }; "
                "$round = ($x, $y) -> { return $x; }; "
                "SELECT $round($to_decimal(id), 2) AS n FROM `/Root/t` ORDER BY id;",
            "$n = 1; $f = ($x) -> { $v = $x + $n; return $v; }; $n = 100; "
                "$q = (SELECT $f(id) AS k FROM `/Root/t`); $alias = $q; "
                "SELECT l.k AS a, r.k AS b FROM $q AS l LEFT JOIN $alias AS r ON l.k = r.k ORDER BY a;",
            "PRAGMA AnsiImplicitCrossJoin; $q = SELECT id FROM `/Root/t`; $r = (SELECT id + 1 AS k FROM $q); "
                "WITH c AS (SELECT k FROM $r) SELECT k FROM c ORDER BY k;",
            "$id = ($x) -> ($x); $make = ($x) -> (($y) -> ($x + $y)); "
                "SELECT $id(id) AS n, $id(value) AS v, $make(id)(2) AS k FROM `/Root/t` ORDER BY n;",
        };
        TVector<TVector<TString>> baseline;
        for (const bool enable : {false, true}) {
            NKikimrConfig::TAppConfig appConfig;
            auto* config = appConfig.MutableTableServiceConfig();
            config->SetEnableNewYqlParser(enable);
            config->SetEnableAstCache(enable);
            config->SetEnableNewRBO(true);
            config->SetEnableNewRBOPhysicalStagePeephole(PhysicalStagePeephole);
            config->SetEnableFallbackToYqlOptimizer(false);
            config->SetEnableFallbackOnMultipleStatements(false);
            config->SetAllowOlapDataQuery(true);
            // A service cap smaller than the SQL LIMIT checks that host wrapping
            // preserves the SQL OFFSET and applies the external cap exactly once.
            config->MutableQueryLimits()->SetResultRowsLimit(2);
            TCapturedLog logs(true);
            auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false).SetLogCapture(logs);
            settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
            settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
            TKikimrRunner kikimr(settings);
            auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto scheme = tableSession.ExecuteSchemeQuery(TStringBuilder()
                << "CREATE TABLE `/Root/t` (id Int64 NOT NULL, value String, PRIMARY KEY(id))"
                << (ColumnStore ? " WITH (STORE = COLUMN);" : ";")).GetValueSync();
            UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            auto rows = TValueBuilder().BeginList()
                .AddListItem().BeginStruct().AddMember("id").Int64(1).AddMember("value").OptionalString("a").EndStruct()
                .AddListItem().BeginStruct().AddMember("id").Int64(2).AddMember("value").OptionalString("a").EndStruct()
                .AddListItem().BeginStruct().AddMember("id").Int64(3).AddMember("value").EmptyOptional(EPrimitiveType::String).EndStruct()
                .AddListItem().BeginStruct().AddMember("id").Int64(4).AddMember("value").OptionalString("b").EndStruct()
                .EndList().Build();
            auto data = kikimr.GetTableClient().BulkUpsert("/Root/t", std::move(rows)).GetValueSync();
            UNIT_ASSERT_C(data.IsSuccess(), data.GetIssues().ToString());
            const auto legacyLogStart = logs.Snapshot().size();
            for (size_t i = 0; i < queries.size(); ++i) {
                const auto result = session.ExecuteQuery(queries[i], NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), queries[i] << ": " << result.GetIssues().ToString());
                TVector<TString> values;
                for (const auto& set : result.GetResultSets()) {
                    values.push_back(FormatResultSetYson(set));
                    for (const auto& column : set.GetColumnsMeta()) {
                        values.push_back(TStringBuilder() << column.Name << ":" << column.Type.ToString());
                    }
                }
                if (!enable) { baseline.push_back(std::move(values)); }
                else { UNIT_ASSERT_VALUES_EQUAL_C(values, baseline[i], queries[i]); }
            }
            // Supply parameter types through the SDK rather than a DECLARE preamble,
            // so this query exercises native parameter binding and scalar annotation.
            auto params = TParamsBuilder().AddParam("$offset").Int64(10).Build().Build();
            const auto parameterized = session.ExecuteQuery(
                "SELECT id + $offset AS n FROM `/Root/t` WHERE id + $offset = 11;",
                NQuery::TTxControl::BeginTx().CommitTx(), params).GetValueSync();
            UNIT_ASSERT_C(parameterized.IsSuccess(), parameterized.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(parameterized.GetResultSet(0)), "[[11]]");
            const auto capped = tableSession.ExecuteDataQuery(
                "SELECT id FROM `/Root/t` ORDER BY id LIMIT 4 OFFSET 1;",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(capped.IsSuccess(), capped.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(capped.GetResultSet(0)), "[[2];[3]]");
            UNIT_ASSERT(capped.GetResultSet(0).Truncated());
            if (enable) {
                // The legacy frontend currently duplicates an endpoint for
                // these independently sorted named queries. Check SQL results
                // directly instead of treating that behavior as the oracle.
                const auto localLimits = session.ExecuteQuery(
                    "$first = SELECT id FROM `/Root/t` ORDER BY id DESC LIMIT 1; "
                    "$last = SELECT id FROM `/Root/t` ORDER BY id LIMIT 1; "
                    "SELECT id FROM $first UNION ALL SELECT id FROM $last ORDER BY id;",
                    NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(localLimits.IsSuccess(), localLimits.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(localLimits.GetResultSet(0)), "[[1];[4]]");
                // Legacy RBO import does not consistently widen set operands.
                // Verify values, NULLs and projection order on the native path.
                const auto widened = session.ExecuteQuery(
                    "SELECT DISTINCT value AS z, id AS a FROM `/Root/t` UNION "
                    "SELECT value AS z, CAST(id AS Int32) AS a FROM `/Root/t` ORDER BY a DESC, z LIMIT 2;",
                    NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(widened.IsSuccess(), widened.GetIssues().ToString());
                const auto& widenedSet = widened.GetResultSet(0);
                UNIT_ASSERT_VALUES_EQUAL(widenedSet.GetColumnsMeta()[0].Name, "z");
                UNIT_ASSERT_VALUES_EQUAL(widenedSet.GetColumnsMeta()[1].Name, "a");
                TResultSetParser parser(widenedSet);
                UNIT_ASSERT(parser.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser("z").GetOptionalString(), "b");
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser("a").GetOptionalInt64(), 4);
                UNIT_ASSERT(parser.TryNextRow());
                UNIT_ASSERT(!parser.ColumnParser("z").GetOptionalString());
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser("a").GetOptionalInt64(), 3);
                UNIT_ASSERT(!parser.TryNextRow());
                const auto successCount = [&] {
                    const auto text = logs.Snapshot();
                    constexpr TStringBuf marker = "New YQL RBO lowering succeeded";
                    size_t count = 0;
                    for (size_t pos = text.find(marker); pos != TString::npos; pos = text.find(marker, pos + marker.size())) { ++count; }
                    return count;
                };
                // One extra result in the multi-statement query, plus the
                // parameterized query, table-service cap, local LIMITs and widening.
                const auto expected = queries.size() + 5;
                const auto deadline = TInstant::Now() + TDuration::Seconds(10);
                while (successCount() < expected && TInstant::Now() < deadline) {
                    Sleep(TDuration::MilliSeconds(10));
                }
                UNIT_ASSERT_C(successCount() >= expected, logs.Snapshot());
                const auto nativeLogs = logs.Snapshot().substr(legacyLogStart);
                UNIT_ASSERT_C(!nativeLogs.Contains("New YQL frontend fallback:"), nativeLogs);
                UNIT_ASSERT_C(!nativeLogs.Contains("Legacy YQL graph compilation started"), nativeLogs);
            }
        }
    }

    Y_UNIT_TEST(NativeCatalogAndResultContract) {
        NKikimrConfig::TAppConfig appConfig;
        auto* config = appConfig.MutableTableServiceConfig();
        config->SetEnableNewYqlParser(true);
        config->SetEnableNewRBO(true);
        config->SetEnableAstCache(true);
        config->SetEnableFallbackToYqlOptimizer(false);
        config->SetEnableFallbackOnMultipleStatements(false);
        TCapturedLog logs(true);
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false).SetLogCapture(logs);
        settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
        settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
        TKikimrRunner kikimr(settings);
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto scheme = tableSession.ExecuteSchemeQuery(
            "CREATE TABLE `/Root/a` (id Int64 NOT NULL, value String, PRIMARY KEY(id));"
            "CREATE TABLE `/Root/b` (id Int64 NOT NULL, PRIMARY KEY(id));").GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto rows = TValueBuilder().BeginList()
            .AddListItem().BeginStruct().AddMember("id").Int64(1).AddMember("value").OptionalString("one").EndStruct()
            .AddListItem().BeginStruct().AddMember("id").Int64(2).AddMember("value").OptionalString("two").EndStruct()
            .EndList().Build();
        auto write = kikimr.GetTableClient().BulkUpsert("/Root/a", std::move(rows)).GetValueSync();
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        auto keys = TValueBuilder().BeginList().AddListItem().BeginStruct().AddMember("id").Int64(2).EndStruct().EndList().Build();
        auto writeKeys = kikimr.GetTableClient().BulkUpsert("/Root/b", std::move(keys)).GetValueSync();
        UNIT_ASSERT_C(writeKeys.IsSuccess(), writeKeys.GetIssues().ToString());

        const auto start = logs.Snapshot().size();
        auto joined = session.ExecuteQuery(
            "PRAGMA TablePathPrefix = '/Root'; WITH c AS (SELECT id FROM b) "
            "SELECT a.id AS id, a.value AS value FROM a JOIN c ON a.id = c.id ORDER BY a.id;",
            NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(joined.IsSuccess(), joined.GetIssues().ToString());
        CompareYson(FormatResultSetYson(joined.GetResultSet(0)), R"([[2;["two"]]])");
        UNIT_ASSERT_VALUES_EQUAL(joined.GetResultSet(0).GetColumnsMeta()[0].Name, "id");
        UNIT_ASSERT_VALUES_EQUAL(joined.GetResultSet(0).GetColumnsMeta()[1].Name, "value");
        // Qualified stars are supported natively even though the legacy
        // forced-YqlSelect translator cannot supply a comparison result.
        auto mixedStar = session.ExecuteQuery("SELECT 1 AS z, a.* FROM `/Root/a` AS a ORDER BY id;",
            NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(mixedStar.IsSuccess(), mixedStar.GetIssues().ToString());
        CompareYson(FormatResultSetYson(mixedStar.GetResultSet(0)), R"([[1;1;["one"]];[1;2;["two"]]])");
        auto qualifiedStar = session.ExecuteQuery(
            "SELECT a.* FROM `/Root/a` AS a JOIN `/Root/b` AS b ON a.id = b.id ORDER BY a.id;",
            NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(qualifiedStar.IsSuccess(), qualifiedStar.GetIssues().ToString());
        CompareYson(FormatResultSetYson(qualifiedStar.GetResultSet(0)), R"([[2;["two"]]])");
        auto scan = kikimr.GetTableClient().StreamExecuteScanQuery(
            "SELECT id FROM `/Root/b` ORDER BY id;").GetValueSync();
        UNIT_ASSERT_C(scan.IsSuccess(), scan.GetIssues().ToString());
        CompareYson(StreamResultToYson(scan), "[[2]]");
        auto grouped = session.ExecuteQuery(
            "SELECT key, COUNT(*) AS n FROM `/Root/a` GROUP BY id % 2 AS key ORDER BY key;",
            NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(grouped.IsSuccess(), grouped.GetIssues().ToString());
        CompareYson(FormatResultSetYson(grouped.GetResultSet(0)), "[[[0];1u];[[1];1u]]");
        const auto nativeLogs = logs.Snapshot().substr(start);
        UNIT_ASSERT_C(nativeLogs.Contains("New YQL native execution contract built"), nativeLogs);
        UNIT_ASSERT_C(!nativeLogs.Contains("Legacy YQL graph compilation started"), nativeLogs);

        // Table information in the native contract must invalidate cached plans
        // after a schema change, including their cached result column metadata.
        const TString star = "SELECT * FROM `/Root/a` ORDER BY id;";
        for (size_t count : {2, 2, 3}) {
            if (count == 3) {
                auto alter = tableSession.ExecuteSchemeQuery("ALTER TABLE `/Root/a` ADD COLUMN extra String;").GetValueSync();
                UNIT_ASSERT_C(alter.IsSuccess(), alter.GetIssues().ToString());
            }
            auto result = session.ExecuteQuery(star, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).GetColumnsMeta().size(), count);
        }

        // Unsupported scalar semantics discovered after catalog loading must
        // restart legacy compilation without losing the loaded table metadata.
        const auto fallbackStart = logs.Snapshot().size();
        auto fallback = session.ExecuteQuery("SELECT TablePath() AS path FROM `/Root/b`;",
            NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!fallback.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(fallback.GetIssues().ToString(), "YqlSelect unsupported: TablePath");
        UNIT_ASSERT_C(logs.Snapshot().substr(fallbackStart).Contains("Legacy YQL graph compilation started"), logs.Snapshot());
        auto structured = session.ExecuteQuery("SELECT (AsStruct(id AS k)).k AS id FROM `/Root/b`;",
            NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(structured.IsSuccess(), structured.GetIssues().ToString());
        CompareYson(FormatResultSetYson(structured.GetResultSet(0)), "[[2]]");
        auto multipleScan = kikimr.GetTableClient().StreamExecuteScanQuery("SELECT 1; SELECT 2;").GetValueSync();
        TString scanIssues;
        if (multipleScan.IsSuccess()) {
            auto part = multipleScan.ReadNext().GetValueSync();
            UNIT_ASSERT(!part.IsSuccess());
            scanIssues = part.GetIssues().ToString();
        } else {
            scanIssues = multipleScan.GetIssues().ToString();
        }
        UNIT_ASSERT_STRING_CONTAINS(scanIssues, "single result set");
        auto duplicate = session.ExecuteQuery("SELECT 1 AS x, 2 AS x;", NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!duplicate.IsSuccess());
        auto invalid = session.ExecuteQuery("PRAGMA YqlSelect = 'invalid'; SELECT id FROM `/Root/a`;",
            NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!invalid.IsSuccess());
        auto missing = session.ExecuteQuery("SELECT id FROM `/Root/absent`;", NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!missing.IsSuccess());
        auto next = session.ExecuteQuery("SELECT id FROM `/Root/b` WHERE id = 2;", NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(next.IsSuccess(), next.GetIssues().ToString());
        CompareYson(FormatResultSetYson(next.GetResultSet(0)), "[[2]]");
    }

    // Native catalog IO and semantic errors are exercised only with new RBO;
    // the old optimizer continues to use its original frontend diagnostics.
    Y_UNIT_TEST_TWIN(ResolvesNamesUsingLoadedMetadata, NewRbo) {
        NKikimrConfig::TAppConfig appConfig;
        auto* config = appConfig.MutableTableServiceConfig();
        config->SetEnableNewYqlParser(true);
        config->SetEnableNewRBO(NewRbo);
        config->SetEnableFallbackToYqlOptimizer(false);
        TCapturedLog logs(true);
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false).SetLogCapture(logs);
        settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
        settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
        TKikimrRunner kikimr(settings);
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto scheme = tableSession.ExecuteSchemeQuery(
            "CREATE TABLE `/Root/names` (id Int64 NOT NULL, value String, amount Decimal(12,2), PRIMARY KEY(id));").GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto valid = session.ExecuteQuery(TString(NewRbo ? "PRAGMA YqlSelect = 'force'; " : "")
            + "SELECT n.id, n.value FROM `/Root/names` AS n WHERE n.id > 0 ORDER BY n.id;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(valid.IsSuccess(), valid.GetIssues().ToString());
        if constexpr (NewRbo) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            while (!logs.Snapshot().Contains("New YQL native execution contract built") && TInstant::Now() < deadline) {
                Sleep(TDuration::MilliSeconds(10));
            }
            UNIT_ASSERT_C(logs.Snapshot().Contains("New YQL name resolution succeeded"), logs.Snapshot());
            UNIT_ASSERT_C(logs.Snapshot().Contains("New YQL type annotation succeeded"), logs.Snapshot());
            UNIT_ASSERT_C(logs.Snapshot().Contains("New YQL native execution contract built"), logs.Snapshot());
        }

        auto params = TParamsBuilder().AddParam("$offset").Int64(3).Build().Build();
        auto parameterized = session.ExecuteQuery("SELECT id + $offset, amount FROM `/Root/names` LIMIT 5;",
            NQuery::TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(parameterized.IsSuccess(), parameterized.GetIssues().ToString());

        auto badType = session.ExecuteQuery("SELECT n.id + n.value FROM `/Root/names` AS n;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT(!badType.IsSuccess());
        if constexpr (NewRbo) { UNIT_ASSERT_STRING_CONTAINS(badType.GetIssues().ToString(), "New YQL type annotation"); }

        auto outerJoin = session.ExecuteQuery("SELECT r.id FROM `/Root/names` AS l "
            "LEFT JOIN `/Root/names` AS r ON l.id = r.id;", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(outerJoin.IsSuccess(), outerJoin.GetIssues().ToString());

        auto aggregate = session.ExecuteQuery("SELECT COUNT(*) FROM `/Root/names`;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(aggregate.IsSuccess(), aggregate.GetIssues().ToString());

        auto missing = session.ExecuteQuery("SELECT n.absent FROM `/Root/names` AS n;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT(!missing.IsSuccess());
        if constexpr (NewRbo) { UNIT_ASSERT_STRING_CONTAINS(missing.GetIssues().ToString(), "New YQL name resolution: Unknown column 'n.absent'"); }

        auto ambiguous = session.ExecuteQuery("SELECT id FROM `/Root/names` AS l JOIN `/Root/names` AS r ON l.id = r.id;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT(!ambiguous.IsSuccess());
        if constexpr (NewRbo) { UNIT_ASSERT_STRING_CONTAINS(ambiguous.GetIssues().ToString(), "Ambiguous column"); }

        // A failed binding must not contaminate the next compilation.
        auto next = session.ExecuteQuery("SELECT id FROM `/Root/names` WHERE id = 7;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(next.IsSuccess(), next.GetIssues().ToString());
    }

    // COUNT must return the same value through the direct aggregate path
    // with either optimizer, while malformed SQL must remain a diagnostic failure.
    Y_UNIT_TEST_TWIN(SelectAndSyntaxErrors, NewRbo) {
        for (bool enableNewParser : {false, true}) {
            NKikimrConfig::TAppConfig appConfig;
            auto* config = appConfig.MutableTableServiceConfig();
            config->SetEnableNewYqlParser(enableNewParser);
            config->SetEnableNewRBO(NewRbo);
            config->SetEnableFallbackToYqlOptimizer(false);
            TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
            auto tableClient = kikimr.GetTableClient();
            auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
            auto scheme = tableSession.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/t1` (a Int64 NOT NULL, PRIMARY KEY(a));").GetValueSync();
            UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

            auto client = kikimr.GetQueryClient();
            auto session = client.GetSession().GetValueSync().GetSession();
            const auto before = GetNewRBOCompileCounters(kikimr);
            const TVector<TString> queries = {
                "SELECT COUNT(*) FROM `/Root/t1`;",
                "--!ansi_lexer\nSELECT COUNT(*) AS \"count\" FROM `/Root/t1`;",
            };
            for (const auto& query : queries) {
                auto result = session.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), "[[0u]]");
                UNIT_ASSERT(!HasOptimizerFallbackIssue(result.GetIssues()));
            }
            const auto after = GetNewRBOCompileCounters(kikimr);
            UNIT_ASSERT_VALUES_EQUAL(after.second, before.second);
            UNIT_ASSERT_VALUES_EQUAL(after.first - before.first, NewRbo ? queries.size() : 0);

            auto invalid = session.ExecuteQuery("SELECT (", NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!invalid.IsSuccess());
            UNIT_ASSERT(!invalid.GetIssues().Empty());
        }
    }

    // The following benchmark cases test compatibility with full original inputs,
    // including declarations and unsupported relational constructs. Their success
    // does not assert native lowering; join_order supplies the strict TPCH/TPCDS oracle.
    Y_UNIT_TEST(TPCH_YQL) {
        CheckYqlWorkload(EBenchType::TPCH, 1, BenchmarkQueryCount[EBenchType::TPCH]);
    }

    // Split TPCDS into bounded test cases without filtering unsupported syntax
    // or changing the full set of benchmark query IDs.
    Y_UNIT_TEST(TPCDS_YQL_1_25) {
        CheckYqlWorkload(EBenchType::TPCDS, 1, 25);
    }

    Y_UNIT_TEST(TPCDS_YQL_26_50) {
        CheckYqlWorkload(EBenchType::TPCDS, 26, 50);
    }

    Y_UNIT_TEST(TPCDS_YQL_51_75) {
        CheckYqlWorkload(EBenchType::TPCDS, 51, 75);
    }

    Y_UNIT_TEST(TPCDS_YQL_76_99) {
        CheckYqlWorkload(EBenchType::TPCDS, 76, BenchmarkQueryCount[EBenchType::TPCDS]);
    }

    // Run the entire ClickBench compatibility corpus here. The workloads target
    // separately inspects operators for the subset that must lower natively.
    Y_UNIT_TEST(ClickBench_YQL) {
        CheckYqlWorkload(EBenchType::CLICKBENCH, 1, BenchmarkQueryCount[EBenchType::CLICKBENCH]);
    }

    // Use the performance fixture's query templates and table API, preserving
    // the same baseline compilation behavior as its original RBO workload test.
    Y_UNIT_TEST(TPCH_YDB_PERF) {
        TVector<TWorkloadQuery> queries;
        const auto constants = NResource::Find("consts.yql");
        for (ui32 id : TpchPerfQueryIds) {
            queries.push_back({id, PrepareTpchPerfQuery(
                NResource::Find(TStringBuilder() << "resfs/file/tpch/queries/yql/q" << id << ".sql"), constants)});
        }
        CheckWorkload(queries, EBenchType::TPCH, /*tpchPerf=*/true);
    }
}

} // namespace NKikimr::NKqp
