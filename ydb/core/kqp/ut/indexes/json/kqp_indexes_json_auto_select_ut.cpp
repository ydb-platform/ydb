#include <ydb/core/kqp/ut/indexes/json/common/kqp_indexes_json_ut_common.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <random>

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

namespace {

TKikimrRunner KikimrCompactJsonAutoSelect() {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableJsonIndex(true);
    featureFlags.SetEnableJsonIndexAutoSelect(true);
    featureFlags.SetEnableCompactFulltextIndex(true);
    featureFlags.SetEnableFulltextIndexPrefix(true);
    featureFlags.SetEnableFulltextIndexRowId(true);
    featureFlags.SetEnableAddUniqueIndex(true);

    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
    return TKikimrRunner(settings);
}

void ExecuteSuccess(TQueryClient& db, const std::string& query) {
    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void UpdateJsonFeatureFlags(TKikimrRunner& kikimr, bool enableJsonIndex, bool enableAutoSelect) {
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    const auto edgeActor = runtime.AllocateEdgeActor();

    NKikimrConfig::TAppConfig config;
    auto* featureFlags = config.MutableFeatureFlags();
    featureFlags->SetEnableJsonIndex(enableJsonIndex);
    featureFlags->SetEnableJsonIndexAutoSelect(enableAutoSelect);
    config.MutableTableServiceConfig()->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    // In production the FeatureFlagsConfigurator updates AppData before KQP subscribers process the
    // same dispatcher snapshot. This direct service-level test mirrors that ordering explicitly.
    runtime.GetAppData().UpdateRuntimeFlags(*featureFlags);

    for (const auto& service : {
            MakeKqpProxyID(runtime.GetNodeId()),
            MakeKqpCompileServiceID(runtime.GetNodeId())}) {
        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        *request->Record.MutableConfig() = config;
        runtime.Send(service, edgeActor, request.Release());
        auto response = runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(
            edgeActor, TDuration::Seconds(10));
        UNIT_ASSERT_C(response, "KQP service must acknowledge the FeatureFlags update");
    }
}

void UpdateJsonAutoSelectConfig(TKikimrRunner& kikimr, bool enabled) {
    UpdateJsonFeatureFlags(kikimr, /*enableJsonIndex=*/true, enabled);
}

void ValidateCompactAutoSelectResults(TQueryClient& db, const std::string& predicate,
    const TString& expected, const std::string& tableName = "TestTable")
{
    ValidateAutoSelect(db, predicate, "json_idx", tableName);

    const auto execute = [&](const std::string& view) {
        const auto query = std::format(
            "SELECT Key FROM {}{} WHERE {} ORDER BY Key;",
            tableName, view, predicate);
        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return FormatResultSetYson(result.GetResultSet(0));
    };

    const auto autoSelected = execute("");
    const auto explicitIndex = execute(" VIEW json_idx");
    const auto fullScan = execute(" VIEW PRIMARY KEY");

    CompareYson(expected, autoSelected);
    CompareYson(fullScan, explicitIndex);
    CompareYson(fullScan, autoSelected);
}

void ValidateOneOfTwoIndexesSelected(TQueryClient& db, const std::string& predicate,
    const TString& idxA, const TString& idxB, const std::string& tableName = "TestTable")
{
    const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
    const auto query = std::format("SELECT * FROM {} WHERE {};", tableName, predicate);

    const auto result = db.ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Explain failed for predicate [" + predicate + "]: " + result.GetIssues().ToString());

    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &planJson, true),
        "Failed to parse plan JSON for predicate [" + predicate + "]");

    const int count = CountPlanNodesByKv(planJson, "Index", idxA) + CountPlanNodesByKv(planJson, "Index", idxB);
    UNIT_ASSERT_C(count == 1,
        "Expected exactly one of (" + idxA + ", " + idxB + ") to be auto-selected for: " + predicate + ", got " + std::to_string(count));
}

TString ExecuteAndAssertJsonPlan(TQueryClient& db, const TString& sql, size_t expectedIndexNodes, const TString& expectedYson,
    TParams params = TParamsBuilder().Build(), const TString& indexName = "json_idx")
{
    const auto settings = TExecuteQuerySettings().StatsMode(EStatsMode::Full);
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx(), params, settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan(), "Execution plan is missing");

    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &planJson, true), "Failed to parse execution plan JSON");
    UNIT_ASSERT_VALUES_EQUAL_C(CountPlanNodesByKv(planJson, "Index", indexName), expectedIndexNodes, sql);

    const TString actual = FormatResultSetYson(result.GetResultSet(0));
    CompareYson(expectedYson, actual, sql);
    return actual;
}
  
TString ExecuteKeys(TQueryClient& db, const std::string& view = {}) {
    const auto query = std::format(R"(
        SELECT Key FROM TestTable{} WHERE JSON_EXISTS(Text, '$.tag') ORDER BY Key;
    )", view);
    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return FormatResultSetYson(result.GetResultSet(0));
}

void ValidateLifecycleResults(TQueryClient& db) {
    const auto primary = ExecuteKeys(db, " VIEW PRIMARY KEY");
    const auto automatic = ExecuteKeys(db);
    CompareYson(R"([[[1u]];[[3u]]])", primary);
    CompareYson(primary, automatic);
}

std::string GetSelectedJsonIndex(TQueryClient& db, const std::string& idxA, const std::string& idxB) {
    const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
    auto result = db.ExecuteQuery(
        R"(SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.tag') ORDER BY Key;)",
        TTxControl::NoTx(), settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan()
        && NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &planJson, true),
        "Failed to parse explain plan");

    const int countA = CountPlanNodesByKv(planJson, "Index", TString(idxA));
    const int countB = CountPlanNodesByKv(planJson, "Index", TString(idxB));
    UNIT_ASSERT_VALUES_EQUAL_C(countA + countB, 1,
        "Expected exactly one JSON index read, got " << countA << " + " << countB);
    return countA == 1 ? idxA : idxB;
}

void ValidateDifferentialResults(TQueryClient& db, const std::string& predicate,
    const std::string& declarations = {}, const TParams& params = TParamsBuilder().Build())
{
    const auto makeQuery = [&](const std::string& view) {
        return declarations + std::format(
            "\nSELECT Key FROM TestTable{} WHERE {} ORDER BY Key;",
            view, predicate);
    };

    const auto execute = [&](const std::string& view) {
        auto result = db.ExecuteQuery(makeQuery(view), TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(),
            "Query failed for predicate [" + predicate + "] and view [" + view + "]: "
                + result.GetIssues().ToString());
        return FormatResultSetYson(result.GetResultSet(0));
    };

    const auto primary = execute(" VIEW PRIMARY KEY");
    const auto explicitIndex = execute(" VIEW json_idx");
    const auto autoSelected = execute("");

    CompareYson(primary, explicitIndex);
    CompareYson(primary, autoSelected);

    const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
    auto explain = db.ExecuteQuery(makeQuery(""), TTxControl::NoTx(), params, settings).ExtractValueSync();
    UNIT_ASSERT_C(explain.IsSuccess(),
        "Explain failed for predicate [" + predicate + "]: " + explain.GetIssues().ToString());
    UNIT_ASSERT_C(explain.GetStats() && explain.GetStats()->GetPlan(),
        "Explain plan is empty for predicate [" + predicate + "]");

    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(*explain.GetStats()->GetPlan(), &planJson, true),
        "Failed to parse explain plan for predicate [" + predicate + "]");
    UNIT_ASSERT_C(CountPlanNodesByKv(planJson, "Index", "json_idx") == 1,
        "json_idx was not auto-selected for predicate [" + predicate + "]");
}

void TestDifferentialJsonCorrectness(const std::string& jsonType) {
    auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
    auto db = kikimr.GetQueryClient();

    ExecuteSuccess(db, std::format(R"(
        CREATE TABLE TestTable (
            Key Uint64,
            Text {},
            PRIMARY KEY (Key),
            INDEX json_idx GLOBAL USING json ON (Text)
        );
    )", jsonType));

    ExecuteSuccess(db, std::format(R"(
        UPSERT INTO TestTable (Key, Text) VALUES
            (1, {}('{{"kind":"plain","number":0,"nested":{{"items":[{{"name":"alpha","score":1}},{{"name":"beta","score":2}}]}}}}')),
            (2, {}('{{"kind":"unicode","label":"Привет 🌍","number":2147483647,"nested":{{"items":[{{"name":"бета","score":-2}}]}}}}')),
            (3, {}('{{"kind":"limits","number":-2147483648,"nested":{{"object":{{"enabled":true}}}}}}')),
            (4, {}('{{"kind":"array","number":1.5,"nested":{{"items":[]}},"mixed":[null,false,{{"value":"x"}}]}}')),
            (5, {}('{{}}')),
            (6, NULL);
    )", jsonType, jsonType, jsonType, jsonType, jsonType));

    // This complements the generated JSON corpus: it verifies the optimizer-selected
    // path as well as explicit index access, while retaining primary scan as oracle.
    const std::vector<std::string> predicates = {
        R"(JSON_EXISTS(Text, '$.kind'))",
        R"(JSON_EXISTS(Text, '$.nested.items[*].name'))",
        R"(JSON_EXISTS(Text, '$.mixed[*].value'))",
        R"(JSON_VALUE(Text, '$.nested.object.enabled' RETURNING Bool))",
        R"(JSON_VALUE(Text, '$.label' RETURNING Utf8) == "Привет 🌍"u)",
        R"(JSON_VALUE(Text, '$.number' RETURNING Int64) == 2147483647)",
        R"(JSON_VALUE(Text, '$.number' RETURNING Int64) == -2147483648)",
        R"(JSON_VALUE(Text, '$.number' RETURNING Double) BETWEEN -2.0 AND 2.0)",
    };

    for (const auto& predicate : predicates) {
        ValidateDifferentialResults(db, predicate);
    }

    ValidateDifferentialResults(db,
        R"(JSON_EXISTS(Text, '$.nested.items[*] ? (@.score == $score)' PASSING $score AS score))",
        "DECLARE $score AS Int64;",
        TParamsBuilder().AddParam("$score").Int64(2).Build().Build());
}

TString ExecuteShapeQuery(TQueryClient& db, const std::string& query) {
    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString() << "\nQuery:\n" << query);
    return FormatResultSetYson(result.GetResultSet(0));
}

int CountJsonIndexReads(TQueryClient& db, const std::string& query) {
    const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
    auto result = db.ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString() << "\nQuery:\n" << query);
    UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan(), "Explain plan is empty");

    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &planJson, true),
        "Failed to parse explain plan");
    return CountPlanNodesByKv(planJson, "Index", "json_idx");
}

void TestJsonQueryShapes(const std::string& jsonType) {
    auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
    auto db = kikimr.GetQueryClient();

    ExecuteSuccess(db, std::format(R"(
        CREATE TABLE TestTable (
            Key Uint64,
            Text {},
            PRIMARY KEY (Key),
            INDEX json_idx GLOBAL USING json ON (Text)
        );
        CREATE TABLE Labels (
            Key Uint64,
            Label Utf8,
            PRIMARY KEY (Key)
        );
    )", jsonType));
    ExecuteSuccess(db, std::format(R"(
        UPSERT INTO TestTable (Key, Text) VALUES
            (1, {}('{{"tag":"wanted"}}')),
            (2, {}('{{"other":1}}')),
            (3, {}('{{"tag":"wanted","other":2}}')),
            (4, {}('{{}}'));
        UPSERT INTO Labels (Key, Label) VALUES
            (1, "one"u), (2, "two"u), (4, "four"u);
    )", jsonType, jsonType, jsonType, jsonType));

    const std::vector<std::tuple<std::string, std::string, std::string, int>> cases = {
        {
            "CTE",
            R"($filtered = SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.tag');
                SELECT Key FROM $filtered ORDER BY Key;)",
            R"($filtered = SELECT Key FROM TestTable VIEW PRIMARY KEY WHERE JSON_EXISTS(Text, '$.tag');
                SELECT Key FROM $filtered ORDER BY Key;)",
            1,
        },
        {
            "subquery",
            R"(SELECT Key FROM (
                    SELECT Key, Text FROM TestTable WHERE JSON_EXISTS(Text, '$.tag'))
                ORDER BY Key;)",
            R"(SELECT Key FROM (
                    SELECT Key, Text FROM TestTable VIEW PRIMARY KEY WHERE JSON_EXISTS(Text, '$.tag'))
                ORDER BY Key;)",
            1,
        },
        {
            "JOIN",
            R"(SELECT t.Key FROM TestTable AS t
                INNER JOIN Labels AS l ON t.Key = l.Key
                WHERE JSON_EXISTS(t.Text, '$.tag') ORDER BY t.Key;)",
            R"(SELECT t.Key FROM TestTable VIEW PRIMARY KEY AS t
                INNER JOIN Labels AS l ON t.Key = l.Key
                WHERE JSON_EXISTS(t.Text, '$.tag') ORDER BY t.Key;)",
            // JSON auto-select currently stays conservative across JOINs.
            0,
        },
        {
            "UNION ALL",
            R"(SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.tag')
                UNION ALL
                SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.missing')
                ORDER BY Key;)",
            R"(SELECT Key FROM TestTable VIEW PRIMARY KEY WHERE JSON_EXISTS(Text, '$.tag')
                UNION ALL
                SELECT Key FROM TestTable VIEW PRIMARY KEY WHERE JSON_EXISTS(Text, '$.missing')
                ORDER BY Key;)",
            2,
        },
    };

    for (const auto& [name, automatic, primary, expectedIndexReads] : cases) {
        const auto expected = ExecuteShapeQuery(db, primary);
        const auto actual = ExecuteShapeQuery(db, automatic);
        CompareYson(expected, actual);
        UNIT_ASSERT_VALUES_EQUAL_C(CountJsonIndexReads(db, automatic), expectedIndexReads,
            name << " has an unexpected auto-select plan");
    }
}

void TestJsonEdgeCorpus(const std::string& jsonType) {
    auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
    auto db = kikimr.GetQueryClient();

    ExecuteSuccess(db, std::format(R"(
        CREATE TABLE TestTable (
            Key Uint64,
            Text {},
            PRIMARY KEY (Key),
            INDEX json_idx GLOBAL USING json ON (Text)
        );
    )", jsonType));

    const std::string largeValue(2048, 'x');
    ExecuteSuccess(db, std::format(R"(
        UPSERT INTO TestTable (Key, Text) VALUES
            (1, {}('{{"a":{{"b":{{"c":{{"d":{{"e":{{"f":{{"g":{{"value":"deep"}}}}}}}}}}}}}}}}')),
            (2, {}('{{"payload":"{}","object":{{"left":1,"right":2}}}}')),
            (3, {}('{{"duplicate":"first","duplicate":"last"}}')),
            (4, {}('{{"label":"café"}}')),
            (5, {}('{{"label":"café"}}'));
    )", jsonType, jsonType, largeValue, jsonType, jsonType, jsonType));

    const std::vector<std::string> predicates = {
        R"(JSON_VALUE(Text, '$.a.b.c.d.e.f.g.value' RETURNING Utf8) == "deep"u)",
        std::format(R"(JSON_VALUE(Text, '$.payload' RETURNING Utf8) == "{}"u)", largeValue),
        R"(JSON_EXISTS(Text, '$.object.left') AND JSON_EXISTS(Text, '$.object.right'))",
        R"(JSON_VALUE(Text, '$.duplicate' RETURNING Utf8) == "last"u)",
        R"(JSON_VALUE(Text, '$.label' RETURNING Utf8) == "café"u)",
        R"(JSON_VALUE(Text, '$.label' RETURNING Utf8) == "café"u)",
    };
    for (const auto& predicate : predicates) {
        ValidateDifferentialResults(db, predicate);
    }

    // An ambiguous duplicate member produces no JSON_VALUE under the default
    // error handling for both storage types. Unicode strings are compared as
    // their original code point sequences; no normalization occurs.
    CompareYson(R"([])", ExecuteShapeQuery(db,
        R"(SELECT Key FROM TestTable VIEW PRIMARY KEY
            WHERE JSON_VALUE(Text, '$.duplicate' RETURNING Utf8) == "last"u ORDER BY Key;)"));
    CompareYson(R"([[[4u]]])", ExecuteShapeQuery(db,
        R"(SELECT Key FROM TestTable VIEW PRIMARY KEY
            WHERE JSON_VALUE(Text, '$.label' RETURNING Utf8) == "café"u ORDER BY Key;)"));
    CompareYson(R"([[[5u]]])", ExecuteShapeQuery(db,
        R"(SELECT Key FROM TestTable VIEW PRIMARY KEY
            WHERE JSON_VALUE(Text, '$.label' RETURNING Utf8) == "café"u ORDER BY Key;)"));
}

struct TGeneratedJsonPredicate {
    std::string Predicate;
    std::string Declarations;
    TParams Params = TParamsBuilder().Build();
};

std::vector<std::optional<std::string>> GenerateJsonPropertyDocuments(ui64 seed) {
    std::mt19937_64 random(seed);
    std::vector<std::optional<std::string>> documents = {std::nullopt};
    static const std::array kinds = {"alpha", "beta", "gamma"};

    for (ui32 caseId = 1; caseId < 25; ++caseId) {
        const auto kind = kinds[random() % kinds.size()];
        const i64 number = static_cast<i64>(random() % 21) - 10;
        const bool flag = random() % 2;
        switch (caseId % 8) {
            case 0:
                documents.emplace_back("{}");
                break;
            case 1: {
                const TString json = TStringBuilder()
                    << R"({"kind":")" << kind
                    << R"(","n":)" << number
                    << R"(,"flag":)" << (flag ? "true" : "false")
                    << R"(,"tag":"deep","a":{"b":{"c":{"d":{"value":)" << number
                    << "}}}}}";
                documents.emplace_back(std::string(json.data(), json.size()));
                break;
            }
            case 2:
                documents.emplace_back(std::format(
                    R"({{"kind":"{}","n":{},"tag":"array","items":[null,{{"value":{}}},true,"x"]}})",
                    kind, number, number));
                break;
            case 3:
                documents.emplace_back(std::format(
                    R"({{"kind":"{}","n":{},"tag":"unicode","label":"{}"}})",
                    kind, number, caseId % 2 ? "café" : "café"));
                break;
            case 4:
                documents.emplace_back(std::format(
                    R"({{"kind":"{}","n":{},"duplicate":"first","duplicate":"last"}})",
                    kind, number));
                break;
            case 5:
                documents.emplace_back(caseId % 16 == 5
                    ? R"({"kind":"boundary","n":9223372036854775807,"tag":"max"})"
                    : R"({"kind":"boundary","n":-9223372036854775808,"tag":"min"})");
                break;
            case 6:
                documents.emplace_back(std::format(
                    R"({{"kind":"{}","n":{},"flag":{},"tag":"object","object":{{"left":{},"right":{}}}}})",
                    kind, number, flag, number, -number));
                break;
            case 7:
                documents.emplace_back(std::format(
                    R"({{"kind":"{}","n":{},"flag":{},"tag":"mixed","nested":[[{}],{{"kind":"{}"}}]}})",
                    kind, number, flag, number, kinds[random() % kinds.size()]));
                break;
        }
    }
    return documents;
}

std::vector<TGeneratedJsonPredicate> GenerateJsonPropertyPredicates(ui64 seed) {
    std::mt19937_64 random(seed ^ 0x9E3779B97F4A7C15ULL);
    std::vector<TGeneratedJsonPredicate> cases;
    static const std::array paths = {"kind", "tag", "flag", "items", "object"};
    static const std::array kinds = {"alpha", "beta", "gamma", "boundary"};

    for (ui32 caseId = 0; caseId < 16; ++caseId) {
        const auto pathA = paths[random() % paths.size()];
        const auto pathB = paths[random() % paths.size()];
        const auto kind = kinds[random() % kinds.size()];
        const i64 number = static_cast<i64>(random() % 11) - 5;

        switch (caseId % 8) {
            case 0:
                cases.push_back({.Predicate = std::format("JSON_EXISTS(Text, '$.{}')", pathA)});
                break;
            case 1:
                cases.push_back({.Predicate = std::format(
                    R"(JSON_VALUE(Text, '$.kind' RETURNING Utf8) == "{}"u)", kind)});
                break;
            case 2:
                cases.push_back({.Predicate = std::format(
                    "JSON_VALUE(Text, '$.n' RETURNING Int64) >= {}", number)});
                break;
            case 3:
                cases.push_back({.Predicate = std::format(
                    R"(JSON_EXISTS(Text, '$.{}') AND JSON_VALUE(Text, '$.kind' RETURNING Utf8) != "missing"u)",
                    pathA)});
                break;
            case 4:
                cases.push_back({.Predicate = std::format(
                    "JSON_EXISTS(Text, '$.{}') OR JSON_EXISTS(Text, '$.{}')", pathA, pathB)});
                break;
            case 5:
                cases.push_back({.Predicate = std::format(
                    "JSON_EXISTS(Text, '$.{}') AND Data >= {}", pathA, number)});
                break;
            case 6: {
                TGeneratedJsonPredicate generated;
                generated.Predicate =
                    R"(JSON_EXISTS(Text, '$.n ? (@ >= $v)' PASSING $v AS v))";
                generated.Declarations = "DECLARE $v AS Int64;";
                generated.Params = TParamsBuilder()
                    .AddParam("$v").Int64(number).Build().Build();
                cases.push_back(std::move(generated));
                break;
            }
            case 7: {
                TGeneratedJsonPredicate generated;
                generated.Predicate =
                    R"(JSON_EXISTS(Text, '$.kind ? (@ == $v)' PASSING $v AS v))";
                generated.Declarations = "DECLARE $v AS Utf8;";
                generated.Params = TParamsBuilder()
                    .AddParam("$v").Utf8(kind).Build().Build();
                cases.push_back(std::move(generated));
                break;
            }
        }
    }
    return cases;
}

void TestGeneratedJsonPropertyCorpus(const std::string& jsonType, bool compact, ui64 seed) {
    auto kikimr = compact
        ? KikimrCompactJsonAutoSelect()
        : Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
    auto db = kikimr.GetQueryClient();

    ExecuteSuccess(db, std::format(R"(
        CREATE TABLE TestTable (
            Key Uint64,
            Text {},
            Data Int64,
            PRIMARY KEY (Key),
            INDEX json_idx GLOBAL USING json ON (Text)
        );
    )", jsonType));

    const auto documents = GenerateJsonPropertyDocuments(seed);
    std::string values;
    for (size_t key = 0; key < documents.size(); ++key) {
        if (!values.empty()) {
            values += ",";
        }
        values += std::format("({}, {}, {})", key,
            documents[key] ? std::format("{}('{}')", jsonType, *documents[key]) : "NULL",
            static_cast<i64>(key % 9) - 4);
    }
    ExecuteSuccess(db, "UPSERT INTO TestTable (Key, Text, Data) VALUES " + values + ";");

    const auto cases = GenerateJsonPropertyPredicates(seed);
    for (size_t caseId = 0; caseId < cases.size(); ++caseId) {
        const auto& generated = cases[caseId];
        const std::string context = std::format(
            "seed={}, case={}, type={}, compact={}, predicate=[{}]",
            seed, caseId, jsonType, compact, generated.Predicate);
        const auto makeQuery = [&](const std::string& view) {
            return generated.Declarations + std::format(
                "\nSELECT Key FROM TestTable{} WHERE {} ORDER BY Key;",
                view, generated.Predicate);
        };
        const auto execute = [&](const std::string& view) {
            auto result = db.ExecuteQuery(
                makeQuery(view), TTxControl::NoTx(), generated.Params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(),
                context << ", view=[" << view << "], issues=" << result.GetIssues().ToString());
            return FormatResultSetYson(result.GetResultSet(0));
        };

        const auto primary = execute(" VIEW PRIMARY KEY");
        const auto explicitIndex = execute(" VIEW json_idx");
        const auto automatic = execute("");
        CompareYson(primary, explicitIndex, TString(context + ", explicit"));
        CompareYson(primary, automatic, TString(context + ", automatic"));

        const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
        auto explain = db.ExecuteQuery(
            makeQuery(""), TTxControl::NoTx(), generated.Params, settings).ExtractValueSync();
        UNIT_ASSERT_C(explain.IsSuccess(), context << ", explain: " << explain.GetIssues().ToString());
        UNIT_ASSERT_C(explain.GetStats() && explain.GetStats()->GetPlan(),
            context << ", missing explain plan");
        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(*explain.GetStats()->GetPlan(), &planJson, true),
            context << ", invalid explain JSON");
        UNIT_ASSERT_VALUES_EQUAL_C(
            CountPlanNodesByKv(planJson, "Index", "json_idx"), 1, context);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpJsonIndexesAutoSelect) {
    Y_UNIT_TEST(FullRangeIsNotAutoSelected) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            const auto addIndexResult = db.ExecuteQuery(R"(
                ALTER TABLE TestTable ADD INDEX json_idx_2 GLOBAL USING json ON (Text)
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(addIndexResult.IsSuccess(), addIndexResult.GetIssues().ToString());

            const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
            const auto query = R"(SELECT * FROM TestTable WHERE JSON_EXISTS(Text, '$[*]');)";

            const auto result = db.ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                "JSON index was not auto-selected: full-range search cannot be performed using full-text search");

            NJson::TJsonValue planJson;
            UNIT_ASSERT_C(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &planJson, true), "Failed to parse plan JSON");
            UNIT_ASSERT_VALUES_EQUAL(CountPlanNodesByKv(planJson, "Index", "json_idx"), 0);
            UNIT_ASSERT_VALUES_EQUAL(CountPlanNodesByKv(planJson, "Index", "json_idx_2"), 0);
        }, /* enableJsonIndexAutoSelect */ true);
    }
    Y_UNIT_TEST(GeneratedPropertyCorpusJsonLegacy) {
        TestGeneratedJsonPropertyCorpus("Json", false, 0x4A534F4E1001ULL);
    }

    Y_UNIT_TEST(GeneratedPropertyCorpusJsonDocumentLegacy) {
        TestGeneratedJsonPropertyCorpus("JsonDocument", false, 0x4A534F4E1002ULL);
    }

    Y_UNIT_TEST(GeneratedPropertyCorpusJsonCompact) {
        TestGeneratedJsonPropertyCorpus("Json", true, 0x4A534F4E2001ULL);
    }

    Y_UNIT_TEST(GeneratedPropertyCorpusJsonDocumentCompact) {
        TestGeneratedJsonPropertyCorpus("JsonDocument", true, 0x4A534F4E2002ULL);
    }

    Y_UNIT_TEST(QueryShapesJson) {
        TestJsonQueryShapes("Json");
    }

    Y_UNIT_TEST(QueryShapesJsonDocument) {
        TestJsonQueryShapes("JsonDocument");
    }

    Y_UNIT_TEST(EdgeCorpusJson) {
        TestJsonEdgeCorpus("Json");
    }

    Y_UNIT_TEST(EdgeCorpusJsonDocument) {
        TestJsonEdgeCorpus("JsonDocument");
    }

    Y_UNIT_TEST(DifferentialCorrectnessJson) {
        TestDifferentialJsonCorrectness("Json");
    }

    Y_UNIT_TEST(DifferentialCorrectnessJsonDocument) {
        TestDifferentialJsonCorrectness("JsonDocument");
    }

    Y_UNIT_TEST(JsonExists) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 2)'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == true && @.k2 == false)'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")'))");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(JsonValue) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Bool)");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) == 10");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) == -10");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) != 10");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) >= 10");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) BETWEEN 10 AND 20");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) NOT BETWEEN 10 AND 20");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) IN (1, 2, 3, 4)");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(AndOrCombinations) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))");
            ValidateAutoSelect(db, R"((JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')) OR JSON_EXISTS(Text, '$.k3'))");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(PrimaryColumnPredicate) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        CreateTestTable(db, "JsonDocument", /* withIndex */ true);

        // JI predicate
        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1')");

        // JI predicate with primary -> primary wins
        ValidateNoAutoSelect(db, "Key > 5 AND JSON_EXISTS(Text, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Key > 5");
        ValidateNoAutoSelect(db, "Key = 1 AND JSON_EXISTS(Text, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Key = 1");

        // Without JI predicate
        ValidateNoAutoSelect(db, "Key > 5");
        ValidateNoAutoSelect(db, "Key = 1");
    }

    Y_UNIT_TEST(SecondaryColumnPredicate) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    Data Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text),
                    INDEX data_idx GLOBAL ON (Data)
                );
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // JI predicate
        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1')");

        // JI predicate with secondary -> secondary wins
        ValidateNoAutoSelect(db, "Data = 'b' AND JSON_EXISTS(Text, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Data = 'b'");
        ValidateNoAutoSelect(db, "Data >= 'a' AND JSON_EXISTS(Text, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Data >= 'a'");

        // Without JI predicate
        ValidateNoAutoSelect(db, "Data = 'b'");
        ValidateNoAutoSelect(db, "Data >= 'a'");
    }

    Y_UNIT_TEST(DataColumnPredicate) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Data = 'd1'");
            ValidateAutoSelect(db, "Data = 'd1' AND JSON_EXISTS(Text, '$.k1')");

            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR Data = 'd1'");
            ValidateNoAutoSelect(db, "Data = 'd1' OR JSON_EXISTS(Text, '$.k1')");

            ValidateAutoSelect(db, "Data = 'd1' AND JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2')");
            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Data = 'd1' AND JSON_EXISTS(Text, '$.k2')");
            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND Data = 'd1'");

            ValidateNoAutoSelect(db, "Data = 'd1' OR JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR Data = 'd1' OR JSON_EXISTS(Text, '$.k2')");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR Data = 'd1'");

            ValidateNoAutoSelect(db, "Data = 'd1' OR JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2')");
            ValidateAutoSelect(db, "Data = 'd1' AND JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')");

            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR Data = 'd1' AND JSON_EXISTS(Text, '$.k2')");
            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Data = 'd1' OR JSON_EXISTS(Text, '$.k2')");

            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') AND Data = 'd1'");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') OR Data = 'd1'");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(TwoJsonIndexes) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    Extra JsonDocument,
                    Data Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx_text GLOBAL USING json ON (Text),
                    INDEX json_idx_extra GLOBAL USING json ON (Extra)
                );
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1')", "json_idx_text");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1')", "json_idx_extra");

        ValidateAutoSelect(db, "JSON_EXISTS(Extra, '$.k1')", "json_idx_extra");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Extra, '$.k1')", "json_idx_text");

        // Cross-column predicates are not supported
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Extra, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Extra, '$.k1')");
    }

    Y_UNIT_TEST(WrongColumn) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    Data JsonDocument,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text),
                );
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1')");

        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Data, '$.k1')");
        ValidateAutoSelect(db, "JSON_EXISTS(Data, '$.k1') AND JSON_EXISTS(Text, '$.k1')");

        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Data, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Data, '$.k1') OR JSON_EXISTS(Text, '$.k1')");

        ValidateNoAutoSelect(db, "JSON_EXISTS(Data, '$.k1')");
    }

    Y_UNIT_TEST(NoJsonIndex) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        CreateTestTable(db, "JsonDocument", /* withIndex */ false);
        FillTestTable(db, "TestTable", "JsonDocument");

        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1')");

        {
            const std::string query = R"(
                ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1'))");
    }

    Y_UNIT_TEST(SchemaLifecycleInvalidatesCachedAutoSelectPlan) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        ExecuteSuccess(db, R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Text JsonDocument,
                PRIMARY KEY (Key)
            );
        )");
        ExecuteSuccess(db, R"(
            UPSERT INTO TestTable (Key, Text) VALUES
                (1, JsonDocument('{"tag":"red"}')),
                (2, JsonDocument('{"other":1}')),
                (3, JsonDocument('{"tag":"blue"}'));
        )");

        // Execute the exact same text repeatedly through one client to warm the
        // compiled-query cache before every schema transition.
        ValidateLifecycleResults(db);
        ValidateLifecycleResults(db);
        ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", "json_idx");

        ExecuteSuccess(db, "ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (Text);");
        ValidateLifecycleResults(db);
        ValidateLifecycleResults(db);
        ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", "json_idx");
        CompareYson(ExecuteKeys(db, " VIEW PRIMARY KEY"), ExecuteKeys(db, " VIEW json_idx"));

        ExecuteSuccess(db, "ALTER TABLE TestTable DROP INDEX json_idx;");
        ValidateLifecycleResults(db);
        ValidateLifecycleResults(db);
        ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", "json_idx");

        ExecuteSuccess(db, "ALTER TABLE TestTable ADD INDEX json_idx_recreated GLOBAL USING json ON (Text);");
        ValidateLifecycleResults(db);
        ValidateLifecycleResults(db);
        ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", "json_idx_recreated");
        CompareYson(ExecuteKeys(db, " VIEW PRIMARY KEY"), ExecuteKeys(db, " VIEW json_idx_recreated"));
    }

    Y_UNIT_TEST(BuildingReadyDroppedLifecycleInvalidatesCachedPlan) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableJsonIndex(true);
        featureFlags.SetEnableJsonIndexAutoSelect(true);
        auto kikimr = TKikimrRunner(TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetUseRealThreads(false));
        auto db = kikimr.GetQueryClient();
        auto* runtime = kikimr.GetTestServer().GetRuntime();

        kikimr.RunCall([&] {
            ExecuteSuccess(db, R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    PRIMARY KEY (Key)
                );
            )");
            ExecuteSuccess(db, R"(
                UPSERT INTO TestTable (Key, Text) VALUES
                    (1, JsonDocument('{"tag":"red"}')),
                    (2, JsonDocument('{"other":1}')),
                    (3, JsonDocument('{"tag":"blue"}'));
            )");

            // Warm the same query text through the same client before the index exists.
            ValidateLifecycleResults(db);
            ValidateLifecycleResults(db);
            UNIT_ASSERT_VALUES_EQUAL(CountJsonIndexReads(db,
                R"(SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.tag') ORDER BY Key;)"), 0);
            return true;
        });

        TVector<TAutoPtr<IEventHandle>> capturedEvents;
        size_t captured = 0;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (captured == 0 &&
                ev->GetTypeRewrite() == TEvDataShard::TEvBuildFulltextIndexRequest::EventType)
            {
                ++captured;
                capturedEvents.push_back(ev.Release());
                return NActors::TTestActorRuntimeBase::EEventAction::DROP;
            }
            return NActors::TTestActorRuntimeBase::EEventAction::PROCESS;
        });

        NYdb::NQuery::TAsyncExecuteQueryResult addIndexFuture;
        kikimr.RunCall([&] {
            addIndexFuture = db.ExecuteQuery(
                "ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (Text);",
                TTxControl::NoTx());
            return true;
        });
        runtime->WaitFor("JSON index build paused", [&] { return captured == 1; });

        kikimr.RunCall([&] {
            // BUILDING indexes must not leak into a cached or freshly explained plan.
            ValidateLifecycleResults(db);
            ValidateLifecycleResults(db);
            UNIT_ASSERT_VALUES_EQUAL(CountJsonIndexReads(db,
                R"(SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.tag') ORDER BY Key;)"), 0);
            return true;
        });

        for (auto& ev : capturedEvents) {
            runtime->Send(ev.Release());
        }
        capturedEvents.clear();
        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        kikimr.RunCall([&] {
            const auto result = addIndexFuture.GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            // READY must invalidate the scan plan and make the index eligible.
            ValidateLifecycleResults(db);
            ValidateLifecycleResults(db);
            UNIT_ASSERT_VALUES_EQUAL(CountJsonIndexReads(db,
                R"(SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.tag') ORDER BY Key;)"), 1);
            CompareYson(ExecuteKeys(db, " VIEW PRIMARY KEY"), ExecuteKeys(db, " VIEW json_idx"));

            ExecuteSuccess(db, "ALTER TABLE TestTable DROP INDEX json_idx;");

            // The same cached query remains correct after the index is fully dropped.
            ValidateLifecycleResults(db);
            ValidateLifecycleResults(db);
            UNIT_ASSERT_VALUES_EQUAL(CountJsonIndexReads(db,
                R"(SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.tag') ORDER BY Key;)"), 0);
            return true;
        });
    }

    Y_UNIT_TEST(SameColumnIndexesLifecycleUsesSingleIndexRead) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        ExecuteSuccess(db, R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Text JsonDocument,
                PRIMARY KEY (Key),
                INDEX json_idx_a GLOBAL USING json ON (Text)
            );
        )");
        ExecuteSuccess(db, R"(
            UPSERT INTO TestTable (Key, Text) VALUES
                (1, JsonDocument('{"tag":"red"}')),
                (2, JsonDocument('{"other":1}')),
                (3, JsonDocument('{"tag":"blue"}'));
        )");

        ValidateLifecycleResults(db);
        ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", "json_idx_a");

        ExecuteSuccess(db, "ALTER TABLE TestTable ADD INDEX json_idx_b GLOBAL USING json ON (Text);");
        const auto initiallySelected = GetSelectedJsonIndex(db, "json_idx_a", "json_idx_b");
        UNIT_ASSERT_VALUES_EQUAL(GetSelectedJsonIndex(db, "json_idx_a", "json_idx_b"), initiallySelected);
        UNIT_ASSERT_VALUES_EQUAL(GetSelectedJsonIndex(db, "json_idx_a", "json_idx_b"), initiallySelected);
        ValidateLifecycleResults(db);
        CompareYson(ExecuteKeys(db, " VIEW PRIMARY KEY"), ExecuteKeys(db, " VIEW json_idx_a"));
        CompareYson(ExecuteKeys(db, " VIEW PRIMARY KEY"), ExecuteKeys(db, " VIEW json_idx_b"));

        const std::string remaining = initiallySelected == "json_idx_a" ? "json_idx_b" : "json_idx_a";
        ExecuteSuccess(db, "ALTER TABLE TestTable DROP INDEX " + initiallySelected + ";");
        ValidateLifecycleResults(db);
        ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", TString(remaining));
        UNIT_ASSERT_VALUES_EQUAL(GetSelectedJsonIndex(db, "json_idx_a", "json_idx_b"), remaining);

        ExecuteSuccess(db, "ALTER TABLE TestTable ADD INDEX " + initiallySelected + " GLOBAL USING json ON (Text);");
        ValidateLifecycleResults(db);
        const auto selectedAfterRecreate = GetSelectedJsonIndex(db, "json_idx_a", "json_idx_b");
        UNIT_ASSERT_VALUES_EQUAL(GetSelectedJsonIndex(db, "json_idx_a", "json_idx_b"), selectedAfterRecreate);
    }

    Y_UNIT_TEST(Negation) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // JE
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key' TRUE ON ERROR)");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') IS NULL");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') IS NOT NULL");
            ValidateNoAutoSelect(db, "COALESCE(JSON_EXISTS(Text, '$.key'), true)");

            ValidateNoAutoSelect(db, "NOT JSON_EXISTS(Text, '$.key')");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') == false");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') != true");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') == Just(false)");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') != Just(true)");

            // JV
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool DEFAULT TRUE ON EMPTY)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool DEFAULT TRUE ON ERROR)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool DEFAULT TRUE ON EMPTY DEFAULT TRUE ON ERROR)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) IS NULL");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) IS NOT NULL");
            ValidateNoAutoSelect(db, "COALESCE(JSON_VALUE(Text, '$.key' RETURNING Bool), true)");

            ValidateNoAutoSelect(db, "NOT JSON_VALUE(Text, '$.key' RETURNING Bool)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) == false");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) != true");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) == Just(false)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) != Just(true)");

            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Int32) IS NULL");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Int32) IS NOT NULL");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Int32) NOT IN (1, 2, 3)");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(FlagDisabled) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1'))");
            ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 2)'))");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Bool)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) == 10");
            ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))");
            ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))");
        }, /* enableJsonIndexAutoSelect */ false);
    }

    Y_UNIT_TEST(DynamicFlagInvalidatesCompileCache) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        ExecuteSuccess(db, R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Text JsonDocument,
                PRIMARY KEY (Key),
                INDEX json_idx GLOBAL USING json ON (Text)
            );
        )");
        ExecuteSuccess(db, R"(
            UPSERT INTO TestTable (Key, Text) VALUES
                (1, JsonDocument('{"tag":"red"}')),
                (2, JsonDocument('{"other":1}')),
                (3, JsonDocument('{"tag":"blue"}'));
        )");

        // Execute the identical query through one client twice so its compiled index-read plan is cached.
        ValidateLifecycleResults(db);
        ValidateLifecycleResults(db);
        ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", "json_idx");

        UpdateJsonAutoSelectConfig(kikimr, /* enabled */ false);
        ValidateLifecycleResults(db);
        ValidateLifecycleResults(db);
        ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", "json_idx");
        CompareYson(ExecuteKeys(db, " VIEW PRIMARY KEY"), ExecuteKeys(db, " VIEW json_idx"));

        UpdateJsonAutoSelectConfig(kikimr, /* enabled */ true);
        ValidateLifecycleResults(db);
        ValidateLifecycleResults(db);
        ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", "json_idx");
    }

    Y_UNIT_TEST(DynamicCreationGatePreservesExistingIndex) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        ExecuteSuccess(db, R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Text JsonDocument,
                PRIMARY KEY (Key),
                INDEX json_idx GLOBAL USING json ON (Text)
            );
        )");
        ExecuteSuccess(db, R"(
            UPSERT INTO TestTable (Key, Text) VALUES
                (1, JsonDocument('{"tag":"red"}')),
                (2, JsonDocument('{"other":1}')),
                (3, JsonDocument('{"tag":"blue"}'));
        )");
        ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", "json_idx");

        UpdateJsonFeatureFlags(kikimr, /*enableJsonIndex=*/false, /*enableAutoSelect=*/true);

        // EnableJsonIndex is a creation gate. A Ready object must remain usable and maintained.
        ExecuteSuccess(db, R"(
            UPSERT INTO TestTable (Key, Text) VALUES
                (4, JsonDocument('{"tag":"green"}'));
        )");
        CompareYson(R"([[[1u]];[[3u]];[[4u]]])", ExecuteKeys(db, " VIEW PRIMARY KEY"));
        CompareYson(ExecuteKeys(db, " VIEW PRIMARY KEY"), ExecuteKeys(db, " VIEW json_idx"));
        CompareYson(ExecuteKeys(db, " VIEW PRIMARY KEY"), ExecuteKeys(db));

        auto rejected = db.ExecuteQuery(R"(
            ALTER TABLE TestTable ADD INDEX rejected_idx GLOBAL USING json ON (Text);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(!rejected.IsSuccess(), rejected.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(rejected.GetIssues().ToString(), "JSON index support is disabled");

        ExecuteSuccess(db, "ALTER TABLE TestTable DROP INDEX json_idx;");
        ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", "json_idx");

        UpdateJsonFeatureFlags(kikimr, /*enableJsonIndex=*/true, /*enableAutoSelect=*/true);
        ExecuteSuccess(db, "ALTER TABLE TestTable ADD INDEX recreated_idx GLOBAL USING json ON (Text);");
        ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))", "recreated_idx");
        CompareYson(ExecuteKeys(db, " VIEW PRIMARY KEY"), ExecuteKeys(db, " VIEW recreated_idx"));
    }

    Y_UNIT_TEST(PassingInJE) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Basic PASSING with literal values
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING 1 AS v))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING true AS v))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING "str"u AS v))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING null AS v))");

            // PASSING with filter predicate at root
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == $v)' PASSING 1 AS v))");

            // PASSING with multiple variables
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == $v1 && @.k2 == $v2)' PASSING 1 AS v1, 2 AS v2))");

            // PASSING with range comparison
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ > $v)' PASSING 5 AS v))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ >= $lo && @ <= $hi)' PASSING 5 AS lo, 10 AS hi))");

            // PASSING combined with AND
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING 1 AS v) AND JSON_EXISTS(Text, '$.k2'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2 ? (@ == $v)' PASSING 2 AS v))");

            // PASSING combined with OR
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING 1 AS v) OR JSON_EXISTS(Text, '$.k2'))");

            // Non-autoselectable: TRUE ON ERROR changes semantics
            ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING 1 AS v TRUE ON ERROR))");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(PassingInJV) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Basic PASSING with literal integer variable in jsonpath filter
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING 5 AS v RETURNING Int64) == 10)");
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING 10 AS v RETURNING Int64) == 10)");

            // PASSING with boolean variable
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING true AS v RETURNING Bool))");

            // PASSING with multiple variables
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ > $lo && @ < $hi)' PASSING 5 AS lo, 20 AS hi RETURNING Int64) == 10)");

            // PASSING combined with AND
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING 5 AS v RETURNING Int64) == 10 AND JSON_EXISTS(Text, '$.k2'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_VALUE(Text, '$.k2 ? (@ == $v)' PASSING 2 AS v RETURNING Int64) == 2)");

            // PASSING combined with OR
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING 10 AS v RETURNING Int64) == 10 OR JSON_EXISTS(Text, '$.k2'))");

            // Non-autoselectable: DEFAULT ON EMPTY/ERROR changes semantics
            ValidateNoAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING 5 AS v RETURNING Int64 DEFAULT -1 ON EMPTY) == 10)");
            ValidateNoAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING 5 AS v RETURNING Int64 DEFAULT -1 ON ERROR) == 10)");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(PassingInJE_WithParameters) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // SQL parameter as PASSING value - integer
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v))");

            // SQL parameter as PASSING value - boolean
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Bool;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v))");

            // SQL parameter as PASSING value - string
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Utf8;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v))");

            // Multiple SQL parameters as PASSING values
            ValidateAutoSelectWithDecl(db, "DECLARE $lo AS Int64; DECLARE $hi AS Int64;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ >= $lo && @ <= $hi)' PASSING $lo AS lo, $hi AS hi))");

            // SQL parameter at root filter
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == $v)' PASSING $v AS v))");

            // Combined with AND
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v) AND JSON_EXISTS(Text, '$.k2'))");

            // Combined with OR
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v) OR JSON_EXISTS(Text, '$.k2'))");

            // Non-autoselectable: TRUE ON ERROR
            ValidateNoAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v TRUE ON ERROR))");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(PassingInJV_WithParameters) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // SQL parameter as PASSING value - integer
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING $v AS v RETURNING Int64) == 10)");

            // SQL parameter as PASSING value with range comparison
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING $v AS v RETURNING Int64) == 10)");

            // Multiple SQL parameters as PASSING values
            ValidateAutoSelectWithDecl(db, "DECLARE $lo AS Int64; DECLARE $hi AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ > $lo && @ < $hi)' PASSING $lo AS lo, $hi AS hi RETURNING Int64) > 0)");

            // SQL parameter as PASSING value - boolean
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Bool;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING $v AS v RETURNING Bool))");

            // Combined with AND
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING $v AS v RETURNING Int64) > 0 AND JSON_EXISTS(Text, '$.k2'))");

            // Combined with OR
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING $v AS v RETURNING Int64) == 10 OR JSON_EXISTS(Text, '$.k2'))");

            // Non-autoselectable: DEFAULT ON EMPTY/ERROR
            ValidateNoAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING $v AS v RETURNING Int64 DEFAULT -1 ON EMPTY) > 0)");
            ValidateNoAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING $v AS v RETURNING Int64 DEFAULT -1 ON ERROR) > 0)");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(TwoJsonIndexes_SameColumn) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    Data Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx_a GLOBAL USING json ON (Text),
                    INDEX json_idx_b GLOBAL USING json ON (Text)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (Key, Text, Data) VALUES
                    (1, JsonDocument('{"color": "red", "size": 10}'), "item1"),
                    (2, JsonDocument('{"color": "blue", "size": 20}'), "item2"),
                    (3, JsonDocument('{"color": "red", "size": 30}'), "item3"),
                    (4, JsonDocument('{"weight": 5}'), "item4"),
                    (5, JsonDocument('{}'), "item5");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Exactly one of the two indexes must appear in the query plan.
        ValidateOneOfTwoIndexesSelected(db, "JSON_EXISTS(Text, '$.color')", "json_idx_a", "json_idx_b");
        ValidateOneOfTwoIndexesSelected(db, "JSON_EXISTS(Text, '$.size')", "json_idx_a", "json_idx_b");
        ValidateOneOfTwoIndexesSelected(db, "JSON_VALUE(Text, '$.size' RETURNING Int64) == 10", "json_idx_a", "json_idx_b");
    }

    Y_UNIT_TEST(TwoJsonIndexes_DifferentColumns_SingleColumnPredicates) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text  JsonDocument,
                    Extra JsonDocument,
                    Data  Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx_text  GLOBAL USING json ON (Text),
                    INDEX json_idx_extra GLOBAL USING json ON (Extra)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (Key, Text, Extra, Data) VALUES
                    (1, JsonDocument('{"a": 1, "b": "hello"}'),JsonDocument('{"x": 10, "y": true}'), "row1"),
                    (2, JsonDocument('{"a": 2}'), JsonDocument('{"x": 20, "y": false}'), "row2"),
                    (3, JsonDocument('{"b": "world"}'), JsonDocument('{"x": 10, "z": null}'), "row3"),
                    (4, JsonDocument('{"a": 1, "c": 3}'), JsonDocument('{"w": 99}'), "row4"),
                    (5, JsonDocument('{}'), JsonDocument('{}'), "row5");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Predicate on Text -> must use json_idx_text, not json_idx_extra.
        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.a')", "json_idx_text",  "TestTable");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.a')", "json_idx_extra", "TestTable");
        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.b')", "json_idx_text",  "TestTable");

        // Predicate on Extra -> must use json_idx_extra, not json_idx_text.
        ValidateAutoSelect (db, "JSON_EXISTS(Extra, '$.x')", "json_idx_extra", "TestTable");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Extra, '$.x')", "json_idx_text",  "TestTable");
        ValidateAutoSelect (db, "JSON_EXISTS(Extra, '$.y')", "json_idx_extra", "TestTable");

        // Multiple predicates on the same column still use a single index.
        ValidateAutoSelect (db, "JSON_EXISTS(Text, '$.a') AND JSON_EXISTS(Text, '$.b')", "json_idx_text", "TestTable");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.a') AND JSON_EXISTS(Text, '$.b')", "json_idx_extra", "TestTable");
    }

    Y_UNIT_TEST(TwoJsonIndexes_DifferentColumns_MixedPredicates) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text  JsonDocument,
                    Extra JsonDocument,
                    Data  Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx_text  GLOBAL USING json ON (Text),
                    INDEX json_idx_extra GLOBAL USING json ON (Extra)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (Key, Text, Extra, Data) VALUES
                    (1, JsonDocument('{"a": 1}'), JsonDocument('{"x": 10}'), "row1"),
                    (2, JsonDocument('{"a": 2}'), JsonDocument('{"y": 20}'), "row2"),
                    (3, JsonDocument('{"b": "hi"}'), JsonDocument('{"x": 10}'), "row3"),
                    (4, JsonDocument('{"a": 1}'), JsonDocument('{"z": 30}'), "row4"),
                    (5, JsonDocument('{}'), JsonDocument('{}'), "row5");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // AND of predicates from two different indexed columns
        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.a') AND JSON_EXISTS(Extra, '$.x')",
            "json_idx_extra", "TestTable");

        // OR of predicates from two different indexed columns
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.a') OR JSON_EXISTS(Extra, '$.x')",
            "json_idx_text",  "TestTable");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.a') OR JSON_EXISTS(Extra, '$.x')",
            "json_idx_extra", "TestTable");

        ValidateNoAutoSelect(db,
            "JSON_EXISTS(Text, '$.a') OR JSON_EXISTS(Extra, '$.x') AND JSON_EXISTS(Extra, '$.y')",
            "json_idx_text", "TestTable");
        ValidateNoAutoSelect(db,
            "JSON_EXISTS(Text, '$.a') OR JSON_EXISTS(Extra, '$.x') AND JSON_EXISTS(Extra, '$.y')",
            "json_idx_extra", "TestTable");

        ValidateNoAutoSelect(db,
            "JSON_VALUE(Text, '$.a' RETURNING Int64) == 1 OR JSON_EXISTS(Extra, '$.x')",
            "json_idx_text", "TestTable");
        ValidateNoAutoSelect(db,
            "JSON_VALUE(Text, '$.a' RETURNING Int64) == 1 OR JSON_EXISTS(Extra, '$.x')",
            "json_idx_extra", "TestTable");

        ValidateNoAutoSelect(db,
            "JSON_EXISTS(Text, '$.a') OR JSON_VALUE(Extra, '$.x' RETURNING Int64) == 10",
            "json_idx_text", "TestTable");
        ValidateNoAutoSelect(db,
            "JSON_EXISTS(Text, '$.a') OR JSON_VALUE(Extra, '$.x' RETURNING Int64) == 10",
            "json_idx_extra", "TestTable");
    }

    Y_UNIT_TEST_TWIN(AutoSelectSqlForms, Compact) {
        auto kikimr = KikimrJson(/* enableJsonIndexAutoSelect */ true, Compact);
        auto db = kikimr.GetQueryClient();

        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    Data Utf8,
                    Active Bool,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO TestTable (Key, Text, Data, Active) VALUES
                    (1, JsonDocument('{"kind":"cat","score":10}'), "first"u, true),
                    (2, JsonDocument('{"kind":"dog","score":20}'), "second"u, true),
                    (3, JsonDocument('{"kind":"cat","score":30}'), "third"u, false),
                    (4, JsonDocument('{"other":true}'), "fourth"u, false);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT d.Key FROM TestTable AS d
            WHERE JSON_VALUE(d.Text, '$.kind' RETURNING Utf8) = "cat"u ORDER BY d.Key;
        )", 1, "[[[1u]];[[3u]]]");

        auto kindParams = TParamsBuilder()
            .AddParam("$kind").Utf8("cat").Build()
            .Build();
        ExecuteAndAssertJsonPlan(db, R"(
            DECLARE $kind AS Utf8;
            SELECT Key FROM TestTable
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = $kind ORDER BY Key;
        )", 1, "[[[1u]];[[3u]]]", kindParams);

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT Key FROM TestTable
            WHERE Active = true AND (Data != "missing"u AND
                  (JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "cat"u)) ORDER BY Key;
        )", 1, "[[[1u]]]");

        ExecuteAndAssertJsonPlan(db, R"(
            $docs = SELECT Key, Text FROM TestTable;
            SELECT Key FROM $docs
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "cat"u ORDER BY Key;
        )", 1, "[[[1u]];[[3u]]]");

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT d.Key FROM (SELECT Key, Text FROM TestTable) AS d
            WHERE JSON_VALUE(d.Text, '$.kind' RETURNING Utf8) = "cat"u ORDER BY d.Key;
        )", 1, "[[[1u]];[[3u]]]");

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT Data FROM TestTable
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "cat"u
            ORDER BY Key DESC LIMIT 1;
        )", 1, R"([[["third"]]])");

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT Key FROM TestTable VIEW PRIMARY KEY
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "cat"u ORDER BY Key;
        )", 0, "[[[1u]];[[3u]]]");

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT Key FROM TestTable
            WHERE String::AsciiToUpper(JSON_VALUE(Text, '$.kind' RETURNING Utf8)) = "CAT"
            ORDER BY Key;
        )", 0, "[[[1u]];[[3u]]]");
    }

    Y_UNIT_TEST_TWIN(JsonIndexOptimizerLifecycle, Compact) {
        auto kikimr = KikimrJson(/* enableJsonIndexAutoSelect */ true, Compact);
        auto db = kikimr.GetQueryClient();

        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE `/Root/Lifecycle` (
                    Key Uint64,
                    Text JsonDocument,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO `/Root/Lifecycle` (Key, Text) VALUES
                    (1, JsonDocument('{"kind":"target"}')),
                    (2, JsonDocument('{"kind":"other"}'));
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString sql = R"(
            SELECT Key FROM `/Root/Lifecycle`
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "target"u
            ORDER BY Key;
        )";

        ExecuteAndAssertJsonPlan(db, sql, 0, "[[[1u]]]");
        ExecuteAndAssertJsonPlan(db, sql, 0, "[[[1u]]]");

        {
            auto result = db.ExecuteQuery(R"(
                ALTER TABLE `/Root/Lifecycle`
                    ADD INDEX json_idx GLOBAL USING json ON (Text);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        ExecuteAndAssertJsonPlan(db, sql, 1, "[[[1u]]]");

        {
            auto result = db.ExecuteQuery(R"(
                UPDATE `/Root/Lifecycle`
                SET Text = JsonDocument('{"kind":"target"}')
                WHERE Key = 2;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        ExecuteAndAssertJsonPlan(db, sql, 1, "[[[1u]];[[2u]]]");

        {
            auto result = db.ExecuteQuery(R"(
                ALTER TABLE `/Root/Lifecycle` DROP INDEX json_idx;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                UPDATE `/Root/Lifecycle`
                SET Text = JsonDocument('{"kind":"other"}')
                WHERE Key = 1;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        ExecuteAndAssertJsonPlan(db, sql, 0, "[[[2u]]]");

        {
            auto result = db.ExecuteQuery(R"(
                ALTER TABLE `/Root/Lifecycle`
                    ADD INDEX json_idx GLOBAL USING json ON (Text);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO `/Root/Lifecycle` (Key, Text) VALUES
                    (3, JsonDocument('{"kind":"target"}'));
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        ExecuteAndAssertJsonPlan(db, sql, 1, "[[[2u]];[[3u]]]");
    }

    Y_UNIT_TEST_TWIN(JsonIndexImplSchemaVersionBump, Compact) {
        auto kikimr = KikimrJson(/* enableJsonIndexAutoSelect */ true, Compact);
        auto db = kikimr.GetQueryClient();

        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE `/Root/SchemaDocs` (
                    Key Uint64,
                    Text JsonDocument,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO `/Root/SchemaDocs` (Key, Text) VALUES
                    (1, JsonDocument('{"kind":"target"}')),
                    (2, JsonDocument('{"kind":"other"}'));
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString sql = R"(
            SELECT Key FROM `/Root/SchemaDocs` VIEW json_idx
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "target"u
            ORDER BY Key;
        )";

        const TString expected = ExecuteAndAssertJsonPlan(db, sql, 1, "[[[1u]]]");
        UNIT_ASSERT_VALUES_EQUAL_C(
            ExecuteAndAssertJsonPlan(db, sql, 1, "[[[1u]]]"), expected, sql);

        Tests::TClient& client = kikimr.GetTestClient();
        const TString scheme = R"(
            Name: "indexImplTable"
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 1
                    SizeToSplit: 100500
                }
            }
        )";
        auto alter = client.AlterTable("/Root/SchemaDocs/json_idx", scheme, {});
        UNIT_ASSERT_VALUES_EQUAL_C(alter->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK,
            alter->Record.ShortDebugString());

        UNIT_ASSERT_VALUES_EQUAL_C(
            ExecuteAndAssertJsonPlan(db, sql, 1, "[[[1u]]]"), expected, sql);
    }

    Y_UNIT_TEST(Prefixed) {
        auto kikimr = KikimrJsonPrefix(true);
        auto db = kikimr.GetQueryClient();

        {
            std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    UserId Uint64,
                    Text JsonDocument,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (UserId, Text)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                UPSERT INTO TestTable (Key, UserId, Text) VALUES
                    (1, 100, JsonDocument('{"k1": "v1"}')),
                    (2, 100, JsonDocument('{"k2": "v2"}')),
                    (3, 200, JsonDocument('{"k1": "v1"}')),
                    (4, 200, JsonDocument('{"k3": "v3"}'));
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Complete prefix equality allows auto-selection; omitting it keeps the table scan.
        ValidateAutoSelect(db, "UserId=100 AND JSON_EXISTS(Text, '$.k1')", "json_idx", "TestTable");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1')", "json_idx", "TestTable");
    }

    Y_UNIT_TEST(PrefixedMultiColumn) {
        auto kikimr = KikimrJsonPrefix(true);
        auto db = kikimr.GetQueryClient();

        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Tenant Utf8,
                    UserId Uint64,
                    Text JsonDocument,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Tenant, UserId, Text)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO TestTable (Key, Tenant, UserId, Text) VALUES
                    (1, "acme"u,   100, JsonDocument('{"kind":"cats","score":10}')),
                    (2, "acme"u,   100, JsonDocument('{"kind":"dogs","score":20}')),
                    (3, "acme"u,   200, JsonDocument('{"kind":"cats","score":20}')),
                    (4, "globex"u, 100, JsonDocument('{"kind":"cats","score":30}'));
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ValidateAutoSelect(db,
            R"(Tenant = "acme"u AND UserId = 100 AND JSON_EXISTS(Text, '$.kind'))",
            "json_idx", "TestTable");
        ValidateAutoSelect(db,
            R"(100 = UserId AND JSON_VALUE(Text, '$.score' RETURNING Int64) = 20 AND "acme"u = Tenant)",
            "json_idx", "TestTable");
        ValidateAutoSelectWithDecl(db,
            "DECLARE $tenant AS Utf8;\nDECLARE $uid AS Uint64;",
            R"(UserId = $uid AND JSON_EXISTS(Text, '$.kind') AND Tenant = $tenant)",
            "json_idx", "TestTable");

        ValidateNoAutoSelect(db,
            R"(UserId = 100 AND JSON_EXISTS(Text, '$.kind'))",
            "json_idx", "TestTable");
        ValidateNoAutoSelect(db,
            R"(Tenant = "acme"u AND JSON_EXISTS(Text, '$.kind'))",
            "json_idx", "TestTable");
        ValidateNoAutoSelect(db,
            R"((Tenant = "acme"u OR Tenant = "globex"u) AND UserId = 100 AND JSON_EXISTS(Text, '$.kind'))",
            "json_idx", "TestTable");
        ValidateNoAutoSelect(db,
            R"(Tenant = "acme"u AND UserId > 0 AND JSON_EXISTS(Text, '$.kind'))",
            "json_idx", "TestTable");
    }

    Y_UNIT_TEST(CompactJsonDocument) {
        auto kikimr = KikimrCompactJsonAutoSelect();
        auto db = kikimr.GetQueryClient();

        ExecuteSuccess(db, R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Text JsonDocument,
                PRIMARY KEY (Key),
                INDEX json_idx GLOBAL USING json ON (Text)
            );
        )");
        ExecuteSuccess(db, R"(
            UPSERT INTO TestTable (Key, Text) VALUES
                (1, JsonDocument('{"tag": "red", "size": 10}')),
                (2, JsonDocument('{"tag": "blue", "size": 20}')),
                (3, JsonDocument('{"tag": "red"}')),
                (4, JsonDocument('{}'));
        )");

        ValidateCompactAutoSelectResults(db,
            R"(JSON_VALUE(Text, '$.tag' RETURNING Utf8) == "red"u)",
            R"([[[1u]];[[3u]]])");
        ValidateCompactAutoSelectResults(db,
            R"(JSON_EXISTS(Text, '$.size'))",
            R"([[[1u]];[[2u]]])");
    }

    Y_UNIT_TEST(CompactJsonMultiShardBuildAndDml) {
        auto kikimr = KikimrCompactJsonAutoSelect();
        auto db = kikimr.GetQueryClient();

        // Explicit boundaries make the build scan four main-table shards. SQL
        // ALTER does not support replacing PARTITION_AT_KEYS, so the lifecycle
        // below exercises deterministic multi-shard build/read/DML without a
        // timing-dependent auto-partitioning split.
        ExecuteSuccess(db, R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Text JsonDocument,
                PRIMARY KEY (Key)
            ) WITH (
                PARTITION_AT_KEYS = (10, 20, 30)
            );
        )");
        ExecuteSuccess(db, R"(
            UPSERT INTO TestTable (Key, Text) VALUES
                (1,  JsonDocument('{"tag":"red"}')),
                (9,  JsonDocument('{"other":1}')),
                (10, JsonDocument('{"tag":"red"}')),
                (11, JsonDocument('{"tag":"blue"}')),
                (19, JsonDocument('{"tag":"red"}')),
                (20, JsonDocument('{"other":2}')),
                (21, JsonDocument('{"tag":"red"}')),
                (29, JsonDocument('{"tag":"blue"}')),
                (30, JsonDocument('{"tag":"red"}')),
                (31, JsonDocument('{}'));
        )");

        // Building after the data is present covers snapshot ingestion from all
        // four partitions into a compact JSON index.
        ExecuteSuccess(db,
            "ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (Text);");
        ValidateCompactAutoSelectResults(db,
            R"(JSON_VALUE(Text, '$.tag' RETURNING Utf8) == "red"u)",
            R"([[[1u]];[[10u]];[[19u]];[[21u]];[[30u]]])");

        ExecuteSuccess(db, R"(
            UPSERT INTO TestTable (Key, Text) VALUES
                (9,  JsonDocument('{"tag":"red"}')),
                (19, JsonDocument('{"other":19}')),
                (25, JsonDocument('{"tag":"blue"}')),
                (40, JsonDocument('{"tag":"red"}'));
            DELETE FROM TestTable WHERE Key = 21;
        )");

        ValidateCompactAutoSelectResults(db,
            R"(JSON_VALUE(Text, '$.tag' RETURNING Utf8) == "red"u)",
            R"([[[1u]];[[9u]];[[10u]];[[30u]];[[40u]]])");
        ValidateCompactAutoSelectResults(db,
            R"(JSON_EXISTS(Text, '$.other'))",
            R"([[[19u]];[[20u]]])");
    }

    Y_UNIT_TEST(CompactPrefixedJsonDocument) {
        auto kikimr = KikimrCompactJsonAutoSelect();
        auto db = kikimr.GetQueryClient();

        ExecuteSuccess(db, R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Tenant Uint64,
                Text JsonDocument,
                PRIMARY KEY (Key),
                INDEX json_idx GLOBAL USING json ON (Tenant, Text)
            );
        )");
        ExecuteSuccess(db, R"(
            UPSERT INTO TestTable (Key, Tenant, Text) VALUES
                (1, 10, JsonDocument('{"tag": "red"}')),
                (2, 10, JsonDocument('{"tag": "blue"}')),
                (3, 20, JsonDocument('{"tag": "red"}')),
                (4, 20, JsonDocument('{}'));
        )");

        ValidateCompactAutoSelectResults(db,
            R"(Tenant = 10 AND JSON_VALUE(Text, '$.tag' RETURNING Utf8) == "red"u)",
            R"([[[1u]]])");
        ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.tag'))");
    }

    Y_UNIT_TEST(CompactJsonDocumentWithStringPkRowId) {
        auto kikimr = KikimrCompactJsonAutoSelect();
        auto db = kikimr.GetQueryClient();

        ExecuteSuccess(db, R"(
            CREATE TABLE TestTable (
                Key Utf8 NOT NULL,
                Text JsonDocument,
                PRIMARY KEY (Key),
                INDEX json_idx GLOBAL USING json ON (Text)
            );
        )");
        ExecuteSuccess(db, R"(
            UPSERT INTO TestTable (Key, Text) VALUES
                ("alpha"u, JsonDocument('{"enabled": true}')),
                ("beta"u, JsonDocument('{"enabled": false}')),
                ("gamma"u, JsonDocument('{"enabled": true}'));
        )");

        ValidateCompactAutoSelectResults(db,
            R"(JSON_VALUE(Text, '$.enabled' RETURNING Bool))",
            R"([["alpha"];["gamma"]])");
    }
}

}  // namespace NKikimr::NKqp
