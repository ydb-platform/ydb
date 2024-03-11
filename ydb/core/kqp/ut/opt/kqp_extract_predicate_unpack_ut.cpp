#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

void PrepareTablesToUnpack(TSession session) {
    auto result1 = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/ComplexKey` (
            Key Int32,
            Fk Int32,
            Value String,
            PRIMARY KEY (Key, Fk)
        );
        CREATE TABLE `/Root/ComplexKeyNotNull` (
            Key Int32 NOT NULL,
            Fk Int32 NOT NULL,
            Value String,
            PRIMARY KEY (Key, Fk)
        );

        CREATE TABLE `/Root/UintComplexKey` (
            Key UInt64,
            Fk Int64,
            Value String,
            PRIMARY KEY (Key, Fk)
        );
        CREATE TABLE `/Root/UintComplexKeyWithIndex` (
            Key UInt64,
            Fk Int64,
            Value String,
            Payload String,
            PRIMARY KEY (Value, Fk),
            INDEX Index GLOBAL ON (Key, Fk)
        );

        CREATE TABLE `/Root/SimpleKey` (
            Key Int32,
            Value String,
            PRIMARY KEY (Key)
        );
        CREATE TABLE `/Root/Uint64Table` (
            Key Uint64,
            Value Uint64,
            PRIMARY KEY (Key)
        );
        CREATE TABLE `/Root/Uint32Table` (
            Key Uint32,
            Value Uint32,
            PRIMARY KEY (Key)
        );

        CREATE TABLE `/Root/UTF8Table` (
            Key UTF8,
            Value UTF8,
            PRIMARY KEY (Key)
        );
    )").GetValueSync();
    UNIT_ASSERT_C(result1.IsSuccess(), result1.GetIssues().ToString());

    auto result2 = session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/ComplexKey` (Key, Fk, Value) VALUES
            (null, null, "NullValue"),
            (1, 101, "Value1"),
            (2, 102, "Value1"),
            (2, 103, "Value3"),
            (3, 103, "Value2"),
            (4, 104, "Value2"),
            (5, 105, "Value3");

        REPLACE INTO `/Root/ComplexKeyNotNull` (Key, Fk, Value) VALUES
            (1, 101, "Value1"),
            (2, 102, "Value1"),
            (2, 103, "Value3"),
            (3, 103, "Value2"),
            (4, 104, "Value2"),
            (5, 105, "Value3");

        REPLACE INTO `/Root/UintComplexKey` (Key, Fk, Value) VALUES
            (null, null, "NullValue"),
            (1, 101, "Value1"),
            (-2, 102, "Value1"),
            (-2, 103, "Value3"),
            (3, 103, "Value2"),
            (4, 104, "Value2"),
            (5, 105, "Value3");

        REPLACE INTO `/Root/UintComplexKeyWithIndex` (Key, Fk, Value, Payload) VALUES
            (null, null, "NullValue", "null"),
            (1, 101, "Value1", "101"),
            (-2, 102, "Value1", "102"),
            (-2, 103, "Value3", "103-1"),
            (3, 103, "Value2", "103-2"),
            (4, 104, "Value2", "104"),
            (5, 105, "Value3", "105");

        REPLACE INTO `/Root/SimpleKey` (Key, Value) VALUES
            (100, "Value20"),
            (101, "Value21"),
            (102, "Value22"),
            (103, "Value23");

        REPLACE INTO `/Root/Uint64Table` (Key, Value) VALUES
            (Cast(-1 AS Uint64), 1),
            (-1, 2),
            (5, -1),
            (3, 3);

        REPLACE INTO `/Root/UTF8Table` (Key, Value) VALUES
            ("1", "2"),
            ("5", "-1"),
            ("3", "3");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result2.IsSuccess(), result2.GetIssues().ToString());

}

Y_UNIT_TEST_SUITE(KqpExtractPredicateLookup) {

void Test(const TString& query, const TString& answer, THashSet<TString> allowScans = {}, NYdb::TParams params = TParamsBuilder().Build()) {
    TKikimrSettings settings;
    settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    PrepareTablesToUnpack(session);

    TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

    auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_EQUAL(result.GetResultSets().size(), 1);
    CompareYson(answer, FormatResultSetYson(result.GetResultSet(0)));

    auto explain = session.ExplainDataQuery(query).ExtractValueSync();
    UNIT_ASSERT(explain.GetPlan().Contains("Lookup"));
    Cerr << explain.GetPlan();

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(explain.GetPlan(), &plan, true);
    UNIT_ASSERT(ValidatePlanNodeIds(plan));
    for (const auto& tableStats : plan.GetMap().at("tables").GetArray()) {
        TString table = tableStats.GetMap().at("name").GetString();
        if (allowScans.contains(table)) {
            continue;
        }

        for (auto& read : tableStats.GetMap().at("reads").GetArray()) {
            UNIT_ASSERT(!read.GetMap().at("type").GetString().Contains("Scan"));
        }
    }
}

void TestRange(const TString& query, const TString& answer, ui64 rowsRead, int stagesCount = 1, bool streamLookup = true) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(streamLookup);

    auto settings = TKikimrSettings()
        .SetAppConfig(appConfig);

    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    PrepareTablesToUnpack(session);

    TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_EQUAL(result.GetResultSets().size(), 1);
    CompareYson(answer, FormatResultSetYson(result.GetResultSet(0)));

    auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
    UNIT_ASSERT_EQUAL(stagesCount, stats.query_phases_size());

    ui64 rowsStats = 0;
    for (auto& phase : stats.query_phases()) {
        for (auto& access : phase.table_access()) {
            rowsStats += access.reads().rows();
        }
    }
    UNIT_ASSERT_EQUAL(rowsStats, rowsRead);
}

Y_UNIT_TEST(OverflowLookup) {
    TestRange(
        R"(
            SELECT * FROM `/Root/Uint64Table`
            WHERE Key = 3;
        )",
        R"([
            [[3u];[3u]]
        ])",
        1);

    TestRange(
        R"(
            SELECT * FROM `/Root/Uint64Table`
            WHERE Key = -1;
        )",
        R"([])",
        0,
        2,
        false);

    TestRange(
        R"(
            SELECT Value FROM `/Root/Uint64Table`
            WHERE Key IS NULL;
        )",
        R"([
            [[1u]]
        ])",
        1);
}

Y_UNIT_TEST(SimpleRange) {
    TestRange(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key >= 101 AND Key < 104;
        )",
        R"([
            [[101];["Value21"]];
            [[102];["Value22"]];
            [[103];["Value23"]]
        ])",
        3);

    TestRange(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key >= 101;
        )",
        R"([
            [[101];["Value21"]];
            [[102];["Value22"]];
            [[103];["Value23"]]
        ])",
        3);

    TestRange(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key < 104;
        )",
        R"([
            [[100];["Value20"]];
            [[101];["Value21"]];
            [[102];["Value22"]];
            [[103];["Value23"]]
        ])",
        4);

    TestRange(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key < 104 AND Key >= 0;
        )",
        R"([
            [[100];["Value20"]];
            [[101];["Value21"]];
            [[102];["Value22"]];
            [[103];["Value23"]]
        ])",
        4);

    TestRange(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key >= 101 AND Key < 104u;
        )",
        R"([
            [[101];["Value21"]];
            [[102];["Value22"]];
            [[103];["Value23"]]
        ])",
        3);

    TestRange(
        R"(
            SELECT * FROM `/Root/UTF8Table`
            WHERE Key = "1";
        )",
        R"([
            [["1"];["2"]]
        ])",
        1);
}

Y_UNIT_TEST(ComplexRange) {
    TestRange(
        R"(
            SELECT Key, Fk, Value FROM `/Root/ComplexKey`
            WHERE Key >= 1 AND Key < 4 AND Fk >= 101 AND Fk < 104;
        )",
        R"([
            [[1];[101];["Value1"]];
            [[2];[102];["Value1"]];
            [[2];[103];["Value3"]];
            [[3];[103];["Value2"]]
        ])",
        4);

    TestRange(
        R"(
            SELECT Key, Fk, Value FROM `/Root/ComplexKey`
            WHERE (Key, Fk) >= (4, 104);
        )",
        R"([
            [[4];[104];["Value2"]];
            [[5];[105];["Value3"]]
        ])",
        2);

    TestRange(
        R"(
            SELECT Key, Fk, Value FROM `/Root/ComplexKeyNotNull`
            WHERE (Key, Fk) >= (1, 101) AND (Key, Fk) < (4, 104);
        )",
        R"([
            [1;101;["Value1"]];
            [2;102;["Value1"]];
            [2;103;["Value3"]];
            [3;103;["Value2"]]
        ])",
        4);

    TestRange(
        R"(
            SELECT Key, Fk, Value FROM `/Root/ComplexKey`
            WHERE Key >= 3 and Key > 4;
        )",
        R"([
            [[5];[105];["Value3"]]
        ])",
        1,
        2);

    TestRange(
        R"(
            SELECT Key, Fk, Value FROM `/Root/ComplexKey`
            WHERE (Key < 2) OR (Key = 2 AND Fk > 102)
        )",
        R"([
            [[1];[101];["Value1"]];[[2];[103];["Value3"]]
        ])",
        2,
        2);
}

Y_UNIT_TEST(PointJoin) {
    Test(
        R"(
            DECLARE $p as Int32;
            SELECT l.Key, l.Fk, l.Value, r.Key, r.Value FROM `/Root/SimpleKey` AS r
            INNER JOIN `/Root/ComplexKey` AS l
               ON l.Fk = r.Key
            WHERE l.Key = 1 + $p and l.Key = l.Key
            ORDER BY r.Value
        )",
        R"([
            [[2];[102];["Value1"];[102];["Value22"]];
            [[2];[103];["Value3"];[103];["Value23"]]
        ])",
        {"/Root/SimpleKey"},
        TParamsBuilder().AddParam("$p").Int32(1).Build().Build());

    Test(
        R"(
            DECLARE $p as Int32;
            SELECT l.Key, l.Fk, l.Value, r.Key, r.Value FROM `/Root/SimpleKey` AS r
            INNER JOIN `/Root/UintComplexKey` AS l
               ON l.Fk = r.Key
            WHERE l.Key = $p and l.Key = l.Key
            ORDER BY r.Value
        )",
        R"([
            [[3u];[103];["Value2"];[103];["Value23"]]
        ])",
        {"/Root/SimpleKey"},
        TParamsBuilder().AddParam("$p").Int32(3).Build().Build());

    Test(
        R"(
            DECLARE $p as Int32;
            SELECT l.Key, l.Fk, l.Value, r.Key, r.Value FROM `/Root/SimpleKey` AS r
            INNER JOIN `/Root/UintComplexKey` AS l
               ON l.Fk = r.Key
            WHERE l.Key = $p and l.Key = l.Key
            ORDER BY r.Value
        )",
        R"([
        ])",
        {"/Root/SimpleKey"},
        TParamsBuilder().AddParam("$p").Int32(-2).Build().Build());

    Test(
        R"(
            DECLARE $p as Int32;
            SELECT l.Key, l.Fk, l.Value, r.Key, r.Value, l.Payload FROM `/Root/SimpleKey` AS r
            INNER JOIN `/Root/UintComplexKeyWithIndex` VIEW Index AS l
               ON l.Fk = r.Key
            WHERE l.Key = $p and l.Key = l.Key
            ORDER BY r.Value
        )",
        R"([
            [[3u];[103];["Value2"];[103];["Value23"];["103-2"]]
        ])",
        {"/Root/SimpleKey", "/Root/UintComplexKeyWithIndex/Index/indexImplTable"},
        TParamsBuilder().AddParam("$p").Int32(3).Build().Build());
}

Y_UNIT_TEST(SqlInJoin) {
    Test(
        R"(
            DECLARE $p AS Int32;
            $rows = (SELECT Key FROM `/Root/SimpleKey`);
            SELECT Key, Fk, Value FROM `/Root/ComplexKey`
                WHERE Fk IN $rows AND Key = 1 + $p
                ORDER BY Key, Fk
        )",
        R"([
            [[2];[102];["Value1"]];
            [[2];[103];["Value3"]]
        ])",
        {"/Root/SimpleKey"},
        TParamsBuilder().AddParam("$p").Int32(1).Build().Build());

    Test(
        R"(
            DECLARE $p AS Int32;
            $rows = (SELECT Key FROM `/Root/SimpleKey`);
            SELECT Key, Fk, Value FROM `/Root/UintComplexKey`
                WHERE Fk IN $rows AND Key = $p
        )",
        R"([
            [[3u];[103];["Value2"]]
        ])",
        {"/Root/SimpleKey"},
        TParamsBuilder().AddParam("$p").Int32(3).Build().Build());

    Test(
        R"(
            DECLARE $p AS Int32;
            $rows = (SELECT Key FROM `/Root/SimpleKey`);
            SELECT Key, Fk, Value FROM `/Root/UintComplexKey`
                WHERE Fk IN $rows AND Key = $p
        )",
        R"([
        ])",
        {"/Root/SimpleKey"},
        TParamsBuilder().AddParam("$p").Int32(-2).Build().Build());


    Test(
        R"(
            DECLARE $p AS Int32;
            $rows = (SELECT Key FROM `/Root/SimpleKey`);
            SELECT Key, Fk, Value, Payload FROM `/Root/UintComplexKeyWithIndex` VIEW Index
                WHERE Fk IN $rows AND Key = $p
        )",
        R"([
            [[3u];[103];["Value2"];["103-2"]]
        ])",
        {"/Root/SimpleKey", "/Root/UintComplexKeyWithIndex/Index/indexImplTable"},
        TParamsBuilder().AddParam("$p").Int32(3).Build().Build());
}

} // suite

} // namespace NKqp
} // namespace NKikimr
