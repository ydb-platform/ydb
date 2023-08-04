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
        CREATE TABLE `/Root/SimpleKey` (
            Key Int32,
            Value String,
            PRIMARY KEY (Key));
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

        REPLACE INTO `/Root/SimpleKey` (Key, Value) VALUES
            (100, "Value20"),
            (101, "Value21"),
            (102, "Value22"),
            (103, "Value23");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result2.IsSuccess(), result2.GetIssues().ToString());

}

Y_UNIT_TEST_SUITE(KqpExtractPredicateLookup) {

void Test(const TString& query, const TString& answer, bool noScans = true, bool checkForLookups = true) {
    TKikimrSettings settings;
    settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    PrepareTablesToUnpack(session);

    TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

    auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_EQUAL(result.GetResultSets().size(), 1);
    CompareYson(answer, FormatResultSetYson(result.GetResultSet(0)));

    auto explain = session.ExplainDataQuery(query).ExtractValueSync();
    if (checkForLookups) {
        UNIT_ASSERT(explain.GetPlan().Contains("Lookup"));
    }
    if (noScans) {
        UNIT_ASSERT(!explain.GetPlan().Contains("Scan"));
    }
}

void TestRange(const TString& query, const TString& answer, int stagesCount = 1) {
    TKikimrSettings settings;
    settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
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
}

Y_UNIT_TEST(SimpleRange) {
    TestRange(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key >= 101 AND Key < 104;
        )",
        R"([
            [[101];["Value21"]];[[102];["Value22"]];[[103];["Value23"]]
        ])");

    TestRange(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key >= 101;
        )",
        R"([
            [[101];["Value21"]];[[102];["Value22"]];[[103];["Value23"]]
        ])");

    TestRange(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key < 104;
        )",
        R"([
            [[100];["Value20"]];[[101];["Value21"]];[[102];["Value22"]];[[103];["Value23"]]
        ])",
        2);

    TestRange(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key < 104 AND Key >= 0;
        )",
        R"([
            [[100];["Value20"]];[[101];["Value21"]];[[102];["Value22"]];[[103];["Value23"]]
        ])");

    TestRange(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key >= 101 AND Key < 104u;
        )",
        R"([
            [[101];["Value21"]];[[102];["Value22"]];[[103];["Value23"]]
        ])");
}

Y_UNIT_TEST(ComplexRange) {
    TestRange(
        R"(
            SELECT Key, Fk, Value FROM `/Root/ComplexKey`
            WHERE Key >= 1 AND Key < 4 AND Fk >= 101 AND Fk < 104;
        )",
        R"([
            [[1];[101];["Value1"]];[[2];[102];["Value1"]];[[2];[103];["Value3"]];[[3];[103];["Value2"]]
        ])");

    TestRange(
        R"(
            SELECT Key, Fk, Value FROM `/Root/ComplexKey`
            WHERE (Key, Fk) >= (1, 101) AND (Key, Fk) < (4, 104);
        )",
        R"([
            [[1];[101];["Value1"]];[[2];[102];["Value1"]];[[2];[103];["Value3"]];[[3];[103];["Value2"]]
        ])");

    TestRange(
        R"(
            SELECT Key, Fk, Value FROM `/Root/ComplexKey`
            WHERE Key >= 3 and Key > 4;
        )",
        R"([
            [[5];[105];["Value3"]]
        ])",
        2);

}

Y_UNIT_TEST(SqlIn) {
    Test(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key IN AsList(100, 102, (100 + 3))
            ORDER BY Key;
        )",
        R"([
            [[100];["Value20"]];[[102];["Value22"]];[[103];["Value23"]]
        ])");
}

Y_UNIT_TEST(BasicLookup) {
    Test(
        R"(
            SELECT * FROM `/Root/SimpleKey`
            WHERE Key = 100 or Key = 102 or Key = 103 or Key = null;
        )",
        R"([
            [[100];["Value20"]];[[102];["Value22"]];[[103];["Value23"]]
        ])");
}

Y_UNIT_TEST(ComplexLookup) {
    Test(
        R"(
            SELECT Key, Value FROM `/Root/SimpleKey`
            WHERE Key = 100 or Key = 102 or Key = (100 + 3);
        )",
        R"([
            [[100];["Value20"]];[[102];["Value22"]];[[103];["Value23"]]
        ])");
}

Y_UNIT_TEST(SqlInComplexKey) {
    Test(
        R"(
            SELECT Key, Fk, Value FROM `/Root/ComplexKey`
            WHERE (Key, Fk) IN AsList(
                (1, 101),
                (2, 102),
                (2, 102 + 1),
            )
            ORDER BY Key, Fk;
        )",
        R"([
            [[1];[101];["Value1"]];[[2];[102];["Value1"]];[[2];[103];["Value3"]]
        ])");
}

Y_UNIT_TEST(BasicLookupComplexKey) {
    Test(
        R"(
            SELECT Key, Fk, Value FROM `/Root/ComplexKey`
            WHERE (Key = 1 and Fk = 101) OR
                (2 = Key and 102 = Fk) OR
                (2 = Key and 103 = Fk);
        )",
        R"([
            [[1];[101];["Value1"]];[[2];[102];["Value1"]];[[2];[103];["Value3"]]
        ])");
}

Y_UNIT_TEST(ComplexLookupComplexKey) {
    Test(
        R"(
            SELECT Key, Fk, Value FROM `/Root/ComplexKey`
            WHERE (Key = 1 and Fk = 101) OR
                (2 = Key and 102 = Fk) OR
                (2 = Key and 102 + 1 = Fk);
        )",
        R"([
            [[1];[101];["Value1"]];[[2];[102];["Value1"]];[[2];[103];["Value3"]]
        ])");
}

Y_UNIT_TEST(PointJoin) {
    Test(
        R"(
            SELECT l.Key, l.Fk, l.Value, r.Key, r.Value FROM `/Root/SimpleKey` AS r
            INNER JOIN `/Root/ComplexKey` AS l
               ON l.Fk = r.Key
            WHERE l.Key = 1 + 1 and l.Key = l.Key
            ORDER BY r.Value
        )",
        R"([
            [[2];[102];["Value1"];[102];["Value22"]];
            [[2];[103];["Value3"];[103];["Value23"]]
        ])",
        /* noScans */ false);
}

} // suite

} // namespace NKqp
} // namespace NKikimr
