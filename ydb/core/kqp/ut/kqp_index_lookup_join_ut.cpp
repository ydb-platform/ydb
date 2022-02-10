#include <ydb/core/kqp/ut/common/kqp_ut_common.h> 

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h> 

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

void PrepareTables(TSession session) {
    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        PRAGMA kikimr.UseNewEngine = "true";
        CREATE TABLE `/Root/Left` (
            Key Int32,
            Fk Int32,
            Value String,
            PRIMARY KEY (Key)
        );
        CREATE TABLE `/Root/Right` (
            Key Int32,
            Value String,
            PRIMARY KEY (Key)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteDataQuery(R"(
        PRAGMA kikimr.UseNewEngine = "true";

        REPLACE INTO `/Root/Left` (Key, Fk, Value) VALUES
            (1, 101, "Value1"),
            (2, 102, "Value1"),
            (3, 103, "Value2"),
            (4, 104, "Value2"),
            (5, 105, "Value3");

        REPLACE INTO `/Root/Right` (Key, Value) VALUES
            (100, "Value20"),
            (101, "Value21"),
            (102, "Value22"),
            (103, "Value23");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
}

Y_UNIT_TEST_SUITE(KqpIndexLookupJoin) {

template <bool UseNewEngine>
void Test(const TString& query, const TString& answer, size_t rightTableReads) {
    TKikimrRunner kikimr;
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    PrepareTables(session);

    TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

    auto result = session.ExecuteDataQuery(Q_(query), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    CompareYson(answer, FormatResultSetYson(result.GetResultSet(0)));

    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), kikimr.IsUsingSnapshotReads() && !UseNewEngine ? 2 : 3);

    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/Left");
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 5);

    ui32 index = 1;
    if (UseNewEngine) {
        UNIT_ASSERT(stats.query_phases(1).table_access().empty()); // keys extraction for lookups
        index = 2;
    }

    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access(0).name(), "/Root/Right");
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access(0).reads().rows(), rightTableReads);
}

Y_UNIT_TEST_NEW_ENGINE(Inner) {
    Test<UseNewEngine>(
        R"(
            SELECT l.Key, l.Fk, l.Value, r.Key, r.Value
            FROM `/Root/Left` AS l
            INNER JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            WHERE l.Value = 'Value1'   -- left table payload filter
              AND r.Value != 'Value22' -- right table payload filter
        )",
        R"([
            [[1];[101];["Value1"];[101];["Value21"]]
        ])",
        2);
}

Y_UNIT_TEST_NEW_ENGINE(Left) {
    Test<UseNewEngine>(
        R"(
            SELECT l.Key, l.Fk, l.Value, r.Key, r.Value
            FROM `/Root/Left` AS l
            LEFT JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            WHERE l.Value != 'Value1'   -- left table payload filter
            ORDER BY l.Key
        )",
        R"([
            [[3];[103];["Value2"];[103];["Value23"]];
            [[4];[104];["Value2"];#;#];
            [[5];[105];["Value3"];#;#]
        ])",
        1);
}

Y_UNIT_TEST_NEW_ENGINE(LeftOnly) {
    Test<UseNewEngine>(
        R"(
            SELECT l.Key, l.Fk, l.Value
            FROM `/Root/Left` AS l
            LEFT ONLY JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            WHERE l.Value != 'Value1'   -- left table payload filter
            ORDER BY l.Key
        )",
        R"([
            [[4];[104];["Value2"]];
            [[5];[105];["Value3"]]
        ])",
        UseNewEngine ? 1 : 3);
}

Y_UNIT_TEST_NEW_ENGINE(LeftSemi) {
    Test<UseNewEngine>(
        R"(
            SELECT l.Key, l.Fk, l.Value
            FROM `/Root/Left` AS l
            LEFT SEMI JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            WHERE l.Value != 'Value1'   -- left table payload filter
        )",
        R"([
            [[3];[103];["Value2"]]
        ])",
        UseNewEngine ? 1 : 3);
}

Y_UNIT_TEST_NEW_ENGINE(RightSemi) {
    Test<UseNewEngine>(
        R"(
            SELECT r.Key, r.Value
            FROM `/Root/Left` AS l
            RIGHT SEMI JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            WHERE r.Value != 'Value22' -- right table payload filter
            ORDER BY r.Key
        )",
        R"([
            [[101];["Value21"]];
            [[103];["Value23"]]
        ])",
        3);
}

} // suite

} // namespace NKqp
} // namespace NKikimr
