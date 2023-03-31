#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

void PrepareTables(TSession session) {
    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
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

        CREATE TABLE `/Root/LaunchByProcessIdAndPinned` (
            idx_processId Utf8,
            idx_pinned Bool,
            idx_launchNumber Int32,
            PRIMARY KEY(idx_processId, idx_pinned, idx_launchNumber)
        );

        CREATE TABLE `/Root/LaunchByProcessIdAndTag` (
            idx_processId Utf8,
            idx_tag Utf8,
            idx_launchNumber Int32,
            PRIMARY KEY(idx_processId, idx_tag, idx_launchNumber)
        );

        CREATE TABLE `/Root/Launch` (
            idx_processId Utf8,
            idx_launchNumber Int32,
            PRIMARY KEY(idx_processId, idx_launchNumber)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteDataQuery(R"(

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

        REPLACE INTO `/Root/LaunchByProcessIdAndPinned` (idx_processId, idx_pinned, idx_launchNumber) VALUES
            ("eProcess", false, 4),
            ("eProcess", true, 5),
            ("eProcess", true, 6);

        REPLACE INTO `/Root/LaunchByProcessIdAndTag` (idx_processId, idx_tag, idx_launchNumber) VALUES
            ("eProcess", "tag1", 4),
            ("eProcess", "tag2", 4),
            ("eProcess", "tag1", 5),
            ("eProcess", "tag3", 5);

        REPLACE INTO `/Root/Launch` (idx_processId, idx_launchNumber) VALUES
            ("dProcess", 1),
            ("eProcess", 2),
            ("eProcess", 3),
            ("eProcess", 4),
            ("eProcess", 5),
            ("eProcess", 6),
            ("eProcess", 7);
    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
}

Y_UNIT_TEST_SUITE(KqpIndexLookupJoin) {

void Test(const TString& query, const TString& answer, size_t rightTableReads) {
    TKikimrSettings settings;
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    PrepareTables(session);

    TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

    auto result = session.ExecuteDataQuery(Q_(query), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    CompareYson(answer, FormatResultSetYson(result.GetResultSet(0)));

    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
    if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);
    }

    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/Left");
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 5);

    ui32 index = 1;
    if (!settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
        UNIT_ASSERT(stats.query_phases(1).table_access().empty()); // keys extraction for lookups
        index = 2;
    }

    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access(0).name(), "/Root/Right");
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access(0).reads().rows(), rightTableReads);
}

Y_UNIT_TEST(MultiJoins) {
    TString query =
        R"(
            SELECT main.idx_processId AS `processId`, main.idx_launchNumber AS `launchNumber`
            FROM (
                  SELECT t1.idx_processId AS processId, t1.idx_launchNumber AS launchNumber
              FROM `/Root/LaunchByProcessIdAndPinned` AS t1
              JOIN `/Root/LaunchByProcessIdAndTag` AS t3 ON t1.idx_processId = t3.idx_processId
                AND t1.idx_launchNumber = t3.idx_launchNumber
              WHERE t1.idx_processId = "eProcess"
                AND t1.idx_pinned = true
                AND t1.idx_launchNumber < 10
                AND t3.idx_tag = "tag1"
             ORDER BY processId DESC, launchNumber DESC
                LIMIT 2
            ) AS filtered
            JOIN `/Root/Launch` AS main
              ON main.idx_processId = filtered.processId
                AND main.idx_launchNumber = filtered.launchNumber
            ORDER BY `processId` DESC, `launchNumber` DESC
            LIMIT 2
        )";

    TString answer =
        R"([
            [["eProcess"];[5]]
        ])";

    TKikimrRunner kikimr;
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    PrepareTables(session);

    TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

    auto result = session.ExecuteDataQuery(Q_(query), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    CompareYson(answer, FormatResultSetYson(result.GetResultSet(0)));
}

Y_UNIT_TEST(Inner) {
    Test(
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
        ])", 2);
}

Y_UNIT_TEST(Left) {
    Test(
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
        ])", 1);
}

Y_UNIT_TEST(LeftOnly) {
    Test(
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
        ])", 1);
}

Y_UNIT_TEST(LeftSemi) {
    Test(
        R"(
            SELECT l.Key, l.Fk, l.Value
            FROM `/Root/Left` AS l
            LEFT SEMI JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            WHERE l.Value != 'Value1'   -- left table payload filter
        )",
        R"([
            [[3];[103];["Value2"]]
        ])", 1);
}

Y_UNIT_TEST(RightSemi) {
    Test(
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
        ])", 3);
}

} // suite

} // namespace NKqp
} // namespace NKikimr
