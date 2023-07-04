#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <fmt/format.h>

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

void CreateSimpleTableWithKeyType(TSession session, const TString& columnType) {
    using namespace fmt::literals;

    const TString query = fmt::format(R"(
            CREATE TABLE `/Root/Table{columnType}` (
                Key {columnType},
                Value String,
                PRIMARY KEY (Key)
            )
        )",
        "columnType"_a = columnType
    );
    UNIT_ASSERT(session.ExecuteSchemeQuery(query).GetValueSync().IsSuccess());
}

TString GetQuery(const TString& joinType, const TString& leftTable, const TString& rightTable) {
    using namespace fmt::literals;

    TString selectColumns;
    if (joinType == "RIGHT SEMI") {
        selectColumns = "r.Key, r.Value";
    } else if (joinType == "LEFT SEMI") {
        selectColumns = "l.Key, l.Value";
    } else if (joinType == "LEFT ONLY") {
        selectColumns = "l.Key, l.Value";
    } else {
        selectColumns = "l.Key, l.Value, r.Key, r.Value";
    }

    return fmt::format(R"(
            SELECT {selectColumns}
            FROM `/Root/Table{leftTable}` AS l
            {joinType} JOIN `/Root/Table{rightTable}` AS r
                ON l.Key = r.Key
        )",
        "selectColumns"_a = selectColumns,
        "leftTable"_a = leftTable,
        "rightTable"_a = rightTable,
        "joinType"_a = joinType
    );
}

void TestKeyCastForAllJoinTypes(TSession session, const TString& leftTable, const TString& rightTable, bool isCast, bool isError) {
    TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

    const THashSet<TString> joinTypes = {"INNER", "LEFT", "LEFT ONLY", "LEFT SEMI", "RIGHT SEMI"};
    for (const auto& joinType : joinTypes) {

        const TString query = GetQuery(joinType, leftTable, rightTable);
        auto result = session.ExecuteDataQuery(Q_(query), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        if (isError) {
            UNIT_ASSERT(!result.IsSuccess());
            return;
        }
        TKikimrSettings settings;
        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        if (!isCast) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
            return;
        }
        if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);
        }
    }
}

Y_UNIT_TEST(CheckAllKeyTypesCast) {
    using namespace fmt::literals;

    TKikimrSettings settings;
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    const THashSet<TString> columnTypes = {
        "Bool",
        "Int8", "Int16", "Int32", "Int64", "Uint16", "Uint32", "Uint64",
        /*"Float", "Double", "Decimal", */"DyNumber",
        "String", "Utf8", /*"Json", "JsonDocument", "Yson", "Uuid",*/
        "Date", "Datetime", "Timestamp", "Interval"/*, "TzDate", "TzDateTime", "TzTimestamp"*/
    };

    const THashSet<std::pair<TString, TString>> allowedDirectionalCast = {
        {"Utf8", "String"}
    };
    const THashSet<std::pair<TString, TString>> allowedBidirectionalCast = {
        {"Int8", "Int16"}, {"Int8", "Int32"}, {"Int8", "Int64"}, {"Int8", "Uint16"}, {"Int8", "Uint32"}, {"Int8", "Uint64"},
        {"Int16", "Int32"}, {"Int16", "Int64"}, {"Int16", "Uint16"}, {"Int16", "Uint32"}, {"Int16", "Uint64"},
        {"Int32", "Int64"}, {"Int32", "Uint16"}, {"Int32", "Uint32"}, {"Int32", "Uint64"},
        {"Int64", "Uint16"}, {"Int64", "Uint32"}, {"Int64", "Uint64"},
        {"Uint16", "Uint32"}, {"Uint16", "Uint64"},
        {"Uint32", "Uint64"}
    };
    const THashSet<std::pair<TString, TString>> allowedCompareTypes = {
        {"Utf8", "String"},
        {"Timestamp", "Datetime"},
        {"Timestamp", "Date"},
        {"Datetime", "Date"},
        {"Int8", "Int16"}, {"Int8", "Int32"}, {"Int8", "Int64"}, {"Int8", "Uint16"}, {"Int8", "Uint32"}, {"Int8", "Uint64"},
        {"Int16", "Int32"}, {"Int16", "Int64"}, {"Int16", "Uint16"}, {"Int16", "Uint32"}, {"Int16", "Uint64"},
        {"Int32", "Int64"}, {"Int32", "Uint16"}, {"Int32", "Uint32"}, {"Int32", "Uint64"},
        {"Int64", "Uint16"}, {"Int64", "Uint32"}, {"Int64", "Uint64"},
        {"Uint16", "Uint32"}, {"Uint16", "Uint64"},
        {"Uint32", "Uint64"}
    };

    for (const auto& columnType : columnTypes) {
        CreateSimpleTableWithKeyType(session, columnType);
    }

    for (const auto& leftColumnType : columnTypes) {
        for (const auto& rightColumnType : columnTypes) {
            if (leftColumnType == rightColumnType) {
                continue;
            }
            auto isCast =
                allowedDirectionalCast.contains(std::pair<TString, TString>(leftColumnType, rightColumnType)) ||
                allowedBidirectionalCast.contains(std::pair<TString, TString>{leftColumnType, rightColumnType}) ||
                allowedBidirectionalCast.contains(std::pair<TString, TString>{rightColumnType, leftColumnType});
            auto isError =
                !allowedCompareTypes.contains(std::pair<TString, TString>(leftColumnType, rightColumnType)) &&
                !allowedCompareTypes.contains(std::pair<TString, TString>(rightColumnType, leftColumnType));
            TestKeyCastForAllJoinTypes(session, leftColumnType, rightColumnType, isCast, isError);
        }
    }
}

void TestKeyCast(TSession session, const TString& joinType, const TString& leftTable, const TString& rightTable,
        TString answer, size_t rightTableReads) {
    TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

    const TString query = GetQuery(joinType, leftTable, rightTable);
    auto result = session.ExecuteDataQuery(Q_(query), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

    TKikimrSettings settings;
    ui32 index = (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup() ? 1 : 2);

    CompareYson(answer, FormatResultSetYson(result.GetResultSet(0)));

    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access(0).reads().rows(), rightTableReads);
}

Y_UNIT_TEST(CheckCastInt32ToInt16) {
    TKikimrSettings settings;
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSimpleTableWithKeyType(session, "Int32");
    CreateSimpleTableWithKeyType(session, "Int16");

    TString query = R"(
        REPLACE INTO `/Root/TableInt32` (Key, Value) VALUES
            (1, "Value11"),
            (-32769, "Value12");
        REPLACE INTO `/Root/TableInt16` (Key, Value) VALUES
            (1, "Value21"),
            (32767, "Value22");
    )";
    UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    TString answer = R"([
        [[-32769];["Value12"];#;#];
        [[1];["Value11"];[1];["Value21"]]
    ])";
    TestKeyCast(session, "LEFT", "Int32", "Int16", answer, 2);
}

Y_UNIT_TEST(CheckCastUint32ToUint16) {
    TKikimrSettings settings;
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSimpleTableWithKeyType(session, "Uint32");
    CreateSimpleTableWithKeyType(session, "Uint16");

    TString query = R"(
        REPLACE INTO `/Root/TableUint32` (Key, Value) VALUES
            (1, "Value11"),
            (4294967295, "Value12");
        REPLACE INTO `/Root/TableUint16` (Key, Value) VALUES
            (1, "Value21"),
            (65535, "Value22");
    )";
    UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    TString answer = R"([
        [[1u];["Value11"];[1u];["Value21"]];
        [[4294967295u];["Value12"];#;#]
    ])";
    TestKeyCast(session, "LEFT", "Uint32", "Uint16", answer, 2);
}

Y_UNIT_TEST(CheckCastUint64ToInt64) {
    TKikimrSettings settings;
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSimpleTableWithKeyType(session, "Uint64");
    CreateSimpleTableWithKeyType(session, "Int64");

    TString query = R"(
        REPLACE INTO `/Root/TableUint64` (Key, Value) VALUES
            (18446744073709551615, "Value11"),
            (1, "Value12"),
            (32768, "Value13");
        REPLACE INTO `/Root/TableInt64` (Key, Value) VALUES
            (1, "Value21"),
            (-1, "Value22");
    )";
    UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    TString answer = R"([
        [[1u];["Value12"];[1];["Value21"]];
        [[32768u];["Value13"];#;#];
        [[18446744073709551615u];["Value11"];#;#]
    ])";
    TestKeyCast(session, "LEFT", "Uint64", "Int64", answer, 2);
}

Y_UNIT_TEST(CheckCastInt64ToUint64) {
    TKikimrSettings settings;
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSimpleTableWithKeyType(session, "Int64");
    CreateSimpleTableWithKeyType(session, "Uint64");

    TString query = R"(
        REPLACE INTO `/Root/TableInt64` (Key, Value) VALUES
            (1, "Value11"),
            (-1, "Value12");
        REPLACE INTO `/Root/TableUint64` (Key, Value) VALUES
            (18446744073709551615, "Value21"),
            (1, "Value22");
    )";
    UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    TString answer = R"([
        [[-1];["Value12"];#;#];
        [[1];["Value11"];[1u];["Value22"]]
    ])";
    TestKeyCast(session, "LEFT", "Int64", "Uint64", answer, 2);
}

Y_UNIT_TEST(CheckCastUtf8ToString) {
    TKikimrSettings settings;
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    CreateSimpleTableWithKeyType(session, "Utf8");
    CreateSimpleTableWithKeyType(session, "String");

    TString query = R"(
        REPLACE INTO `/Root/TableUtf8` (Key, Value) VALUES
            (Utf8("six"), "Value11"),
            (Utf8("seven"), "Value12");
        REPLACE INTO `/Root/TableString` (Key, Value) VALUES
            ("six", "Value21"),
            ("eight", "Value22");
    )";
    UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    TString answer = R"([
        [["seven"];["Value12"];#;#];
        [["six"];["Value11"];["six"];["Value21"]]
    ])";
    TestKeyCast(session, "LEFT", "Utf8", "String", answer, 1);
}

} // suite

} // namespace NKqp
} // namespace NKikimr
