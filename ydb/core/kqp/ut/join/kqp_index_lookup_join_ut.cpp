#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <fmt/format.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace fmt::literals;

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

        CREATE TABLE `/Root/Kv` (
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
            (5, 105, "Value3"),
            (6, NULL, "Value6"),
            (7, NULL, "Value7");

        REPLACE INTO `/Root/Right` (Key, Value) VALUES
            (100, "Value20"),
            (101, "Value21"),
            (102, "Value22"),
            (103, "Value23"),
            (NULL, "Value24"),
            (104, NULL);

        REPLACE INTO `/Root/Kv` (Key, Value) VALUES
            (1, "Value1"),
            (2, "Value2"),
            (3, "Value3"),
            (4, "Value4");    

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

void Test(const TString& query, const TString& answer, size_t rightTableReads, bool useStreamLookup = false) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(useStreamLookup);

    auto settings = TKikimrSettings().SetAppConfig(appConfig);
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
    if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin()) {
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 2);
        for (const auto& tableStat : stats.query_phases(0).table_access()) {
            if (tableStat.name() == "/Root/Left") {
                UNIT_ASSERT_VALUES_EQUAL(tableStat.reads().rows(), 7);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(tableStat.name(), "/Root/Right");
                UNIT_ASSERT_VALUES_EQUAL(tableStat.reads().rows(), rightTableReads);
            }
        }
    } else if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/Left");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 7);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/Right");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), rightTableReads);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/Left");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 7);

        UNIT_ASSERT(stats.query_phases(1).table_access().empty()); // keys extraction for lookups

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(2).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(2).table_access(0).name(), "/Root/Right");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(2).table_access(0).reads().rows(), rightTableReads);
    }
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

Y_UNIT_TEST_TWIN(Inner, StreamLookup) {
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
        ])", 2, StreamLookup);
}

Y_UNIT_TEST_TWIN(JoinWithSubquery, StreamLookup) {
    const auto query = R"(
        $join = (SELECT l.Key AS lKey, l.Value AS lValue, r.Value AS rValue
            FROM `/Root/Left` AS l
            INNER JOIN `/Root/Right` AS r
                ON l.Fk = r.Key
        );
        SELECT j.lValue AS Value FROM $join AS j INNER JOIN `/Root/Kv` AS kv
            ON j.lKey = kv.Key;
    )";

    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookup);

    auto settings = TKikimrSettings().SetAppConfig(appConfig);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    PrepareTables(session);

    TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

    auto result = session.ExecuteDataQuery(Q_(query), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    CompareYson(R"([
        [["Value1"]];
        [["Value1"]];
        [["Value2"]];
        [["Value2"]]
    ])", FormatResultSetYson(result.GetResultSet(0)));
}

Y_UNIT_TEST_TWIN(Left, StreamLookup) {
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
            [[4];[104];["Value2"];[104];#];
            [[5];[105];["Value3"];#;#];
            [[6];#;["Value6"];#;#];
            [[7];#;["Value7"];#;#]
        ])", 2, StreamLookup);
}

Y_UNIT_TEST_TWIN(LeftOnly, StreamLookup) {
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
            [[5];[105];["Value3"]];
            [[6];#;["Value6"]];
            [[7];#;["Value7"]]
        ])", 2, StreamLookup);
}

Y_UNIT_TEST(LeftSemi) {
    Test(
        R"(
            SELECT l.Key, l.Fk, l.Value
            FROM `/Root/Left` AS l
            LEFT SEMI JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            WHERE l.Value != 'Value1'   -- left table payload filter
            ORDER BY l.Key
        )",
        R"([
            [[3];[103];["Value2"]];
            [[4];[104];["Value2"]]
        ])", 2);
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
        ])", 4);
}

Y_UNIT_TEST_TWIN(SimpleInnerJoin, StreamLookup) {
    Test(
        R"(
            SELECT l.Key, l.Fk, l.Value, r.Key, r.Value
            FROM `/Root/Left` AS l
            INNER JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            ORDER BY l.Key;
        )",
        R"([
            [[1];[101];["Value1"];[101];["Value21"]];
            [[2];[102];["Value1"];[102];["Value22"]];
            [[3];[103];["Value2"];[103];["Value23"]];
            [[4];[104];["Value2"];[104];#]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(InnerJoinCustomColumnOrder, StreamLookup) {
    Test(
        R"(
            SELECT r.Value, l.Key, r.Key, l.Value, l.Fk
            FROM `/Root/Left` AS l
            INNER JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            ORDER BY r.Key;
        )",
        R"([
            [["Value21"];[1];[101];["Value1"];[101]];
            [["Value22"];[2];[102];["Value1"];[102]];
            [["Value23"];[3];[103];["Value2"];[103]];
            [#;[4];[104];["Value2"];[104]]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(InnerJoinOnlyRightColumn, StreamLookup) {
    Test(
        R"(
            SELECT r.Value
            FROM `/Root/Left` AS l
            INNER JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            ORDER BY r.Value;
        )",
        R"([
            [#];
            [["Value21"]];
            [["Value22"]];
            [["Value23"]]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(InnerJoinOnlyLeftColumn, StreamLookup) {
    Test(
        R"(
            SELECT l.Fk
            FROM `/Root/Left` AS l
            INNER JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            ORDER BY l.Fk;
        )",
        R"([
            [[101]];
            [[102]];
            [[103]];
            [[104]]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(InnerJoinLeftFilter, StreamLookup) {
    Test(
        R"(
            SELECT l.Key, l.Fk, l.Value, r.Key, r.Value
            FROM `/Root/Left` AS l
            INNER JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            WHERE l.Value != 'Value1'
            ORDER BY l.Key;
        )",
        R"([
            [[3];[103];["Value2"];[103];["Value23"]];
            [[4];[104];["Value2"];[104];#]
        ])", 2, StreamLookup);
}

Y_UNIT_TEST_TWIN(SimpleLeftJoin, StreamLookup) {
    Test(
        R"(
            SELECT l.Key, l.Fk, l.Value, r.Key, r.Value
            FROM `/Root/Left` AS l
            LEFT JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            ORDER BY l.Key;
        )",
        R"([
            [[1];[101];["Value1"];[101];["Value21"]];
            [[2];[102];["Value1"];[102];["Value22"]];
            [[3];[103];["Value2"];[103];["Value23"]];
            [[4];[104];["Value2"];[104];#];
            [[5];[105];["Value3"];#;#];
            [[6];#;["Value6"];#;#];
            [[7];#;["Value7"];#;#]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(LeftJoinCustomColumnOrder, StreamLookup) {
    Test(
        R"(
            SELECT r.Value, l.Key, r.Key, l.Value, l.Fk
            FROM `/Root/Left` AS l
            LEFT JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            ORDER BY l.Key;
        )",
        R"([
            [["Value21"];[1];[101];["Value1"];[101]];
            [["Value22"];[2];[102];["Value1"];[102]];
            [["Value23"];[3];[103];["Value2"];[103]];
            [#;[4];[104];["Value2"];[104]];
            [#;[5];#;["Value3"];[105]];
            [#;[6];#;["Value6"];#];
            [#;[7];#;["Value7"];#]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(LeftJoinOnlyRightColumn, StreamLookup) {
    Test(
        R"(
            SELECT r.Value
            FROM `/Root/Left` AS l
            LEFT JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            ORDER BY r.Value;
        )",
        R"([
            [#];
            [#];
            [#];
            [#];
            [["Value21"]];
            [["Value22"]];
            [["Value23"]]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(LeftJoinOnlyLeftColumn, StreamLookup) {
    Test(
        R"(
            SELECT l.Fk
            FROM `/Root/Left` AS l
            LEFT JOIN `/Root/Right` AS r
               ON l.Fk = r.Key
            ORDER BY l.Fk;
        )",
        R"([
            [#];
            [#];
            [[101]];
            [[102]];
            [[103]];
            [[104]];
            [[105]]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(SimpleLeftOnlyJoin, StreamLookup) {
    Test(
        R"(
            SELECT l.Key, l.Fk, l.Value
            FROM `/Root/Left` AS l
            LEFT ONLY JOIN `/Root/Right` AS r
                ON l.Fk = r.Key
            ORDER BY l.Key
        )",
        R"([
            [[5];[105];["Value3"]];
            [[6];#;["Value6"]];
            [[7];#;["Value7"]]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(LeftOnlyJoinValueColumn, StreamLookup) {
    Test(
        R"(
            SELECT l.Value
            FROM `/Root/Left` AS l
            LEFT ONLY JOIN `/Root/Right` AS r
                ON l.Fk = r.Key
            ORDER BY l.Value
        )",
        R"([
            [["Value3"]];
            [["Value6"]];
            [["Value7"]]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(LeftJoinRightNullFilter, StreamLookup) {
    Test(
        R"(
            SELECT l.Value, r.Value
            FROM `/Root/Left` AS l
            LEFT JOIN `/Root/Right` AS r
                ON l.Fk = r.Key
            WHERE r.Value IS NULL
            ORDER BY l.Value
        )",
        R"([
            [["Value2"];#];
            [["Value3"];#];
            [["Value6"];#];
            [["Value7"];#]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(LeftJoinSkipNullFilter, StreamLookup) {
    Test(
        R"(
            SELECT l.Value, r.Value
            FROM `/Root/Left` AS l
            LEFT JOIN `/Root/Right` AS r
                ON l.Fk = r.Key
            WHERE r.Value IS NOT NULL
            ORDER BY l.Value, r.Value
        )",
        R"([
            [["Value1"];["Value21"]];
            [["Value1"];["Value22"]];
            [["Value2"];["Value23"]]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(SimpleLeftSemiJoin, StreamLookup) {
    Test(
        R"(
            SELECT l.Value
            FROM `/Root/Left` AS l
            LEFT SEMI JOIN `/Root/Right` AS r
                ON l.Fk = r.Key
            ORDER BY l.Value
        )",
        R"([
            [["Value1"]];
            [["Value1"]];
            [["Value2"]];
            [["Value2"]]
        ])", 4, StreamLookup);
}

Y_UNIT_TEST_TWIN(LeftSemiJoinWithLeftFilter, StreamLookup) {
    Test(
        R"(
            SELECT l.Value
            FROM `/Root/Left` AS l
            LEFT SEMI JOIN `/Root/Right` AS r
                ON l.Fk = r.Key
            WHERE l.Value != 'Value1'  
            ORDER BY l.Value
        )",
        R"([
            [["Value2"]];
            [["Value2"]]
        ])", 2, StreamLookup);
}

void CreateSimpleTableWithKeyType(TSession session, const TString& tableName, const TString& columnType) {
    const TString query = fmt::format(R"(
            CREATE TABLE `/Root/{tableName}` (
                Key {columnType},
                Value String,
                PRIMARY KEY (Key)
            )
        )",
        "tableName"_a = tableName,
        "columnType"_a = columnType
    );
    UNIT_ASSERT(session.ExecuteSchemeQuery(query).GetValueSync().IsSuccess());
}

TString GetQuery(const TString& joinType, const TString& leftTable, const TString& rightTable) {
    TString selectColumns;
    TString sortColumns;
    if (joinType == "RIGHT SEMI") {
        selectColumns = "r.Key, r.Value";
        sortColumns = "r.Key";
    } else if (joinType == "LEFT SEMI") {
        selectColumns = "l.Key, l.Value";
        sortColumns = "l.Key";
    } else if (joinType == "LEFT ONLY") {
        selectColumns = "l.Key, l.Value";
        sortColumns = "l.Key";
    } else {
        selectColumns = "l.Key, l.Value, r.Key, r.Value";
        sortColumns = "l.Key";
    }

    return fmt::format(R"(
            SELECT {selectColumns}
            FROM `/Root/{leftTable}` AS l
            {joinType} JOIN `/Root/{rightTable}` AS r
                ON l.Key = r.Key ORDER BY {sortColumns}
        )",
        "selectColumns"_a = selectColumns,
        "leftTable"_a = leftTable,
        "rightTable"_a = rightTable,
        "joinType"_a = joinType,
        "sortColumns"_a = sortColumns
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

        if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        } else if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
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
        CreateSimpleTableWithKeyType(session, columnType, columnType);
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

void TestKeyCast(const TKikimrSettings& settings, TSession session, const TString& joinType, const TString& leftTable, const TString& rightTable,
        TString answer, size_t rightTableReads) {
    TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

    const TString query = GetQuery(joinType, leftTable, rightTable);
    auto result = session.ExecuteDataQuery(Q_(query), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

    ui32 index = settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin() ? 0 
        : (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup() ? 1 : 2);

    CompareYson(answer, FormatResultSetYson(result.GetResultSet(0)));

    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
    for (const auto& tableStats : stats.query_phases(index).table_access()) {
        if (tableStats.name() == rightTable) {
            UNIT_ASSERT_VALUES_EQUAL(tableStats.reads().rows(), rightTableReads);
        }
    }
}

Y_UNIT_TEST_QUAD(CheckCastInt32ToInt16, StreamLookupJoin, NotNull) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
    auto settings = TKikimrSettings().SetAppConfig(appConfig);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    const TString leftKeyColumnType = "Int32";
    const TString rightKeyColumnType = "Int16";
    const TString rightTableName = rightKeyColumnType + (NotNull ? "NotNull" : "");
    const TString rightType = rightKeyColumnType + (NotNull ? " NOT NULL" : "");

    CreateSimpleTableWithKeyType(session, leftKeyColumnType, leftKeyColumnType);
    CreateSimpleTableWithKeyType(session, rightTableName, rightType);

    TString query = fmt::format(
        R"(
            REPLACE INTO `/Root/{leftTable}` (Key, Value) VALUES
                (1, "Value11"),
                (-32769, "Value12");
            REPLACE INTO `/Root/{rightTable}` (Key, Value) VALUES
                (1, "Value21"),
                (32767, "Value22"); 
        )",
        "leftTable"_a = leftKeyColumnType,
        "rightTable"_a = rightTableName
    );

    UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    const TString answer = R"([
        [[-32769];["Value12"];#;#];
        [[1];["Value11"];[1];["Value21"]]
    ])";

    TestKeyCast(settings, session, "LEFT", leftKeyColumnType, rightTableName, answer, StreamLookupJoin ? 1 : 2);        
}

Y_UNIT_TEST_QUAD(CheckCastUint32ToUint16, StreamLookupJoin, NotNull) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
    auto settings = TKikimrSettings().SetAppConfig(appConfig);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    const TString leftKeyColumnType = "Uint32";
    const TString rightKeyColumnType = "Uint16";
    const TString rightTableName = rightKeyColumnType + (NotNull ? "NotNull" : "");
    const TString rightType = rightKeyColumnType + (NotNull ? " NOT NULL" : "");

    CreateSimpleTableWithKeyType(session, leftKeyColumnType, leftKeyColumnType);
    CreateSimpleTableWithKeyType(session, rightTableName, rightType);

    TString query = fmt::format(
        R"(
            REPLACE INTO `/Root/{leftTable}` (Key, Value) VALUES
                (1, "Value11"),
                (4294967295, "Value12");
            REPLACE INTO `/Root/{rightTable}` (Key, Value) VALUES
                (1, "Value21"),
                (65535, "Value22");
        )",
        "leftTable"_a = leftKeyColumnType,
        "rightTable"_a = rightTableName
    );

    UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    const TString answer = R"([
        [[1u];["Value11"];[1u];["Value21"]];
        [[4294967295u];["Value12"];#;#]
    ])";

    TestKeyCast(settings, session, "LEFT", leftKeyColumnType, rightTableName, answer, StreamLookupJoin ? 1 : 2);        
}

Y_UNIT_TEST_QUAD(CheckCastUint64ToInt64, StreamLookupJoin, NotNull) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
    auto settings = TKikimrSettings().SetAppConfig(appConfig);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    const TString leftKeyColumnType = "Uint64";
    const TString rightKeyColumnType = "Int64";
    const TString rightTableName = rightKeyColumnType + (NotNull ? "NotNull" : "");
    const TString rightType = rightKeyColumnType + (NotNull ? " NOT NULL" : "");

    CreateSimpleTableWithKeyType(session, leftKeyColumnType, leftKeyColumnType);
    CreateSimpleTableWithKeyType(session, rightTableName, rightType);

    TString query = fmt::format(
        R"(
            REPLACE INTO `/Root/{leftTable}` (Key, Value) VALUES
                (18446744073709551615, "Value11"),
                (1, "Value12"),
                (32768, "Value13");
            REPLACE INTO `/Root/{rightTable}` (Key, Value) VALUES
                (1, "Value21"),
                (-1, "Value22");
        )",
        "leftTable"_a = leftKeyColumnType,
        "rightTable"_a = rightTableName
    );

    UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    const TString answer = R"([
        [[1u];["Value12"];[1];["Value21"]];
        [[32768u];["Value13"];#;#];
        [[18446744073709551615u];["Value11"];#;#]
    ])";

    TestKeyCast(settings, session, "LEFT", leftKeyColumnType, rightTableName, answer, StreamLookupJoin ? 1 : 2);        
}

Y_UNIT_TEST_QUAD(CheckCastInt64ToUint64, StreamLookupJoin, NotNull) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
    auto settings = TKikimrSettings().SetAppConfig(appConfig);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    const TString leftKeyColumnType = "Int64";
    const TString rightKeyColumnType = "Uint64";
    const TString rightTableName = rightKeyColumnType + (NotNull ? "NotNull" : "");
    const TString rightType = rightKeyColumnType + (NotNull ? " NOT NULL" : "");

    CreateSimpleTableWithKeyType(session, leftKeyColumnType, leftKeyColumnType);
    CreateSimpleTableWithKeyType(session, rightTableName, rightType);

    TString query = fmt::format(
        R"(
            REPLACE INTO `/Root/{leftTable}` (Key, Value) VALUES
                (1, "Value11"),
                (-1, "Value12");
            REPLACE INTO `/Root/{rightTable}` (Key, Value) VALUES
                (18446744073709551615, "Value21"),
                (1, "Value22");
        )",
        "leftTable"_a = leftKeyColumnType,
        "rightTable"_a = rightTableName
    );

    UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    const TString answer = R"([
        [[-1];["Value12"];#;#];
        [[1];["Value11"];[1u];["Value22"]]
    ])";

    TestKeyCast(settings, session, "LEFT", leftKeyColumnType, rightTableName, answer, StreamLookupJoin ? 1 : 2);        
}

Y_UNIT_TEST_QUAD(CheckCastUtf8ToString, StreamLookupJoin, NotNull) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
    auto settings = TKikimrSettings().SetAppConfig(appConfig);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    const TString leftKeyColumnType = "Utf8";
    const TString rightKeyColumnType = "String";
    const TString rightTableName = rightKeyColumnType + (NotNull ? "NotNull" : "");
    const TString rightType = rightKeyColumnType + (NotNull ? " NOT NULL" : "");

    CreateSimpleTableWithKeyType(session, leftKeyColumnType, leftKeyColumnType);
    CreateSimpleTableWithKeyType(session, rightTableName, rightType);

    TString query = fmt::format(
        R"(
            REPLACE INTO `/Root/{leftTable}` (Key, Value) VALUES
                (Utf8("six"), "Value11"),
                (Utf8("seven"), "Value12");
            REPLACE INTO `/Root/{rightTable}` (Key, Value) VALUES
                ("six", "Value21"),
                ("eight", "Value22");
        )",
        "leftTable"_a = leftKeyColumnType,
        "rightTable"_a = rightTableName
    );

    UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    const TString answer = R"([
        [["seven"];["Value12"];#;#];
        [["six"];["Value11"];["six"];["Value21"]]
    ])";

    TestKeyCast(settings, session, "LEFT", leftKeyColumnType, rightTableName, answer, 1);
}

Y_UNIT_TEST_TWIN(JoinByComplexKeyWithNullComponents, StreamLookupJoin) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
    auto settings = TKikimrSettings().SetAppConfig(appConfig);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    {  // create tables
        const TString query = R"(
            CREATE TABLE `/Root/Left` (
                Key1 Int64,
                Key2 String,
                Value String,
                PRIMARY KEY (Key1, Key2)
            );

            CREATE TABLE `/Root/Right` (
                Key1 Int64,
                Key2 String,
                Value String,
                PRIMARY KEY (Key1, Key2)
            );
        )";
        UNIT_ASSERT(session.ExecuteSchemeQuery(query).GetValueSync().IsSuccess());
    }

    {  // fill tables
        const TString query = R"(
            REPLACE INTO `/Root/Left` (Key1, Key2, Value) VALUES
                (1, "one", "value1"),
                (2, NULL, "value2"),
                (NULL, "three", "value3");
        
            REPLACE INTO `/Root/Right` (Key1, Key2, Value) VALUES
                (1, "one", "value1"),
                (2, NULL, "value2"),
                (NULL, "three", "value3");
        )";
        UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
    }

    {  // execute join
        TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        const TString query = R"(
            SELECT l.Key1, l.Key2, l.Value, r.Key1, r.Key2, r.Value
            FROM `/Root/Left` AS l
            INNER JOIN `/Root/Right` AS r
                ON l.Key1 = r.Key1 AND l.Key2 = r.Key2 ORDER BY l.Key1, l.Key2, l.Value
        )";

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        CompareYson(R"([
            [[1];["one"];["value1"];[1];["one"];["value1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        const ui32 index = (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin() ? 0 : 1);
        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        for (const auto& tableStats : stats.query_phases(index).table_access()) {
            if (tableStats.name() == "/Root/Right") {
                UNIT_ASSERT_VALUES_EQUAL(tableStats.reads().rows(), 1);
            }
        }
    }
}

Y_UNIT_TEST_TWIN(JoinWithComplexCondition, StreamLookupJoin) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);

   TString stats = R"(
        {"/Root/Left":{"n_rows":3}, "/Root/Right":{"n_rows":3}}
    )";

    TVector<NKikimrKqp::TKqpSetting> settings;

    NKikimrKqp::TKqpSetting setting;
    setting.SetName("OptOverrideStatistics");
    setting.SetValue(stats);
    settings.push_back(setting);

    TKikimrSettings serverSettings = TKikimrSettings().SetAppConfig(appConfig);;
    serverSettings.SetKqpSettings(settings);

    TKikimrRunner kikimr(serverSettings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    {  // create tables
        const TString query = R"(
            CREATE TABLE `/Root/Left` (
                Key1 Int64,
                Key2 Int64,
                Fk Int64,
                Value1 String,
                Value2 String,
                PRIMARY KEY (Key1, Key2)
            );

            CREATE TABLE `/Root/Right` (
                Key1 Int64,
                Key2 String,
                Value String,
                PRIMARY KEY (Key1, Key2)
            );
        )";
        UNIT_ASSERT(session.ExecuteSchemeQuery(query).GetValueSync().IsSuccess());
    }

    {  // fill tables
        const TString query = R"(
            REPLACE INTO `/Root/Left` (Key1, Key2, Fk, Value1, Value2) VALUES
                (1, 1, 1, "one", "value1"),
                (2, 2, 20, "two", "two"),
                (NULL, 3, NULL, "three", "value3");

            REPLACE INTO `/Root/Right` (Key1, Key2, Value) VALUES
                (1, "one", "value1"),
                (2, "two", "value2"),
                (NULL, "three", "value3");
        )";
        UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
    }

    {  // execute join with left filter before lookup join: l.Key1 = l.Key2 = l.Fk
        TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        const TString query = R"(
            SELECT l.Key1, l.Key2, l.Fk, r.Key1
            FROM `/Root/Left` AS l
            INNER JOIN `/Root/Right` AS r
                ON l.Key1 = r.Key1
                AND l.Key2 = r.Key1
                AND l.Fk = r.Key1
            ORDER BY l.Key1, l.Key2, l.Fk, r.Key1
        )";

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        CompareYson(R"([
            [[1];[1];[1];[1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        const ui32 index = (serverSettings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin() ? 0 : 1);
        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        for (const auto& tableStats : stats.query_phases(index).table_access()) {
            if (tableStats.name() == "/Root/Right") {
                UNIT_ASSERT_VALUES_EQUAL(tableStats.reads().rows(), 1);
            }
        }
    }

    {  // execute left join with left filter for join keys before lookup join: l.Key1 = l.Key2 = l.Fk
        TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        const TString query = R"(
            SELECT l.Key1, l.Key2, l.Fk, r.Key1
            FROM `/Root/Left` AS l
            LEFT JOIN `/Root/Right` AS r
                ON l.Key1 = r.Key1
                AND l.Key2 = r.Key1
                AND l.Fk = r.Key1
            ORDER BY l.Key1, l.Key2, l.Fk, r.Key1
        )";

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        CompareYson(R"([
            [#;[3];#;#];
            [[1];[1];[1];[1]];
            [[2];[2];[20];#]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        const ui32 index = (serverSettings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin() ? 0 : 1);
        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        for (const auto& tableStats : stats.query_phases(index).table_access()) {
            if (tableStats.name() == "/Root/Right") {
                UNIT_ASSERT_VALUES_EQUAL(tableStats.reads().rows(), 1);
            }
        }
    }

    {  // execute join with left filter before lookup join: l.Key1 = l.Key2 AND l.Value1 = l.Value2
        TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        const TString query = R"(
            SELECT l.Key1, l.Key2, r.Key1, l.Value1, l.Value2, r.Key2
            FROM `/Root/Left` AS l
            INNER JOIN `/Root/Right` AS r
                ON l.Key1 = r.Key1
                AND l.Key2 = r.Key1
                AND l.Value1 = r.Key2
                AND l.Value2 = r.Key2
            ORDER BY l.Key1, l.Key2, r.Key1
        )";

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        CompareYson(R"([
            [[2];[2];[2];["two"];["two"];["two"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        const ui32 index = (serverSettings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin() ? 0 : 1);
        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        for (const auto& tableStats : stats.query_phases(index).table_access()) {
            if (tableStats.name() == "/Root/Right") {
                UNIT_ASSERT_VALUES_EQUAL(tableStats.reads().rows(), 1);
            }
        }
    }

    {  // execute left join with left filter for join keys before lookup join: l.Key1 = l.Key2 AND l.Value1 = l.Value2
        TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        const TString query = R"(
            SELECT l.Key1, l.Key2, r.Key1, l.Value1, l.Value2, r.Key2
            FROM `/Root/Left` AS l
            LEFT JOIN `/Root/Right` AS r
                ON l.Key1 = r.Key1
                AND l.Key2 = r.Key1
                AND l.Value1 = r.Key2
                AND l.Value2 = r.Key2
            ORDER BY l.Key1, l.Key2, r.Key1
        )";

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        CompareYson(R"([
            [#;[3];#;["three"];["value3"];#];
            [[1];[1];#;["one"];["value1"];#];
            [[2];[2];[2];["two"];["two"];["two"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        const ui32 index = (serverSettings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin() ? 0 : 1);
        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        for (const auto& tableStats : stats.query_phases(index).table_access()) {
            if (tableStats.name() == "/Root/Right") {
                UNIT_ASSERT_VALUES_EQUAL(tableStats.reads().rows(), 1);
            }
        }
    }
}

Y_UNIT_TEST_TWIN(LeftSemiJoinWithDuplicatesInRightTable, StreamLookupJoin) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
    auto settings = TKikimrSettings().SetAppConfig(appConfig);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    {  // create tables
        const TString query = R"(
            CREATE TABLE `/Root/Left` (
                Key1 Int64,
                Key2 Int64,
                Value String,
                PRIMARY KEY (Key1, Key2)
            );

            CREATE TABLE `/Root/Right` (
                Key1 Int64,
                Key2 Int64,
                Value String,
                PRIMARY KEY (Key1, Key2)
            );
        )";
        UNIT_ASSERT(session.ExecuteSchemeQuery(query).GetValueSync().IsSuccess());
    }

    {  // fill tables
        const TString query = R"(
            REPLACE INTO `/Root/Left` (Key1, Key2, Value) VALUES
                (1, 10, "value1"),
                (2, 20, "value2"),
                (3, 30, "value3");
        
            REPLACE INTO `/Root/Right` (Key1, Key2, Value) VALUES
                (10, 100, "value1"),
                (10, 101, "value1"),
                (10, 102, "value1"),
                (20, 200, "value2"),
                (20, 201, "value2"),
                (30, 300, "value3");
        )";
        UNIT_ASSERT(session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
    }

    {
        const TString query = R"(
            SELECT l.Key1, l.Key2, l.Value
            FROM `/Root/Left` AS l
            LEFT SEMI JOIN `/Root/Right` AS r
                ON l.Key2 = r.Key1 ORDER BY l.Key1, l.Key2, l.Value
        )";

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        CompareYson(R"([
            [[1];[10];["value1"]];
            [[2];[20];["value2"]];
            [[3];[30];["value3"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

} // suite

} // namespace NKqp
} // namespace NKikimr
