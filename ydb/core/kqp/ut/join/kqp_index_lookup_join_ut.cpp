#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <library/cpp/json/json_reader.h>
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

        create table A (
            a int32, b int32,
            primary key(a)
        );

        create table B (
            a int32, b int32,
            primary key(b, a)
        );

        create table C (
            a int32, b int32,
            primary key(a)
        );

        create table D (
            a int32, b int16,
            primary key(a)
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

        CREATE TABLE UserItemRelation (
            item_id	String,
            user_id	String,
            PRIMARY KEY(item_id, user_id),
            INDEX relation_by_user_id GLOBAL  ON (user_id)
        );

        CREATE TABLE Items (
            id	String,
            idx_a	String,
            idx_b	Int64,
            PRIMARY KEY(id),
            INDEX idx GLOBAL ON (idx_a, idx_b)
        );

        CREATE TABLE `Level1` ( `Id` Int32 NOT NULL, `Name` Utf8, `Date` Timestamp NOT NULL, `Level2_Name` Utf8, `OneToOne_Required_PK_Date` Timestamp, `Level1_Required_Id` Int32, `Level1_Optional_Id` Int32, `Level3_Name` Utf8,
            `Level2_Required_Id` Int32, `Level2_Optional_Id` Int32, `Level4_Name` Utf8, `Level3_Required_Id` Int32, `Level3_Optional_Id` Int32, `OneToOne_Optional_PK_Inverse4Id` Int32, `OneToMany_Required_Inverse4Id` Int32,
            `OneToMany_Optional_Inverse4Id` Int32, `OneToOne_Optional_PK_Inverse3Id` Int32, `OneToMany_Required_Inverse3Id` Int32, `OneToMany_Optional_Inverse3Id` Int32, `OneToOne_Optional_PK_Inverse2Id` Int32, `OneToMany_Required_Inverse2Id` Int32, `OneToMany_Optional_Inverse2Id` Int32,
            INDEX `IX_Level1_Level1_Optional_Id` GLOBAL SYNC ON (`Level1_Optional_Id`),
            INDEX `IX_Level1_Level1_Required_Id` GLOBAL SYNC ON (`Level1_Required_Id`),
            INDEX `IX_Level1_Level2_Optional_Id` GLOBAL SYNC ON (`Level2_Optional_Id`),
            INDEX `IX_Level1_Level2_Required_Id` GLOBAL SYNC ON (`Level2_Required_Id`),
            INDEX `IX_Level1_Level3_Optional_Id` GLOBAL SYNC ON (`Level3_Optional_Id`),
            INDEX `IX_Level1_Level3_Required_Id` GLOBAL SYNC ON (`Level3_Required_Id`),
            INDEX `IX_Level1_OneToMany_Optional_Inverse2Id` GLOBAL SYNC ON (`OneToMany_Optional_Inverse2Id`),
            INDEX `IX_Level1_OneToMany_Optional_Inverse3Id` GLOBAL SYNC ON (`OneToMany_Optional_Inverse3Id`),
            INDEX `IX_Level1_OneToMany_Optional_Inverse4Id` GLOBAL SYNC ON (`OneToMany_Optional_Inverse4Id`),
            INDEX `IX_Level1_OneToMany_Required_Inverse2Id` GLOBAL SYNC ON (`OneToMany_Required_Inverse2Id`),
            INDEX `IX_Level1_OneToMany_Required_Inverse3Id` GLOBAL SYNC ON (`OneToMany_Required_Inverse3Id`),
            INDEX `IX_Level1_OneToMany_Required_Inverse4Id` GLOBAL SYNC ON (`OneToMany_Required_Inverse4Id`),
            INDEX `IX_Level1_OneToOne_Optional_PK_Inverse2Id` GLOBAL SYNC ON (`OneToOne_Optional_PK_Inverse2Id`),
            INDEX `IX_Level1_OneToOne_Optional_PK_Inverse3Id` GLOBAL SYNC ON (`OneToOne_Optional_PK_Inverse3Id`),
            INDEX `IX_Level1_OneToOne_Optional_PK_Inverse4Id` GLOBAL SYNC ON (`OneToOne_Optional_PK_Inverse4Id`),
            PRIMARY KEY (`Id`)
        );
        CREATE TABLE X (x_id Int32, a Int32, b Int32, PRIMARY KEY(x_id));
        CREATE TABLE Y (y_id Int32, a Int32, b Int32, c Int32, PRIMARY KEY(y_id), INDEX ix_a GLOBAL ON (a));

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

        $a = AsList(
            AsStruct(1 as a, 2 as b),
            AsStruct(2 as a, 2 as b),
            AsStruct(3 as a, 2 as b),
            AsStruct(4 as a, 2 as b),
        );

        $b = AsList(
            AsStruct(1 as a, 2 as b),
            AsStruct(2 as a, 2 as b),
            AsStruct(3 as a, 2 as b),
            AsStruct(4 as a, 2 as b),
        );

        $c = AsList(
            AsStruct(1 as a, 5 as b),
            AsStruct(2 as a, 2 as b),
            AsStruct(3 as a, 5 as b),
            AsStruct(4 as a, 2 as b),
        );

        insert into D select a, CAST(b as Int16) as b from AS_TABLE($c);
        insert into C select * from AS_TABLE($c);
        insert into B select * from AS_TABLE($b);
        insert into A select * from AS_TABLE($a);
        insert into B (a, b) values (5, null);

        UPSERT INTO X (x_id,a,b) VALUES
            (111, 1, 1), (112, 1, 2),  (113, 1, 3),
            (121, 2, 1), (122, 2, 2),  (123, 2, 3),
            (131, 3, 1), (132, 3, 2),  (133, 3, 3);
            UPSERT INTO Y (y_id,a,b,c) VALUES
            (211, 1, 1, 2), (212, 1, 2, 3),  (213, 1, 3, 4),
            (221, 2, 1, 3), (222, 2, 2, 4),  (223, 2, 3, 5),
            (231, 3, 1, 4), (232, 3, 2, 5),  (233, 3, 3, 6);

        UPSERT INTO Items(id, idx_a, idx_b)
				VALUES
		("item_1", "root_1", 0),
		("item_2", "root_1", 0);

    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
}

void ValidateStats(const auto& result, bool isIdxLookupJoinEnabled, size_t rightTableReads, size_t leftTableReads = 7) {

    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
    if (isIdxLookupJoinEnabled) {
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 2);

        for (const auto& tableStat : stats.query_phases(0).table_access()) {
            if (tableStat.name() == "/Root/Left") {
                UNIT_ASSERT_VALUES_EQUAL(tableStat.reads().rows(), leftTableReads);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(tableStat.name(), "/Root/Right");
                UNIT_ASSERT_VALUES_EQUAL(tableStat.reads().rows(), rightTableReads);
            }
        }
    } else {
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/Left");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 7);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/Right");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), rightTableReads);
    }
}

class TTester {
public:
    TString Query;
    TString Answer;
    bool StreamLookup;
    size_t RightTableReads = 0;
    size_t LeftTableReads = 7;
    // TODO: DqReplicate is not applicable here, the number of reading rows were increased because of not optimal plan enabled by filter push down over left
    // join optional side.
    bool DqReplicate = false;
    bool DoValidateStats = true;
    bool OnlineReadOnly = false;
    NYdb::TParamsBuilder ParamsBuilder;
    bool NeedParams = false;

    TTester& Run() {
        auto settings = TKikimrSettings();
        settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookup);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        PrepareTables(session);

        TString ysonResult;

        auto params = ParamsBuilder.Build();

        {
            auto dbQuery = kikimr.GetQueryClient();
            auto sessionQuery = dbQuery.GetSession().GetValueSync().GetSession();
            NYdb::NQuery::TExecuteQuerySettings execSettings;
            execSettings.StatsMode(NQuery::EStatsMode::Full);
            auto txSettings = NYdb::NQuery::TTxSettings();
            if (OnlineReadOnly) {
                txSettings.OnlineRO().OnlineSettings(NYdb::NQuery::TTxOnlineSettings().AllowInconsistentReads(true));
            }

            auto result = sessionQuery.ExecuteQuery(Q_(Query), NYdb::NQuery::TTxControl::BeginTx(txSettings).CommitTx(), params, execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            ysonResult = FormatResultSetYson(result.GetResultSet(0));
            Cerr << result.GetStats()->GetAst() << Endl;
            if (DoValidateStats) {
                ValidateStats(
                    result, settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin(),
                    DqReplicate ? RightTableReads / 2 : RightTableReads, DqReplicate ? LeftTableReads / 2: LeftTableReads);
            }
            CompareYson(Answer, ysonResult);
        }

        {
            TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Full);
            auto result = session.ExecuteDataQuery(Q_(Query), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            ysonResult = FormatResultSetYson(result.GetResultSet(0));
            Cerr << result.GetStats()->GetAst() << Endl;
            if (DoValidateStats) {
                ValidateStats(result, settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin(), RightTableReads, LeftTableReads);
            }

            CompareYson(Answer, ysonResult);
        }

        return *this;
    }
};

void Test(const TString& query, const TString& answer, size_t rightTableReads, bool useStreamLookup = false, size_t leftTableReads = 7, bool dqReplicate = false) {
    TTester{.Query=query, .Answer=answer, .StreamLookup=useStreamLookup, .RightTableReads=rightTableReads, .LeftTableReads=leftTableReads, .DqReplicate=dqReplicate}.Run();
}

Y_UNIT_TEST_SUITE(KqpIndexLookupJoin) {

Y_UNIT_TEST_TWIN(MultiJoins, StreamLookup) {
    auto tester = TTester{
        .Query=R"(
            SELECT main.idx_processId AS `processId`, main.idx_launchNumber AS `launchNumber`
            FROM (
                  SELECT t1.idx_processId AS processId, t1.idx_launchNumber AS launchNumber
              FROM `/Root/LaunchByProcessIdAndPinned` AS t1
              JOIN `/Root/LaunchByProcessIdAndTag` AS t3 ON t1.idx_processId = t3.idx_processId
                AND t1.idx_launchNumber = t3.idx_launchNumber
              WHERE t1.idx_processId = "eProcess"
                AND t1.idx_pinned = true
                AND t1.idx_launchNumber < 10
                AND t3.idx_tag in ("tag1",)
             ORDER BY processId DESC, launchNumber DESC
                LIMIT 2
            ) AS filtered
            JOIN `/Root/Launch` AS main
              ON main.idx_processId = filtered.processId
                AND main.idx_launchNumber = filtered.launchNumber
            ORDER BY `processId` DESC, `launchNumber` DESC
            LIMIT 2
        )",
        .Answer=R"([
            [["eProcess"];[5]]
        ])"};

    tester.StreamLookup = StreamLookup;
    tester.DoValidateStats = false;
    tester.OnlineReadOnly = true;
    tester.Run();
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
    auto tester = TTester{
        .Query=R"(
        $join = (SELECT l.Key AS lKey, l.Value AS lValue, r.Value AS rValue
            FROM `/Root/Left` AS l
            INNER JOIN `/Root/Right` AS r
                ON l.Fk = r.Key
        );
        SELECT j.lValue AS Value FROM $join AS j INNER JOIN `/Root/Kv` AS kv
            ON j.lKey = kv.Key
        ORDER BY j.lValue;
        )",
        .Answer=R"([
            [["Value1"]];
            [["Value1"]];
            [["Value2"]];
            [["Value2"]]
        ])"};

    tester.StreamLookup = StreamLookup;
    tester.DoValidateStats = false;
    tester.Run();
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

Y_UNIT_TEST_TWIN(LeftSemi, StreamLookup) {
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
        ])", 2, StreamLookup);
}

Y_UNIT_TEST_TWIN(RightSemi, StreamLookup) {
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
        ])", 4, StreamLookup);
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
        ])", 4, StreamLookup, 7);
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
        ])", 4, StreamLookup, 7);
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
        } else {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
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

    ui32 index = settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin() ? 0 : 1;

    CompareYson(answer, FormatResultSetYson(result.GetResultSet(0)));

    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
    for (const auto& tableStats : stats.query_phases(index).table_access()) {
        if (tableStats.name() == rightTable) {
            UNIT_ASSERT_VALUES_EQUAL(tableStats.reads().rows(), rightTableReads);
        }
    }
}

Y_UNIT_TEST_QUAD(CheckCastInt32ToInt16, StreamLookupJoin, NotNull) {
    TKikimrSettings settings;
    settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
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
    TKikimrSettings settings;
    settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
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
    TKikimrSettings settings;
    settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
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
    TKikimrSettings settings;
    settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
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
    TKikimrSettings settings;
    settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
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
    TKikimrSettings settings;
    settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
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


Y_UNIT_TEST_TWIN(LeftJoinOnRightTableOverIndex, StreamLookupJoin) {
    auto tester = TTester{
        .Query=R"(
            SELECT x.a, x.b, y.a, y.b, y.c
            FROM X AS x LEFT JOIN Y VIEW ix_a AS y ON x.a=y.a AND x.b=y.b
            WHERE x.a=3;
        )",
        .Answer=R"([
            [[3];[1];[3];[1];[4]];
            [[3];[2];[3];[2];[5]];
            [[3];[3];[3];[3];[6]]
        ])",
        .StreamLookup=StreamLookupJoin,
        .DoValidateStats=false,
    };
    tester.Run();
}


Y_UNIT_TEST(StreamLookupJoin_RowSeqNoCollision_Repro) {
    // Regression for YQL cookie packing bug that may lead to:
    // ydb/core/kqp/runtime/kqp_compute.cpp:163: Condition violated: `it != state.AllRowsAreNull.end()`
    //
    // The failure requires:
    // - stream index lookup join enabled (cookie-based sequencing)
    // - many DQ tasks (>= 17) producing overlapping RowSeqNo after Encode/Decode truncation
    // - LEFT join over a non-unique index producing multi-row sequences per left row
    TKikimrSettings settings;
    settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(true);

    TKikimrRunner kikimr(settings);
    auto tableClient = kikimr.GetTableClient();
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    {
        const TString scheme = R"(
            CREATE TABLE `/Root/CollideLeft` (
                pk Uint32 NOT NULL,
                a  Uint32 NOT NULL,
                b  Uint32 NOT NULL,
                PRIMARY KEY (pk)
            )
            WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_BY_LOAD = DISABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 32,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 32,
                UNIFORM_PARTITIONS = 32
            );

            CREATE TABLE `/Root/CollideRight` (
                id Uint32 NOT NULL,
                a  Uint32 NOT NULL,
                b  Uint32 NOT NULL,
                PRIMARY KEY (id),
                INDEX ix_a GLOBAL ON (a)
            );
        )";
        UNIT_ASSERT_C(session.ExecuteSchemeQuery(scheme).GetValueSync().IsSuccess(), "failed to create tables");
    }

    {
        TStringBuilder dml;
        dml << "REPLACE INTO `/Root/CollideRight` (id, a, b) VALUES ";
        // Non-unique index key `a=1` yields multiple lookup candidates for each left row.
        // Keep candidate list SHORT (2 rows) to make collision-induced interleavings more likely to hit the exact
        // `LastRow && !FirstRow` path with missing state in TKqpIndexLookupJoinWrapper.
        dml << "(1u, 1u, 1u),\n";
        dml << "(2u, 1u, 2u);\n";

        dml << "REPLACE INTO `/Root/CollideLeft` (pk, a, b) VALUES ";
        // Spread rows across many partitions; with UNIFORM_PARTITIONS the key-space is split across the full Uint32 range,
        // so sequential pk values would land on the first shard only.
        // Use b in [1..4] so that some rows match and some rows don't, but every left row still produces
        // a multi-row cookie sequence (2 candidates from ix_a lookup).
        const ui32 Rows = 4096;
        // Knuth multiplicative hash (odd constant) spreads small integers across 32-bit space.
        constexpr ui32 Spread = 2654435761u;
        for (ui32 i = 1; i <= Rows; ++i) {
            const ui32 pk = i * Spread;
            const ui32 b = (i % 4) + 1;
            dml << "(" << pk << "u, 1u, " << b << "u)" << (i == Rows ? ";\n" : ",\n");
        }

        auto res = session.ExecuteDataQuery(dml, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    }

    auto queryClient = kikimr.GetQueryClient();
    auto querySession = queryClient.GetSession().GetValueSync().GetSession();

    const TString query = R"sql(
        PRAGMA ydb.OverridePlanner = @@ [
            { "tx": 0, "stage": 0, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 1, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 2, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 3, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 4, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 5, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 6, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 7, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 8, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 9, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 10, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 11, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 12, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 13, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 14, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 15, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 16, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 17, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 18, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 19, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 20, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 21, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 22, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 23, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 24, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 25, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 26, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 27, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 28, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 29, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 30, "tasks": 64, "optional": 1 },
            { "tx": 0, "stage": 31, "tasks": 64, "optional": 1 }
        ] @@;
        PRAGMA ydb.MaxTasksPerStage = "64";

        $left = (
            SELECT pk, a, b FROM `/Root/CollideLeft` WHERE b % 4u = 0u
            UNION ALL
            SELECT pk, a, b FROM `/Root/CollideLeft` WHERE b % 4u = 1u
            UNION ALL
            SELECT pk, a, b FROM `/Root/CollideLeft` WHERE b % 4u = 2u
            UNION ALL
            SELECT pk, a, b FROM `/Root/CollideLeft` WHERE b % 4u = 3u
        );

        SELECT COUNT(*) AS cnt
        FROM (
            SELECT l.pk
            FROM $left AS l
            LEFT JOIN `/Root/CollideRight` VIEW ix_a AS r
                ON l.a = r.a AND l.b = r.b
            WHERE l.a = 1u
        );
    )sql";

    NYdb::NQuery::TExecuteQuerySettings execSettings;
    execSettings.StatsMode(NYdb::NQuery::EStatsMode::Full);
    auto params = NYdb::TParamsBuilder().Build();
    // Repeat to increase probability of reproducing task interleaving in buggy builds.
    for (ui32 attempt = 0; attempt < 10; ++attempt) {
        auto result = querySession.ExecuteQuery(
            Q_(query),
            NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
            params,
            execSettings).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats(), "Expected stats (StatsMode::Full)");
        const TString ast = *result.GetStats()->GetAst();
        if (attempt == 0) {
            Cerr << "AST:\n" << ast << Endl;
            if (result.GetStats()->GetPlan()) {
                Cerr << "Plan:\n" << *result.GetStats()->GetPlan() << Endl;

                // Sanity: ensure planner actually created enough tasks to allow RowSeqNo collisions
                // between taskIds that differ by 16.
                NJson::TJsonValue plan;
                UNIT_ASSERT_C(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan), "Failed to parse plan JSON");
                ui64 maxTasks = 0;
                std::function<void(const NJson::TJsonValue&)> walk = [&](const NJson::TJsonValue& n) {
                    if (n.GetMap().contains("Stats") && n["Stats"].GetMap().contains("Tasks")) {
                        maxTasks = std::max<ui64>(maxTasks, n["Stats"]["Tasks"].GetUIntegerSafe());
                    }
                    if (n.GetMap().contains("Plans")) {
                        for (const auto& child : n["Plans"].GetArraySafe()) {
                            walk(child);
                        }
                    }
                };
                walk(plan);
                // UNIT_ASSERT_C(maxTasks >= 17, TStringBuilder() << "Expected >=17 tasks in some plan node, got " << maxTasks);
            }
        }
        UNIT_ASSERT_C(ast.Contains("KqpIndexLookupJoin"),
            "AST does not contain KqpIndexLookupJoin, test is not exercising the intended runtime path");
        UNIT_ASSERT_C(ast.Contains("DqCnUnionAll"),
            "AST does not contain DqCnUnionAll, test is not merging multiple upstream streams");
        CompareYson(R"([[4096u]])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

Y_UNIT_TEST_TWIN(TestEntityFramework, StreamLookupJoin) {
    auto tester = TTester{
        .Query=R"(
            SELECT
                `l1`.`Id`,
                `l1`.`OneToOne_Required_PK_Date`,
                `l1`.`Level1_Optional_Id`,
                `l1`.`Level1_Required_Id`,
                `l1`.`Level2_Name`,
                `l1`.`OneToMany_Optional_Inverse2Id`,
                `l1`.`OneToMany_Required_Inverse2Id`,
                `l1`.`OneToOne_Optional_PK_Inverse2Id`
            FROM `Level1` AS `l`
            LEFT JOIN (
                SELECT
                    `l0`.`Id` AS `Id`,
                    `l0`.`OneToOne_Required_PK_Date` AS `OneToOne_Required_PK_Date`,
                    `l0`.`Level1_Optional_Id` AS `Level1_Optional_Id`,
                    `l0`.`Level1_Required_Id` AS `Level1_Required_Id`,
                    `l0`.`Level2_Name` AS `Level2_Name`,
                    `l0`.`OneToMany_Optional_Inverse2Id` AS `OneToMany_Optional_Inverse2Id`,
                    `l0`.`OneToMany_Required_Inverse2Id` AS `OneToMany_Required_Inverse2Id`,
                    `l0`.`OneToOne_Optional_PK_Inverse2Id` AS `OneToOne_Optional_PK_Inverse2Id`
                FROM `Level1` AS `l0`
                WHERE
                    `l0`.`OneToOne_Required_PK_Date` IS NOT NULL AND
                    `l0`.`Level1_Required_Id` IS NOT NULL AND
                    `l0`.`OneToMany_Required_Inverse2Id` IS NOT NULL
            ) AS `l1`
            ON
                `l`.`Id` = CASE
                    WHEN `l1`.`OneToOne_Required_PK_Date` IS NOT NULL AND `l1`.`Level1_Required_Id` IS NOT NULL AND `l1`.`OneToMany_Required_Inverse2Id` IS NOT NULL THEN `l1`.`Id`
                ELSE NULL
                END
            LEFT JOIN `Level1` AS `l2` ON `l1`.`Level1_Required_Id` = `l2`.`Id`
            WHERE
                `l1`.`OneToOne_Required_PK_Date` IS NOT NULL AND
                `l1`.`Level1_Required_Id` IS NOT NULL AND
                `l1`.`OneToMany_Required_Inverse2Id` IS NOT NULL AND `l2`.`Id` IN (1, 2)
        )",
        .Answer=R"([
        ])",
        .StreamLookup=StreamLookupJoin,
        .DoValidateStats=false,
    };
    tester.Run();
}

Y_UNIT_TEST_TWIN(JoinLeftJoinPostJoinFilterTest, StreamLookupJoin) {
    auto tester = TTester{
        .Query=R"(
            select A.a, A.b, B.a, B.b from A
            left join (select * from B where a > 2 and a < 3) as B
            on A.b = B.b
            ORDER BY A.a, A.b
        )",
        .Answer=R"([
            [[1];[2];#;#];[[2];[2];#;#];[[3];[2];#;#];[[4];[2];#;#]
        ])",
        .StreamLookup=StreamLookupJoin,
        .DoValidateStats=false,
    };
    tester.Run();
}

Y_UNIT_TEST_TWIN(JoinInclusionTestSemiJoin, StreamLookupJoin) {
    auto tester = TTester{
        .Query=R"(
            select A.a, A.b, from A
            left semi join (select * from B where a > 1 and a < 3) as B
            ON A.b = B.b
            ORDER BY A.a, A.b
        )",
        .Answer=R"([
            [[1];[2]];[[2];[2]];[[3];[2]];[[4];[2]]
        ])",
        .StreamLookup=StreamLookupJoin,
        .DoValidateStats=false,
    };
    tester.Run();
}

Y_UNIT_TEST_TWIN(LeftJoinNonPkJoinConditions, StreamLookupJoin) {
    auto tester = TTester{
        .Query=R"(
            select A.a, A.b, C.a, C.b from A
            left join (select * from C) as C
            ON A.a = C.a and A.b = C.b
            ORDER BY A.a , A.b
        )",
        .Answer=R"([
            [[1];[2];#;#];[[2];[2];[2];[2]];[[3];[2];#;#];[[4];[2];[4];[2]]
        ])",
        .StreamLookup=StreamLookupJoin,
        .DoValidateStats=false,
    };
    tester.Run();
}

Y_UNIT_TEST_TWIN(LeftJoinPointPredicateAndJoinAfterThat, StreamLookupJoin) {
    auto tester = TTester{
        .Query=R"(
           	DECLARE $idx_a AS List<String>;
			DECLARE $user_id AS String?;
			DECLARE $idx_b AS List<Int64>;
			$items = (SELECT
				Items.idx_a AS idx_a,
				Items.id AS item_id
			FROM Items
			VIEW idx AS Items
			WHERE
				idx_a IN $idx_a
				AND Coalesce(idx_b, 0) NOT IN $idx_b);

			$user_hidden = (
				SELECT item_id, user_id
				FROM UserItemRelation VIEW relation_by_user_id
				WHERE user_id = $user_id
			);
			SELECT
				c.idx_a AS id,
				CAST(COUNT(*) AS Int64) AS count
			FROM $items AS c
			LEFT JOIN $user_hidden AS uh ON c.item_id = uh.item_id
			WHERE uh.user_id IS NULL
			GROUP BY c.idx_a;
        )",
        .Answer=R"([
            [[root_1];[2]]
        ])",
        .StreamLookup=StreamLookupJoin,
        .DoValidateStats=false,
    };

    tester.ParamsBuilder
        .AddParam("$idx_a").BeginList().AddListItem().String("root_1").EndList().Build()
        .AddParam("$user_id").OptionalString(std::nullopt).Build()
        .AddParam("$idx_b").BeginList().AddListItem().Int64(3).AddListItem().Int64(2).EndList().Build();
    tester.Run();
}


Y_UNIT_TEST_TWIN(LeftJoinNonPkJoinConditionsWithCast, StreamLookupJoin) {
    auto tester = TTester{
        .Query=R"(
            select A.a, A.b, D.a, D.b from A
            left join (select * from D) as D
            ON A.a = D.a and A.b = D.b
            ORDER BY A.a, A.b
        )",
        .Answer=R"([
            [[1];[2];#;#];[[2];[2];[2];[2]];[[3];[2];#;#];[[4];[2];[4];[2]]
        ])",
        .StreamLookup=StreamLookupJoin,
        .DoValidateStats=false,
    };
    tester.Run();
}



Y_UNIT_TEST_TWIN(JoinInclusionTest, StreamLookupJoin) {
    auto tester = TTester{
        .Query=R"(
            select A.a, A.b, B.a, B.b from A
            left join (select * from B where b is null) as B
            on A.a = B.a and A.b = B.b
            ORDER BY A.a, B.b
        )",
        .Answer=R"([
            [[1];[2];#;#];[[2];[2];#;#];[[3];[2];#;#];[[4];[2];#;#]
        ])",
        .StreamLookup=StreamLookupJoin,
        .DoValidateStats=false,
    };
    tester.Run();
}

Y_UNIT_TEST_TWIN(JoinWithComplexCondition, StreamLookupJoin) {
   TString stats = R"(
        {"/Root/Left":{"n_rows":3}, "/Root/Right":{"n_rows":3}}
    )";

    TVector<NKikimrKqp::TKqpSetting> settings;

    NKikimrKqp::TKqpSetting setting;
    setting.SetName("OptOverrideStatistics");
    setting.SetValue(stats);
    settings.push_back(setting);

    TKikimrSettings serverSettings;
    serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
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
    TKikimrSettings settings;
    settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
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
