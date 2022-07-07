#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpWrite) {
    Y_UNIT_TEST_QUAD(UpsertNullKey, UseNewEngine, UseSessionActor) {
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_KqpYqlSyntaxVersion");
        setting.SetValue("1");

        auto kikimr = KikimrRunnerEnableSessionActor(UseNewEngine && UseSessionActor, {setting});
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            const TString query = Q_(R"(
                UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES
                    (Null, "Value1");
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(result.IsSuccess());
        }

        {

            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/KeyValue`;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL("[[#;[\"Value1\"]];[[1u];[\"One\"]];[[2u];[\"Two\"]]]",
                    NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const TString query = Q_(R"(
                UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES
                    (Null, "Value2");
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(result.IsSuccess());
        }

        {

            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/KeyValue`;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL("[[#;[\"Value2\"]];[[1u];[\"One\"]];[[2u];[\"Two\"]]]",
                    NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const TString query = Q_(R"(
                UPSERT INTO `/Root/KeyValue` (Key) VALUES
                    (Null);
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(result.IsSuccess());
        }

        {

            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/KeyValue`;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL("[[#;[\"Value2\"]];[[1u];[\"One\"]];[[2u];[\"Two\"]]]",
                    NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InplaceUpdate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = db.GetParamsBuilder()
            .AddParam("$key").Uint64(201).Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            PRAGMA Kikimr.UseNewEngine = 'false';
            PRAGMA kikimr.CommitSafety = "Moderate";

            DECLARE $key AS Uint64;

            $data = (SELECT Data FROM `/Root/EightShard` WHERE Key = $key);
            $newData = COALESCE($data, 0u) + 1;
            $tuple = (SELECT $key AS Key, $newData AS Data);

            UPSERT INTO `/Root/EightShard`
            SELECT * FROM $tuple;
        )", TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(result.IsSuccess());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 2); // KIKIMR-7302
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 1);
    }

    Y_UNIT_TEST(InplaceUpdateBatch) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto queryPrefix = R"(
            PRAGMA Kikimr.UseNewEngine = 'false';
            PRAGMA kikimr.CommitSafety = "Moderate";
        )";

        auto declareTemplate = R"(
            DECLARE $key%1$d AS Uint64;
        )";

        auto readTemplate = R"(
            $data%1$d = (SELECT Data FROM `/Root/EightShard` WHERE Key = $key%1$d);
            $newData%1$d = COALESCE($data%1$d, 0u) + 1;
            $tuple%1$d = (SELECT $key%1$d AS Key, $newData%1$d AS Data);
        )";

        auto writeTemplate = R"(
            UPSERT INTO `/Root/EightShard`
            SELECT * FROM $tuple%1$d;
        )";

        auto makeQuery = [&](ui32 batchSize) {
            auto paramsBuilder = db.GetParamsBuilder();
            TStringBuilder queryDeclares;
            TStringBuilder queryReads;
            TStringBuilder queryWrites;
            for (ui32 i = 0; i < batchSize; ++i) {
                queryDeclares << Sprintf(declareTemplate, i);
                queryReads << Sprintf(readTemplate, i);
                queryWrites << Sprintf(writeTemplate, i);

                paramsBuilder.AddParam(Sprintf("$key%1$d", i)).Uint64(100 + i).Build();
            }

            auto query = TStringBuilder() << queryPrefix << queryDeclares << queryReads << queryWrites;

            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            return session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                paramsBuilder.Build(), execSettings).ExtractValueSync();
        };

        auto result10 = makeQuery(10);
        UNIT_ASSERT_VALUES_EQUAL_C(result10.GetStatus(), EStatus::SUCCESS, result10.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result10.GetStats());

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 6); // KIKIMR-7302
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 10);

        auto result20 = makeQuery(20);
        UNIT_ASSERT_VALUES_EQUAL_C(result20.GetStatus(), EStatus::GENERIC_ERROR, result20.GetIssues().ToString());
    }

    Y_UNIT_TEST(InplaceUpdateBigRow) {
        auto keysLimitSetting = NKikimrKqp::TKqpSetting();
        keysLimitSetting.SetName("_CommitPerShardKeysSizeLimitBytes");
        keysLimitSetting.SetValue("100");

        TKikimrRunner kikimr({keysLimitSetting});
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Temp` (
                Key Uint32,
                Value1 String,
                Value2 String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync().IsSuccess());

        auto result = session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/Temp` (Key, Value1) VALUES
                (1u, "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
                (3u, "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TString query(R"(
            PRAGMA Kikimr.UseNewEngine = 'false';
            PRAGMA kikimr.CommitSafety = "%s";

            DECLARE $Key AS Uint32;

            $value1 = (SELECT Value1 FROM `/Root/Temp` WHERE Key = $Key);
            $tuple = (SELECT $Key AS Key, $value1 AS Value1, $value1 AS Value2);

            UPSERT INTO `/Root/Temp`
            SELECT * FROM $tuple;
        )");

        auto params = db.GetParamsBuilder()
            .AddParam("$Key").Uint32(1).Build()
            .Build();

        result = session.ExecuteDataQuery(Sprintf(query.c_str(), "Moderate"),
            TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR, [](const NYql::TIssue& issue) {
            return issue.Message.Contains("READ_SIZE_EXECEEDED");
        }));

        result = session.ExecuteDataQuery(R"(
            SELECT Value2 FROM `/Root/Temp` ORDER BY Value2;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[#];[#]])", FormatResultSetYson(result.GetResultSet(0)));

        result = session.ExecuteDataQuery(Sprintf(query.c_str(), "Safe"),
            TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT Value2 FROM `/Root/Temp` ORDER BY Value2;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [#];[["123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(CommitSafetyDisabled) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetAllowUnsafeCommit(false);

        TKikimrRunner kikimr(appConfig);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$key").Uint64(201).Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            PRAGMA Kikimr.UseNewEngine = 'false';
            PRAGMA kikimr.CommitSafety = "Moderate";

            DECLARE $key AS Uint64;

            $data = (SELECT Data FROM `/Root/EightShard` WHERE Key = $key);
            $newData = COALESCE($data, 0u) + 1;
            $tuple = (SELECT $key AS Key, $newData AS Data);

            UPSERT INTO `/Root/EightShard`
            SELECT * FROM $tuple;
        )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));
    }

    Y_UNIT_TEST_QUAD(Insert, UseNewEngine, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseNewEngine && UseSessionActor);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TxCheck` (
                Id Uint32,
                PRIMARY KEY (Id)
            );
        )").GetValueSync().IsSuccess());

        auto insertQuery = Q1_(R"(
            DECLARE $rows AS List<Struct<Key: Uint64, Value: String>>;
            DECLARE $id AS Uint32;

            INSERT INTO `/Root/KeyValue`
            SELECT * FROM AS_TABLE($rows);

            UPSERT INTO `/Root/TxCheck` (Id) VALUES ($id);
        )");

        auto params = db.GetParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(1)
                        .AddMember("Value").String("One1")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(10)
                        .AddMember("Value").String("Ten1")
                    .EndStruct()
                .EndList()
                .Build()
            .AddParam("$id")
                .Uint32(1)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(insertQuery,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION));

        params = db.GetParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(10)
                        .AddMember("Value").String("Ten2")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(11)
                        .AddMember("Value").String("Eleven2")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(10)
                        .AddMember("Value").String("None2")
                    .EndStruct()
                .EndList()
                .Build()
            .AddParam("$id")
                .Uint32(2)
                .Build()
            .Build();

        result = session.ExecuteDataQuery(insertQuery,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION));

        params = db.GetParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(10)
                        .AddMember("Value").String("Ten3")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(11)
                        .AddMember("Value").String("Eleven3")
                    .EndStruct()
                .EndList()
                .Build()
            .AddParam("$id")
                .Uint32(3)
                .Build()
            .Build();

        result = session.ExecuteDataQuery(insertQuery,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue`;
            SELECT * FROM `/Root/TxCheck`;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"]];
            [[2u];["Two"]];
            [[10u];["Ten3"]];
            [[11u];["Eleven3"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[3u]]])", FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST_QUAD(InsertRevert, UseNewEngine, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseNewEngine && UseSessionActor);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TxCheck` (
                Id Uint32,
                PRIMARY KEY (Id)
            );
        )").GetValueSync().IsSuccess());

        auto insertQuery = Q1_(R"(
            DECLARE $rows AS List<Struct<Key: Uint64, Value: String>>;
            DECLARE $id AS Uint32;

            INSERT OR REVERT INTO `/Root/KeyValue`
            SELECT * FROM AS_TABLE($rows);

            UPSERT INTO `/Root/TxCheck` (Id) VALUES ($id);
        )");

        auto params = db.GetParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(1)
                        .AddMember("Value").String("One1")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(10)
                        .AddMember("Value").String("Ten1")
                    .EndStruct()
                .EndList()
                .Build()
            .AddParam("$id")
                .Uint32(1)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(insertQuery,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        if constexpr (UseNewEngine) {
            // ¯\_(ツ)_/¯
            UNIT_ASSERT_C(result.GetIssues().Size() == 0, result.GetIssues().ToString());
        } else {
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_OPERATION_REVERTED));
        }

        params = db.GetParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(10)
                        .AddMember("Value").String("Ten2")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(11)
                        .AddMember("Value").String("Eleven2")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(10)
                        .AddMember("Value").String("None2")
                    .EndStruct()
                .EndList()
                .Build()
            .AddParam("$id")
                .Uint32(2)
                .Build()
            .Build();

        result = session.ExecuteDataQuery(insertQuery,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        if constexpr (UseNewEngine) {
            // ¯\_(ツ)_/¯
            UNIT_ASSERT_C(result.GetIssues().Size() == 0, result.GetIssues().ToString());
        } else {
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_OPERATION_REVERTED));
        }

        params = db.GetParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(10)
                        .AddMember("Value").String("Ten3")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(11)
                        .AddMember("Value").String("Eleven3")
                    .EndStruct()
                .EndList()
                .Build()
            .AddParam("$id")
                .Uint32(3)
                .Build()
            .Build();

        result = session.ExecuteDataQuery(insertQuery,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue`;
            SELECT * FROM `/Root/TxCheck`;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"]];
            [[2u];["Two"]];
            [[10u];["Ten3"]];
            [[11u];["Eleven3"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[1u]];[[2u]];[[3u]]])", FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST_QUAD(ProjectReplace, UseNewEngine, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseNewEngine && UseSessionActor);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = db.GetParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("In_Group").OptionalUint32(10)
                        .AddMember("In_Name").OptionalString("Replace1")
                        .AddMember("In_Amount").OptionalUint64(100)
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("In_Group").OptionalUint32(10)
                        .AddMember("In_Name").OptionalString("Replace2")
                        .AddMember("In_Amount").OptionalUint64(200)
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $rows AS List<Struct<
                In_Group : Uint32?,
                In_Name : String?,
                In_Amount : Uint64?
            >>;

            REPLACE INTO `/Root/Test`
            SELECT
                In_Group AS Group,
                In_Name AS Name,
                In_Amount AS Amount
            FROM AS_TABLE($rows);
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), UseNewEngine ? 2 : 1);
    }

    Y_UNIT_TEST(Uint8Key) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            CREATE TABLE `/Root/Temp` (
                Key Uint8,
                PRIMARY KEY (Key)
            );
        )").GetValueSync().IsSuccess());

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            PRAGMA Kikimr.UseNewEngine = 'false';
            UPSERT INTO `/Root/Temp` (Key) VALUES (127);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            PRAGMA Kikimr.UseNewEngine = 'false';
            UPSERT INTO `/Root/Temp` (Key) VALUES (128);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            PRAGMA Kikimr.UseNewEngine = 'false';
            DELETE FROM `/Root/Temp` ON (Key) VALUES (140);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            PRAGMA Kikimr.UseNewEngine = 'false';
            SELECT * FROM `/Root/Temp`;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[[127u]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST_QUAD(CastValues, UseNewEngine, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseNewEngine && UseSessionActor);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$items")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Int32(101)
                        .AddMember("Data").Uint32(1000)
                    .EndStruct()
                .EndList()
            .Build()
        .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Int32,'Data':Uint32>>;

            UPSERT INTO EightShard
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM EightShard WHERE Key = 101;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1000];[101u];["Value1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST_QUAD(CastValuesOptional, UseNewEngine, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseNewEngine && UseSessionActor);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$items")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").OptionalInt32(101)
                        .AddMember("Data").OptionalUint32(1000)
                    .EndStruct()
                .EndList()
            .Build()
        .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Int32?,'Data':Uint32?>>;

            UPSERT INTO EightShard
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM EightShard WHERE Key = 101;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1000];[101u];["Value1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

} // namespace NKqp
} // namespace NKikimr
