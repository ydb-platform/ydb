#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpWrite) {
    Y_UNIT_TEST(UpsertNullKey) {
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_KqpYqlSyntaxVersion");
        setting.SetValue("1");

        auto kikimr = DefaultKikimrRunner({setting});
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

    Y_UNIT_TEST(Insert) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(InsertRevert) {
        auto kikimr = DefaultKikimrRunner();
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

        // TODO:
        // UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_OPERATION_REVERTED));

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

        // TODO:
        // UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_OPERATION_REVERTED));

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

    Y_UNIT_TEST(ProjectReplace) {
        auto kikimr = DefaultKikimrRunner();
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

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
    }

    Y_UNIT_TEST(CastValues) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(CastValuesOptional) {
        auto kikimr = DefaultKikimrRunner();
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
