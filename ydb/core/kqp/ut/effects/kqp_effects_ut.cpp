#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpEffects) {
    Y_UNIT_TEST(InsertAbort_Literal_Success) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TwoShard` (Value1, Key) VALUES
                ("foo", 10u), ("bar", 11u)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key > 5 AND Key < 100 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[10u];["foo"];#];
            [[11u];["bar"];#]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertAbort_Literal_Duplicates) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TwoShard` (Value1, Key) VALUES
                ("foo", 10u), ("bar", 11u), ("baz", 10u)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("Duplicated keys found.");
        }));

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key > 5 AND Key < 100 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertAbort_Literal_Conflict) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TwoShard` (Value1, Key) VALUES
                ("foo", 1u), ("bar", 11u)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("Conflict with existing key.");
        }), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1 OR Key = 11 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertAbort_Params_Success) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = TParamsBuilder().AddParam("$in")
            .BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("Key").Uint32(10)
                    .AddMember("Value1").String("foo")
                    .AddMember("Value2").Int32(5)
                    .EndStruct()
                .AddListItem().BeginStruct()
                    .AddMember("Key").Uint32(11)
                    .AddMember("Value1").String("bar")
                    .AddMember("Value2").Int32(7)
                    .EndStruct()
            .EndList().Build().Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            DECLARE $in AS List<Struct<Key: Uint32, Value1: String, Value2:Int32>>;
            INSERT INTO `/Root/TwoShard` SELECT * FROM AS_TABLE($in)
        )", TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key > 5 AND Key < 100 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[10u];["foo"];[5]];
            [[11u];["bar"];[7]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertAbort_Params_Duplicates) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = TParamsBuilder().AddParam("$in")
            .BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("Key").Uint32(1)
                    .AddMember("Value1").String("foo")
                    .AddMember("Value2").Int32(5)
                    .EndStruct()
                .AddListItem().BeginStruct()
                    .AddMember("Key").Uint32(11)
                    .AddMember("Value1").String("bar")
                    .AddMember("Value2").Int32(7)
                    .EndStruct()
                .AddListItem().BeginStruct()
                    .AddMember("Key").Uint32(1)
                    .AddMember("Value1").String("baz")
                    .AddMember("Value2").Int32(9)
                    .EndStruct()
            .EndList().Build().Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            DECLARE $in AS List<Struct<Key: Uint32, Value1: String, Value2:Int32>>;
            INSERT INTO `/Root/TwoShard` SELECT * FROM AS_TABLE($in)
        )", TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("Duplicated keys found.");
        }));

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1 OR Key = 11 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertAbort_Params_Conflict) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = TParamsBuilder().AddParam("$in")
            .BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("Key").Uint32(1)
                    .AddMember("Value1").String("foo")
                    .AddMember("Value2").Int32(5)
                    .EndStruct()
                .AddListItem().BeginStruct()
                    .AddMember("Key").Uint32(11)
                    .AddMember("Value1").String("bar")
                    .AddMember("Value2").Int32(7)
                    .EndStruct()
            .EndList().Build().Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            DECLARE $in AS List<Struct<Key: Uint32, Value1: String, Value2:Int32>>;
            INSERT INTO `/Root/TwoShard` SELECT * FROM AS_TABLE($in)
        )", TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("Conflict with existing key.");
        }));

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1 OR Key = 11 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertAbort_Select_Success) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto ret = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `Foo` (
                Key Uint32,
                Value1 String,
                Value2 Int32,
                PRIMARY KEY (Key)
            )
        )").ExtractValueSync();
        UNIT_ASSERT_C(ret.IsSuccess(), ret.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/Foo` (Key, Value1, Value2) VALUES
                (10u, "foo", 5), (11u, "bar", 7)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TwoShard` SELECT * FROM `/Root/Foo`
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key > 5 AND Key < 100 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[10u];["foo"];[5]];
            [[11u];["bar"];[7]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertAbort_Select_Duplicates) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto ret = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `Foo` (
                Key Uint32,
                Value1 String,
                Value2 Int32,
                Value3 Uint32,
                PRIMARY KEY (Key)
            )
        )").ExtractValueSync();
        UNIT_ASSERT_C(ret.IsSuccess(), ret.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/Foo` (Key, Value1, Value2, Value3) VALUES
                (1u, "foo", 5, 10), (2u, "bar", 7, 11), (3u, "baz", 9, 10)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TwoShard` SELECT Value3 as Key, Value1, Value2 FROM `/Root/Foo`
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("Duplicated keys found.");
        }));

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1 OR Key = 10 OR Key = 11 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertAbort_Select_Conflict) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto ret = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `Foo` (
                Key Uint32,
                Value1 String,
                Value2 Int32,
                PRIMARY KEY (Key)
            )
        )").ExtractValueSync();
        UNIT_ASSERT_C(ret.IsSuccess(), ret.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/Foo` (Key, Value1, Value2) VALUES
                (1u, "foo", 5), (11u, "bar", 7)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TwoShard` SELECT * FROM `/Root/Foo`
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("Conflict with existing key.");
        }));

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1 OR Key = 11 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertRevert_Literal_Success) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            INSERT OR REVERT INTO `/Root/TwoShard` (Value1, Key) VALUES
                ("foo", 10u), ("bar", 11u)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key > 5 AND Key < 100 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[10u];["foo"];#];
            [[11u];["bar"];#]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertRevert_Literal_Duplicates) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            INSERT OR REVERT INTO `/Root/TwoShard` (Value1, Key) VALUES
                ("foo", 10u), ("bar", 11u), ("baz", 10u)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key > 5 AND Key < 100 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertRevert_Literal_Conflict) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            INSERT OR REVERT INTO `/Root/TwoShard` (Value1, Key) VALUES
                ("foo", 1u), ("bar", 11u)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1 OR Key = 11 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(UpdateOn_Literal) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(

            UPDATE `/Root/TwoShard` ON (Key, Value1) VALUES
                (1u, "Updated"), (4000000010u, "Updated")
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["Updated"];[-1]];
            [[2u];["Two"];[0]];
            [[3u];["Three"];[1]];
            [[4000000001u];["BigOne"];[-1]];
            [[4000000002u];["BigTwo"];[0]];
            [[4000000003u];["BigThree"];[1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(UpdateOn_Params) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = TParamsBuilder().AddParam("$in")
            .BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("Key").Uint32(1)
                    .AddMember("Value1").String("Updated")
                    .AddMember("Value2").Int32(-10)
                    .EndStruct()
                .AddListItem().BeginStruct()
                    .AddMember("Key").Uint32(4000000010)
                    .AddMember("Value1").String("Updated")
                    .AddMember("Value2").Int32(7)
                    .EndStruct()
            .EndList().Build().Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            DECLARE $in AS List<Struct<Key: Uint32, Value1: String, Value2:Int32>>;
            UPDATE `/Root/TwoShard` ON (Key, Value1, Value2) SELECT Key, Value1, Value2 FROM AS_TABLE($in)
        )", TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["Updated"];[-10]];
            [[2u];["Two"];[0]];
            [[3u];["Three"];[1]];
            [[4000000001u];["BigOne"];[-1]];
            [[4000000002u];["BigTwo"];[0]];
            [[4000000003u];["BigThree"];[1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(UpdateOn_Select) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(

            $to_update = (
                SELECT Key, "Updated" as Value1, (Value2 + 5) as Value2
                FROM `/Root/TwoShard`
                WHERE Value2 >= 0
            );
            UPDATE `/Root/TwoShard` ON SELECT * FROM $to_update;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"];[-1]];
            [[2u];["Updated"];[5]];
            [[3u];["Updated"];[6]];
            [[4000000001u];["BigOne"];[-1]];
            [[4000000002u];["Updated"];[5]];
            [[4000000003u];["Updated"];[6]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(DeletePkPrefixWithIndex) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            CREATE TABLE Tmp (
                Key1 Uint32,
                Key2 String,
                Value String,
                PRIMARY KEY (Key1, Key2),
                INDEX Index GLOBAL ON (Value, Key1)
            )
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(schemeResult.GetStatus(), EStatus::SUCCESS, schemeResult.GetIssues().ToString());

        auto result = session.ExplainDataQuery(R"(
            --!syntax_v1

            DECLARE $key1 as Uint32;

            DELETE FROM Tmp WHERE Key1 = $key1;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        auto table = plan["tables"][0];
        UNIT_ASSERT_VALUES_EQUAL(table["name"], "/Root/Tmp");
        auto reads = table["reads"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(reads.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reads[0]["type"], "Scan");
        UNIT_ASSERT_VALUES_EQUAL(reads[0]["columns"].GetArraySafe().size(), 3);
    }
}

} // namespace NKqp
} // namespace NKikimr
