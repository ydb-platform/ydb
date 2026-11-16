#include "kqp_effects_helpers.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpEffectsNewRbo) {
    Y_UNIT_TEST_TWIN(RejectPgNotNullWriteConstraints, PhysicalStagePeephole) {
        auto config = NewRboAppConfig();
        config.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(PhysicalStagePeephole);
        config.MutableTableServiceConfig()->SetEnableNewRBO(false);
        TKikimrRunner kikimr(TKikimrSettings(config)
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto scheme = session.ExecuteSchemeQuery(R"(
            CREATE TABLE Checked (
                Key Uint64 NOT NULL,
                A PgInt2 NOT NULL,
                B PgInt2 NOT NULL,
                PRIMARY KEY (Key)
            );
            CREATE TABLE Audit (Key Uint64 NOT NULL, PRIMARY KEY (Key));
        )").ExtractValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

        // Prepare the PG NOT NULL fixture with the old optimizer, then verify
        // that strict New RBO rejects these constraints before any write runs.
        auto setup = session.ExecuteDataQuery(R"(
            UPSERT INTO Checked (Key, A, B) VALUES (1u, PgInt2(10), PgInt2(20));
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(setup.IsSuccess(), setup.GetIssues().ToString());
        EnableNewRbo(kikimr);
        UNIT_ASSERT(session.Close().GetValueSync().IsSuccess());
        session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        for (const TString& write : {
            TString("INSERT INTO Checked (Key, A, B) VALUES ($key, $a, $b);"),
            TString("INSERT OR REVERT INTO Checked (Key, A, B) VALUES ($key, $a, $b);"),
            TString("UPSERT INTO Checked (Key, A, B) VALUES ($key, $a, $b);"),
            TString("REPLACE INTO Checked (Key, A, B) VALUES ($key, $a, $b);"),
            TString("UPDATE Checked ON (Key, A, B) VALUES ($key, $a, $b);"),
            TString("UPDATE Checked SET A = $a, B = $b WHERE Key = $key;")}) {
            // Keep values parameterized so the constraints remain in the plan.
            // Reject both NULL and non-NULL values: the feature is unsupported,
            // regardless of whether a particular row would satisfy the check.
            const TString query = TStringBuilder() << R"(
                DECLARE $key AS Uint64;
                DECLARE $a AS PgInt2;
                DECLARE $b AS PgInt2;
                UPSERT INTO Audit (Key) VALUES (1u);
            )" << write;
            const ui64 key = write.StartsWith("INSERT") ? 2 : 1;
            for (const TString& nullColumn : {TString("A"), TString("B"), TString()}) {
                auto params = TParamsBuilder()
                    .AddParam("$key").Uint64(key).Build()
                    .AddParam("$a").Pg(TPgValue(nullColumn == "A" ? TPgValue::VK_NULL : TPgValue::VK_TEXT,
                        nullColumn == "A" ? "" : "30", TPgType("pgint2"))).Build()
                    .AddParam("$b").Pg(TPgValue(nullColumn == "B" ? TPgValue::VK_NULL : TPgValue::VK_TEXT,
                        nullColumn == "B" ? "" : "40", TPgType("pgint2"))).Build()
                    .Build();
                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
                UNIT_ASSERT_C(!result.IsSuccess(), write);
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                    "PG write constraints are not supported in New RBO");

                // A fatal compilation error invalidates the session. Check the
                // stored state and submit the next case through a fresh one.
                session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                // Rejection must prevent the other effect from running too,
                // and leave existing destination rows untouched.
                auto state = session.ExecuteDataQuery(R"(
                    SELECT Key, A, B FROM Checked ORDER BY Key;
                    SELECT COUNT(*) FROM Audit;
                )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(state.IsSuccess(), state.GetIssues().ToString());
                CompareYson(R"([[1u;"10";"20"]])", FormatResultSetYson(state.GetResultSet(0)));
                CompareYson("[[0u]]", FormatResultSetYson(state.GetResultSet(1)));
            }
        }
    }

    Y_UNIT_TEST(MixedResultsAndMultipleEffects) {
        auto kikimr = NewRboKikimrRunner();
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto scheme = session.ExecuteSchemeQuery(R"(
            CREATE TABLE EffectsOrder (Key Uint64, Value String, PRIMARY KEY (Key));
        )").ExtractValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

        auto params = TParamsBuilder().AddParam("$key").Uint64(1).Build().Build();
        auto result = session.ExecuteDataQuery(R"(
            DECLARE $key AS Uint64;
            SELECT Key, Value FROM EffectsOrder;
            UPSERT INTO EffectsOrder (Key, Value) VALUES ($key, "first");
            UPSERT INTO EffectsOrder (Key, Value) VALUES (2u, "second");
            SELECT Key, Value FROM EffectsOrder WHERE Key = $key;
            UPDATE EffectsOrder SET Value = "updated" WHERE Key = $key;
            DELETE FROM EffectsOrder WHERE Key = 2u;
            SELECT Key, Value FROM EffectsOrder ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3);
        CompareYson("[]", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[1u];["first"]]])", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[[1u];["updated"]]])", FormatResultSetYson(result.GetResultSet(2)));

        // A failure in a later physical transaction must roll back earlier writes.
        result = session.ExecuteDataQuery(R"(
            UPSERT INTO EffectsOrder (Key, Value) VALUES (3u, "rollback");
            INSERT INTO EffectsOrder (Key, Value) VALUES (1u, "conflict");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        result = session.ExecuteDataQuery("SELECT Key, Value FROM EffectsOrder ORDER BY Key;",
            TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[[1u];["updated"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(IndexedReplaceClearsOmittedColumns) {
        auto kikimr = NewRboKikimrRunner();
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto scheme = session.ExecuteSchemeQuery(R"(
            CREATE TABLE EffectsIndex (
                Key Uint64, Value String, Payload String, PRIMARY KEY (Key),
                INDEX ByValue GLOBAL ON (Value)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO EffectsIndex (Key, Value, Payload) VALUES (1u, "old", "payload");
            REPLACE INTO EffectsIndex (Key, Value) VALUES (1u, "new");
            SELECT Key, Value, Payload FROM EffectsIndex VIEW ByValue;
            SELECT Key, Value FROM EffectsIndex VIEW ByValue;
            SELECT Key FROM EffectsIndex VIEW ByValue WHERE Value = "old";
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3);
        CompareYson(R"([[[1u];["new"];#]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[1u];["new"]]])", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson("[]", FormatResultSetYson(result.GetResultSet(2)));
    }

    Y_UNIT_TEST(InsertAbort_Literal_Success) {
        auto kikimr = NewRboKikimrRunner();
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

    Y_UNIT_TEST(InsertAbort_ValuesExpressions) {
        auto kikimr = NewRboKikimrRunner();
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto params = TParamsBuilder()
            .AddParam("$key").Uint32(10).Build()
            .AddParam("$value").String("foo").Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            DECLARE $key AS Uint32;
            DECLARE $value AS String;
            INSERT INTO `/Root/TwoShard` (Value2, Value1, Key) VALUES
                (NULL, $value, $key), (42, $value || "bar", $key + 1u);
        )", TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key >= 10u AND Key <= 11u ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [[10u];["foo"];#];
            [[11u];["foobar"];[42]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertAbort_Literal_Duplicates) {
        auto kikimr = NewRboKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TwoShard` (Value1, Key) VALUES
                ("foo", 10u), ("bar", 11u), ("baz", 10u)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [&](const auto& issue) {
            return issue.GetMessage().contains("Conflict with existing key.");
        }));

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key > 5 AND Key < 100 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InsertAbort_Literal_Conflict) {
        auto kikimr = NewRboKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TwoShard` (Value1, Key) VALUES
                ("foo", 1u), ("bar", 11u)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const auto& issue) {
            return issue.GetMessage().contains("Conflict with existing key.");
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
        auto kikimr = NewRboKikimrRunner();
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
        auto kikimr = NewRboKikimrRunner();
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
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const auto& issue) {
            return issue.GetMessage().contains("Conflict with existing key.");
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
        auto kikimr = NewRboKikimrRunner();
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
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const auto& issue) {
            return issue.GetMessage().contains("Conflict with existing key.");
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
        auto kikimr = NewRboKikimrRunner();
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
        auto kikimr = NewRboKikimrRunner();
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
                (1u, "foo", 5, 10u), (2u, "bar", 7, 11u), (3u, "baz", 9, 10u)
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TwoShard` SELECT Value3 as Key, Value1, Value2 FROM `/Root/Foo`
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const auto& issue) {
            return issue.GetMessage().contains("Conflict with existing key.");
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
        auto kikimr = NewRboKikimrRunner();
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
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const auto& issue) {
            return issue.GetMessage().contains("Conflict with existing key.");
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
        auto kikimr = NewRboKikimrRunner();
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
        auto kikimr = NewRboKikimrRunner();
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
        auto kikimr = NewRboKikimrRunner();
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

    Y_UNIT_TEST_TWIN(InsertRevert_NullKeys, UniqueIndex) {
        auto kikimr = NewRboKikimrRunner();
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto schema = TString(R"(
            CREATE TABLE `/Root/RevertKeys` (
                Key Uint32,
                Value String,
                PRIMARY KEY (Key)
        )");
        if constexpr (UniqueIndex) {
            schema += ", INDEX by_value GLOBAL UNIQUE ON (Value)";
        }
        schema += ");";
        auto created = session.ExecuteSchemeQuery(schema).ExtractValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());

        auto execute = [&](const TString& query) {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result;
        };
        // A NULL primary key participates in both duplicate and existing-key checks.
        execute(R"(INSERT OR REVERT INTO `/Root/RevertKeys` (Key, Value) VALUES
            (NULL, "one"), (NULL, "two"), (1u, "unused"))");
        auto empty = execute("SELECT * FROM `/Root/RevertKeys`");
        CompareYson("[]", FormatResultSetYson(empty.GetResultSet(0)));

        execute(R"(INSERT OR REVERT INTO `/Root/RevertKeys` (Key, Value) VALUES (NULL, "existing"))");
        execute(R"(INSERT OR REVERT INTO `/Root/RevertKeys` (Key, Value) VALUES
            (NULL, "conflict"), (1u, "unused"))");
        if constexpr (UniqueIndex) {
            execute(R"(INSERT OR REVERT INTO `/Root/RevertKeys` (Key, Value) VALUES
                (2u, "duplicate"), (3u, "duplicate"), (4u, "unused"))");
            execute(R"(INSERT OR REVERT INTO `/Root/RevertKeys` (Key, Value) VALUES
                (5u, "existing"), (6u, "unused"))");
        }
        // Unlike primary keys, NULL values in a unique index may repeat.
        execute(R"(INSERT OR REVERT INTO `/Root/RevertKeys` (Key, Value) VALUES
            (7u, NULL), (8u, NULL))");
        auto result = execute("SELECT * FROM `/Root/RevertKeys` ORDER BY Key");
        CompareYson(R"([[#;["existing"]];[[7u];#];[[8u];#]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(UpdateOn_Literal) {
        auto kikimr = NewRboKikimrRunner();
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

    Y_UNIT_TEST(DeleteOn_Literal) {
        auto kikimr = NewRboKikimrRunner();
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            DELETE FROM `/Root/TwoShard` ON (Key) VALUES
                (1u), (4000000002u), (4000000010u);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
            [[2u];["Two"];[0]];
            [[3u];["Three"];[1]];
            [[4000000001u];["BigOne"];[-1]];
            [[4000000003u];["BigThree"];[1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(UpdateOn_Params) {
        auto kikimr = NewRboKikimrRunner();
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
        auto kikimr = NewRboKikimrRunner();
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

    Y_UNIT_TEST_TWIN(DeletePkPrefixWithIndex, UseIndexStreamWrite) {
        auto app = NewRboAppConfig();
        app.MutableTableServiceConfig()->SetEnableIndexStreamWrite(UseIndexStreamWrite);

        TEffectsKikimrRunner kikimr(app);

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
        if constexpr (!UseIndexStreamWrite) {
            // RBO does not expand legacy index effects. Reject this mode rather
            // than producing a main-table-only delete that leaves stale indexes.
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "EnableIndexStreamWrite");
            return;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        size_t reads = 0;
        auto checkReads = [&](const auto& self, const NJson::TJsonValue& node) -> void {
            if (node["Node Type"].GetStringSafe() == "TableRangeScan") {
                ++reads;
                const auto& read = node["Operators"][0];
                UNIT_ASSERT_VALUES_EQUAL(read["Table"].GetStringSafe(), "Tmp");
                UNIT_ASSERT_VALUES_EQUAL(read["ReadColumns"].GetArraySafe().size(), 3);
                UNIT_ASSERT_VALUES_EQUAL(read["ReadRangesKeys"][0].GetStringSafe(), "Key1");
            }
            if (node.Has("Plans")) {
                for (const auto& child : node["Plans"].GetArraySafe()) {
                    self(self, child);
                }
            }
        };
        checkReads(checkReads, plan["SimplifiedPlan"]);
        UNIT_ASSERT_VALUES_EQUAL(reads, 1);

    }

    Y_UNIT_TEST(EmptyUpdate) {
        TKikimrSettings settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
        TEffectsKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        {
            auto schemeResult = session.ExecuteSchemeQuery(R"(
                --!syntax_v1
                CREATE TABLE T1 (
                    Key Uint32,
                    Value Uint32,
                    Timestamp Timestamp,
                    PRIMARY KEY (Key)
                );
                CREATE TABLE T2 (
                    Key Uint32,
                    Value Uint32,
                    PRIMARY KEY (Key)
                );
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(schemeResult.GetStatus(), EStatus::SUCCESS, schemeResult.GetIssues().ToString());
        }
        Cerr << "!!!UPDATE TABLE" << Endl;
        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1
                $data = SELECT 1u AS Key, 1u AS Value;
                UPDATE T1 ON SELECT Key, Value FROM $data;
                DELETE FROM T2 WHERE Key = 1;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        Cerr << "!!!DROP TABLE" << Endl;
        {
            auto schemeResult = session.DropTable("/Root/T1").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(schemeResult.GetStatus(), EStatus::SUCCESS, schemeResult.GetIssues().ToString());
        }
    }
    
    Y_UNIT_TEST(AlterDuringUpsertTransaction) {
        auto kikimr = NewRboKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto ret = session1.ExecuteSchemeQuery(R"(
            CREATE TABLE `TestTable` (
                Key Uint32,
                Value1 String,
                PRIMARY KEY (Key)
            )
        )").ExtractValueSync();
        UNIT_ASSERT_C(ret.IsSuccess(), ret.GetIssues().ToString());

        auto txControl = TTxControl::BeginTx();
        auto upsertResult = session1.ExecuteDataQuery(R"(
            UPSERT INTO `TestTable` (Key, Value1) VALUES
                (1u, "First"),
                (2u, "Second")
        )", txControl).ExtractValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        auto tx1 = upsertResult.GetTransaction();
        UNIT_ASSERT(tx1);

        auto alterResult = session2.ExecuteSchemeQuery(R"(
            ALTER TABLE `TestTable` ADD COLUMN Value2 Int32
        )").ExtractValueSync();
        UNIT_ASSERT_C(alterResult.IsSuccess(), alterResult.GetIssues().ToString());

        auto commitResult = tx1->Commit().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::ABORTED, commitResult.GetIssues().ToString());
        UNIT_ASSERT_C(commitResult.GetIssues().ToString().contains("Scheme changed. Table: `/Root/TestTable`.")
            || commitResult.GetIssues().ToString().contains("Table '/Root/TestTable' scheme changed."),
            commitResult.GetIssues().ToString());
    }

    Y_UNIT_TEST(AlterAfterUpsertTransaction) {
        auto kikimr = NewRboKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto ret = session1.ExecuteSchemeQuery(R"(
            CREATE TABLE `TestTable` (
                Key Uint32,
                Value1 String,
                PRIMARY KEY (Key)
            )
        )").ExtractValueSync();
        UNIT_ASSERT_C(ret.IsSuccess(), ret.GetIssues().ToString());

        auto txControl = TTxControl::BeginTx();
        auto upsertResult = session1.ExecuteDataQuery(R"(
            UPSERT INTO `TestTable` (Key, Value1) VALUES
                (1u, "First"),
                (2u, "Second");
            SELECT * FROM `TestTable`;
        )", txControl).ExtractValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        auto tx1 = upsertResult.GetTransaction();
        UNIT_ASSERT(tx1);

        auto alterResult = session2.ExecuteSchemeQuery(R"(
            ALTER TABLE `TestTable` ADD COLUMN Value2 Int32
        )").ExtractValueSync();
        UNIT_ASSERT_C(alterResult.IsSuccess(), alterResult.GetIssues().ToString());

        auto commitResult = tx1->Commit().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::ABORTED, commitResult.GetIssues().ToString());
    }

    Y_UNIT_TEST(AlterAfterUpsertBeforeUpsertTransaction) {
        auto kikimr = NewRboKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto ret = session1.ExecuteSchemeQuery(R"(
            CREATE TABLE `TestTable` (
                Key Uint32,
                Value1 String,
                PRIMARY KEY (Key)
            )
        )").ExtractValueSync();
        UNIT_ASSERT_C(ret.IsSuccess(), ret.GetIssues().ToString());

        auto txControl = TTxControl::BeginTx();
        auto upsertResult = session1.ExecuteDataQuery(R"(
            UPSERT INTO `TestTable` (Key, Value1) VALUES
                (1u, "First"),
                (2u, "Second");
            SELECT * FROM `TestTable` WHERE Key = 1u;
        )", txControl).ExtractValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        auto tx1 = upsertResult.GetTransaction();
        UNIT_ASSERT(tx1);

        auto alterResult = session2.ExecuteSchemeQuery(R"(
            ALTER TABLE `TestTable` ADD COLUMN Value2 Int32
        )").ExtractValueSync();
        UNIT_ASSERT_C(alterResult.IsSuccess(), alterResult.GetIssues().ToString());

        // The statement is compiled against a schema version the transaction has not seen, so
        // it is refused right away instead of being deferred and failing at commit.
        auto upsertResult2 = session1.ExecuteDataQuery(R"(
            UPSERT INTO `TestTable` (Key, Value1) VALUES
                (1u, "First"),
                (2u, "Second");
        )", TTxControl::Tx(*tx1)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(upsertResult2.GetStatus(), EStatus::ABORTED, upsertResult2.GetIssues().ToString());
    }

    Y_UNIT_TEST(AlterAfterUpsertBeforeUpsertSelectTransaction) {
        auto kikimr = NewRboKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto ret = session1.ExecuteSchemeQuery(R"(
            CREATE TABLE `TestTable` (
                Key Uint32,
                Value1 String,
                PRIMARY KEY (Key)
            )
        )").ExtractValueSync();
        UNIT_ASSERT_C(ret.IsSuccess(), ret.GetIssues().ToString());

        auto txControl = TTxControl::BeginTx();
        auto upsertResult = session1.ExecuteDataQuery(R"(
            UPSERT INTO `TestTable` (Key, Value1) VALUES
                (1u, "First"),
                (2u, "Second");
            SELECT * FROM `TestTable` WHERE Key = 1u;
        )", txControl).ExtractValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        auto tx1 = upsertResult.GetTransaction();
        UNIT_ASSERT(tx1);

        auto alterResult = session2.ExecuteSchemeQuery(R"(
            ALTER TABLE `TestTable` ADD COLUMN Value2 Int32
        )").ExtractValueSync();
        UNIT_ASSERT_C(alterResult.IsSuccess(), alterResult.GetIssues().ToString());

        auto upsertResult2 = session1.ExecuteDataQuery(R"(
            UPSERT INTO `TestTable` (Key, Value1) VALUES
                (1u, "First"),
                (2u, "Second");
            SELECT * FROM `TestTable` WHERE Key = 1u;
        )", TTxControl::Tx(*tx1)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(upsertResult2.GetStatus(), EStatus::ABORTED, upsertResult2.GetIssues().ToString());
    }

    // FIXME: Re-enable both RandomWithIndex variants after implementing JOIN USING
    // in yql/essentials/sql/v1/translation/sql_select_yql.cpp (currently rejected
    // as "YqlSelect unsupported: USING pure_column_or_named_list"). Lower USING
    // keys and output-column resolution with the correct LEFT JOIN nullability,
    // and verify that RBO preserves the shared $rowUpdates input for both REPLACEs
    // (including the generated RandomNumber key). Keep optimizer fallback disabled;
    // both variants must pass the row-count and key-association checks below.
#if 0
    Y_UNIT_TEST_TWIN(RandomWithIndex, UseSecondaryIndex) {
        auto kikimr = NewRboKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

       {
            const TString query = Sprintf(R"(
                CREATE TABLE `/Root/Rows` (
                    rowKey Uint64 NOT NULL,
                    v1 Uint64 NOT NULL,
                    v2 Uint64 NOT NULL,
                    %s
                    PRIMARY KEY (rowKey)
                );
            )", (UseSecondaryIndex ? "INDEX idx_2 GLOBAL ON (v2)," : ""));
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                CREATE TABLE `/Root/GlobalKeyToRows` (
                    globalKey Uint64 NOT NULL,
                    rowKey Uint64 NOT NULL,
                    PRIMARY KEY (globalKey, rowKey)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                $globalKey = 1;
                $addRows = SELECT 1 AS v1, 2 AS v2;

                $rowKeys = SELECT rowKey FROM `/Root/GlobalKeyToRows` WHERE globalKey = $globalKey;

                $existingRows = SELECT rowKey, v1, v2 FROM `/Root/Rows` WHERE rowKey IN $rowKeys;

                $rowUpdates = SELECT
                    r.rowKey ?? CAST(RandomNumber(r.rowKey) AS Uint64) AS rowKey,
                    l.v1 AS v1,
                    l.v2 AS v2
                FROM $addRows AS l
                LEFT JOIN $existingRows AS r
                USING (v1, v2);

                REPLACE INTO `/Root/Rows`
                SELECT rowKey, v1, v2 FROM $rowUpdates;

                REPLACE INTO `/Root/GlobalKeyToRows`
                SELECT $globalKey AS globalKey, rowKey FROM $rowUpdates;
            )";
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT COUNT(*) FROM `/Root/Rows`;
                SELECT COUNT(*) FROM `/Root/GlobalKeyToRows`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), R"([[1u]])");
            CompareYson(FormatResultSetYson(result.GetResultSet(1)), R"([[1u]])");
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                $rowKeys = SELECT rowKey FROM `/Root/GlobalKeyToRows` WHERE globalKey = 1;
                SELECT COUNT(*) FROM `/Root/Rows` WHERE rowKey IN $rowKeys;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), R"([[1u]])");
        }
    }
#endif // FIXME: JOIN USING support in YqlSelect/RBO

    Y_UNIT_TEST_TWIN(DeleteWithJoinAndIndex, UseSecondaryIndex) {
        auto kikimr = NewRboKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

       {
            const TString query = Sprintf(R"(
                CREATE TABLE `/Root/Rows` (
                    rowKey Uint64 NOT NULL,
                    v1 Uint64 NOT NULL,
                    v2 Uint64 NOT NULL,
                    %s
                    PRIMARY KEY (rowKey)
                );
            )", (UseSecondaryIndex ? "INDEX idx_2 GLOBAL ON (v2)," : ""));
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                CREATE TABLE `/Root/GlobalKeyToRows` (
                    globalKey Uint64 NOT NULL,
                    rowKey Uint64 NOT NULL,
                    PRIMARY KEY (globalKey, rowKey)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                $globalKey = 1;
                
                INSERT INTO `/Root/Rows` (rowKey, v1, v2) VALUES
                    (1u, 1u, 1u),
                    (2u, 2u, 2u),
                    (3u, 3u, 3u);

                INSERT INTO `/Root/GlobalKeyToRows` (globalKey, rowKey) VALUES
                    ($globalKey, 1u),
                    ($globalKey, 2u),
                    ($globalKey + 1, 3u);
            )";
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                PRAGMA AnsiInForEmptyOrNullableItemsCollections;

                $globalKey = 1;
                
                $toDelete = SELECT
                    r.rowKey AS rowKey
                FROM `/Root/GlobalKeyToRows` AS l
                INNER JOIN `/Root/Rows` AS r ON l.rowKey = r.rowKey
                WHERE l.globalKey = $globalKey;

                DELETE FROM `/Root/Rows`
                ON SELECT rowKey FROM $toDelete;

                DELETE FROM `/Root/GlobalKeyToRows`
                ON SELECT $globalKey AS globalKey, rowKey FROM $toDelete;
            )";
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT COUNT(*) FROM `/Root/Rows`;
                SELECT COUNT(*) FROM `/Root/GlobalKeyToRows`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            // Wrong uncommitted changes TODO: fix it
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), R"([[1u]])");
            CompareYson(FormatResultSetYson(result.GetResultSet(1)), R"([[1u]])");
        }
    }

    Y_UNIT_TEST_TWIN(DeleteWithIndex, UseSecondaryIndex) {
        auto kikimr = NewRboKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

       {
            const TString query = Sprintf(R"(
                CREATE TABLE `/Root/Rows` (
                    rowKey Uint64 NOT NULL,
                    v1 Uint64 NOT NULL,
                    v2 Uint64 NOT NULL,
                    %s
                    PRIMARY KEY (rowKey)
                );
            )", (UseSecondaryIndex ? "INDEX idx_2 GLOBAL ON (v2)," : ""));
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                CREATE TABLE `/Root/GlobalKeyToRows` (
                    globalKey Uint64 NOT NULL,
                    rowKey Uint64 NOT NULL,
                    PRIMARY KEY (globalKey, rowKey)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                $globalKey = 1;
                
                INSERT INTO `/Root/Rows` (rowKey, v1, v2) VALUES
                    (1u, 1u, 1u),
                    (2u, 2u, 2u),
                    (3u, 3u, 3u);

                INSERT INTO `/Root/GlobalKeyToRows` (globalKey, rowKey) VALUES
                    ($globalKey, 1u),
                    ($globalKey, 2u),
                    ($globalKey, 3u);
            )";
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                PRAGMA AnsiInForEmptyOrNullableItemsCollections;

                $globalKey = 1;
                
                $toDelete = SELECT
                    Unwrap(rowKey * 2u) AS rowKey
                FROM `/Root/Rows`;

                SELECT COUNT(*) FROM $toDelete;

                DELETE FROM `/Root/Rows`
                ON SELECT Unwrap(rowKey / 2u) AS rowKey FROM $toDelete;

                DELETE FROM `/Root/GlobalKeyToRows`
                ON SELECT $globalKey AS globalKey, Unwrap(rowKey / 2u) AS rowKey FROM $toDelete;

                SELECT COUNT(*) FROM $toDelete;

                UPSERT INTO `/Root/Rows` (rowKey, v1, v2) VALUES
                    (5u, 1u, 1u);

                UPSERT INTO `/Root/GlobalKeyToRows` (globalKey, rowKey) VALUES
                    ($globalKey, 5u);

                SELECT COUNT(*) FROM $toDelete;

                DELETE FROM `/Root/Rows`
                ON SELECT Unwrap(rowKey / 2u) AS rowKey FROM $toDelete;

                DELETE FROM `/Root/GlobalKeyToRows`
                ON SELECT $globalKey AS globalKey, Unwrap(rowKey / 2u) AS rowKey FROM $toDelete;

                SELECT COUNT(*) FROM $toDelete;
            )";
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), R"([[3u]])");
            CompareYson(FormatResultSetYson(result.GetResultSet(1)), R"([[3u]])");
            // Wrong uncommitted changes TODO: fix it
            CompareYson(FormatResultSetYson(result.GetResultSet(2)), UseSecondaryIndex ? R"([[0u]])" : R"([[3u]])");
            CompareYson(FormatResultSetYson(result.GetResultSet(3)), UseSecondaryIndex ? R"([[1u]])" : R"([[3u]])");
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT COUNT(*) FROM `/Root/Rows`;
                SELECT COUNT(*) FROM `/Root/GlobalKeyToRows`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            // Wrong uncommitted changes TODO: fix it
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), UseSecondaryIndex ? R"([[0u]])" : R"([[1u]])");
            CompareYson(FormatResultSetYson(result.GetResultSet(1)), UseSecondaryIndex ? R"([[0u]])" : R"([[1u]])");
        }
    }

    // FIXME: Re-enable EffectWithSelect after supporting NOT IN COMPACT in the
    // YqlSelect path of yql/essentials/sql/v1/translation/sql_expression.cpp
    // (currently rejected as "YqlSelect unsupported: COMPACT"). Carry the COMPACT
    // hint through membership/anti-join lowering without changing ANSI IN behavior
    // for empty inputs or nullable keys. Preserve shared subquery evaluation across
    // SELECT and UPSERT/DELETE roots so the single-table-read contract still holds.
    // Update the legacy plan["tables"] assertions to inspect RBO's SimplifiedPlan,
    // retaining the single-read check. Validate every Explain case below through
    // both TableService and QueryService with optimizer fallback disabled.
#if 0
    Y_UNIT_TEST(EffectWithSelect) {
        auto kikimr = NewRboKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto client = kikimr.GetQueryClient();

       {
            const TString query = Sprintf(R"(
                CREATE TABLE `/Root/Rows` (
                    k1 Uint64,
                    k2 Uint64,
                    v1 Uint64,
                    v2 Uint64,
                    PRIMARY KEY (k1, k2)
                );
            )");
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const TString query = R"(
                INSERT INTO `/Root/Rows` (k1, k2, v1, v2) VALUES
                    (1u, 1u, 1u, 1u),
                    (1u, 2u, 2u, 2u),
                    (1u, 3u, 3u, 3u);
            )";
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                --!syntax_v1

                PRAGMA AnsiInForEmptyOrNullableItemsCollections;

                DECLARE $k AS Uint64;

                DECLARE $rows AS List<
                    Struct<
                        k2: Uint64,
                        v1: Uint64,
                        v2: Uint64
                    >
                >;

                $k2_by_k1 = (
                    SELECT k2 FROM `/Root/Rows`
                    WHERE k1 = $k
                );

                $new_rows = (
                    SELECT * FROM AS_TABLE($rows)
                    WHERE k2 NOT IN COMPACT $k2_by_k1
                );

                SELECT COUNT(*) FROM $new_rows;

                SELECT * FROM $new_rows LIMIT 1000;

                UPSERT INTO `/Root/Rows`
                SELECT $k AS k1, k2, v1, v2 FROM $new_rows;
            )";

            {
                auto result = session.ExplainDataQuery(query).GetValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());


                NJson::TJsonValue plan;
                NJson::ReadJsonTree(result.GetPlan(), &plan, true);
                UNIT_ASSERT_C(plan["tables"].GetArray().size() == 1, plan["tables"].GetArray().size());
                UNIT_ASSERT_C(plan["tables"][0]["reads"].GetArray().size() == 1, plan["tables"][0]["reads"].GetArray().size());
            }

            {
                auto settings = NYdb::NQuery::TExecuteQuerySettings()
                    .ExecMode(NYdb::NQuery::EExecMode::Explain);
                auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).GetValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());


                NJson::TJsonValue plan;
                NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true);
                UNIT_ASSERT_C(plan["tables"].GetArray().size() == 1, plan["tables"].GetArray().size());
                UNIT_ASSERT_C(plan["tables"][0]["reads"].GetArray().size() == 1, plan["tables"][0]["reads"].GetArray().size());
            }
        }

        {
            const TString query = R"(
                --!syntax_v1

                PRAGMA AnsiInForEmptyOrNullableItemsCollections;

                DECLARE $k AS Uint64;

                DECLARE $rows AS List<
                    Struct<
                        k2: Uint64,
                        v1: Uint64,
                        v2: Uint64
                    >
                >;

                $k2_by_k1 = (
                    SELECT k2 FROM `/Root/Rows`
                    WHERE k1 = $k
                );

                $new_rows = (
                    SELECT * FROM AS_TABLE($rows)
                    WHERE k2 NOT IN COMPACT $k2_by_k1
                );

                SELECT COUNT(*) FROM $new_rows;

                SELECT * FROM $new_rows LIMIT 1000;

                $other_transformation = (
                    SELECT $k * 10 AS k1, k2, v1 + v2 AS v1, v1 * v2 AS v2 FROM $new_rows
                );

                UPSERT INTO `/Root/Rows`
                SELECT * FROM $other_transformation;
            )";

            {
                auto result = session.ExplainDataQuery(query).GetValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());


                NJson::TJsonValue plan;
                NJson::ReadJsonTree(result.GetPlan(), &plan, true);
                UNIT_ASSERT_C(plan["tables"].GetArray().size() == 1, plan["tables"].GetArray().size());
                UNIT_ASSERT_C(plan["tables"][0]["reads"].GetArray().size() == 1, plan["tables"][0]["reads"].GetArray().size());
            }

            {
                auto settings = NYdb::NQuery::TExecuteQuerySettings()
                    .ExecMode(NYdb::NQuery::EExecMode::Explain);
                auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).GetValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());


                NJson::TJsonValue plan;
                NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true);
                UNIT_ASSERT_C(plan["tables"].GetArray().size() == 1, plan["tables"].GetArray().size());
                UNIT_ASSERT_C(plan["tables"][0]["reads"].GetArray().size() == 1, plan["tables"][0]["reads"].GetArray().size());
            }
        }

        {
            const TString query = R"(
                --!syntax_v1

                PRAGMA AnsiInForEmptyOrNullableItemsCollections;

                DECLARE $k AS Uint64;

                $deleted_rows = (
                    SELECT k1, k2 FROM `/Root/Rows`
                    WHERE k1 = $k
                );

                SELECT COUNT(*) FROM $deleted_rows;

                DELETE FROM `/Root/Rows` ON
                SELECT * FROM $deleted_rows;
            )";

            {
                auto result = session.ExplainDataQuery(query).GetValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());


                NJson::TJsonValue plan;
                NJson::ReadJsonTree(result.GetPlan(), &plan, true);
                UNIT_ASSERT_C(plan["tables"].GetArray().size() == 1, plan["tables"].GetArray().size());
                UNIT_ASSERT_C(plan["tables"][0]["reads"].GetArray().size() == 1, plan["tables"][0]["reads"].GetArray().size());
            }

            {
                auto settings = NYdb::NQuery::TExecuteQuerySettings()
                    .ExecMode(NYdb::NQuery::EExecMode::Explain);
                auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).GetValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());


                NJson::TJsonValue plan;
                NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true);
                UNIT_ASSERT_C(plan["tables"].GetArray().size() == 1, plan["tables"].GetArray().size());
                UNIT_ASSERT_C(plan["tables"][0]["reads"].GetArray().size() == 1, plan["tables"][0]["reads"].GetArray().size());
            }
        }
    }
#endif // FIXME: NOT IN COMPACT and shared-read explain coverage

}

} // namespace NKqp
} // namespace NKikimr
