#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void CreateTableWithMultishardIndex(Tests::TClient& client, NKikimrSchemeOp::EIndexType type) {
    const TString scheme =  R"(Name: "MultiShardIndexed"
        Columns { Name: "key"    Type: "Uint64" NotNull: true }
        Columns { Name: "fk"    Type: "Uint32" NotNull: true }
        Columns { Name: "fk2"    Type: "Uint32" NotNull: true }
        Columns { Name: "value"  Type: "String" NotNull: true }
        KeyColumnNames: ["key"]
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 3 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
    )";

    NKikimrSchemeOp::TTableDescription desc;
    bool parseOk = ::google::protobuf::TextFormat::ParseFromString(scheme, &desc);
    UNIT_ASSERT(parseOk);

    auto status = client.TClient::CreateTableWithUniformShardedIndex("/Root", desc, "index", {"fk", "fk2"}, type);
    UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
}

void TestUpdateWithoutChangingNotNullColumn(TSession& session) {
    {  /* init table */
        const auto query = Q_(R"(
            UPSERT INTO t (id, val, created_on) VALUES 
            (123, 'xxx', 1);
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {  /* update not null column */
        const auto query = Q_("UPDATE t SET val = 'a' WHERE id = 123;");
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        result = session.ExecuteDataQuery(R"(
            SELECT * FROM t;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[1u;123u;["a"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {  /* update not null column */
        const auto query = Q_("UPDATE t SET val = 'a', created_on = NULL WHERE id = 123;");
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }

    {  /* update on not null column */
        const auto query = Q_(R"(
            $to_update = (
                SELECT id, 'b' AS val FROM t
                WHERE id = 123
            );
            UPDATE t ON SELECT * FROM $to_update;
        )");
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        result = session.ExecuteDataQuery(R"(
            SELECT * FROM t;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[1u;123u;["b"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {  /* update on not null column */
        const auto query = Q_(R"(
            $to_update = (
                SELECT id, 'b' AS val, NULL as created_on FROM t
                WHERE id = 123
            );
            UPDATE t ON SELECT * FROM $to_update;
        )");
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNotNullColumns) {
    Y_UNIT_TEST(CreateTableWithDisabledNotNullDataColumns) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false).SetEnableNotNullDataColumns(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestCreateTable` (
                    Key Uint64 NOT NULL,
                    Value String NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestCreateTable` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(InsertNotNullPk) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestInsertNotNullPk` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("INSERT INTO `/Root/TestInsertNotNullPk` (Key, Value) VALUES (1, 'Value1')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null pk column */
            const auto query = Q_("INSERT INTO `/Root/TestInsertNotNullPk` (Value) VALUES ('Value2')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED), result.GetIssues().ToString());
        }

        {  /* set NULL to not null pk column */
            const auto query = Q_("INSERT INTO `/Root/TestInsertNotNullPk` (Key, Value) VALUES (NULL, 'Value3')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(InsertNotNullPkPg) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE Pg (
                key int2 PRIMARY KEY,
                value int2
            ))");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg (key, value) VALUES (
                    1::int2, 1::int2
            ))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {   /* missing not null pk column */
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg (key, value) VALUES (
                    NULL::int2
            ))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
            UNIT_ASSERT_NO_DIFF(result.GetIssues().ToString(), "<main>: Error: Execution, code: 1060\n"
            "    <main>: Error: Tried to insert NULL value into NOT NULL column: key, code: 2031\n");
        }

        {   /* set NULL to not null pk column */
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg (key, value) VALUES (
                    NULL::int2, 123::int2
            ))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
            UNIT_ASSERT_NO_DIFF(result.GetIssues().ToString(), "<main>: Error: Execution, code: 1060\n"
            "    <main>: Error: Tried to insert NULL value into NOT NULL column: key, code: 2031\n");
        }

        {   /* set NULL to nullable column */
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg (key, value) VALUES (
                    123::int2, NULL::int2
            ))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UpsertNotNullPk) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestUpsertNotNullPk` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNullPk` (Key, Value) VALUES (1, 'Value1')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null pk column */
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNullPk` (Value) VALUES ('Value2')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED), result.GetIssues().ToString());
        }

        {  /* set NULL to not null pk column */
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNullPk` (Key, Value) VALUES (NULL, 'Value3')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UpsertNotNullPkPg) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestUpsertNotNullPk` (
                    Key PgInt2 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNullPk` (Key, Value) VALUES (PgInt2(1), 'Value1')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null pk column */
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNullPk` (Value) VALUES ('Value2')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED), result.GetIssues().ToString());
        }

        {  /* set NULL to not null pk column */
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNullPk` (Key, Value) VALUES (PgCast(NULL, PgInt2), 'Value3')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }


    Y_UNIT_TEST(ReplaceNotNullPk) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestReplaceNotNullPk` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNullPk` (Key, Value) VALUES (1, 'Value1')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null pk column */
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNullPk` (Value) VALUES ('Value2')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED), result.GetIssues().ToString());
        }

        {  /* set NULL to not null pk column */
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNullPk` (Key, Value) VALUES (NULL, 'Value3')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ReplaceNotNullPkPg) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestReplaceNotNullPk` (
                    Key PgInt2 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNullPk` (Key, Value) VALUES (PgInt2(1), 'Value1')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null pk column */
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNullPk` (Value) VALUES ('Value2')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED), result.GetIssues().ToString());
        }

        {  /* set NULL to not null pk column */
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNullPk` (Key, Value) VALUES (PgCast(NULL, PgInt2), 'Value3')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UpdateNotNullPk) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestUpdateNotNullPk` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* init table */
            const auto query = Q_(R"(
                REPLACE INTO `/Root/TestUpdateNotNullPk` (Key, Value) VALUES
                    (1, 'Value1'),
                    (2, 'Value2'),
                    (3, 'Value3');
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* update data column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNullPk` SET Value = 'NewValue'");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* update not null pk column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNullPk` SET Key = 10 WHERE Key = 1");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }

        {  /* set NULL to not null pk column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNullPk` SET Key = NULL WHERE Key = 1");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UpdateNotNullPkPg) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestUpdateNotNullPk` (
                    Key PgInt2 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* init table */
            const auto query = Q_(R"(
                REPLACE INTO `/Root/TestUpdateNotNullPk` (Key, Value) VALUES
                    (PgInt2(1), 'Value1'),
                    (PgInt2(2), 'Value2'),
                    (PgInt2(3), 'Value3');
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* update data column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNullPk` SET Value = 'NewValue'");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* update not null pk column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNullPk` SET Key = PgInt2(10) WHERE Key = PgInt2(1)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }

        {  /* set NULL to not null pk column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNullPk` SET Key = PgCast(NULL, PgInt2) WHERE Key = PgInt2(1)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(SelectNotNullColumns) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestSelectNotNullPk` (
                    Key Uint64 NOT NULL,
                    Value String NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* init table */
            const auto query = Q_(R"(
                REPLACE INTO `/Root/TestSelectNotNullPk` (Key, Value) VALUES
                    (1, 'Value1'),
                    (2, 'Value2'),
                    (3, 'Value3');
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("SELECT * FROM `/Root/TestSelectNotNullPk`");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("SELECT * FROM `/Root/TestSelectNotNullPk` WHERE Key = 1");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("SELECT * FROM `/Root/TestSelectNotNullPk` WHERE Value = 'Value1'");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(InsertNotNull) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestInsertNotNull` (
                    Key Uint64,
                    Value String NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("INSERT INTO `/Root/TestInsertNotNull` (Key, Value) VALUES (1, 'Value1')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null column */
            const auto query = Q_("INSERT INTO `/Root/TestInsertNotNull` (Key) VALUES (2)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE), result.GetIssues().ToString());
        }

        {  /* set NULL to not null column */
            const auto query = Q_("INSERT INTO `/Root/TestInsertNotNull` (Key, Value) VALUES (3, NULL)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(InsertFromSelect) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto q1 = Q_(R"(
                CREATE TABLE `/Root/TestInsertNotNull` (
                    Key Uint64,
                    Value String NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(q1).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            
            const auto q2 = Q_(R"(
                CREATE TABLE `/Root/TestInsert` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key))
            )");

            result = session.ExecuteSchemeQuery(q2).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("INSERT INTO `/Root/TestInsert` (Key, Value) VALUES (1, NULL)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null column */
            const auto query = Q_("INSERT INTO `/Root/TestInsertNotNull` (Key, Value) SELECT Key, Value FROM `/Root/TestInsert`");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(InsertNotNullPg) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestInsertNotNull` (
                    Key Uint64,
                    Value PgInt2 NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("INSERT INTO `/Root/TestInsertNotNull` (Key, Value) VALUES (1, PgInt2(1))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null column */
            const auto query = Q_("INSERT INTO `/Root/TestInsertNotNull` (Key) VALUES (2)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE), result.GetIssues().ToString());
        }

        {  /* set NULL to not null column */
            const auto query = Q_("INSERT INTO `/Root/TestInsertNotNull` (Key, Value) VALUES (3, PgCast(NULL, PgInt2))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
            UNIT_ASSERT_NO_DIFF(result.GetIssues().ToString(), "<main>: Error: Execution, code: 1060\n"
            "    <main>: Error: Tried to insert NULL value into NOT NULL column: Value, code: 2031\n");
        }
    }

    Y_UNIT_TEST(UpsertNotNull) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestUpsertNotNull` (
                    Key Uint64,
                    Value String NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNull` (Key, Value) VALUES (1, 'Value1')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null column */
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNull` (Key) VALUES (2)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE), result.GetIssues().ToString());
        }

        {  /* set NULL to not null column */
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNull` (Key, Value) VALUES (3, NULL)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UpsertNotNullPg) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestUpsertNotNull` (
                    Key Uint64,
                    Value PgText NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNull` (Key, Value) VALUES (1, 'Value1'pt)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null column */
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNull` (Key) VALUES (2)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE), result.GetIssues().ToString());
        }

        {  /* set NULL to not null column */
            const auto query = Q_("UPSERT INTO `/Root/TestUpsertNotNull` (Key, Value) VALUES (3, PgCast(NULL, PgText))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ReplaceNotNull) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestReplaceNotNull` (
                    Key Uint64,
                    Value String NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNull` (Key, Value) VALUES (1, 'Value1')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null column */
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNull` (Key) VALUES (2)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE), result.GetIssues().ToString());
        }

        {  /* set NULL to not null column */
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNull` (Key, Value) VALUES (3, NULL)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ReplaceNotNullPg) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestReplaceNotNull` (
                    Key Uint64,
                    Value PgVarchar NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNull` (Key, Value) VALUES (1, 'Value1'pv)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* missing not null column */
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNull` (Key) VALUES (2)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE), result.GetIssues().ToString());
        }

        {  /* set NULL to not null column */
            const auto query = Q_("REPLACE INTO `/Root/TestReplaceNotNull` (Key, Value) VALUES (3, PgCast(NULL, PgVarchar))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UpdateNotNull) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestUpdateNotNull` (
                    Key Uint64,
                    Value String NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* init table */
            const auto query = Q_(R"(
                REPLACE INTO `/Root/TestUpdateNotNull` (Key, Value) VALUES
                    (1, 'Value1'),
                    (2, 'Value2'),
                    (3, 'Value3');
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* update not null column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNull` SET Value = 'NewValue1'");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* update not null column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNull` SET Value = 'NewValue1' WHERE Key = 1");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* set NULL to not null column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNull` SET Value = NULL WHERE Key = 1");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }
    
    Y_UNIT_TEST(UpdateTable_DontChangeNotNull) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            const auto query = Q_(R"(
                CREATE TABLE t
                (
                    id Uint64 NOT NULL,
                    val String,
                    created_on Uint64 NOT NULL,
                    PRIMARY KEY(id)
                );
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        TestUpdateWithoutChangingNotNullColumn(session);
    }

    Y_UNIT_TEST(UpdateTable_DontChangeNotNullWithIndex) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            const auto query = Q_(R"(
                CREATE TABLE t
                (
                    id Uint64 NOT NULL,
                    val String,
                    created_on Uint64 NOT NULL,
                    PRIMARY KEY(id),
                    INDEX Index GLOBAL SYNC ON (id, val)
                );
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        TestUpdateWithoutChangingNotNullColumn(session);
    }

    Y_UNIT_TEST(UpdateTable_UniqIndex) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        CreateTableWithMultishardIndex(kikimr.GetTestClient(), NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            const auto  query = Q_(R"(
                UPSERT INTO `/Root/MultiShardIndexed` (key, fk, fk2, value) VALUES
                (123, 1, 1, 'v1');
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* update without not null column */
            const auto query = Q_("UPDATE `/Root/MultiShardIndexed` SET value = 'a' WHERE key = 123;");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/MultiShardIndexed`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;1u;123u;"a"]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {  /* update on without not null column */
            const auto query = Q_(R"(
                $to_update = (
                    SELECT key, 'b' as value FROM `/Root/MultiShardIndexed`
                    WHERE key = 123
                );
                UPDATE `/Root/MultiShardIndexed` ON SELECT * FROM $to_update;
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/MultiShardIndexed`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;1u;123u;"b"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
         
        { /* same fk */
            const auto  query = Q_(R"(
                UPSERT INTO `/Root/MultiShardIndexed` (key, fk, fk2, value) VALUES
                (124, 1, 1, 'v2');
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }

        {  /* update not null column */
            const auto query = Q_("UPDATE `/Root/MultiShardIndexed` SET value = 'a', fk = NULL WHERE key = 123;");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }

        {  /* update on select */
            const auto query = Q_(R"(
                UPDATE `/Root/MultiShardIndexed` ON SELECT * FROM `/Root/MultiShardIndexed` WHERE key = 123;
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/MultiShardIndexed`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;1u;123u;"b"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateTable_UniqIndexPg) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto tableClient = kikimr.GetTableClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t(
                    id int4 primary key,
                    value int4,
                    label text NOT NULL,
                    label2 text NOT NULL,
                    side int4 NOT NULL,
                    constraint uniq1 unique (value, label),
                    constraint uniq2 unique (label2)
                );
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES (1, 1, 'label1_1', 'label2_1', 1), (2, 2, 'label1_2', 'label2_2', 2);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        { 
            auto result = db.ExecuteQuery(R"(
                UPDATE t SET value = 100 WHERE id = 1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([["1";"100";"label1_1";"label2_1";"1"];["2";"2";"label1_2";"label2_2";"2"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
        { 
            auto result = db.ExecuteQuery(R"(
                UPDATE t SET label2 = 'label2_1' WHERE id = 1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([["1";"100";"label1_1";"label2_1";"1"];["2";"2";"label1_2";"label2_2";"2"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
        { 
            auto result = db.ExecuteQuery(R"(
                UPDATE t SET side = id + 1, label = 'new_label';
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([["1";"100";"new_label";"label2_1";"2"];["2";"2";"new_label";"label2_2";"3"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
        { 
            auto result = db.ExecuteQuery(R"(
                UPDATE t SET value = 100, label = 'new_label' WHERE id = 2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            result = db.ExecuteQuery(R"(
                UPDATE t SET label2 = 'new_label';
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UpdateTable_Immediate) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            const auto query = Q_(R"(
                CREATE TABLE t
                (
                    id Uint64 NOT NULL,
                    val String,
                    created_on Uint64 NOT NULL,
                    PRIMARY KEY(id),
                    INDEX Index GLOBAL SYNC ON (id, val)
                );
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        { 
            const auto query = Q_(R"(
                UPSERT INTO t (id, val, created_on) VALUES 
                (123, 'xxx', 1),
                (124, 'yyy', 2);
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                UPDATE t SET val = 'yyy' WHERE id = 123;
                DELETE FROM t WHERE val = 'yyy';
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            result = session.ExecuteDataQuery(R"(
                SELECT * FROM t;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
        }    
        {
            const auto query = Q_(R"(
                UPSERT INTO t (id, val, created_on) VALUES 
                (123, 'xxx', 1),
                (124, 'yyy', 2);
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const auto query = Q_(R"(
                UPDATE t SET created_on = 11;
                DELETE FROM t WHERE val = 'xxx';
                UPDATE t SET val = 'abc' WHERE created_on = 11;
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            result = session.ExecuteDataQuery(R"(
                SELECT * FROM t;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[11u;124u;["abc"]]])", FormatResultSetYson(result.GetResultSet(0)));
        }    
    }

    Y_UNIT_TEST(UpdateNotNullPg) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestUpdateNotNull` (
                    Key Uint64,
                    Value PgText NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* init table */
            const auto query = Q_(R"(
                REPLACE INTO `/Root/TestUpdateNotNull` (Key, Value) VALUES
                    (1, 'Value1'pt),
                    (2, 'Value2'pt),
                    (3, 'Value3'pt);
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* update not null column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNull` SET Value = 'NewValue1'pt");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* update not null column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNull` SET Value = 'NewValue1'pt WHERE Key = 1");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* set NULL to not null column */
            const auto query = Q_("UPDATE `/Root/TestUpdateNotNull` SET Value = PgCast(NULL, PgText) WHERE Key = 1");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UpdateOnNotNull) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestUpdateOnNotNull` (
                    Key Uint64,
                    Value String NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* init table */
            const auto query = Q_(R"(
                REPLACE INTO `/Root/TestUpdateOnNotNull` (Key, Value) VALUES
                    (1, 'Value1'),
                    (2, 'Value2'),
                    (3, 'Value3');
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* update not null column */
            const auto query = Q_("UPDATE `/Root/TestUpdateOnNotNull` ON (Key, Value) VALUES (2, 'NewValue2')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* set NULL to not null column */
            const auto query = Q_("UPDATE `/Root/TestUpdateOnNotNull` ON (Key, Value) VALUES (2, NULL)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UpdateOnNotNullPg) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestUpdateOnNotNull` (
                    Key Uint64,
                    Value PgText NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* init table */
            const auto query = Q_(R"(
                REPLACE INTO `/Root/TestUpdateOnNotNull` (Key, Value) VALUES
                    (1, 'Value1'pt),
                    (2, 'Value2'pt),
                    (3, 'Value3'pt);
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* update not null column */
            const auto query = Q_("UPDATE `/Root/TestUpdateOnNotNull` ON (Key, Value) VALUES (2, 'NewValue2'pt)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  /* set NULL to not null column */
            const auto query = Q_("UPDATE `/Root/TestUpdateOnNotNull` ON (Key, Value) VALUES (2, PgCast(NULL, PgText))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterAddNotNullColumn) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestAddNotNullColumn` (
                    Key Uint64,
                    Value1 String,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("ALTER TABLE `/Root/TestAddNotNullColumn` ADD COLUMN Value2 String NOT NULL");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterAddNotNullColumnPg) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestAddNotNullColumn` (
                    Key Uint64,
                    Value1 String,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("ALTER TABLE `/Root/TestAddNotNullColumn` ADD COLUMN Value2 PgInt2 NOT NULL");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
    }


    Y_UNIT_TEST(AlterDropNotNullColumn) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestDropNotNullColumn` (
                    Key Uint64,
                    Value1 String,
                    Value2 String NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("ALTER TABLE `/Root/TestDropNotNullColumn` DROP COLUMN Value2");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(FailedMultiEffects) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestNotNull` (
                    Key Uint64 NOT NULL,
                    Value String NOT NULL,
                    PRIMARY KEY (Key))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("REPLACE INTO `/Root/TestNotNull` (Key, Value) VALUES (1, 'Value1')");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                UPDATE `/Root/TestNotNull` SET Value = 'NewValue1' WHERE Key = 1;
                UPSERT INTO `/Root/TestNotNull` (Key, Value) VALUES (2, NULL);
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE), result.GetIssues().ToString());

            auto yson = ReadTablePartToYson(session, "/Root/TestNotNull");
            CompareYson(R"([[[1u];["Value1"]]])", yson);
        }
    }

    Y_UNIT_TEST(CreateIndexedTableWithDisabledNotNullDataColumns) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(false);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q1_(R"(
                CREATE TABLE `/Root/TestCreateIndexedTable` (
                    Key Uint64 NOT NULL,
                    SecondaryKey Uint64,
                    Value String NOT NULL,
                    PRIMARY KEY (Key),
                    INDEX Index GLOBAL ON (SecondaryKey)
                    COVER (Value))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }

        {
            const auto query = Q1_(R"(
                CREATE TABLE `/Root/TestCreateIndexedTable` (
                    Key Uint64 NOT NULL,
                    SecondaryKey Uint64,
                    Value String,
                    PRIMARY KEY (Key),
                    INDEX Index GLOBAL ON (SecondaryKey)
                    COVER (Value))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(SecondaryKeyWithNotNullColumn) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q1_(R"(
                CREATE TABLE `/Root/TestNotNullSecondaryKey` (
                    Key1 Uint64 NOT NULL,
                    Key2 Uint64 NOT NULL,
                    Key3 Uint64,
                    Value String,
                    PRIMARY KEY (Key1),
                    INDEX Index GLOBAL ON (Key2, Key3));
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        /* upsert row and update not null secondary Key2 */
        {
            const auto query = Q_(R"(
                UPSERT INTO `/Root/TestNotNullSecondaryKey` (Key1, Key2, Key3, Value) VALUES (1, 11, 111, 'Value1')
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("UPDATE `/Root/TestNotNullSecondaryKey` SET Key2 = NULL WHERE Key1 = 1");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }

        {
            auto yson = ReadTablePartToYson(session, "/Root/TestNotNullSecondaryKey/Index/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(yson, R"([[[11u];[111u];[1u]]])");
        }

        /* missing not null secondary Key2 */
        {
            const auto query = Q_("INSERT INTO `/Root/TestNotNullSecondaryKey` (Key1) VALUES (2)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_("UPSERT INTO `/Root/TestNotNullSecondaryKey` (Key1) VALUES (3)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_("REPLACE INTO `/Root/TestNotNullSecondaryKey` (Key1) VALUES (4)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE),
                result.GetIssues().ToString());
        }

        /* set NULL to not null secondary Key2 */
        {
            const auto query = Q_(R"(
                INSERT INTO `/Root/TestNotNullSecondaryKey` (Key1, Key2, Value) VALUES (5, NULL, 'Value5')
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                UPSERT INTO `/Root/TestNotNullSecondaryKey` (Key1, Key2, Value) VALUES (6, NULL, 'Value6')
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                REPLACE INTO `/Root/TestNotNullSecondaryKey` (Key1, Key2, Value) VALUES (7, NULL, 'Value7')
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(SecondaryIndexWithNotNullDataColumn) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q1_(R"(
                CREATE TABLE `/Root/TestNotNullSecondaryIndex` (
                    Key Uint64 NOT NULL,
                    Value String,
                    Index1 String NOT NULL,
                    Index2 String,
                    PRIMARY KEY (Key),
                    INDEX Index GLOBAL ON (Index1, Index2)
                    COVER (Value));
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        /* upsert row and update not null index column Index1 */
        {
            const auto query = Q_(R"(
                UPSERT INTO `/Root/TestNotNullSecondaryIndex` (Key, Index1, Index2)
                    VALUES (1, 'Secondary1', 'Secondary11')
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("UPDATE `/Root/TestNotNullSecondaryIndex` SET Index1 = NULL WHERE Key = 1");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }

        {
            auto yson = ReadTablePartToYson(session, "/Root/TestNotNullSecondaryIndex/Index/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(yson, R"([[["Secondary1"];["Secondary11"];[1u];#]])");
        }

        /* missing not null index column Index1 */
        {
            const auto query = Q_("INSERT INTO `/Root/TestNotNullSecondaryIndex` (Key) VALUES (2)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_("UPSERT INTO `/Root/TestNotNullSecondaryIndex` (Key) VALUES (3)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_("REPLACE INTO `/Root/TestNotNullSecondaryIndex` (Key) VALUES (4)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE),
                result.GetIssues().ToString());
        }

        /* set NULL to not null index column Index1 */
        {
            const auto query = Q_(R"(
                INSERT INTO `/Root/TestNotNullSecondaryIndex` (Key, Value, Index1) VALUES (5, 'Value5', NULL)
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                UPSERT INTO `/Root/TestNotNullSecondaryIndex` (Key, Value, Index1) VALUES (6, 'Value6', NULL)
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                REPLACE INTO `/Root/TestNotNullSecondaryIndex` (Key, Value, Index1) VALUES (7, 'Value7', NULL)
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }
    }

    // TODO: fix TxPlanSerializer with PG keys
    Y_UNIT_TEST(SecondaryIndexWithNotNullDataColumnPg) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q1_(R"(
                CREATE TABLE `/Root/TestNotNullSecondaryIndex` (
                    Key PgInt2 NOT NULL,
                    Value String,
                    Index1 PgText NOT NULL,
                    Index2 String,
                    PRIMARY KEY (Key),
                    INDEX Index GLOBAL ON (Index1, Index2)
                    COVER (Value));
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        /* upsert row and update not null index column Index1 */
        {
            const auto query = Q_(R"(
                UPSERT INTO `/Root/TestNotNullSecondaryIndex` (Key, Index1, Index2)
                    VALUES (PgInt2(1), 'Secondary1'pt, 'Secondary11')
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("UPDATE `/Root/TestNotNullSecondaryIndex` SET Index1 = PgCast(NULL, PgText) WHERE Key = PgInt2(1)");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }

        {
            auto yson = ReadTablePartToYson(session, "/Root/TestNotNullSecondaryIndex/Index/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(yson, R"([["Secondary1";["Secondary11"];"1";#]])");
        }

        /* missing not null index column Index1 */
        {
            const auto query = Q_("INSERT INTO `/Root/TestNotNullSecondaryIndex` (Key) VALUES (PgInt2(2))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_("UPSERT INTO `/Root/TestNotNullSecondaryIndex` (Key) VALUES (PgInt2(3))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_("REPLACE INTO `/Root/TestNotNullSecondaryIndex` (Key) VALUES (PgInt2(4))");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE),
                result.GetIssues().ToString());
        }

        /* set NULL to not null index column Index1 */
        {
            const auto query = Q_(R"(
                INSERT INTO `/Root/TestNotNullSecondaryIndex` (Key, Value, Index1) VALUES (PgInt2(5), 'Value5', PgCast(NULL, PgText))
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                UPSERT INTO `/Root/TestNotNullSecondaryIndex` (Key, Value, Index1) VALUES (PgInt2(6), 'Value6', PgCast(NULL, PgText))
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                REPLACE INTO `/Root/TestNotNullSecondaryIndex` (Key, Value, Index1) VALUES (PgInt2(7), 'Value7', PgCast(NULL, PgText))
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE),
                result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_TWIN(JoinBothTablesWithNotNullPk, StreamLookup) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookup);
        auto settings = TKikimrSettings().SetAppConfig(appConfig);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            auto createTableResult = session.ExecuteSchemeQuery(Q1_(R"(
                CREATE TABLE `/Root/Left` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )")).ExtractValueSync();
            UNIT_ASSERT_C(createTableResult.IsSuccess(), createTableResult.GetIssues().ToString());

            auto result = session.ExecuteDataQuery(Q1_(R"(
                UPSERT INTO `/Root/Left` (Key, Value) VALUES (1, 'lValue1'), (2, 'lValue2');
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto createTableResult = session.ExecuteSchemeQuery(Q1_(R"(
                CREATE TABLE `/Root/Right` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )")).ExtractValueSync();
            UNIT_ASSERT_C(createTableResult.IsSuccess(), createTableResult.GetIssues().ToString());

            auto result = session.ExecuteDataQuery(Q1_(R"(
                UPSERT INTO `/Root/Right` (Key, Value) VALUES (1, 'rValue1'), (3, 'rValue3');
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = Q1_(R"(
                SELECT l.Value, r.Value FROM `/Root/Left` AS l JOIN `/Root/Right` AS r ON l.Key = r.Key;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[["lValue1"];["rValue1"]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST_TWIN(JoinLeftTableWithNotNullPk, StreamLookup) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookup);
        auto settings = TKikimrSettings().SetAppConfig(appConfig);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            auto createTableResult = session.ExecuteSchemeQuery(Q1_(R"(
                CREATE TABLE `/Root/Left` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )")).ExtractValueSync();
            UNIT_ASSERT_C(createTableResult.IsSuccess(), createTableResult.GetIssues().ToString());

            auto result = session.ExecuteDataQuery(Q1_(R"(
                UPSERT INTO `/Root/Left` (Key, Value) VALUES (1, 'lValue1'), (2, 'lValue2');
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto createTableResult = session.ExecuteSchemeQuery(Q1_(R"(
                CREATE TABLE `/Root/Right` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )")).ExtractValueSync();
            UNIT_ASSERT_C(createTableResult.IsSuccess(), createTableResult.GetIssues().ToString());

            auto result = session.ExecuteDataQuery(Q1_(R"(
                UPSERT INTO `/Root/Right` (Key, Value) VALUES (1, 'rValue1'), (3, 'rValue3'), (NULL, 'rValue');
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {  // inner
            const auto query = Q1_(R"(
                SELECT l.Value, r.Value FROM `/Root/Left` AS l JOIN `/Root/Right` AS r ON l.Key = r.Key;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[["lValue1"];["rValue1"]]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {  // left
            const auto query = Q1_(R"(
                SELECT l.Value, r.Value FROM `/Root/Left` AS l LEFT JOIN `/Root/Right` AS r ON l.Key = r.Key ORDER BY l.Value;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[["lValue1"];["rValue1"]];[["lValue2"];#]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {  // right
            const auto query = Q1_(R"(
                SELECT r.Value, l.Value FROM `/Root/Left` AS l RIGHT JOIN `/Root/Right` AS r ON l.Key = r.Key ORDER BY r.Value;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[["rValue"];#];[["rValue1"];["lValue1"]];[["rValue3"];#]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST_TWIN(JoinRightTableWithNotNullColumns, StreamLookup) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookup);
        auto settings = TKikimrSettings().SetAppConfig(appConfig);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            auto createTableResult = session.ExecuteSchemeQuery(Q1_(R"(
                CREATE TABLE `/Root/Left` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )")).ExtractValueSync();
            UNIT_ASSERT_C(createTableResult.IsSuccess(), createTableResult.GetIssues().ToString());

            auto result = session.ExecuteDataQuery(Q1_(R"(
                UPSERT INTO `/Root/Left` (Key, Value) VALUES (1, 'lValue1'), (2, 'lValue2');
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto createTableResult = session.ExecuteSchemeQuery(Q1_(R"(
                CREATE TABLE `/Root/Right` (
                    Key Uint64 NOT NULL,
                    Value String NOT NULL,
                    PRIMARY KEY (Key)
                );
            )")).ExtractValueSync();
            UNIT_ASSERT_C(createTableResult.IsSuccess(), createTableResult.GetIssues().ToString());

            auto result = session.ExecuteDataQuery(Q1_(R"(
                UPSERT INTO `/Root/Right` (Key, Value) VALUES (1, 'rValue1'), (3, 'rValue3'), (4, 'rValue4');
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {  // left join
            const auto query = Q1_(R"(
                SELECT l.Key, l.Value, r.Key, r.Value FROM `/Root/Left` AS l LEFT
                    JOIN `/Root/Right` AS r
                    ON l.Key = r.Key
                    ORDER BY l.Key;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["lValue1"];[1u];["rValue1"]];
                [[2u];["lValue2"];#;#]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(Describe) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        auto createTableResult = session.ExecuteSchemeQuery(Q1_(R"(
            CREATE TABLE `/Root/DescribeTest` (
                Key1 Uint64 NOT NULL,
                Key2 Uint64,
                Value1 String,
                Value2 PgInt2,
                Value3 PgInt2 NOT NULL,
                PRIMARY KEY (Key1, Key2)
            );
        )")).ExtractValueSync();
        UNIT_ASSERT_C(createTableResult.IsSuccess(), createTableResult.GetIssues().ToString());

        auto describeTableResult = session.DescribeTable("/Root/DescribeTest").GetValueSync();
        UNIT_ASSERT_C(describeTableResult.IsSuccess(), describeTableResult.GetIssues().ToString());

        const THashMap<std::string_view, std::string_view> columnTypes = {
            {"Key1", "Uint64"},
            {"Key2", "Uint64?"},
            {"Value1", "String?"},
            {"Value2", "Pg('pgint2','',21,0,0)"},
            {"Value3", "Pg('pgint2','',21,0,0)"}
        };

        const THashMap<std::string_view, bool> columnNonNullability = {
            {"Key1", false},
            {"Key2", false},
            {"Value1", false},
            {"Value2", false},
            {"Value3", true}
        };

        const auto& columns = describeTableResult.GetTableDescription().GetTableColumns();
        for (const auto& column : columns) {
            UNIT_ASSERT_VALUES_EQUAL_C(column.Type.ToString(), columnTypes.at(column.Name), column.Name);
            bool isNotNull = columnNonNullability.at(column.Name);
            if (isNotNull) {
                UNIT_ASSERT_VALUES_EQUAL_C(column.NotNull.value(), true, column.Name);
            } else {
                UNIT_ASSERT_C(!column.NotNull.has_value() || !column.NotNull.value(), column.Name);
            }
        }
    }

    Y_UNIT_TEST(CreateTableWithNotNullColumns) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            TTableBuilder builder;
            builder.AddNullableColumn("1", EPrimitiveType::Uint64);
            builder.AddNonNullableColumn("2", EPrimitiveType::Uint64);
            builder.AddNullableColumn("3", TPgType("pgint2"));
            builder.AddNonNullableColumn("4", TPgType("pgint2"));
            builder.SetPrimaryKeyColumn("1");
            auto result = session.CreateTable("/Root/NotNullCheck", builder.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        auto describeTableResult = session.DescribeTable("/Root/NotNullCheck").GetValueSync();
        UNIT_ASSERT_C(describeTableResult.IsSuccess(), describeTableResult.GetIssues().ToString());
        const THashMap<std::string_view, bool> columnNonNullability = {
            {"1", false},
            {"2", false},
            {"3", false},
            {"4", true},
        };

        const auto& columns = describeTableResult.GetTableDescription().GetTableColumns();
        for (const auto& column : columns) {
            bool isNotNull = columnNonNullability.at(column.Name);
            if (isNotNull) {
                UNIT_ASSERT_VALUES_EQUAL_C(column.NotNull.value(), true, column.Name);
            } else {
                UNIT_ASSERT_C(!column.NotNull.has_value() || !column.NotNull.value(), column.Name);
            }
        }

        {
            auto proto = NYdb::TProtoAccessor::GetProto(describeTableResult.GetTableDescription());
            proto.mutable_columns()->begin()->set_not_null(true);
            auto result = session.CreateTable("/Root/NotNullCheck2", TTableDescription(std::move(proto), {})).GetValueSync();
            UNIT_ASSERT_C(!result.GetIssues().Empty(), "ok with faulty protobuf");
            UNIT_ASSERT(result.GetIssues().ToString().Contains("Error: Not consistent column type and not_null option for column: 1"));
        }
    }

    Y_UNIT_TEST(AlterAddIndex) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        auto server = &kikimr.GetTestServer();

        {
            auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/TestTable` (
                    Key1 Int64 NOT NULL,
                    Key2 Utf8 NOT NULL,
                    Key3 PgInt2 NOT NULL,
                    Value1 Utf8,
                    Value2 Bool,
                    PRIMARY KEY (Key1, Key2, Key3));
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteSchemeQuery(R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX Index GLOBAL SYNC ON (Key2, Value1, Value2);
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto describeTable = [&server](const TString& path) {
            auto& runtime = *server->GetRuntime();
            auto sender = runtime.AllocateEdgeActor();
            TAutoPtr<IEventHandle> handle;

            auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
            request->Record.MutableDescribePath()->SetPath(path);
            request->Record.MutableDescribePath()->MutableOptions()->SetShowPrivateTable(true);
            runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(handle);

            return *reply->MutableRecord();
        };

        auto extractNotNullColumns = [](const auto& desc) {
            THashSet<TString> notNullColumns;
            for (const auto& column : desc.GetPathDescription().GetTable().GetColumns()) {
                if (column.GetNotNull()) {
                    notNullColumns.insert(column.GetName());
                }
            }

            return notNullColumns;
        };

        {
            auto mainTableNotNullColumns = extractNotNullColumns(describeTable("/Root/TestTable"));
            auto indexTableNotNullColumns = extractNotNullColumns(describeTable("/Root/TestTable/Index/indexImplTable"));
            UNIT_ASSERT_VALUES_EQUAL_C(mainTableNotNullColumns, indexTableNotNullColumns, "Not null columns mismatch");
        }
    }

    Y_UNIT_TEST(OptionalParametersDataQuery) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/TestTable` (
                    Key1 Utf8 NOT NULL,
                    Key2  Int32 NOT NULL,
                    Value1 Utf8,
                    Value2 Utf8,
                    PRIMARY KEY (Key1, Key2));
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                UPSERT INTO `/Root/TestTable` (Key1, Key2, Value1, Value2) VALUES
                    ('One', 1, NULL, NULL),
                    ('Two', 2, 'Value2', 'Value20'),
                    ('Three', 3, NULL, 'Value30');
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  // select by full pk
            const auto query = Q1_(R"(
                DECLARE $key1 AS Utf8?;
                DECLARE $key2 AS Int32?;

                SELECT a.Key1, a.Key2, a.Value1, a.Value2
                    FROM TestTable AS a
                    WHERE a.Key1 = $key1 AND a.Key2 = $key2;
            )");

            auto params = TParamsBuilder()
                .AddParam("$key1").OptionalUtf8("One").Build()
                .AddParam("$key2").OptionalInt32(1).Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([["One";1;#;#]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {  // select by key1
            const auto query = Q1_(R"(
                DECLARE $key1 AS Utf8?;

                SELECT a.Key1, a.Key2, a.Value1, a.Value2
                    FROM TestTable AS a
                    WHERE a.Key1 = $key1;
            )");

            auto params = TParamsBuilder()
                .AddParam("$key1").OptionalUtf8("Two").Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([["Two";2;["Value2"];["Value20"]]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {  // select by key2
            const auto query = Q1_(R"(
                DECLARE $key2 AS Int32?;

                SELECT a.Key1, a.Key2, a.Value1, a.Value2
                    FROM TestTable AS a
                    WHERE a.Key2 = $key2;
            )");

            auto params = TParamsBuilder()
                .AddParam("$key2").OptionalInt32(3).Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([["Three";3;#;["Value30"]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(OptionalParametersScanQuery) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/TestTable` (
                    Key1 Utf8 NOT NULL,
                    Key2  Int32 NOT NULL,
                    Value1 Utf8,
                    Value2 Utf8,
                    PRIMARY KEY (Key1, Key2));
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                UPSERT INTO `/Root/TestTable` (Key1, Key2, Value1, Value2) VALUES
                    ('One', 1, NULL, NULL),
                    ('Two', 2, 'Value2', 'Value20'),
                    ('Three', 3, NULL, 'Value30');
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {  // select by full pk
            const auto query = Q1_(R"(
                DECLARE $key1 AS Utf8?;
                DECLARE $key2 AS Int32?;

                SELECT a.Key1, a.Key2, a.Value1, a.Value2
                    FROM TestTable AS a
                    WHERE a.Key1 = $key1 AND a.Key2 = $key2;
            )");

            auto params = TParamsBuilder()
                .AddParam("$key1").OptionalUtf8("One").Build()
                .AddParam("$key2").OptionalInt32(1).Build()
                .Build();

            auto it = client.StreamExecuteScanQuery(query, std::move(params)).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            CompareYson(R"([["One";1;#;#]])", StreamResultToYson(it));
        }

        {  // select by key1
            const auto query = Q1_(R"(
                DECLARE $key1 AS Utf8?;

                SELECT a.Key1, a.Key2, a.Value1, a.Value2
                    FROM TestTable AS a
                    WHERE a.Key1 = $key1;
            )");

            auto params = TParamsBuilder()
                .AddParam("$key1").OptionalUtf8("Two").Build()
                .Build();

            auto it = client.StreamExecuteScanQuery(query, std::move(params)).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            CompareYson(R"([["Two";2;["Value2"];["Value20"]]])", StreamResultToYson(it));
        }

        {  // select by key2
            const auto query = Q1_(R"(
                DECLARE $key2 AS Int32?;

                SELECT a.Key1, a.Key2, a.Value1, a.Value2
                    FROM TestTable AS a
                    WHERE a.Key2 = $key2;
            )");

            auto params = TParamsBuilder()
                .AddParam("$key2").OptionalInt32(3).Build()
                .Build();

            auto it = client.StreamExecuteScanQuery(query, std::move(params)).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            CompareYson(R"([["Three";3;#;["Value30"]]])", StreamResultToYson(it));
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
