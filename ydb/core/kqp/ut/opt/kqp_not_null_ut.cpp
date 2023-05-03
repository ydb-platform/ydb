#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpNotNullColumns) {
    Y_UNIT_TEST(CreateTableWithDisabledNotNullDataColumns) {
        TKikimrRunner kikimr;
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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
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
        TKikimrRunner kikimr;
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

    Y_UNIT_TEST(UpsertNotNullPk) {
        TKikimrRunner kikimr;
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

    Y_UNIT_TEST(ReplaceNotNullPk) {
        TKikimrRunner kikimr;
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

    Y_UNIT_TEST(UpdateNotNullPk) {
        TKikimrRunner kikimr;
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

    Y_UNIT_TEST(SelectNotNullColumns) {
        auto settings = TKikimrSettings()
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

    Y_UNIT_TEST(UpsertNotNull) {
        auto settings = TKikimrSettings()
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

    Y_UNIT_TEST(ReplaceNotNull) {
        auto settings = TKikimrSettings()
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

    Y_UNIT_TEST(UpdateNotNull) {
        auto settings = TKikimrSettings()
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

    Y_UNIT_TEST(UpdateOnNotNull) {
        auto settings = TKikimrSettings()
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

    Y_UNIT_TEST(AlterDropNotNullColumn) {
        auto settings = TKikimrSettings()
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
        TKikimrRunner kikimr;
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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
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

    Y_UNIT_TEST(JoinBothTablesWithNotNullPk) {
        TKikimrRunner kikimr;
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

    Y_UNIT_TEST(JoinLeftTableWithNotNullPk) {
        TKikimrRunner kikimr;
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

    Y_UNIT_TEST(Describe) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        auto createTableResult = session.ExecuteSchemeQuery(Q1_(R"(
            CREATE TABLE `/Root/DescribeTest` (
                Key1 Uint64 NOT NULL,
                Key2 Uint64,
                Value String,
                PRIMARY KEY (Key1, Key2)
            );
        )")).ExtractValueSync();
        UNIT_ASSERT_C(createTableResult.IsSuccess(), createTableResult.GetIssues().ToString());

        auto describeTableResult = session.DescribeTable("/Root/DescribeTest").GetValueSync();
        UNIT_ASSERT_C(describeTableResult.IsSuccess(), describeTableResult.GetIssues().ToString());

        const THashMap<std::string_view, std::string_view> columnTypes = {
            {"Key1", "Uint64"},
            {"Key2", "Uint64?"},
            {"Value", "String?"}
        };

        const auto& columns = describeTableResult.GetTableDescription().GetTableColumns();
        for (const auto& column : columns) {
            UNIT_ASSERT_VALUES_EQUAL(column.Type.ToString(), columnTypes.at(column.Name));
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
                    Value1 Utf8,
                    Value2 Bool,
                    PRIMARY KEY (Key1, Key2));
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
}

} // namespace NKqp
} // namespace NKikimr
