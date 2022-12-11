#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {
    void CreateTestTable(TSession& session) {
        AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            CREATE TABLE TestImmediateEffects (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync());

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            INSERT INTO TestImmediateEffects (Key, Value) VALUES
                (1u, "One"),
                (2u, "Two");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}  // namespase

Y_UNIT_TEST_SUITE(KqpImmediateEffects) {
    Y_UNIT_TEST(Upsert) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT * FROM TestImmediateEffects;
                UPSERT INTO TestImmediateEffects (Key, Value) VALUES
                    (3u, "Three"),
                    (4u, "Four");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]];
                [[3u];["Three"]];
                [[4u];["Four"]]
            ])", FormatResultSetYson(result.GetResultSet(1)));
        }

        {  // multiple effects
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                UPSERT INTO TestImmediateEffects (Key, Value) VALUES (5u, "Five");
                UPSERT INTO TestImmediateEffects (Key, Value) VALUES (6u, "Six");
                UPSERT INTO TestImmediateEffects (Key, Value) VALUES (7u, "Seven");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]];
                [[3u];["Three"]];
                [[4u];["Four"]];
                [[5u];["Five"]];
                [[6u];["Six"]];
                [[7u];["Seven"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpsertDuplicates) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT * FROM TestImmediateEffects;
                UPSERT INTO TestImmediateEffects (Key, Value) VALUES
                    (3u, "SomeValue1"),
                    (3u, "SomeValue2");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]];
                [[3u];["SomeValue2"]]
            ])", FormatResultSetYson(result.GetResultSet(1)));
        }
    }

    Y_UNIT_TEST(UpsertExistingKey) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT * FROM TestImmediateEffects;
                UPSERT INTO TestImmediateEffects (Key, Value) VALUES (1u, "SomeValue1");
                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([
                [[1u];["SomeValue1"]];
                [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(1)));
        }

        {  // multiple effects
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                UPSERT INTO TestImmediateEffects (Key, Value) VALUES (1u, "SomeValue11");
                UPSERT INTO TestImmediateEffects (Key, Value) VALUES (2u, "SomeValue2");
                UPSERT INTO TestImmediateEffects (Key, Value) VALUES (2u, "SomeValue22");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["SomeValue11"]];
                [[2u];["SomeValue22"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(Replace) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT * FROM TestImmediateEffects;
                REPLACE INTO TestImmediateEffects (Key, Value) VALUES
                    (3u, "Three"),
                    (4u, "Four");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]];
                [[3u];["Three"]];
                [[4u];["Four"]]
            ])", FormatResultSetYson(result.GetResultSet(1)));
        }

        {  // multiple effects
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                REPLACE INTO TestImmediateEffects (Key, Value) VALUES (5u, "Five");
                REPLACE INTO TestImmediateEffects (Key, Value) VALUES (6u, "Six");
                REPLACE INTO TestImmediateEffects (Key, Value) VALUES (7u, "Seven");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]];
                [[3u];["Three"]];
                [[4u];["Four"]];
                [[5u];["Five"]];
                [[6u];["Six"]];
                [[7u];["Seven"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ReplaceDuplicates) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT * FROM TestImmediateEffects;
                REPLACE INTO TestImmediateEffects (Key, Value) VALUES
                    (3u, "SomeValue1"),
                    (3u, "SomeValue2");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]];
                [[3u];["SomeValue2"]]
            ])", FormatResultSetYson(result.GetResultSet(1)));
        }
    }

    Y_UNIT_TEST(ReplaceExistingKey) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT * FROM TestImmediateEffects;
                REPLACE INTO TestImmediateEffects (Key, Value) VALUES (1u, "SomeValue1");
                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([
                [[1u];["SomeValue1"]];
                [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(1)));
        }

        {  // multiple effects
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                REPLACE INTO TestImmediateEffects (Key, Value) VALUES (1u, "SomeValue11");
                REPLACE INTO TestImmediateEffects (Key, Value) VALUES (2u, "SomeValue2");
                REPLACE INTO TestImmediateEffects (Key, Value) VALUES (2u, "SomeValue22");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["SomeValue11"]];
                [[2u];["SomeValue22"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(Insert) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT * FROM TestImmediateEffects;
                INSERT INTO TestImmediateEffects (Key, Value) VALUES
                    (3u, "Three"),
                    (4u, "Four");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]];
                [[3u];["Three"]];
                [[4u];["Four"]]
            ])", FormatResultSetYson(result.GetResultSet(1)));
        }

        {  // multiple effects
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                INSERT INTO TestImmediateEffects (Key, Value) VALUES (5u, "Five");
                INSERT INTO TestImmediateEffects (Key, Value) VALUES (6u, "Six");
                INSERT INTO TestImmediateEffects (Key, Value) VALUES (7u, "Seven");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]];
                [[3u];["Three"]];
                [[4u];["Four"]];
                [[5u];["Five"]];
                [[6u];["Six"]];
                [[7u];["Seven"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertDuplicates) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT * FROM TestImmediateEffects;
                INSERT INTO TestImmediateEffects (Key, Value) VALUES
                    (3u, "Three"),
                    (3u, "SomeValue");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("Duplicated keys found.");
            }));
        }
    }

    Y_UNIT_TEST(InsertExistingKey) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT * FROM TestImmediateEffects;
                INSERT INTO TestImmediateEffects (Key, Value) VALUES
                    (2u, "SomeValue");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("Conflict with existing key.");
            }));
        }
    }

    Y_UNIT_TEST(UpdateOn) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT * FROM TestImmediateEffects;
                UPDATE TestImmediateEffects ON (Key, Value) VALUES
                    (1u, "Updated1"),
                    (2u, "Updated2");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([
                [[1u];["Updated1"]];
                [[2u];["Updated2"]]
            ])", FormatResultSetYson(result.GetResultSet(1)));
        }

        {  // multiple effects
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                UPDATE TestImmediateEffects ON (Key, Value) VALUES
                    (1u, "Updated3"),
                    (2u, "Updated4");

                UPDATE TestImmediateEffects ON (Key, Value) VALUES
                    (1u, "Updated5");

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["Updated5"]];
                [[2u];["Updated4"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(Delete) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                SELECT * FROM TestImmediateEffects;
                DELETE FROM TestImmediateEffects WHERE Key = 2;

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[1u];["One"]];
                [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([
                [[1u];["One"]]
            ])", FormatResultSetYson(result.GetResultSet(1)));
        }

        {  // multiple effects
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                UPSERT INTO TestImmediateEffects (Key, Value) VALUES
                    (3u, "Three"),
                    (4u, "Four");

                DELETE FROM TestImmediateEffects WHERE Key > 3;
                DELETE FROM TestImmediateEffects WHERE Key < 3;

                SELECT * FROM TestImmediateEffects;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[3u];["Three"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateAfterUpsert) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            UPSERT INTO TestImmediateEffects (Key, Value) VALUES (3u, "Three");
            UPSERT INTO TestImmediateEffects (Key, Value) VALUES (4u, "Four");

            UPDATE TestImmediateEffects SET Value = "Updated2" WHERE Key = 2;
            UPDATE TestImmediateEffects SET Value = "Updated3" WHERE Key = 3;

            SELECT * FROM TestImmediateEffects;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"]];
            [[2u];["Updated2"]];
            [[3u];["Updated3"]];
            [[4u];["Four"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(DeleteAfterUpsert) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            UPSERT INTO TestImmediateEffects (Key, Value) VALUES (3u, "Three");
            UPSERT INTO TestImmediateEffects (Key, Value) VALUES (4u, "Four");

            DELETE FROM TestImmediateEffects WHERE Key = 2;
            DELETE FROM TestImmediateEffects WHERE Key = 3;

            SELECT * FROM TestImmediateEffects;

            UPSERT INTO TestImmediateEffects (Key, Value) VALUES (2u, "Value2");
            UPSERT INTO TestImmediateEffects (Key, Value) VALUES (3u, "Value3");

            SELECT * FROM TestImmediateEffects;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"]];
            [[4u];["Four"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[1u];["One"]];
            [[2u];["Value2"]];
            [[3u];["Value3"]];
            [[4u];["Four"]]
        ])", FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST(UpdateAfterInsert) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            INSERT INTO TestImmediateEffects (Key, Value) VALUES (3u, "Three");
            INSERT INTO TestImmediateEffects (Key, Value) VALUES (4u, "Four");

            UPDATE TestImmediateEffects SET Value = "Updated2" WHERE Key = 2;
            UPDATE TestImmediateEffects SET Value = "Updated3" WHERE Key = 3;

            SELECT * FROM TestImmediateEffects;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"]];
            [[2u];["Updated2"]];
            [[3u];["Updated3"]];
            [[4u];["Four"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(DeleteAfterInsert) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpImmediateEffects(true);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateTestTable(session);


        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            INSERT INTO TestImmediateEffects (Key, Value) VALUES (3u, "Three");
            INSERT INTO TestImmediateEffects (Key, Value) VALUES (4u, "Four");

            DELETE FROM TestImmediateEffects WHERE Key = 2;
            DELETE FROM TestImmediateEffects WHERE Key = 3;

            SELECT * FROM TestImmediateEffects;

            INSERT INTO TestImmediateEffects (Key, Value) VALUES (2u, "Two");
            INSERT INTO TestImmediateEffects (Key, Value) VALUES (3u, "Three");

            SELECT * FROM TestImmediateEffects;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"]];
            [[4u];["Four"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[1u];["One"]];
            [[2u];["Two"]];
            [[3u];["Three"]];
            [[4u];["Four"]]
        ])", FormatResultSetYson(result.GetResultSet(1)));
    }
}

} // namespace NKqp
} // namespace NKikimr