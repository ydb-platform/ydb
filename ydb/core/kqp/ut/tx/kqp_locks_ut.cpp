#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpLocks) {
    Y_UNIT_TEST(Invalidate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();

        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/Test`
            SELECT Group + 10U AS Group, Name, Amount, Comment ?? "" || "Updated" AS Comment
            FROM `/Root/Test`
            WHERE Group == 1U AND Name == "Paul";
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto tx1 = result.GetTransaction();
        UNIT_ASSERT(tx1);

        result = session2.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/Test` (Group, Name, Comment)
            VALUES (1U, "Paul", "Changed");
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session1.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/Test` (Group, Name, Comment)
            VALUES (11U, "Sergey", "BadRow");
        )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
            [] (const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("/Root/Test");
            }));

        result = session2.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[300u];["Changed"];[1u];["Paul"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InvalidateOnCommit) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();

        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/Test`
            SELECT Group + 10U AS Group, Name, Amount, Comment ?? "" || "Updated" AS Comment
            FROM `/Root/Test`
            WHERE Group == 1U AND Name == "Paul";
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto tx1 = result.GetTransaction();
        UNIT_ASSERT(tx1);

        result = session2.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/Test` (Group, Name, Comment)
            VALUES (1U, "Paul", "Changed");
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto commitResult = tx1->Commit().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::ABORTED, commitResult.GetIssues().ToString());
        commitResult.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(HasIssue(commitResult.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
            [] (const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("/Root/Test");
            }));

        result = session2.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[300u];["Changed"];[1u];["Paul"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(DifferentKeyUpdate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();

        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Group = 1;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto tx1 = result.GetTransaction();
        UNIT_ASSERT(tx1);

        result = session2.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/Test` (Group, Name, Comment)
            VALUES (2U, "Paul", "Changed");
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        result = session1.ExecuteDataQuery(Q_(R"(
            SELECT "Nothing";
        )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
    }

    Y_UNIT_TEST(EmptyRange) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();

        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM Test WHERE Group = 11;
        )"), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

        auto tx1 = result.GetTransaction();
        UNIT_ASSERT(tx1);

        result = session2.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM Test WHERE Group = 11;

            UPSERT INTO Test (Group, Name, Amount) VALUES
                (11, "Session2", 2);
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

        result = session1.ExecuteDataQuery(Q1_(R"(
            UPSERT INTO Test (Group, Name, Amount) VALUES
                (11, "Session1", 1);
        )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
            [] (const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("/Root/Test");
            }));

        result = session1.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM Test WHERE Group = 11;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[2u];#;[11u];["Session2"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(EmptyRangeAlreadyBroken) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();

        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM Test WHERE Group = 10;
        )"), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

        auto tx1 = result.GetTransaction();
        UNIT_ASSERT(tx1);

        result = session2.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM Test WHERE Group = 11;

            UPSERT INTO Test (Group, Name, Amount) VALUES
                (11, "Session2", 2);
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

        result = session1.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM Test WHERE Group = 11;

            UPSERT INTO Test (Group, Name, Amount) VALUES
                (11, "Session1", 1);
        )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED));

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
            [] (const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("/Root/Test");
            }));

        result = session1.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM Test WHERE Group = 11;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[2u];#;[11u];["Session2"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(TwoPhaseTx) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();

        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(Q_(R"(
            REPLACE INTO `/Root/Test` (Group, Name, Comment) VALUES (1U, "Paul", "Changed");
            SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto tx1 = result.GetTransaction();
        UNIT_ASSERT(tx1);

        result = session2.ExecuteDataQuery(Q_(R"(
            REPLACE INTO `/Root/Test` (Group, Name, Comment)
            VALUES (1U, "Paul", "Changed");
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session1.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `KeyValue`;
        )"), TTxControl::Tx(*tx1)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto commitResult = tx1->Commit().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        commitResult.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_C(HasIssue(commitResult.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
            [] (const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("/Root/Test");
            }), commitResult.GetIssues().ToString());
    }

    Y_UNIT_TEST(MixedTxFail) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        appConfig.MutableTableServiceConfig()->SetEnableHtapTx(true);
        auto settings = TKikimrSettings().SetAppConfig(appConfig).SetWithSampleTables(false);

        auto kikimr = std::make_unique<TKikimrRunner>(settings);

        auto client = kikimr->GetQueryClient();

        {
            auto createTable = client.ExecuteQuery(R"sql(
                CREATE TABLE `/Root/DataShard` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32 NOT NULL,
                    Col3 String,
                    PRIMARY KEY (Col1, Col2)
                ) WITH (STORE = ROW);
                CREATE TABLE `/Root/ColumnShard` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32 NOT NULL,
                    Col3 String,
                    PRIMARY KEY (Col1, Col2)
                ) WITH (STORE = COLUMN);
            )sql", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(createTable.IsSuccess(), createTable.GetIssues().ToString());
        }

        {
            auto replaceValues = client.ExecuteQuery(R"sql(
                REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES
                    (1u, 1, "row"), (1u, 2, "row"), (1u, 3, "row"), (2u, 3, "row");
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(replaceValues.IsSuccess(), replaceValues.GetIssues().ToString());
        }
        {
            auto replaceValues = client.ExecuteQuery(R"sql(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2, Col3) VALUES
                    (1u, 1, "row"), (1u, 2, "row"), (1u, 3, "row"), (2u, 3, "row");
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(replaceValues.IsSuccess(), replaceValues.GetIssues().ToString());
        }

        auto session = client.GetSession().GetValueSync().GetSession();
        auto writeSession = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"sql(
            SELECT * FROM `/Root/DataShard`;
        )sql", NYdb::NQuery::TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto tx = result.GetTransaction();

        result = writeSession.ExecuteQuery(R"sql(
            INSERT INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES (2u, 1, "test");
        )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteQuery(R"sql(
            INSERT INTO `/Root/ColumnShard` (Col1, Col2, Col3) VALUES (2u, 1, "test");
        )sql", NYdb::NQuery::TTxControl::Tx(tx->GetId()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Transaction locks invalidated. Table: `/Root/DataShard`", result.GetIssues().ToString());
    }
}

} // namespace NKqp
} // namespace NKikimr
