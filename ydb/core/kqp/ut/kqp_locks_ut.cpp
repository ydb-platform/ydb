#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpLocks) {
    Y_UNIT_TEST_NEW_ENGINE(Invalidate) {
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
                return issue.Message.Contains("/Root/Test");
            }));

        result = session2.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[300u];["Changed"];[1u];["Paul"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST_NEW_ENGINE(InvalidateOnCommit) {
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
                return issue.Message.Contains("/Root/Test");
            }));

        result = session2.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[300u];["Changed"];[1u];["Paul"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST_NEW_ENGINE(DifferentKeyUpdate) {
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

    Y_UNIT_TEST_NEW_ENGINE(EmptyRange) {
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
                return issue.Message.Contains("/Root/Test");
            }));

        result = session1.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM Test WHERE Group = 11;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[2u];#;[11u];["Session2"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST_NEW_ENGINE(EmptyRangeAlreadyBroken) {
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
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
            [] (const NYql::TIssue& issue) {
                return issue.Message.Contains("/Root/Test");
            }));

        result = session1.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM Test WHERE Group = 11;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[2u];#;[11u];["Session2"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

} // namespace NKqp
} // namespace NKikimr
