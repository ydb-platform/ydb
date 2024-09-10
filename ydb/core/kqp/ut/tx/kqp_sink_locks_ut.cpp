#include "kqp_sink_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpSinkLocks) {
    class TInvalidate : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test`
                SELECT Group + 10U AS Group, Name, Amount, Comment ?? "" || "Updated" AS Comment
                FROM `/Root/Test`
                WHERE Group == 1U AND Name == "Paul";
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test` (Group, Name, Comment)
                VALUES (1U, "Paul", "Changed");
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session1.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test` (Group, Name, Comment)
                VALUES (11U, "Sergey", "BadRow");
            )"), TTxControl::Tx(tx1->GetId()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            result.GetIssues().PrintTo(Cerr);
            if (!GetIsOlap()) {
                UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
                    [] (const NYql::TIssue& issue) {
                        return issue.GetMessage().Contains("/Root/Test");
                    }), result.GetIssues().ToString());
            }

            result = session2.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[300u];["Changed"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TInvalidate) {
        TInvalidate tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(TInvalidateOlap) {
        TInvalidate tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TInvalidateOnCommit : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test`
                SELECT Group + 10U AS Group, Name, Amount, Comment ?? "" || "Updated" AS Comment
                FROM `/Root/Test`
                WHERE Group == 1U AND Name == "Paul";
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test` (Group, Name, Comment)
                VALUES (1U, "Paul", "Changed");
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto commitResult = tx1->Commit().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::ABORTED, commitResult.GetIssues().ToString());
            commitResult.GetIssues().PrintTo(Cerr);
            if (!GetIsOlap()) {
                UNIT_ASSERT_C(HasIssue(commitResult.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
                    [] (const NYql::TIssue& issue) {
                        return issue.GetMessage().Contains("/Root/Test");
                    }), commitResult.GetIssues().ToString());
            }

            result = session2.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[300u];["Changed"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(InvalidateOnCommit) {
        TInvalidateOnCommit tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(InvalidateOlapOnCommit) {
        TInvalidateOnCommit tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TDifferentKeyUpdate : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Group = 1;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());

            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test` (Group, Name, Comment)
                VALUES (2U, "Paul", "Changed");
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());

            result = session1.ExecuteQuery(Q_(R"(
                SELECT "Nothing";
            )"), TTxControl::Tx(tx1->GetId()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(DifferentKeyUpdate) {
        TDifferentKeyUpdate tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(DifferentKeyUpdateOlap) {
        TDifferentKeyUpdate tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TEmptyRange : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q1_(R"(
                SELECT * FROM Test WHERE Group = 11;
            )"), TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            result = session2.ExecuteQuery(Q1_(R"(
                SELECT * FROM Test WHERE Group = 11;
                UPSERT INTO Test (Group, Name, Amount) VALUES
                    (11, "Session2", 2);
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

            result = session1.ExecuteQuery(Q1_(R"(
                UPSERT INTO Test (Group, Name, Amount) VALUES
                    (11, "Session1", 1);
            )"), TTxControl::Tx(tx1->GetId()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            result.GetIssues().PrintTo(Cerr);
            if (!GetIsOlap()) {
                UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
                    [] (const NYql::TIssue& issue) {
                        return issue.GetMessage().Contains("/Root/Test");
                    }), result.GetIssues().ToString());
            }

            result = session1.ExecuteQuery(Q1_(R"(
                SELECT * FROM Test WHERE Group = 11;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[2u];#;11u;"Session2"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(EmptyRange) {
        TEmptyRange tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(EmptyRangeOlap) {
        TEmptyRange tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TEmptyRangeAlreadyBroken : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q1_(R"(
                SELECT * FROM Test WHERE Group = 10;
            )"), TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            result = session2.ExecuteQuery(Q1_(R"(
                SELECT * FROM Test WHERE Group = 11;

                UPSERT INTO Test (Group, Name, Amount) VALUES
                    (11, "Session2", 2);
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

            result = session1.ExecuteQuery(Q1_(R"(
                SELECT * FROM Test WHERE Group = 11;

                UPSERT INTO Test (Group, Name, Amount) VALUES
                    (11, "Session1", 1);
            )"), TTxControl::Tx(tx1->GetId()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            result.GetIssues().PrintTo(Cerr);
            if (!GetIsOlap()) {
                UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
                    [] (const NYql::TIssue& issue) {
                        return issue.GetMessage().Contains("/Root/Test");
                    }), result.GetIssues().ToString());
            }

            result = session1.ExecuteQuery(Q1_(R"(
                SELECT * FROM Test WHERE Group = 11;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[2u];#;11u;"Session2"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(EmptyRangeAlreadyBroken) {
        TEmptyRangeAlreadyBroken tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(EmptyRangeAlreadyBrokenOlap) {
        TEmptyRangeAlreadyBroken tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TUncommittedRead : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q1_(R"(
                UPSERT INTO Test (Group, Name, Amount) VALUES
                    (11, "TEST", 2);
            )"), TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            {
                result = session2.ExecuteQuery(Q1_(R"(
                    SELECT * FROM Test WHERE Group = 11;
                )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
            }

            {
                result = session1.ExecuteQuery(Q1_(R"(
                    SELECT * FROM Test WHERE Group = 11;
                )"), TTxControl::Tx(tx1->GetId())).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[[2u];#;11u;"TEST"]])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedRead) {
        TUncommittedRead tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapUncommittedRead) {
        TUncommittedRead tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }
}

} // namespace NKqp
} // namespace NKikimr
