#include "kqp_sink_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpSnapshotIsolation) {
    class TSimple : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = client.GetSession().GetValueSync().GetSession();

            {
                auto result = session1.ExecuteQuery(Q_(R"(
                    SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[[300u];["None"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
            }

            {
                auto result = session1.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/Test` (Group, Name, Comment)
                    VALUES (1U, "Paul", "Changed");
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(1000))).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            {
                auto result = session1.ExecuteQuery(Q_(R"(
                    SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[[300u];["Changed"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(TSimpleOltp) {
        TSimple tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(TSimpleOltpNoSink) {
        TSimple tester;
        tester.SetIsOlap(false);
        tester.SetDisableSinks(true);
        tester.Execute();
    }

    Y_UNIT_TEST(TSimpleOlap) {
        TSimple tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TConflictWrite : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV`;
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test` (Group, Name, Comment)
                VALUES (1U, "Paul", "Changed Other");
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session1.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test` (Group, Name, Comment)
                VALUES (1U, "Paul", "Changed");
            )"), TTxControl::Tx(tx1->GetId()).CommitTx()).ExtractValueSync();
            // Keys changed since taking snapshot.
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());

            result = session2.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[300u];["Changed Other"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TConflictWriteOltp) {
        TConflictWrite tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(TConflictWriteOltpNoSink) {
        TConflictWrite tester;
        tester.SetIsOlap(false);
        tester.SetDisableSinks(true);
        tester.Execute();
    }

    Y_UNIT_TEST(TConflictWriteOlap) {
        TConflictWrite tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TConflictReadWrite : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test`;
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test` (Group, Name, Comment)
                VALUES (1U, "NOT Paul", "Changed Other");
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session1.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test` (Group, Name, Comment)
                VALUES (1U, "Paul", "Changed");
            )"), TTxControl::Tx(tx1->GetId()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session2.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[300u];["Changed"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TConflictReadWriteOltp) {
        TConflictReadWrite tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(TConflictReadWriteOltpNoSink) {
        TConflictReadWrite tester;
        tester.SetIsOlap(false);
        tester.SetDisableSinks(true);
        tester.Execute();
    }

    Y_UNIT_TEST(TConflictReadWriteOlap) {
        TConflictReadWrite tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TReadOnly : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[300u];["None"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));

            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test` (Group, Name, Comment)
                VALUES (1U, "Paul", "Changed Other");
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
            )"), TTxControl::Tx(tx1->GetId()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[300u];["None"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));

            result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[300u];["Changed Other"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TReadOnlyOltp) {
        TReadOnly tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(TReadOnlyOltpNoSink) {
        TReadOnly tester;
        tester.SetIsOlap(false);
        tester.SetDisableSinks(true);
        tester.Execute();
    }

    Y_UNIT_TEST(TReadOnlyOlap) {
        TReadOnly tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }
}

} // namespace NKqp
} // namespace NKikimr
