#include "kqp_sink_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/base/tablet_pipecache.h>

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
        return;
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
        std::string WriteOperation = "insert";

    public:
        TConflictWrite(const std::string& writeOperation) : WriteOperation(writeOperation) {}

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

            if (WriteOperation == "insert") {
                result = session1.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/Test` (Group, Name, Comment)
                    VALUES (1U, "Paul", "Changed");
                )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
            } else if (WriteOperation == "upsert_partial") {
                result = session1.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/Test` (Group, Name, Comment)
                    VALUES (1U, "Paul", "Changed");
                )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
            } else if (WriteOperation == "upsert_full") {
                result = session1.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/Test` (Group, Name, Comment, Amount)
                    VALUES (1U, "Paul", "Changed", 301ul);
                )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
            } else if (WriteOperation == "replace") {
                result = session1.ExecuteQuery(Q_(R"(
                    REPLACE INTO `/Root/Test` (Group, Name, Comment)
                    VALUES (1U, "Paul", "Changed");
                )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
            } else if (WriteOperation == "delete") {
                result = session1.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/Test` WHERE Name == "Paul";
                )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
            } else {
                UNIT_ASSERT(false);
            }

            if (WriteOperation == "insert") {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            }

            result = session2.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[300u];["Changed Other"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TConflictWriteOltp) {
        TConflictWrite tester("upsert_partial");
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(TConflictWriteOltpNoSink) {
        return;
        TConflictWrite tester("upsert_partial");
        tester.SetIsOlap(false);
        tester.SetDisableSinks(true);
        tester.Execute();
    }

    Y_UNIT_TEST(TConflictWriteOlapInsert) {
        TConflictWrite tester("insert");
        tester.SetIsOlap(true);
        tester.Execute();
    }

    Y_UNIT_TEST(TConflictWriteOlapUpsertPartial) {
        TConflictWrite tester("upsert_partial");
        tester.SetIsOlap(true);
        tester.Execute();
    }

    Y_UNIT_TEST(TConflictWriteOlapUpsertFull) {
        TConflictWrite tester("upsert_full");
        tester.SetIsOlap(true);
        tester.Execute();
    }

    Y_UNIT_TEST(TConflictWriteOlapReplace) {
        TConflictWrite tester("replace");
        tester.SetIsOlap(true);
        tester.Execute();
    }

    Y_UNIT_TEST(TConflictWriteOlapDelete) {
        TConflictWrite tester("delete");
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
            )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
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
        return;
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
            )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
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
        return;
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

    class TSnapshotTwoInsert : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            auto& runtime = *Kikimr->GetTestServer().GetRuntime();

            {
                const TString insertQuery(Q1_(R"(
                    INSERT INTO `/Root/KV` (Key, Value) VALUES (4u, "test");
                    INSERT INTO `/Root/KV2` (Key, Value) VALUES (5u, "test");
                )"));

                std::vector<std::unique_ptr<IEventHandle>> writes;
                size_t evWriteCounter = 0;

                auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                    if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWrite::EventType) {
                        auto* evWrite = ev->Get<NKikimr::NEvents::TDataEvents::TEvWrite>();
                        UNIT_ASSERT(evWrite->Record.OperationsSize() <= 1);
                        if (evWrite->Record.OperationsSize() == 1) {
                            UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetStep() != 0);
                            UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetTxId() != 0);
                        }

                        if (evWriteCounter++ == 1) {
                            writes.emplace_back(ev.Release());
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }

                    return TTestActorRuntime::EEventAction::PROCESS;
                };

                auto saveObserver = runtime.SetObserverFunc(grab);
                Y_DEFER {
                    runtime.SetObserverFunc(saveObserver);
                };

                auto future = Kikimr->RunInThreadPool([&]{
                    auto txc = NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SnapshotRW()).CommitTx();
                    return session1.ExecuteQuery(insertQuery, txc).ExtractValueSync();
                });

                {
                    TDispatchOptions opts;
                    opts.FinalEvents.emplace_back([&](IEventHandle&) {
                        return evWriteCounter == 2;
                    });
                    runtime.DispatchEvents(opts);
                    UNIT_ASSERT(evWriteCounter == 2);
                    UNIT_ASSERT(writes.size() == 1);
                }

                {
                    // Another request changes data
                    auto insetResult = Kikimr->RunCall([&]{
                        auto txc = NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx();
                        return session2.ExecuteQuery(insertQuery, txc).ExtractValueSync();
                    });

                    UNIT_ASSERT_VALUES_EQUAL_C(insetResult.GetStatus(), EStatus::SUCCESS, insetResult.GetIssues().ToString());
                }

                UNIT_ASSERT(evWriteCounter == 6);
                UNIT_ASSERT(writes.size() == 1);

                for(auto& ev: writes) {
                    runtime.Send(ev.release());
                }

                auto result = runtime.WaitFuture(future);
                // Must be ABORTED, not PRECONDTION_FAILED
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            }
        }
    };

    Y_UNIT_TEST(TSnapshotTwoInsertOlap) {
        TSnapshotTwoInsert tester;
        tester.SetIsOlap(true);
        tester.SetDisableSinks(false);
        tester.SetUseRealThreads(false);
       tester.Execute();
    }

    class TReadOwnChanges : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = client.GetSession().GetValueSync().GetSession();

            // tx1 reads KV2
            auto result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV2`;
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            // tx1 upserts a row
            result = session1.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/KV2` (Key, Value)
                VALUES (1U, "val1");
            )"), TTxControl::Tx(*tx1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            tx1 = result.GetTransaction();

            // tx1 reads KV2 and sees the row
            result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV2`;
            )"), TTxControl::Tx(*tx1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u;["val1"]]])", FormatResultSetYson(result.GetResultSet(0)));
            tx1 = result.GetTransaction();

            // tx1 commits
            result = tx1->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(TReadOwnChangesOltp) {
        TReadOwnChanges tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(TReadOwnChangesOltpNoSink) {
        return;
        TReadOwnChanges tester;
        tester.SetIsOlap(false);
        tester.SetDisableSinks(true);
        tester.Execute();
    }

    Y_UNIT_TEST(TReadOwnChangesOlap) {
        TReadOwnChanges tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }
}

} // namespace NKqp
} // namespace NKikimr
