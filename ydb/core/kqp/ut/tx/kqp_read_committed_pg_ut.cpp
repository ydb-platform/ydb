#include "kqp_sink_common.h"

#include <ydb/core/kqp/rm_service/kqp_snapshot_manager.h>
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

Y_UNIT_TEST_SUITE(KqpReadCommittedPg) {

    // Helper: create a simple table with a single integer primary key for range tests
    static void CreateRangeTable(TKikimrRunner& kikimr) {
        auto tableClient = kikimr.GetTableClient();
        auto result = kikimr.RunCall([&] {
            return tableClient.GetSession().GetValueSync().GetSession().ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/RangeTest` (
                    Id Int32 NOT NULL,
                    Name String,
                    PRIMARY KEY (Id)
                );
            )").GetValueSync();
        });
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }

    static void FillRangeTable(TKikimrRunner& kikimr) {
        auto client = kikimr.GetQueryClient();
        auto result = kikimr.RunCall([&] {
            return client.ExecuteQuery(R"(
                REPLACE INTO `/Root/RangeTest` (Id, Name) VALUES
                    (10, "Row10"),
                    (11, "Row11"),
                    (12, "Row12");
            )", TTxControl::NoTx()).ExtractValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // =========================================================================
    // Update locks an existing row
    // =========================================================================
    class TUpdateLocksExistingRow : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            // Tx A: UPDATE existing row (Paul), keep transaction open
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` SET Comment = "TxA" WHERE Group = 1u AND Name = "Paul";
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: UPDATE same row — should block on Tx A's lock
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` SET Comment = "TxB" WHERE Group = 1u AND Name = "Paul";
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            // Dispatch events with a short timeout — Tx B should not complete
            {
                TDispatchOptions opts;
                opts.CustomFinalCondition = [&]() {
                    return futureB.HasValue() || futureB.HasException();
                };
                opts.FinalEvents.emplace_back([](IEventHandle&) { return false; });
                runtime.DispatchEvents(opts, TDuration::Seconds(2));
                UNIT_ASSERT_C(!futureB.HasValue() && !futureB.HasException(),
                    "Tx B should be blocked waiting for Tx A's lock");
            }

            // Commit Tx A — releases the lock
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Now Tx B can proceed
            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            auto txB = resultB.GetTransaction();
            if (txB) {
                auto commitB = Kikimr->RunInThreadPool([&] { return txB->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitB);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify final state: Tx B's value should win
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Comment FROM `/Root/Test` WHERE Group = 1u AND Name = "Paul";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[["TxB"]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TUpdateLocksExistingRow) {
        TUpdateLocksExistingRow tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Delete locks an existing row
    // =========================================================================
    class TDeleteLocksExistingRow : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            // Tx A: DELETE existing row, keep transaction open
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/Test` WHERE Group = 2u AND Name = "Tony";
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: UPDATE same row — should block on Tx A's lock
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` SET Comment = "TxB" WHERE Group = 2u AND Name = "Tony";
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            {
                TDispatchOptions opts;
                opts.CustomFinalCondition = [&]() {
                    return futureB.HasValue() || futureB.HasException();
                };
                opts.FinalEvents.emplace_back([](IEventHandle&) { return false; });
                runtime.DispatchEvents(opts, TDuration::Seconds(2));
                UNIT_ASSERT_C(!futureB.HasValue() && !futureB.HasException(),
                    "Tx B UPDATE should be blocked waiting for Tx A's lock");
            }

            // Commit Tx A (the DELETE) — releases the lock
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Now Tx B can proceed — row was deleted by Tx A, so Tx B updates 0 rows
            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            auto txB = resultB.GetTransaction();
            if (txB) {
                auto commitB = Kikimr->RunInThreadPool([&] { return txB->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitB);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify: Tony should be deleted
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT * FROM `/Root/Test` WHERE Group = 2u AND Name = "Tony";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TDeleteLocksExistingRow) {
        TDeleteLocksExistingRow tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Update of non-existing row locks nothing
    // =========================================================================
    class TUpdateNonExistingLocksNothing : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            size_t evLockCounter = 0;

            auto grab = [&](TAutoPtr<IEventHandle>& ev) -> auto {
                if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvLockRows::EventType) {
                    ++evLockCounter;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };

            auto saveObserver = runtime.SetObserverFunc(grab);
            Y_DEFER { runtime.SetObserverFunc(saveObserver); };

            // Tx A: UPDATE non-existing row
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` SET Comment = "TxA" WHERE Group = 999u AND Name = "NonExist";
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(evLockCounter, 0); // No locks taken on non-existing row

            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: INSERT the missing row — should not be blocked
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/Test` (Group, Name, Comment) VALUES (999u, "NonExist", "Inserted");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).ExtractValueSync();
            });

            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            // Cleanup
            {
                auto commitA = Kikimr->RunCall([&] { return txA->Commit().ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(commitA.GetStatus(), EStatus::SUCCESS, commitA.GetIssues().ToString());
            }

            // Verify
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Comment FROM `/Root/Test` WHERE Group = 999u AND Name = "NonExist";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[["Inserted"]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TUpdateNonExistingLocksNothing) {
        TUpdateNonExistingLocksNothing tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Delete of non-existing row locks nothing
    // =========================================================================
    class TDeleteNonExistingLocksNothing : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            size_t evLockCounter = 0;

            auto grab = [&](TAutoPtr<IEventHandle>& ev) -> auto {
                if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvLockRows::EventType) {
                    ++evLockCounter;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };

            auto saveObserver = runtime.SetObserverFunc(grab);
            Y_DEFER { runtime.SetObserverFunc(saveObserver); };

            // Tx A: DELETE non-existing row
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/Test` WHERE Group = 999u AND Name = "NonExist";
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(evLockCounter, 0); // No locks taken on non-existing row

            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: INSERT the missing row — should not be blocked
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/Test` (Group, Name, Comment) VALUES (999u, "NonExist", "Inserted");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).ExtractValueSync();
            });

            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            // Cleanup
            {
                auto commitA = Kikimr->RunCall([&] { return txA->Commit().ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(commitA.GetStatus(), EStatus::SUCCESS, commitA.GetIssues().ToString());
            }

            // Verify
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Comment FROM `/Root/Test` WHERE Group = 999u AND Name = "NonExist";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[["Inserted"]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TDeleteNonExistingLocksNothing) {
        TDeleteNonExistingLocksNothing tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Range update locks only existing matching rows
    // =========================================================================
    class TRangeUpdateLocksExistingRows : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            CreateRangeTable(*Kikimr);
            FillRangeTable(*Kikimr);

            // Tx A: UPDATE rows in range [10, 12] — locks existing rows 10, 11, 12
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/RangeTest` SET Name = "TxA" WHERE Id >= 10 AND Id <= 12;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: UPDATE one of the existing matched rows (Id = 11) — should block
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/RangeTest` SET Name = "TxB" WHERE Id = 11;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            {
                TDispatchOptions opts;
                opts.CustomFinalCondition = [&]() {
                    return futureB.HasValue() || futureB.HasException();
                };
                opts.FinalEvents.emplace_back([](IEventHandle&) { return false; });
                runtime.DispatchEvents(opts, TDuration::Seconds(2));
                UNIT_ASSERT_C(!futureB.HasValue() && !futureB.HasException(),
                    "Tx B should be blocked waiting for Tx A's lock on existing row");
            }

            // Commit Tx A — releases the lock
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Now Tx B can proceed
            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            auto txB = resultB.GetTransaction();
            if (txB) {
                auto commitB = Kikimr->RunInThreadPool([&] { return txB->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitB);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify final state: Tx A updated 10,11,12 to "TxA"; Tx B re-updated row 11 to "TxB"
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Id, Name FROM `/Root/RangeTest` ORDER BY Id;
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[10;["TxA"]];[11;["TxB"]];[12;["TxA"]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TRangeUpdateLocksExistingRows) {
        TRangeUpdateLocksExistingRows tester;
        tester.SetIsOlap(false);
        tester.SetFillTables(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Range update does not lock gaps
    // =========================================================================
    class TRangeUpdateDoesNotLockGaps : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            CreateRangeTable(*Kikimr);
            FillRangeTable(*Kikimr);

            // Tx A: UPDATE rows in range [10, 20] — only 10, 11, 12 exist, gap 13-19 not locked
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/RangeTest` SET Name = "TxA" WHERE Id >= 10 AND Id <= 20;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: INSERT into gap (Id = 15) — should NOT be blocked
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/RangeTest` (Id, Name) VALUES (15, "GapInsert");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).ExtractValueSync();
            });

            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            // Commit Tx A
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify final state: rows 10,11,12 = "TxA"; gap insert row 15 = "GapInsert"
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Id, Name FROM `/Root/RangeTest` ORDER BY Id;
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[10;["TxA"]];[11;["TxA"]];[12;["TxA"]];[15;["GapInsert"]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TRangeUpdateDoesNotLockGaps) {
        TRangeUpdateDoesNotLockGaps tester;
        tester.SetIsOlap(false);
        tester.SetFillTables(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Range delete locks only existing matching rows
    // =========================================================================
    class TRangeDeleteLocksExistingRows : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            CreateRangeTable(*Kikimr);
            FillRangeTable(*Kikimr);

            // Tx A: DELETE rows in range [10, 12] — locks existing rows 10, 11, 12
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/RangeTest` WHERE Id >= 10 AND Id <= 12;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: UPDATE one of the existing matched rows (Id = 11) — should block
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/RangeTest` SET Name = "TxB" WHERE Id = 11;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            {
                TDispatchOptions opts;
                opts.CustomFinalCondition = [&]() {
                    return futureB.HasValue() || futureB.HasException();
                };
                opts.FinalEvents.emplace_back([](IEventHandle&) { return false; });
                runtime.DispatchEvents(opts, TDuration::Seconds(2));
                UNIT_ASSERT_C(!futureB.HasValue() && !futureB.HasException(),
                    "Tx B should be blocked waiting for Tx A's lock on existing row");
            }

            // Commit Tx A — releases the lock
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Now Tx B can proceed — row 11 was deleted by Tx A, so Tx B updates 0 rows
            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            auto txB = resultB.GetTransaction();
            if (txB) {
                auto commitB = Kikimr->RunInThreadPool([&] { return txB->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitB);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify final state: Tx A deleted rows 10,11,12; Tx B updated 0 rows (row 11 was gone)
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Id, Name FROM `/Root/RangeTest` ORDER BY Id;
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TRangeDeleteLocksExistingRows) {
        TRangeDeleteLocksExistingRows tester;
        tester.SetIsOlap(false);
        tester.SetFillTables(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Range delete does not lock gaps
    // =========================================================================
    class TRangeDeleteDoesNotLockGaps : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            CreateRangeTable(*Kikimr);
            FillRangeTable(*Kikimr);

            // Tx A: DELETE rows in range [10, 20] — only 10, 11, 12 exist, gap not locked
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/RangeTest` WHERE Id >= 10 AND Id <= 20;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: INSERT into gap (Id = 15) — should NOT be blocked
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/RangeTest` (Id, Name) VALUES (15, "GapInsert");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).ExtractValueSync();
            });

            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            // Commit Tx A
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify final state: Tx A deleted rows 10,11,12; gap insert row 15 remains
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Id, Name FROM `/Root/RangeTest` ORDER BY Id;
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[15;["GapInsert"]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TRangeDeleteDoesNotLockGaps) {
        TRangeDeleteDoesNotLockGaps tester;
        tester.SetIsOlap(false);
        tester.SetFillTables(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Inserted but uncommitted row conflicts with same unique key
    // =========================================================================
    class TInsertUncommittedConflictsSameKey : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            // Tx A: INSERT a new row, keep open
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/Test` (Group, Name, Comment) VALUES (500u, "NewRow", "TxA");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: INSERT same key — should block on Tx A's lock
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/Test` (Group, Name, Comment) VALUES (500u, "NewRow", "TxB");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            // Dispatch events with a short timeout — Tx B should not complete
            {
                TDispatchOptions opts;
                opts.CustomFinalCondition = [&]() {
                    return futureB.HasValue() || futureB.HasException();
                };
                opts.FinalEvents.emplace_back([](IEventHandle&) { return false; });
                runtime.DispatchEvents(opts, TDuration::Seconds(2));
                UNIT_ASSERT_C(!futureB.HasValue() && !futureB.HasException(),
                    "Tx B should be blocked waiting for Tx A's lock");
            }

            // Commit Tx A — releases the lock
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Now Tx B can proceed — the key already exists (committed by Tx A),
            // so the INSERT fails with PRECONDITION_FAILED
            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_C(resultB.GetStatus() == EStatus::PRECONDITION_FAILED, resultB.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(TInsertUncommittedConflictsSameKey) {
        TInsertUncommittedConflictsSameKey tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Update does not wait for uncommitted inserted row with same key
    // (Read Committed: uncommitted row is not visible, so UPDATE finds 0 rows)
    // =========================================================================
    class TUpdateDoesNotWaitForUncommittedInsert : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            // Tx A: INSERT a new row, keep open
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/Test` (Group, Name, Comment) VALUES (600u, "NewRow", "TxA");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: UPDATE the same key
            // In Read Committed, the uncommitted row is NOT visible.
            // The UPDATE finds 0 rows and completes immediately — no blocking.
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` SET Comment = "TxB" WHERE Group = 600u AND Name = "NewRow";
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            // Tx B should complete immediately (no blocking — uncommitted row not visible)
            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            auto txB = resultB.GetTransaction();
            if (txB) {
                auto commitB = Kikimr->RunInThreadPool([&] { return txB->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitB);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Tx A commits
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify: row exists with Tx A's value (Tx B updated 0 rows)
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Comment FROM `/Root/Test` WHERE Group = 600u AND Name = "NewRow";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[["TxA"]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TUpdateDoesNotWaitForUncommittedInsert) {
        TUpdateDoesNotWaitForUncommittedInsert tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Delete does not wait for uncommitted inserted row with same key
    // (Read Committed: uncommitted row is not visible, so DELETE finds 0 rows)
    // =========================================================================
    class TDeleteDoesNotWaitForUncommittedInsert : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            // Tx A: INSERT a new row, keep open
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/Test` (Group, Name, Comment) VALUES (700u, "NewRow", "TxA");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: DELETE the same key
            // In Read Committed, the uncommitted row is NOT visible.
            // The DELETE finds 0 rows and completes immediately — no blocking.
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/Test` WHERE Group = 700u AND Name = "NewRow";
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            // Tx B should complete immediately (no blocking — uncommitted row not visible)
            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            auto txB = resultB.GetTransaction();
            if (txB) {
                auto commitB = Kikimr->RunInThreadPool([&] { return txB->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitB);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Tx A commits
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify: row exists with Tx A's value (Tx B deleted 0 rows)
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Comment FROM `/Root/Test` WHERE Group = 700u AND Name = "NewRow";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[["TxA"]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TDeleteDoesNotWaitForUncommittedInsert) {
        TDeleteDoesNotWaitForUncommittedInsert tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // New statement sees newly committed row
    // In READ COMMITTED, each statement gets a fresh snapshot.
    // =========================================================================
    class TNewStatementSeesNewlyCommittedRow : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            // Tx A: DELETE rows in a range, keep open
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/Test` WHERE Group = 1u;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: INSERT a new row matching the same condition, commit
            {
                auto resultB = Kikimr->RunCall([&] {
                    return session2.ExecuteQuery(Q_(R"(
                        INSERT INTO `/Root/Test` (Group, Name, Comment) VALUES (1u, "NewInTx", "Inserted");
                    )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).ExtractValueSync();
                });
                UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());
            }

            // Tx A runs same DELETE again — in READ COMMITTED, sees the newly committed row
            auto resultA2 = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/Test` WHERE Group = 1u;
                )"), TTxControl::Tx(*txA)).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(resultA2.GetStatus(), EStatus::SUCCESS, resultA2.GetIssues().ToString());

            // Commit Tx A
            {
                auto commitA = Kikimr->RunCall([&] { return txA->Commit().ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(commitA.GetStatus(), EStatus::SUCCESS, commitA.GetIssues().ToString());
            }

            // Verify: all Group=1 rows deleted (including the one inserted by Tx B)
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT * FROM `/Root/Test` WHERE Group = 1u;
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TNewStatementSeesNewlyCommittedRow) {
        TNewStatementSeesNewlyCommittedRow tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // A single statement does not chase future inserted rows
    // =========================================================================
    class TSingleStatementDoesNotChaseFutureRows : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            size_t evReadCounter = 0;
            std::vector<std::unique_ptr<IEventHandle>> blockedReads;

            auto grab = [&](TAutoPtr<IEventHandle>& ev) -> auto {
                if (ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvRead::EventType) {
                    ++evReadCounter;
                    if (evReadCounter == 1) {
                        blockedReads.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };

            auto saveObserver = runtime.SetObserverFunc(grab);
            Y_DEFER { runtime.SetObserverFunc(saveObserver); };

            // Tx A: Start a DELETE statement over a range — block the read phase
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/Test` WHERE Group = 1u;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return evReadCounter >= 1;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT(evReadCounter == 1);
            }

            // Tx B: INSERT a new row matching the same condition, commit
            {
                auto resultB = Kikimr->RunCall([&] {
                    return session2.ExecuteQuery(Q_(R"(
                        INSERT INTO `/Root/Test` (Group, Name, Comment) VALUES (1u, "FutureRow", "Inserted");
                    )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).ExtractValueSync();
                });
                UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());
            }

            // Release the blocked read — Tx A's statement resumes with its original snapshot
            for (auto& ev : blockedReads) {
                runtime.Send(ev.release());
            }

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // The newly inserted row should NOT have been deleted by Tx A's first statement
            // Verify with a new read
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Name FROM `/Root/Test` WHERE Group = 1u AND Name = "FutureRow";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([["FutureRow"]])", FormatResultSetYson(verify.GetResultSet(0)));

            // Cleanup
            {
                auto commitA = Kikimr->RunCall([&] { return txA->Commit().ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(commitA.GetStatus(), EStatus::SUCCESS, commitA.GetIssues().ToString());
            }
        }
    };

    Y_UNIT_TEST(TSingleStatementDoesNotChaseFutureRows) {
        TSingleStatementDoesNotChaseFutureRows tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Concurrent update of same row uses latest committed version
    // =========================================================================
    class TConcurrentUpdateUsesLatestCommitted : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            // Tx A: UPDATE existing row (Paul), keep transaction open
            // Use Name + Comment (non-key column) to force a scan instead of
            // point lookup by full PK (Group, Name).
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` SET Comment = "TxA" WHERE Name = "Paul" AND Comment >= "";
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: UPDATE same row — should block
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` SET Comment = "TxB" WHERE Name = "Paul" AND Comment >= "";
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            // Tx B should be blocked
            {
                TDispatchOptions opts;
                opts.CustomFinalCondition = [&]() {
                    return futureB.HasValue() || futureB.HasException();
                };
                opts.FinalEvents.emplace_back([](IEventHandle&) { return false; });
                runtime.DispatchEvents(opts, TDuration::Seconds(2));
                UNIT_ASSERT_C(!futureB.HasValue() && !futureB.HasException(),
                    "Tx B should be blocked waiting for Tx A's lock");
            }

            // Commit Tx A — releases the lock
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Tx B resumes — rechecks condition against latest committed version, then updates
            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            auto txB = resultB.GetTransaction();
            if (txB) {
                auto commitB = Kikimr->RunInThreadPool([&] { return txB->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitB);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify final state: Tx B's value should win (it ran after Tx A committed)
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Comment FROM `/Root/Test` WHERE Group = 1u AND Name = "Paul";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[["TxB"]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TConcurrentUpdateUsesLatestCommitted) {
        TConcurrentUpdateUsesLatestCommitted tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Waiting update may update zero rows after recheck
    // =========================================================================
    class TWaitingUpdateMayUpdateZeroRows : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            // Tx A: UPDATE row so it no longer matches Tx B's condition.
            // Paul has Amount=300. Tx A sets Amount=999, making it not match Amount=300.
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` SET Amount = 999u WHERE Group = 1u AND Name = "Paul" AND Amount = 300u;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: UPDATE rows WHERE Amount = 300 — Paul had Amount=300, but Tx A changed it
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` SET Comment = "TxB" WHERE Amount = 300u;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            // Tx B should be blocked on the lock
            {
                TDispatchOptions opts;
                opts.CustomFinalCondition = [&]() {
                    return futureB.HasValue() || futureB.HasException();
                };
                opts.FinalEvents.emplace_back([](IEventHandle&) { return false; });
                runtime.DispatchEvents(opts, TDuration::Seconds(2));
                UNIT_ASSERT_C(!futureB.HasValue() && !futureB.HasException(),
                    "Tx B should be blocked waiting for Tx A's lock");
            }

            // Commit Tx A (changes Amount from 300 to 999)
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Tx B resumes — rechecks Amount=300, but it's now 999, so updates 0 rows
            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            auto txB = resultB.GetTransaction();
            if (txB) {
                auto commitB = Kikimr->RunInThreadPool([&] { return txB->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitB);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify: Paul's Comment should NOT be "TxB" (Tx B updated 0 rows)
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Comment, Amount FROM `/Root/Test` WHERE Group = 1u AND Name = "Paul";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[["None"];[999u]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TWaitingUpdateMayUpdateZeroRows) {
        TWaitingUpdateMayUpdateZeroRows tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // Waiting delete may delete zero rows after recheck
    // =========================================================================
    class TWaitingDeleteMayDeleteZeroRows : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            // Tx A: UPDATE row so it no longer matches Tx B's DELETE condition.
            // Paul has Amount=300. Tx A sets Amount=999.
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` SET Amount = 999u WHERE Group = 1u AND Name = "Paul" AND Amount = 300u;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: DELETE rows WHERE Amount = 300 — Paul had Amount=300, but Tx A changed it
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/Test` WHERE Amount = 300u;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            // Tx B should be blocked on the lock
            {
                TDispatchOptions opts;
                opts.CustomFinalCondition = [&]() {
                    return futureB.HasValue() || futureB.HasException();
                };
                opts.FinalEvents.emplace_back([](IEventHandle&) { return false; });
                runtime.DispatchEvents(opts, TDuration::Seconds(2));
                UNIT_ASSERT_C(!futureB.HasValue() && !futureB.HasException(),
                    "Tx B should be blocked waiting for Tx A's lock");
            }

            // Commit Tx A (changes Amount from 300 to 999)
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Tx B resumes — rechecks Amount=300, but it's now 999, so deletes 0 rows
            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            auto txB = resultB.GetTransaction();
            if (txB) {
                auto commitB = Kikimr->RunInThreadPool([&] { return txB->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitB);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify: Paul should still exist (Tx B deleted 0 rows)
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Name, Amount FROM `/Root/Test` WHERE Group = 1u AND Name = "Paul";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([["Paul";[999u]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TWaitingDeleteMayDeleteZeroRows) {
        TWaitingDeleteMayDeleteZeroRows tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // UPDATE ON does not lock a missing key
    // UPDATE ON issues TEvLockRows with SkipAbsent=true, so missing keys are
    // skipped and not locked. A concurrent INSERT of the same key proceeds
    // without blocking.
    // =========================================================================
    class TUpdateOnDoesNotLockMissingKey : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            // Tx A: UPDATE ON a missing key — succeeds (0 rows affected),
            // does not lock the missing key (SkipAbsent=true).
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` ON (Group, Name, Comment) VALUES (999u, "Missing", "TxA");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: INSERT the same missing key — should NOT block (no lock held)
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/Test` (Group, Name, Comment) VALUES (999u, "Missing", "TxB");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).ExtractValueSync();
            });

            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            // Commit Tx A — wrote nothing (row was missing)
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);
                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify: row exists with Tx B's value (Tx A wrote nothing)
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Comment FROM `/Root/Test` WHERE Group = 999u AND Name = "Missing";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[["TxB"]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TUpdateOnDoesNotLockMissingKey) {
        TUpdateOnDoesNotLockMissingKey tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // =========================================================================
    // DELETE ON does not lock a missing key
    // DELETE ON issues TEvLockRows with SkipAbsent=true, so missing keys are
    // skipped and not locked. A concurrent INSERT of the same key proceeds
    // without blocking.
    // =========================================================================
    class TDeleteOnDoesNotLockMissingKey : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            // Tx A: DELETE ON a missing key — succeeds (0 rows affected),
            // does not lock the missing key (SkipAbsent=true).
            auto futureA = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/Test` ON (Group, Name) VALUES (999u, "Missing");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
            });

            auto resultA = runtime.WaitFuture(futureA);
            UNIT_ASSERT_VALUES_EQUAL_C(resultA.GetStatus(), EStatus::SUCCESS, resultA.GetIssues().ToString());
            auto txA = resultA.GetTransaction();
            UNIT_ASSERT(txA);
            UNIT_ASSERT(txA->IsActive());

            // Tx B: INSERT the same missing key — should NOT block (no lock held)
            auto futureB = Kikimr->RunInThreadPool([&] {
                return session2.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/Test` (Group, Name, Comment) VALUES (999u, "Missing", "TxB");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).ExtractValueSync();
            });

            auto resultB = runtime.WaitFuture(futureB);
            UNIT_ASSERT_VALUES_EQUAL_C(resultB.GetStatus(), EStatus::SUCCESS, resultB.GetIssues().ToString());

            // Commit Tx A — erased nothing (row was missing)
            {
                auto commitA = Kikimr->RunInThreadPool([&] { return txA->Commit().ExtractValueSync(); });
                auto commitResult = runtime.WaitFuture(commitA);

                UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
            }

            // Verify: row exists with Tx B's value (Tx A erased nothing)
            auto verify = Kikimr->RunCall([&] {
                return session1.ExecuteQuery(Q_(R"(
                    SELECT Comment FROM `/Root/Test` WHERE Group = 999u AND Name = "Missing";
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(verify.GetStatus(), EStatus::SUCCESS, verify.GetIssues().ToString());
            CompareYson(R"([[["TxB"]]])", FormatResultSetYson(verify.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(TDeleteOnDoesNotLockMissingKey) {
        TDeleteOnDoesNotLockMissingKey tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    class TLockOnlyShardDistributedCommit : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            {
                auto r = Kikimr->RunCall([&] {
                    return client.ExecuteQuery(R"(
                        CREATE TABLE `/Root/T1` (
                            Key Uint32 NOT NULL,
                            Val String,
                            PRIMARY KEY (Key)
                        ) WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
                        CREATE TABLE `/Root/T2` (
                            Key Uint32 NOT NULL,
                            Val String,
                            PRIMARY KEY (Key)
                        ) WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
                    )", TTxControl::NoTx()).ExtractValueSync();
                });
                UNIT_ASSERT_C(r.IsSuccess(), r.GetIssues().ToString());
            }

            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/T1` (Key, Val) VALUES (1u, "a");
                    UPDATE `/Root/T2` ON (Key, Val) VALUES (2u, "b");
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).ExtractValueSync();
            });

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(TLockOnlyShardDistributedCommit) {
        TLockOnlyShardDistributedCommit tester;
        tester.SetIsOlap(false);
        tester.SetFillTables(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

} // Y_UNIT_TEST_SUITE(KqpReadCommittedPg)

} // namespace NKqp
} // namespace NKikimr
