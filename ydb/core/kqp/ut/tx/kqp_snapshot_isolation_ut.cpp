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
                UPSERT INTO `/Root/Test` (Group, Name, Comment, Amount)
                VALUES (1U, "Paul", "Changed Other", 100u);
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
            } else if (WriteOperation == "update") {
                result = session1.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` SET Amount = 101u WHERE Name == "Paul";
                )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
            } else if (WriteOperation == "delete_on") {
                result = session1.ExecuteQuery(Q_(R"(
                    DELETE FROM `/Root/Test` ON (Group, Name) VALUES (1U, "Paul");
                )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
            } else if (WriteOperation == "update_on") {
                result = session1.ExecuteQuery(Q_(R"(
                    UPDATE `/Root/Test` ON (Group, Name, Amount) VALUES (1U, "Paul", 101u);
                )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
            } else {
                UNIT_ASSERT(false);
            }

            if (WriteOperation == "insert" && GetFillTables() && GetIsOlap()) { // olap needs to return aborted too?
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            } else if (!GetFillTables() && (WriteOperation == "delete" || WriteOperation == "update")) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            }

            result = session2.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[100u];["Changed Other"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST_QUAD(ConflictWrite, IsOlap, FillTables) {
        for (const std::string operation : {"insert", "upsert_partial", "upsert_full", "replace", "delete", "update", "delete_on", "update_on"}) {
            TConflictWrite tester(operation);
            tester.SetIsOlap(IsOlap);
            tester.SetFillTables(FillTables);
            tester.Execute();
        }
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

    Y_UNIT_TEST(TSnapshotTwoInsertOltp) {
        TSnapshotTwoInsert tester;
        tester.SetIsOlap(false);
        tester.SetDisableSinks(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    class TSnapshotTwoUpdate : public TTableDataModificationTester {
    public:
        bool UpdateAfterInsert = false;

    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            auto& runtime = *Kikimr->GetTestServer().GetRuntime();

            auto edgeActor = runtime.AllocateEdgeActor();

            const auto& describe = DescribeTable(
                 &Kikimr->GetTestServer(),
                edgeActor,
                "/Root/KV2");

            if (!UpdateAfterInsert) {
                const TString insertQuery(Q1_(R"(
                    INSERT INTO `/Root/KV2` (Key, Value) VALUES (4u, "test");
                )"));
                auto insetResult = Kikimr->RunCall([&]{
                    auto txc = NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SnapshotRW()).CommitTx();
                    return session2.ExecuteQuery(insertQuery, txc).ExtractValueSync();
                });

                UNIT_ASSERT_VALUES_EQUAL_C(insetResult.GetStatus(), EStatus::SUCCESS, insetResult.GetIssues().ToString());
            }

            {
                const TString updateQuery(Q1_(R"(
                    UPDATE `/Root/KV2` ON (Key, Value) VALUES (4u, "test2");
                    UPDATE `/Root/KV` ON (Key, Value) VALUES (4u, "test2");
                )"));

                std::vector<std::unique_ptr<IEventHandle>> writes;
                size_t evWriteCounter = 0;
                size_t evWriteResultCounter = 0;

                auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                    if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWrite::EventType) {
                        ++evWriteCounter;
                        auto* evWrite = ev->Get<NKikimr::NEvents::TDataEvents::TEvWrite>();
                        UNIT_ASSERT(evWrite->Record.OperationsSize() <= 1);
                        if (evWrite->Record.OperationsSize() == 1) {
                            if (evWriteCounter <= 2) {
                                UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetStep() == 0);
                                UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetTxId() == 0);
                            }

                            if (evWrite->Record.GetOperations()[0].GetTableId().GetTableId() == describe.GetPathId() && evWriteCounter <= 2) {
                                writes.emplace_back(ev.Release());
                                return TTestActorRuntime::EEventAction::DROP;
                            }
                        }
                        
                    } else if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWriteResult::EventType) {
                        ++evWriteResultCounter;
                    }

                    return TTestActorRuntime::EEventAction::PROCESS;
                };

                auto saveObserver = runtime.SetObserverFunc(grab);
                Y_DEFER {
                    runtime.SetObserverFunc(saveObserver);
                };

                auto future = Kikimr->RunInThreadPool([&]{
                    auto txc = NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SnapshotRW()).CommitTx();
                    return session1.ExecuteQuery(updateQuery, txc).ExtractValueSync();
                });

                {
                    TDispatchOptions opts;
                    opts.FinalEvents.emplace_back([&](IEventHandle&) {
                        return evWriteCounter == 2 && evWriteResultCounter == 1;
                    });
                    runtime.DispatchEvents(opts);
                    UNIT_ASSERT(evWriteCounter == 2);
                    UNIT_ASSERT(evWriteResultCounter == 1);
                    UNIT_ASSERT(writes.size() == 1);
                }

                {
                    // Another request changes data
                    const TString insertQuery(Q1_(std::format(R"(
                        UPSERT INTO `{}` (Key, Value) VALUES (4u, "test");
                    )", UpdateAfterInsert ? "/Root/KV2" : "/Root/KV")));
                    auto insetResult = Kikimr->RunCall([&]{
                        auto txc = NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SnapshotRW()).CommitTx();
                        return session2.ExecuteQuery(insertQuery, txc).ExtractValueSync();
                    });

                    UNIT_ASSERT_VALUES_EQUAL_C(insetResult.GetStatus(), EStatus::SUCCESS, insetResult.GetIssues().ToString());
                }

                UNIT_ASSERT(evWriteCounter == (GetIsOlap() ? 4 : 3));
                UNIT_ASSERT(writes.size() == 1);

                for(auto& ev: writes) {
                    runtime.Send(ev.release());
                }

                auto result = runtime.WaitFuture(future);
                // Tx was write only, so it is executed with snapshot timestamp = commit timestamp, like serializable.
                UNIT_ASSERT_VALUES_EQUAL_C(
                    result.GetStatus(),
                    GetIsOlap()
                        ? (UpdateAfterInsert ? EStatus::SUCCESS : EStatus::ABORTED)
                        : EStatus::SUCCESS, result.GetIssues().ToString());
            }
        }
    };

    Y_UNIT_TEST_TWIN(TSnapshotTwoUpdateOlap, UpdateAfterInsert) {
        TSnapshotTwoUpdate tester;
        tester.SetIsOlap(true);
        tester.SetDisableSinks(false);
        tester.SetUseRealThreads(false);
        tester.SetFillTables(false);
        tester.UpdateAfterInsert = UpdateAfterInsert;
        tester.Execute();
    }

    Y_UNIT_TEST_TWIN(TSnapshotTwoUpdateOltp, UpdateAfterInsert) {
        TSnapshotTwoUpdate tester;
        tester.SetIsOlap(false);
        tester.SetDisableSinks(false);
        tester.SetUseRealThreads(false);
        tester.SetFillTables(false);
        tester.UpdateAfterInsert = UpdateAfterInsert;
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

    Y_UNIT_TEST(TReadOwnChangesOlap) {
        TReadOwnChanges tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TPragmaSetting : public TTableDataModificationTester {
    public:
        TPragmaSetting(std::string isolation)
            : Isolation(isolation) {}

    private:
        std::string Isolation;

    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            auto& runtime = *Kikimr->GetTestServer().GetRuntime();

            {
                std::vector<std::unique_ptr<IEventHandle>> writes;
                size_t evWriteCounter = 0;

                auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                    if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWrite::EventType) {
                        auto* evWrite = ev->Get<NKikimr::NEvents::TDataEvents::TEvWrite>();
                        UNIT_ASSERT(evWrite->Record.OperationsSize() <= 1);
                        if (evWrite->Record.OperationsSize() == 1 && evWriteCounter == 0) {
                            if (Isolation == "SnapshotRW" || GetIsOlap()) {
                                UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetStep() != 0);
                                UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetTxId() != 0);
                            } else {
                                UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetStep() == 0);
                                UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetTxId() == 0);
                            }

                            ++evWriteCounter;
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
                    return session1.ExecuteQuery(std::format(R"(
                        PRAGMA ydb.DefaultTxMode="{}";

                        SELECT * FROM `/Root/KV`;

                        UPSERT INTO `/Root/KV2` (Key, Value)
                        VALUES (1, "1");
                    )", Isolation), TTxControl::NoTx()).ExtractValueSync();
                });

                {
                    TDispatchOptions opts;
                    opts.FinalEvents.emplace_back([&](IEventHandle&) {
                        return evWriteCounter == 1;
                    });
                    runtime.DispatchEvents(opts);
                    UNIT_ASSERT(!GetIsOlap() || evWriteCounter == 1);
                    UNIT_ASSERT(writes.size() == 1);
                }

                {
                    // Another request changes data
                    auto insetResult = Kikimr->RunCall([&]{
                        return session2.ExecuteQuery(std::format(R"(
                            PRAGMA ydb.DefaultTxMode="{}";

                            SELECT * FROM `/Root/KV2`;

                            UPSERT INTO `/Root/KV2` (Key, Value)
                            VALUES (1, "other");
                        )", Isolation), TTxControl::NoTx()).ExtractValueSync();
                    });

                    UNIT_ASSERT_VALUES_EQUAL_C(insetResult.GetStatus(), EStatus::SUCCESS, insetResult.GetIssues().ToString());
                }

                UNIT_ASSERT(writes.size() == 1);

                for(auto& ev: writes) {
                    runtime.Send(ev.release());
                }

                {
                    auto result = runtime.WaitFuture(future);
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        result.GetStatus(),
                        Isolation == "SnapshotRW" ? EStatus::ABORTED : EStatus::SUCCESS,
                        result.GetIssues().ToString());
                }
            }
        }
    };

    Y_UNIT_TEST_TWIN(TPragmaSettingOltp, IsSnapshotIsolation) {
        TPragmaSetting tester(IsSnapshotIsolation ? "SnapshotRW" : "SerializableRW");
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    Y_UNIT_TEST_TWIN(TPragmaSettingOlap, IsSnapshotIsolation) {
        TPragmaSetting tester(IsSnapshotIsolation ? "SnapshotRW" : "SerializableRW");
        tester.SetIsOlap(true);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    class TUniqueSecondaryIndex : public TTableDataModificationTester {
    public:
        bool EnableIndexStreamWrite = false;

    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(EnableIndexStreamWrite);
        }

        void DoExecute() override {
            {
                auto client = Kikimr->GetTableClient();
                auto session = client.CreateSession().GetValueSync().GetSession();

                auto tableBuilder = client.GetTableBuilder();
                tableBuilder
                    .AddNullableColumn("Key", EPrimitiveType::String)
                    .AddNullableColumn("Value", EPrimitiveType::String);
                tableBuilder.SetPrimaryKeyColumns(std::vector<std::string>{"Key"});
                tableBuilder.AddUniqueSecondaryIndex("IndexUniq", {"Value"});
                auto result = session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            }

            auto client = Kikimr->GetQueryClient();
            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/TestTable`;
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/TestTable` (Key, Value)
                VALUES ("key", "value");
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session1.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/TestTable` (Key, Value)
                VALUES ("other key", "value");
            )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST_TWIN(TUniqueSecondaryIndexOltp, EnableIndexStreamWrite) {
        TUniqueSecondaryIndex tester;
        tester.EnableIndexStreamWrite = EnableIndexStreamWrite;
        tester.SetIsOlap(false);
        tester.SetFillTables(false);
        tester.Execute();
    }

    class TUniqueSecondaryWriteIndex : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            {
                auto client = Kikimr->GetTableClient();
                auto session = client.CreateSession().GetValueSync().GetSession();

                auto tableBuilder = client.GetTableBuilder();
                tableBuilder
                    .AddNullableColumn("Key", EPrimitiveType::String)
                    .AddNullableColumn("Value", EPrimitiveType::String);
                tableBuilder.SetPrimaryKeyColumns(std::vector<std::string>{"Key"});
                tableBuilder.AddUniqueSecondaryIndex("IndexUniq", {"Value"});
                auto result = session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            }

            auto client = Kikimr->GetQueryClient();
            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            auto result = session1.ExecuteQuery(Q_(R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                UPSERT INTO `/Root/TestTable` (Key, Value)
                VALUES ("other key", "value");
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);

            result = session2.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/TestTable` (Key, Value)
                VALUES ("key", "value");
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session1.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/TestTable`;
            )"), TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(TUniqueSecondaryWriteIndexOltp) {
        TUniqueSecondaryWriteIndex tester;
        tester.SetIsOlap(false);
        tester.SetFillTables(false);
        tester.Execute();
    }

    class TUpdateOrDeleteOnSnapshotCheck : public TTableDataModificationTester {
    public:
        YDB_ACCESSOR(bool, IsUpdate, true);

    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            auto& runtime = *Kikimr->GetTestServer().GetRuntime();

            {
                std::vector<std::unique_ptr<IEventHandle>> writes;

                auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                    if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWrite::EventType) {
                        auto* evWrite = ev->Get<NKikimr::NEvents::TDataEvents::TEvWrite>();
                        UNIT_ASSERT(evWrite->Record.OperationsSize() <= 1);
                        if (evWrite->Record.OperationsSize() == 1
                                && (evWrite->Record.GetOperations()[0].GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE
                                    || evWrite->Record.GetOperations()[0].GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE)) {
                            
                            UNIT_ASSERT(evWrite->Record.GetLockMode() == NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
                            UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetStep() != 0);
                            UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetTxId() != 0);

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

                // no writes (/Root/KV is empty) => tx commits successfully
                auto future = Kikimr->RunInThreadPool([&]{
                    return session1.ExecuteQuery(std::format(R"(
                        PRAGMA ydb.DefaultTxMode="SnapshotRW";

                        SELECT * FROM `/Root/KV`;

                        {}
                    )", GetIsUpdate()
                        ? R"(UPDATE `/Root/KV` ON (Key, Value) VALUES (1u, "one");)"
                        : R"(DELETE FROM `/Root/KV` ON (Key) VALUES (1u);)"),
                        TTxControl::NoTx()).ExtractValueSync();
                });

                {
                    TDispatchOptions opts;
                    opts.FinalEvents.emplace_back([&](IEventHandle&) {
                        return writes.size() == 1;
                    });
                    runtime.DispatchEvents(opts);
                    UNIT_ASSERT(writes.size() == 1);
                }

                {
                    // Another request changes data
                    auto insetResult = Kikimr->RunCall([&]{
                        return session2.ExecuteQuery(R"(
                            PRAGMA ydb.DefaultTxMode="SnapshotRW";

                            UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "two");
                        )", TTxControl::NoTx()).ExtractValueSync();
                    });

                    UNIT_ASSERT_VALUES_EQUAL_C(insetResult.GetStatus(), EStatus::SUCCESS, insetResult.GetIssues().ToString());
                }

                UNIT_ASSERT(writes.size() == 1);

                for(auto& ev: writes) {
                    runtime.Send(ev.release());
                }

                {
                    auto result = runtime.WaitFuture(future);
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        result.GetStatus(),
                        EStatus::ABORTED, // Must be EStatus::SUCCESS
                        result.GetIssues().ToString());
                }
            }
        }
    };

    Y_UNIT_TEST_TWIN(TUpdateOrDeleteOnSnapshotCheckOltp, IsUpdate) {
        TUpdateOrDeleteOnSnapshotCheck tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.SetFillTables(false);
        tester.SetIsUpdate(IsUpdate);
        tester.Execute();
    }

    Y_UNIT_TEST_TWIN(TUpdateOrDeleteOnSnapshotCheckOlap, IsUpdate) {
        TUpdateOrDeleteOnSnapshotCheck tester;
        tester.SetIsOlap(true);
        tester.SetUseRealThreads(false);
        tester.SetFillTables(false);
        tester.SetIsUpdate(IsUpdate);
        tester.Execute();
    }

    class TUpdateOrDeleteOnLocks : public TTableDataModificationTester {
    public:
        YDB_ACCESSOR(bool, IsUpdate, true);

    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            auto& runtime = *Kikimr->GetTestServer().GetRuntime();

            {
                std::vector<std::unique_ptr<IEventHandle>> writes;
                bool hasWrite = false;
                auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                    if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWrite::EventType) {
                        auto* evWrite = ev->Get<NKikimr::NEvents::TDataEvents::TEvWrite>();
                        UNIT_ASSERT(evWrite->Record.OperationsSize() <= 1);

                        // Data
                        if (evWrite->Record.OperationsSize() == 1
                                && (evWrite->Record.GetOperations()[0].GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE
                                    || evWrite->Record.GetOperations()[0].GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE)) {
                            
                            hasWrite = true;
                            UNIT_ASSERT(evWrite->Record.GetLockMode() == NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
                            UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetStep() != 0);
                            UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetTxId() != 0);
                            return TTestActorRuntime::EEventAction::PROCESS;
                        }

                        // Commit
                        if (writes.empty() && evWrite->Record.OperationsSize() == 0) {
                            // ColumnShard doesn't need info about snapshot/lockmode for PREPARE/COMMIT
                            UNIT_ASSERT(GetIsOlap() || evWrite->Record.GetLockMode() == NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
                            UNIT_ASSERT(GetIsOlap() || evWrite->Record.GetMvccSnapshot().GetStep() != 0);
                            UNIT_ASSERT(GetIsOlap() || evWrite->Record.GetMvccSnapshot().GetTxId() != 0);

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

                // no writes (/Root/KV is empty) => tx commits successfully
                auto future = Kikimr->RunInThreadPool([&]{
                    return session1.ExecuteQuery(std::format(R"(
                        PRAGMA ydb.DefaultTxMode="SnapshotRW";

                        {}

                        -- force flush writes
                        SELECT * FROM `/Root/KV`;
                    )", GetIsUpdate()
                        ? R"(UPDATE `/Root/KV` ON (Key, Value) VALUES (1u, "one");)"
                        : R"(DELETE FROM `/Root/KV` ON (Key) VALUES (1u);)"),
                        TTxControl::NoTx()).ExtractValueSync();
                });

                {
                    TDispatchOptions opts;
                    opts.FinalEvents.emplace_back([&](IEventHandle&) {
                        return writes.size() == 1;
                    });
                    runtime.DispatchEvents(opts);
                    UNIT_ASSERT(writes.size() == 1);
                    UNIT_ASSERT(hasWrite);
                }

                {
                    // Another request changes data
                    auto insetResult = Kikimr->RunCall([&]{
                        return session2.ExecuteQuery(R"(
                            PRAGMA ydb.DefaultTxMode="SnapshotRW";

                            UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "two");
                        )", TTxControl::NoTx()).ExtractValueSync();
                    });

                    UNIT_ASSERT_VALUES_EQUAL_C(insetResult.GetStatus(), EStatus::SUCCESS, insetResult.GetIssues().ToString());
                }

                UNIT_ASSERT(writes.size() == 1);

                for(auto& ev: writes) {
                    runtime.Send(ev.release());
                }

                {
                    auto result = runtime.WaitFuture(future);
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        result.GetStatus(),
                        EStatus::ABORTED, // Must be EStatus::SUCCESS
                        result.GetIssues().ToString());
                }
            }
        }
    };

    Y_UNIT_TEST_TWIN(TUpdateOrDeleteOnLocksOltp, IsUpdate) {
        TUpdateOrDeleteOnLocks tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.SetFillTables(false);
        tester.SetIsUpdate(IsUpdate);
        tester.Execute();
    }

    Y_UNIT_TEST_TWIN(TUpdateOrDeleteOnLocksOlap, IsUpdate) {
        TUpdateOrDeleteOnLocks tester;
        tester.SetIsOlap(true);
        tester.SetUseRealThreads(false);
        tester.SetFillTables(false);
        tester.SetIsUpdate(IsUpdate);
        tester.Execute();
    }
}

} // namespace NKqp
} // namespace NKikimr
