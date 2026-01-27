#include "kqp_sink_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpSinkTx) {
    class TDeferredEffects : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session = client.GetSession().GetValueSync().GetSession();
            auto result = session.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/Test`
                SELECT Group, "Sergey" AS Name
                FROM `/Root/Test`;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto tx = result.GetTransaction();

            result = session.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Group = 1 ORDER BY Name;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[3500u];["None"];1u;"Anna"];
                [[300u];["None"];1u;"Paul"]
            ])", FormatResultSetYson(result.GetResultSet(0)));

            auto commitResult = tx->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            result = session.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/Test` WHERE Group = 1 ORDER BY Name;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[3500u];["None"];1u;"Anna"];
                [[300u];["None"];1u;"Paul"];
                [#;#;1u;"Sergey"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(DeferredEffects) {
        TDeferredEffects tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapDeferredEffects) {
        TDeferredEffects tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TExplicitTcl : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session = client.GetSession().GetValueSync().GetSession();
            auto tx = session.BeginTransaction(TTxSettings::SerializableRW())
                .ExtractValueSync()
                .GetTransaction();
            UNIT_ASSERT(tx.IsActive());

            auto result = session.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES (10u, "New");
            )"), TTxControl::Tx(tx)).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());

            result = session.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV` WHERE Value = "New";
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

            auto commitResult = tx.Commit().ExtractValueSync();
            UNIT_ASSERT_C(commitResult.IsSuccess(), commitResult.GetIssues().ToString());

            result = session.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV` WHERE Value = "New";
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([[10u;["New"]]])", FormatResultSetYson(result.GetResultSet(0)));

            commitResult = tx.Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::NOT_FOUND, commitResult.GetIssues().ToString());
            UNIT_ASSERT_C(HasIssue(commitResult.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND), commitResult.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(ExplicitTcl) {
        TExplicitTcl tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapExplicitTcl) {
        TExplicitTcl tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TLocksAbortOnCommit : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session = client.GetSession().GetValueSync().GetSession();
            {
                auto result = session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (1, "One");
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (2, "Two");
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (3, "Three");
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (4, "Four");
                )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }


            auto result = session.ExecuteQuery(Q_(R"(
                SELECT * FROM `/Root/KV`;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto tx = result.GetTransaction();

            result = session.ExecuteQuery(Q_(R"(
                UPDATE `/Root/KV` SET Value = "second" WHERE Key = 3;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session.ExecuteQuery(Q_(R"(
                UPDATE `/Root/KV` SET Value = "third" WHERE Key = 4;
            )"), TTxControl::Tx(*tx)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto commitResult = tx->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::ABORTED, commitResult.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(LocksAbortOnCommit) {
        TLocksAbortOnCommit tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapLocksAbortOnCommit) {
        TLocksAbortOnCommit tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TInvalidateOnError : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session = client.GetSession().GetValueSync().GetSession();
            auto tx = session.BeginTransaction(TTxSettings::SerializableRW())
                .ExtractValueSync()
                .GetTransaction();
            UNIT_ASSERT(tx.IsActive());

            auto result = session.ExecuteQuery(Q_(R"(
                INSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "New");
            )"), TTxControl::Tx(tx)).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

            result = session.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "New");
            )"), TTxControl::Tx(tx)).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(InvalidateOnError) {
        TInvalidateOnError tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapInvalidateOnError) {
        TInvalidateOnError tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TInteractive : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session = client.GetSession().GetValueSync().GetSession();
            auto tx = session.BeginTransaction(TTxSettings::SerializableRW())
                .ExtractValueSync()
                .GetTransaction();
            UNIT_ASSERT(tx.IsActive());

            auto result = session.ExecuteQuery(R"(
                SELECT * FROM `/Root/KV`
            )", TTxControl::Tx(tx)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session.ExecuteQuery(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "New");
            )", TTxControl::Tx(tx).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session.ExecuteQuery(R"(
                SELECT * FROM `/Root/KV` WHERE Key < 3 ORDER BY Key
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [1u;["New"]];
                [2u;["Two"]]
                ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(Interactive) {
        TInteractive tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapInteractive) {
        TInteractive tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TSnapshotRO : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session = client.GetSession().GetValueSync().GetSession();
            // Read Immediate
            auto result = session.ExecuteQuery(Q1_(R"(
                SELECT * FROM KV WHERE Key = 2;
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[2u;["Two"]]])", FormatResultSetYson(result.GetResultSet(0)));

            // Read Distributed
            result = session.ExecuteQuery(Q1_(R"(
                SELECT COUNT(*) FROM KV WHERE Value = "One";
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u]])", FormatResultSetYson(result.GetResultSet(0)));

            // Write
            result = session.ExecuteQuery(Q1_(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES
                    (100, "100500"),
                    (100500, "100");
            )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));
        }
    };

    Y_UNIT_TEST(SnapshotRO) {
        TSnapshotRO tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapSnapshotRO) {
        TSnapshotRO tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TSnapshotROInteractive1 : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session = client.GetSession().GetValueSync().GetSession();
            auto readQuery = Q1_(R"(
                SELECT * FROM KV WHERE Key = 1u;
            )");

            auto readResult = R"([
                [1u;["One"]]
            ])";

            auto result = session.ExecuteQuery(readQuery,
                TTxControl::BeginTx(TTxSettings::SnapshotRO())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(readResult, FormatResultSetYson(result.GetResultSet(0)));

            auto tx = result.GetTransaction();
            UNIT_ASSERT(tx);
            UNIT_ASSERT(tx->IsActive());

            result = session.ExecuteQuery(Q1_(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES
                    (1u, "value");
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session.ExecuteQuery(readQuery,
                TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(readResult, FormatResultSetYson(result.GetResultSet(0)));
        }
    };

    Y_UNIT_TEST(SnapshotROInteractive1) {
        TSnapshotROInteractive1 tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapSnapshotROInteractive1) {
        TSnapshotROInteractive1 tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TSnapshotROInteractive2 : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            auto session = client.GetSession().GetValueSync().GetSession();
            auto readQuery = Q1_(R"(
                SELECT COUNT(*) FROM KV WHERE Value = "One";
            )");

            auto readResult = R"([
                [1u]
            ])";

            auto tx = session.BeginTransaction(TTxSettings::SnapshotRO())
                .ExtractValueSync()
                .GetTransaction();
            UNIT_ASSERT(tx.IsActive());

            auto result = session.ExecuteQuery(readQuery,
                TTxControl::Tx(tx)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(readResult, FormatResultSetYson(result.GetResultSet(0)));

            result = session.ExecuteQuery(Q1_(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES
                    (100500u, "One");
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = session.ExecuteQuery(readQuery,
                TTxControl::Tx(tx)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(readResult, FormatResultSetYson(result.GetResultSet(0)));

            auto commitResult = tx.Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(SnapshotROInteractive2) {
        TSnapshotROInteractive2 tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(OlapSnapshotROInteractive2) {
        TSnapshotROInteractive2 tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TIsolationSetting : public TTableDataModificationTester {
    public:
        TIsolationSetting(std::string isolation, bool usePragma)
            : Isolation(isolation)
            , UsePragma(usePragma) {}

    private:
        std::string Isolation;
        bool UsePragma;

    protected:
        void Setup(TKikimrSettings& settings) override {   
            if (!UsePragma) {
                settings.AppConfig.MutableTableServiceConfig()->SetDefaultTxMode([&]() {
                    if (Isolation == "SerializableRW") {
                        return NKikimrConfig::TTableServiceConfig::SerializableRW;
                    } else if (Isolation == "SnapshotRW") {
                        return NKikimrConfig::TTableServiceConfig::SnapshotRW;
                    } else if (Isolation == "SnapshotRO") {
                        return NKikimrConfig::TTableServiceConfig::SnapshotRO;
                    } else if (Isolation == "StaleRO") {
                        return NKikimrConfig::TTableServiceConfig::StaleRO;
                    } else {
                        ythrow yexception() << "unknonw isolation: " << Isolation;
                    }
                }());     
            }
        }

        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            auto& runtime = *Kikimr->GetTestServer().GetRuntime();

            {
                std::vector<std::unique_ptr<IEventHandle>> writes;
                size_t evWriteCounter = 0;

                auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                    if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWrite::EventType) {
                        auto* evWrite = ev->Get<NKikimr::NEvents::TDataEvents::TEvWrite>();
                        UNIT_ASSERT(evWrite->Record.OperationsSize() <= 1);
                        if (evWrite->Record.OperationsSize() == 1 ) {
                            ++evWriteCounter;
                            if (Isolation == "SnapshotRW" || GetIsOlap()) {
                                UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetStep() != 0);
                                UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetTxId() != 0);
                            } else {
                                UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetStep() == 0);
                                UNIT_ASSERT(evWrite->Record.GetMvccSnapshot().GetTxId() == 0);
                            }
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
                        {}

                        SELECT * FROM `/Root/KV` WHERE Key = 1;

                        UPSERT INTO `/Root/KV2` (Key, Value)
                        VALUES (1, "1");
                    )", UsePragma
                        ? std::format(R"(PRAGMA ydb.DefaultTxMode="{}";)", Isolation)
                        : std::string{}),
                    TTxControl::NoTx()).ExtractValueSync();
                });

                auto result = runtime.WaitFuture(future);
                if (Isolation == "SerializableRW" || Isolation == "SnapshotRW") {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        result.GetStatus(),
                        EStatus::SUCCESS,
                        result.GetIssues().ToString());

                    UNIT_ASSERT(evWriteCounter == 1);
                } else if (Isolation == "SnapshotRO" || (!GetIsOlap() && Isolation == "StaleRO")) {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        result.GetStatus(),
                        EStatus::GENERIC_ERROR,
                        result.GetIssues().ToString());
                    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "can't be performed in read only transaction");
                } else if (GetIsOlap() && Isolation == "StaleRO") {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        result.GetStatus(),
                        EStatus::PRECONDITION_FAILED,
                        result.GetIssues().ToString());
                    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Read from column-oriented tables is not supported in Online Read-Only or Stale Read-Only transaction modes");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        result.GetStatus(),
                        EStatus::GENERIC_ERROR,
                        result.GetIssues().ToString());
                    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown DefaultTxMode");
                }
            }
        }
    };

    Y_UNIT_TEST_QUAD(TIsolationSettingTest, IsOlap, UsePragma) {
        for (const std::string isolation : {"SerializableRW", "SnapshotRW", "SnapshotRO", "StaleRO", "OnlineRO"}) {
            if (isolation == "OnlineRO" && !UsePragma) {
                continue;
            }

            TIsolationSetting tester(isolation, UsePragma);
            tester.SetIsOlap(IsOlap);
            tester.SetFillTables(false);
            tester.SetUseRealThreads(false);
            tester.Execute();
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
