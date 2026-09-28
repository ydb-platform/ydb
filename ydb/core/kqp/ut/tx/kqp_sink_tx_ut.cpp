#include "kqp_sink_common.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/runtime/kqp_write_actor_settings.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

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

    class TDisableOnlineRO : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetDisableOnlineRO(true);
        }

        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session = client.GetSession().GetValueSync().GetSession();

            {
                auto result = session.ExecuteQuery(Q_(R"(
                    SELECT * FROM `/Root/KV` ;
                )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
                result.GetIssues().PrintTo(Cerr);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            {
                auto result = session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "New");
                )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
                result.GetIssues().PrintTo(Cerr);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Operation 'Upsert' can't be performed in read only transaction");
            }
        }
    };

    Y_UNIT_TEST(DisableOnlineRO) {
        TDisableOnlineRO tester;
        tester.SetIsOlap(false);
        tester.SetFillTables(true);
        tester.Execute();
    }

    // Reads between upserts force a chain of uncommitted writes; results must be identical
    // whether or not KQP attaches WriteSeqNum.
    class TUncommittedWriteSeqNum : public TTableDataModificationTester {
    protected:
        YDB_ACCESSOR(bool, Enabled, true);
        YDB_ACCESSOR(TString, Table, "/Root/KV");

        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetEnableDataShardUncommittedWriteSeqNum(Enabled);
        }

        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session = client.GetSession().GetValueSync().GetSession();

            auto tx = session.BeginTransaction(TTxSettings::SerializableRW())
                .ExtractValueSync()
                .GetTransaction();

            {
                auto result = session.ExecuteQuery(Sprintf(R"(
                    UPSERT INTO `%s` (Key, Value) VALUES (10u, "Ten"), (4000000010u, "BigTen");
                )", Table.c_str()), TTxControl::Tx(tx)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            // Forces the first flush; must observe the transaction's own writes
            {
                auto result = session.ExecuteQuery(Sprintf(R"(
                    SELECT Key, Value FROM `%s` WHERE Key IN (10u, 4000000010u) ORDER BY Key;
                )", Table.c_str()), TTxControl::Tx(tx)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[10u;["Ten"]];[4000000010u;["BigTen"]]])",
                    FormatResultSetYson(result.GetResultSet(0)));
            }

            {
                auto result = session.ExecuteQuery(Sprintf(R"(
                    UPSERT INTO `%s` (Key, Value) VALUES (11u, "Eleven");
                )", Table.c_str()), TTxControl::Tx(tx)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            // The second flush is chained onto the first
            {
                auto result = session.ExecuteQuery(Sprintf(R"(
                    SELECT Key, Value FROM `%s` WHERE Key IN (10u, 11u) ORDER BY Key;
                )", Table.c_str()), TTxControl::Tx(tx)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[10u;["Ten"]];[11u;["Eleven"]]])",
                    FormatResultSetYson(result.GetResultSet(0)));
            }

            auto commitResult = tx.Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            {
                auto result = session.ExecuteQuery(Sprintf(R"(
                    SELECT Key, Value FROM `%s` WHERE Key IN (10u, 11u, 4000000010u) ORDER BY Key;
                )", Table.c_str()), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[10u;["Ten"]];[11u;["Eleven"]];[4000000010u;["BigTen"]]])",
                    FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    };

    // Keys 10 and 4000000010 land on different shards, so the commit goes through ValidateLocks.
    Y_UNIT_TEST_TWIN(UncommittedWriteSeqNum, Enabled) {
        TUncommittedWriteSeqNum tester;
        tester.SetEnabled(Enabled);
        tester.SetIsOlap(false);
        tester.Execute();
    }

    // KQP must skip ColumnShard, which does not implement WriteSeqNum
    Y_UNIT_TEST(UncommittedWriteSeqNumOlap) {
        TUncommittedWriteSeqNum tester;
        tester.SetEnabled(true);
        tester.SetIsOlap(true);
        tester.Execute();
    }

// A resent uncommitted write is answered twice: with the original result and, once the
    // shard has seen it, with IsDuplicate set. KQP does not inspect IsDuplicate: the write
    // actor drops the second reply as already acknowledged (dedup by message cookie).
    class TUncommittedWriteSeqNumAnsweredTwice : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetEnableDataShardUncommittedWriteSeqNum(true);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session = Kikimr->RunCall([&] {
                return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::SerializableRW())
                    .ExtractValueSync().GetTransaction(); });

            size_t answeredTwice = 0;
            auto answerTwice = [&](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
                    const auto& record = ev->Get<NEvents::TDataEvents::TEvWriteResult>()->Record;
                    if (record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED
                        && record.GetTxLocks().size() == 1
                        && record.GetTxLocks(0).WriteSeqNumsSize() == 1)
                    {
                        auto again = std::make_unique<NEvents::TDataEvents::TEvWriteResult>();
                        again->Record = record;
                        again->Record.SetIsDuplicate(true);
                        runtime.Send(new IEventHandle(ev->GetRecipientRewrite(), ev->Sender,
                            again.release(), 0, ev->Cookie));
                        ++answeredTwice;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };
            auto saveObserver = runtime.SetObserverFunc(answerTwice);

            {
                auto result = Kikimr->RunCall([&] { return session.ExecuteQuery(R"(
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (10u, "Ten");
                )", TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            // Forces the flush, whose result is then delivered a second time
            {
                auto result = Kikimr->RunCall([&] { return session.ExecuteQuery(R"(
                    SELECT Key, Value FROM `/Root/KV` WHERE Key = 10u;
                )", TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[10u;["Ten"]]])", FormatResultSetYson(result.GetResultSet(0)));
            }

            runtime.SetObserverFunc(saveObserver);
            UNIT_ASSERT_C(answeredTwice > 0, answeredTwice);

            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            {
                auto result = Kikimr->RunCall([&] { return session.ExecuteQuery(R"(
                    SELECT Key, Value FROM `/Root/KV` WHERE Key = 10u;
                )", TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[10u;["Ten"]]])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumAnsweredTwice) {
        TUncommittedWriteSeqNumAnsweredTwice tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A shard echo whose WriteSeqNum for a known writer regresses below what KQP already
    // recorded means the shard's uncommitted write chain collapsed underneath us. KQP must
    // treat that as a broken lock (abort), not crash or silently ignore it.
    class TUncommittedWriteSeqNumRegression : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetEnableDataShardUncommittedWriteSeqNum(true);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();
            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync().GetTransaction(); });

            // First flush establishes KQP's stored write seq num for the shard's lock.
            {
                auto result = Kikimr->RunCall([&] { return session.ExecuteQuery(R"(
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (10u, "Ten");
                    SELECT Key, Value FROM `/Root/KV` WHERE Key = 10u;
                )", TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            // Regress the write seq num of the next completed write result: the key is on the
            // same shard/lock as the first flush, so KQP already stored seq 1 and will see the
            // injected 0 as a regression below it.
            bool regressed = false;
            auto regressSeqNum = [&](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
                    auto& record = ev->Get<NEvents::TDataEvents::TEvWriteResult>()->Record;
                    if (record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED
                        && record.GetTxLocks().size() == 1
                        && record.GetTxLocks(0).WriteSeqNumsSize() == 1)
                    {
                        auto* lock = record.MutableTxLocks(0);
                        lock->MutableWriteSeqNums(0)->SetWriteSeqNum(0);
                        regressed = true;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };
            auto saveObserver = runtime.SetObserverFunc(regressSeqNum);

            // The second flush to the same shard must abort with locks invalidated.
            auto result = Kikimr->RunCall([&] { return session.ExecuteQuery(R"(
                UPSERT INTO `/Root/KV` (Key, Value) VALUES (11u, "Eleven");
                SELECT Key, Value FROM `/Root/KV` WHERE Key IN (10u, 11u);
            )", TTxControl::Tx(tx)).ExtractValueSync(); });

            runtime.SetObserverFunc(saveObserver);
            UNIT_ASSERT_C(regressed, "expected a completed write result carrying a WriteSeqNum");
            UNIT_ASSERT_C(result.GetStatus() == EStatus::ABORTED
                    || result.GetStatus() == EStatus::GENERIC_ERROR,
                TStringBuilder() << result.GetStatus() << ": " << result.GetIssues().ToString());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED),
                result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumRegression) {
        TUncommittedWriteSeqNumRegression tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A shard reboot (stand-in for a move) while uncommitted writes are in flight
    // must not break the transaction: the new generation restores the writer
    // chain from the locks table and KQP resumes the chain from the restored
    // seq num. The commit must apply every row exactly once.
    class TUncommittedWriteSeqNumRebootBetweenFlushes : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetEnableDataShardUncommittedWriteSeqNum(true);
            // Shorten the paced retries so the delivery problem after the reboot is
            // handled in a bounded, fast way instead of a full default backoff.
            auto& writeActorSettings = *settings.AppConfig.MutableTableServiceConfig()->MutableWriteActorSettings();
            writeActorSettings.SetStartRetryDelayMs(100);
            writeActorSettings.SetMaxRetryDelayMs(1000);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/KV");
            UNIT_ASSERT_C(!shards.empty(), "expected /Root/KV to have shards");
            // Key 10 is in the first uniform partition; 4000000010 is in a far one.
            const ui64 rebootedShard = shards[0];

            auto client = Kikimr->GetQueryClient();
            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync().GetTransaction(); });

            // Drop the completed write result of the shard we are about to reboot,
            // so that shard's batch stays in flight when the tablet dies.
            bool dropped = false;
            auto observer = [&](TAutoPtr<IEventHandle>& ev) {
                if (!dropped && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
                    const auto& record = ev->Get<NEvents::TDataEvents::TEvWriteResult>()->Record;
                    if (record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED
                        && record.GetOrigin() == rebootedShard)
                    {
                        dropped = true;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };
            auto saveObserver = runtime.SetObserverFunc(observer);

            // Flush uncommitted writes to /Root/KV; the rebooted shard's result is dropped.
            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(Sprintf(R"(
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (10u, "Ten"), (4000000010u, "BigTen");
                    SELECT Key, Value FROM `/Root/KV` WHERE Key IN (10u, 4000000010u) ORDER BY Key;
                )"), TTxControl::Tx(tx)).ExtractValueSync(); });
            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) { return dropped; });
                runtime.DispatchEvents(opts);
            }
            UNIT_ASSERT(dropped);

            // Restart the shard while its write is still unacknowledged.
            RebootTablet(runtime, rebootedShard, edgeActor);

            // KQP gets a delivery problem, retries the batch; the new generation
            // restores the seq num chain and answers exactly once. The retry settings
            // keep the paced backoff short, so a bounded wait is enough.
            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[10u;["Ten"]];[4000000010u;["BigTen"]]])",
                FormatResultSetYson(result.GetResultSet(0)));

            runtime.SetObserverFunc(saveObserver);

            // Another flush chains a new write onto the restored seq num.
            {
                auto next = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Sprintf(R"(
                        UPSERT INTO `/Root/KV` (Key, Value) VALUES (11u, "Eleven");
                    )"), TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(next.GetStatus(), EStatus::SUCCESS, next.GetIssues().ToString());
            }
            {
                auto next = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Sprintf(R"(
                        SELECT Key, Value FROM `/Root/KV` WHERE Key IN (10u, 11u, 4000000010u) ORDER BY Key;
                    )"), TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(next.GetStatus(), EStatus::SUCCESS, next.GetIssues().ToString());
                CompareYson(R"([[10u;["Ten"]];[11u;["Eleven"]];[4000000010u;["BigTen"]]])",
                    FormatResultSetYson(next.GetResultSet(0)));
            }

            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Sprintf(R"(
                        SELECT Key, Value FROM `/Root/KV` WHERE Key IN (10u, 11u, 4000000010u) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(R"([[10u;["Ten"]];[11u;["Eleven"]];[4000000010u;["BigTen"]]])",
                    FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumRebootBetweenFlushes) {
        TUncommittedWriteSeqNumRebootBetweenFlushes tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A consistent (seq-num) write whose batch is permanently rejected by one shard must not
    // retry forever: KQP resends a bounded number of times per resolve round, re-resolves with
    // backoff, and after a bounded number of consecutive re-resolves caused by that shard fails
    // the query with UNAVAILABLE (deterministic) instead of hanging the transaction.
    class TUncommittedWriteSeqNumPersistentWrongShardState : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetEnableDataShardUncommittedWriteSeqNum(true);
            // Shrink the shard-retry backoff so the paced retries (1s,2s,4s,... by default)
            // don't dominate the test wall-clock time. With 100ms..1s delays a full retry
            // round takes ~2.5s and all 5 re-resolve rounds fit well under the wait below.
            auto& writeActorSettings = *settings.AppConfig.MutableTableServiceConfig()->MutableWriteActorSettings();
            writeActorSettings.SetStartRetryDelayMs(100);
            writeActorSettings.SetMaxRetryDelayMs(1000);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/KV");
            UNIT_ASSERT_C(!shards.empty(), "expected /Root/KV to have shards");
            // Key 10 is in the first uniform partition.
            const ui64 stuckShard = shards[0];

            auto client = Kikimr->GetQueryClient();
            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync().GetTransaction(); });

            // Permanently rewrite COMPLETED results from the stuck shard into WRONG_SHARD_STATE,
            // so every resend is rejected again and the shard never acknowledges the batch.
            std::atomic<size_t> rejected{0};
            auto rejectResult = [&](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
                    auto& record = ev->Get<NEvents::TDataEvents::TEvWriteResult>()->Record;
                    if (record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED
                        && record.GetOrigin() == stuckShard)
                    {
                        record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE);
                        ++rejected;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };
            auto saveObserver = runtime.SetObserverFunc(rejectResult);

            // The flush to the stuck shard is rejected on every resend; the write actor retries a
            // bounded number of attempts per resolve round and then fails the query with UNAVAILABLE.
            // The SELECT after the UPSERT forces the flush inside the same query.
            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (10u, "Ten");
                    SELECT Key, Value FROM `/Root/KV` WHERE Key = 10u ORDER BY Key;
                )"), TTxControl::Tx(tx)).ExtractValueSync(); });
            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));

            runtime.SetObserverFunc(saveObserver);
            UNIT_ASSERT_C(rejected.load() >= 5,
                "expected several rejected resends before the retry bound was exhausted");
            UNIT_ASSERT_C(result.GetStatus() == EStatus::UNAVAILABLE || result.GetStatus() == EStatus::GENERIC_ERROR,
                TStringBuilder() << result.GetStatus() << ": " << result.GetIssues().ToString());
            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE),
                result.GetIssues().ToString());

            // The cluster stays usable: a fresh session read succeeds.
            auto checkSession = Kikimr->RunCall([&] {
                return client.GetSession().GetValueSync().GetSession(); });
            auto check = Kikimr->RunCall([&] {
                return checkSession.ExecuteQuery(Q_(R"(
                    SELECT Key, Value FROM `/Root/KV` WHERE Key = 10u;
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumPersistentWrongShardState) {
        TUncommittedWriteSeqNumPersistentWrongShardState tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // ALTER TABLE during an in-flight InconsistentTx write must fail the query, not retry forever.
    class TSchemeChangedDuringInconsistentWrite : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            // UseRealThreads=false requires GetSession() through RunCall.
            auto session1 = Kikimr->RunCall([&] {
                return client.GetSession().GetValueSync().GetSession();
            });
            auto session2 = Kikimr->RunCall([&] {
                return client.GetSession().GetValueSync().GetSession();
            });

            std::atomic<size_t> evWriteCount{0};
            std::vector<std::unique_ptr<IEventHandle>> held;
            bool queryRequestPatched = false;

            auto grab = [&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
                // IsStreamingQuery=true makes the sink compile with InconsistentTx=true.
                if (!queryRequestPatched &&
                    ev->GetTypeRewrite() == TEvKqp::TEvQueryRequest::EventType)
                {
                    queryRequestPatched = true;
                    auto* req = ev->Get<TEvKqp::TEvQueryRequest>();
                    auto userCtx = MakeIntrusive<TUserRequestContext>("", "/Root", "");
                    userCtx->IsStreamingQuery = true;
                    req->SetUserRequestContext(std::move(userCtx));
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
                if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWrite::EventType) {
                    ++evWriteCount;
                    held.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };

            auto savedObserver = runtime.SetObserverFunc(grab);
            Y_DEFER { runtime.SetObserverFunc(savedObserver); };

            auto future = Kikimr->RunInThreadPool([&] {
                return session1.ExecuteQuery(
                    Q_(R"(UPSERT INTO `/Root/KV` (Key, Value) VALUES (42u, "test");)"),
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()
                ).ExtractValueSync();
            });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return evWriteCount > 0;
                });
                runtime.DispatchEvents(opts);
                UNIT_ASSERT_C(evWriteCount > 0, "TEvWrite was not intercepted");
            }

            auto alterResult = Kikimr->RunCall([&] {
                return session2.ExecuteQuery(
                    Q_(R"(ALTER TABLE `/Root/KV` ADD COLUMN Extra String;)"),
                    TTxControl::NoTx()
                ).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(
                alterResult.GetStatus(), EStatus::SUCCESS,
                alterResult.GetIssues().ToString());

            for (auto& ev : held) {
                runtime.Send(ev.release());
            }
            held.clear();

            auto result = runtime.WaitFuture(future, TDuration::Seconds(30));
            UNIT_ASSERT_VALUES_EQUAL_C(
                result.GetStatus(), EStatus::ABORTED,
                result.GetIssues().ToString());
            UNIT_ASSERT_C(
                result.GetIssues().ToString().contains("Scheme changed"),
                TStringBuilder() << "Expected scheme-mismatch issue, got: "
                    << result.GetIssues().ToString());
        }
    };

    Y_UNIT_TEST(SchemeChangedDuringInconsistentWrite) {
        TSchemeChangedDuringInconsistentWrite tester;
        tester.SetIsOlap(false);
        tester.SetFillTables(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    static void SetSplitMergePartCountLimit(TTestActorRuntime* runtime, i64 val) {
        TControlBoard::SetValue(val, runtime->GetAppData().Icb->SchemeShardControls.SplitMergePartCountLimit);
    }

    static ui64 AsyncSplitTable(
            Tests::TServer& server,
            TActorId sender,
            const TString& path,
            ui64 sourceTablet,
            ui64 splitKey)
    {
        auto& runtime = *server.GetRuntime();
        // A freshly-created table's shards may not have reported Ready to the
        // scheme shard yet; re-propose the split until it is accepted.
        for (ui32 attempt = 0; attempt < 120; ++attempt) {
            auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            request->Record.SetExecTimeoutPeriod(Max<ui64>());

            auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);

            auto& desc = *tx.MutableSplitMergeTablePartitions();
            desc.SetTablePath(path);
            desc.AddSourceTabletId(sourceTablet);
            desc.AddSplitBoundary()->MutableKeyPrefix()->AddTuple()->MutableOptional()->SetUint32(splitKey);

            runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
            if (ev->Get()->Record.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
                return ev->Get()->Record.GetTxId();
            }
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([](IEventHandle&) { return false; });
            runtime.DispatchEvents(opts, TDuration::MilliSeconds(50));
        }
        ythrow yexception() << "split was not accepted within the retry budget";
    }

    static void WaitTxNotification(Tests::TServer& server, TActorId sender, ui64 txId) {
        auto& runtime = *server.GetRuntime();
        auto& settings = server.GetSettings();

        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        request->Record.SetTxId(txId);
        auto tid = NKikimr::Tests::ChangeStateStorage(NKikimr::Tests::SchemeRoot, settings.Domain);
        runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries());
        runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
    }

    static ui64 AsyncMergeTable(
            Tests::TServer& server,
            TActorId sender,
            const TString& path,
            const TVector<ui64>& sourceTabletIds)
    {
        auto& runtime = *server.GetRuntime();
        // A freshly-created table's shards may not have reported Ready to the
        // scheme shard yet; re-propose the merge until it is accepted.
        for (ui32 attempt = 0; attempt < 120; ++attempt) {
            auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            request->Record.SetExecTimeoutPeriod(Max<ui64>());

            auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);

            auto& desc = *tx.MutableSplitMergeTablePartitions();
            desc.SetTablePath(path);
            for (const ui64 tabletId : sourceTabletIds) {
                desc.AddSourceTabletId(tabletId);
            }

            runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
            if (ev->Get()->Record.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
                return ev->Get()->Record.GetTxId();
            }
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([](IEventHandle&) { return false; });
            runtime.DispatchEvents(opts, TDuration::MilliSeconds(50));
        }
        ythrow yexception() << "merge was not accepted within the retry budget";
    }

    // Common setup for the uncommitted-write (WriteSeqNum) split/merge tests: enable
    // the WriteSeqNum protocol and the split lock transfer, keep the paced write
    // retries short so the re-resolve + re-route rounds stay fast.
    static void SetupUncommittedWriteSeqNum(TKikimrSettings& settings) {
        settings.AppConfig.MutableFeatureFlags()->SetEnableDataShardUncommittedWriteSeqNum(true);
        // Split/merge must transfer the tx's write-only locks (with their WriteSeqNum
        // chains) to the new shards: the re-routed batches carry OriginalShard and
        // validate against the transferred ancestor chains.
        settings.AppConfig.MutableFeatureFlags()->SetEnableDataShardLocksTransferOnSplit(true);
        auto& writeActorSettings = *settings.AppConfig.MutableTableServiceConfig()->MutableWriteActorSettings();
        writeActorSettings.SetStartRetryDelayMs(100);
        writeActorSettings.SetMaxRetryDelayMs(1000);
    }

    // "UPSERT INTO `<table>` (Key, Value) VALUES (1u, "V1"), ..., (<maxKey>, "V<maxKey>"),
    // (3000000000u, "Big")" — the Big key belongs to the second shard of the two-shard
    // tables used by the tests below.
    static TString BuildKvUpsertQuery(const TString& table, const ui32 maxKey) {
        TStringBuilder upsert;
        upsert << "UPSERT INTO `" << table << "` (Key, Value) VALUES ";
        for (ui32 key = 1; key <= maxKey; ++key) {
            if (key > 1) {
                upsert << ", ";
            }
            upsert << "(" << key << "u, \"V" << key << "\")";
        }
        upsert << ", (3000000000u, \"Big\")";
        return upsert;
    }

    // The expected yson of "SELECT Key, Value ... WHERE Key IN (1..maxKey, Big)
    // ORDER BY Key": [[1u;["V1"]];...;[maxKey;["V<maxKey>"]];[3000000000u;["Big"]]],
    // with optional per-key value overrides.
    static TString BuildExpectedKvYson(const ui32 maxKey, const THashMap<ui32, TString>& valueOverrides = {}) {
        TStringBuilder yson;
        yson << "[";
        for (ui32 key = 1; key <= maxKey; ++key) {
            if (key > 1) {
                yson << ";";
            }
            TString value = TStringBuilder() << "V" << key;
            const auto overrideIt = valueOverrides.find(key);
            if (overrideIt != valueOverrides.end()) {
                value = overrideIt->second;
            }
            yson << "[" << key << "u;[\"" << value << "\"]]";
        }
        yson << ";[3000000000u;[\"Big\"]]]";
        return yson;
    }

    // Wire-level observation state for the uncommitted-write (WriteSeqNum) split/merge
    // tests: captures TEvWrite operations (WriteSeqNum + OriginalShard per
    // destination), prepare messages (attached ancestor locks, ReceivingShards) and
    // duplicate TEvWriteResults; optionally suppresses the results of chosen tablets
    // (the write stays applied on the shard, the batch stays pending) and holds the
    // first data EvWrite to chosen tablets instead of delivering it. Installed as the
    // runtime observer for the whole test; the test thread reads the captures under
    // the same mutex. InitialShards / SuppressResultsFrom / CookieTablet must be
    // configured before the observer is installed and are not modified afterwards;
    // HoldWritesTo may be modified later under Mutex. The write actor's SelfId is
    // captured from the first observed data EvWrite.
    struct TUncommittedWriteWire {
        mutable TMutex Mutex;
        // Tablets existing before the partitioning change; data EvWrites to any other
        // tablet are counted in NewShardSends.
        THashSet<ui64> InitialShards;
        // Hide every TEvWriteResult of these tablets.
        THashSet<ui64> SuppressResultsFrom;
        // Hold the first data EvWrite to a tablet from this set instead of delivering
        // it; the test re-delivers the held event later.
        THashSet<ui64> HoldWritesTo;
        std::unique_ptr<IEventHandle> HeldWrite;
        ui64 HeldWriteTablet = 0;
        // Track the send cookies of data EvWrites to this tablet (surviving-shard
        // re-send check); Max<ui64>() disables the tracking.
        ui64 CookieTablet = Max<ui64>();
        THashSet<ui64> ObservedCookies;

        std::atomic<ui64> NewShardSends{0};
        std::atomic<ui64> SuppressedAckCompleted{0};

        // The SelfId of the write actor that sent the first observed data EvWrite
        // (the Sender of the TEvPipeCache::TEvForward to the pipe cache).
        TActorId WriteActorId;

        TActorId WriteActorIdSnapshot() const {
            with_lock(Mutex) {
                return WriteActorId;
            }
        }

        // (destination, WriteSeqNum, OriginalShard) of every data-mode operation.
        TVector<std::tuple<ui64, ui64, ui64>> WriteOps;
        // Tablets that answered with IsDuplicate=true.
        THashSet<ui64> DuplicateResponders;
        // Prepare destination -> DataShards of the attached ancestor locks.
        THashMap<ui64, THashSet<ui64>> PrepareAncestorLocks;
        // Prepare destination -> the ReceivingShards list of the prepare message.
        THashMap<ui64, THashSet<ui64>> PrepareReceivingShards;

        TTestActorRuntime::EEventAction Observe(TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPipeCache::EvForward) {
                auto* fwd = ev->Get<TEvPipeCache::TEvForward>();
                if (fwd->Ev && fwd->Ev->Type() == NEvents::TDataEvents::TEvWrite::EventType) {
                    return ObserveWrite(ev, *fwd);
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            }
            if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
                return ObserveWriteResult(ev);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        }

        TVector<std::tuple<ui64, ui64, ui64>> WriteOpsSnapshot() const {
            TVector<std::tuple<ui64, ui64, ui64>> result;
            with_lock(Mutex) {
                result = WriteOps;
            }
            return result;
        }

        THashSet<ui64> DuplicateRespondersSnapshot() const {
            THashSet<ui64> result;
            with_lock(Mutex) {
                result = DuplicateResponders;
            }
            return result;
        }

        THashSet<ui64> ObservedCookiesSnapshot() const {
            THashSet<ui64> result;
            with_lock(Mutex) {
                result = ObservedCookies;
            }
            return result;
        }

        THashMap<ui64, THashSet<ui64>> PrepareAncestorsSnapshot() const {
            THashMap<ui64, THashSet<ui64>> result;
            with_lock(Mutex) {
                result = PrepareAncestorLocks;
            }
            return result;
        }

        THashMap<ui64, THashSet<ui64>> PrepareReceivingSnapshot() const {
            THashMap<ui64, THashSet<ui64>> result;
            with_lock(Mutex) {
                result = PrepareReceivingShards;
            }
            return result;
        }

    private:
        TTestActorRuntime::EEventAction ObserveWrite(TAutoPtr<IEventHandle>& ev, TEvPipeCache::TEvForward& fwd) {
            // The type of the inner event was checked by the caller.
            const auto& record = static_cast<const NEvents::TDataEvents::TEvWrite&>(*fwd.Ev).Record;
            const bool isPrepare = record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_PREPARE
                || record.GetTxMode() == NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE;
            if (isPrepare) {
                with_lock(Mutex) {
                    auto& ancestors = PrepareAncestorLocks[fwd.TabletId];
                    for (const auto& lock : record.GetLocks().GetLocks()) {
                        if (lock.GetDataShard() != fwd.TabletId) {
                            ancestors.insert(lock.GetDataShard());
                        }
                    }
                    auto& receiving = PrepareReceivingShards[fwd.TabletId];
                    for (const ui64 shardId : record.GetLocks().GetReceivingShards()) {
                        receiving.insert(shardId);
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            if (!InitialShards.contains(fwd.TabletId)) {
                NewShardSends++;
            }
            bool hold = false;
            with_lock(Mutex) {
                if (WriteActorId == TActorId()) {
                    WriteActorId = ev->Sender;
                }
                for (const auto& op : record.GetOperations()) {
                    WriteOps.emplace_back(
                        fwd.TabletId,
                        op.GetWriteSeqNum().GetWriteSeqNum(),
                        op.GetOriginalShard());
                }
                if (CookieTablet == fwd.TabletId) {
                    ObservedCookies.insert(ev->Cookie);
                }
                hold = HoldWritesTo.contains(fwd.TabletId) && !HeldWrite;
                if (hold) {
                    HeldWrite.reset(ev.Release());
                    HeldWriteTablet = fwd.TabletId;
                }
            }
            return hold
                ? TTestActorRuntime::EEventAction::DROP
                : TTestActorRuntime::EEventAction::PROCESS;
        }

        TTestActorRuntime::EEventAction ObserveWriteResult(TAutoPtr<IEventHandle>& ev) {
            const auto& record = ev->Get<NEvents::TDataEvents::TEvWriteResult>()->Record;
            const ui64 origin = record.GetOrigin();
            bool suppress = false;
            with_lock(Mutex) {
                suppress = SuppressResultsFrom.contains(origin);
            }
            if (suppress) {
                if (record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
                    SuppressedAckCompleted++;
                }
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (record.GetIsDuplicate()) {
                with_lock(Mutex) {
                    DuplicateResponders.insert(origin);
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        }
    };

    // A partition change (split) during an in-flight Inconsistent (streaming) write
    // must affect only the removed and the resulting new shards. The old shard's
    // pending batches are re-routed to the new shards; the in-flight write of a
    // shard whose tablet id survived must stay untouched (its batch is kept unacked
    // for the duration, and re-sending it would surface a new message cookie after
    // the old central v1 `ReshardData()` wipes all shard state).
    class TInconsistentWritePartitioningChangeRoutesOnlyDeletedShards : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            // The split re-route relies on paced retries against the removed shard;
            // keep the backoff short so the re-resolve rounds stay fast.
            auto& writeActorSettings = *settings.AppConfig.MutableTableServiceConfig()->MutableWriteActorSettings();
            writeActorSettings.SetStartRetryDelayMs(100);
            writeActorSettings.SetMaxRetryDelayMs(1000);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKV` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKV");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKV to have 2 shards");
            const ui64 splitShard = shards[0];
            const ui64 unaffectedShard = shards[1];
            THashSet<ui64> initialShards(shards.begin(), shards.end());

            std::unique_ptr<IEventHandle> heldSplitShardWrite;
            TMutex cookiesMutex;
            THashSet<ui64> observedS2Cookies;
            std::atomic<ui64> newShardSends{0};
            bool queryRequestPatched = false;

            auto observer = [&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
                // IsStreamingQuery=true makes the sink compile with InconsistentTx=true.
                if (!queryRequestPatched &&
                    ev->GetTypeRewrite() == TEvKqp::TEvQueryRequest::EventType) {
                    queryRequestPatched = true;
                    auto* req = ev->Get<TEvKqp::TEvQueryRequest>();
                    auto userCtx = MakeIntrusive<TUserRequestContext>("", "/Root", "");
                    userCtx->IsStreamingQuery = true;
                    req->SetUserRequestContext(std::move(userCtx));
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

                if (ev->GetTypeRewrite() == TEvPipeCache::EvForward) {
                    auto* fwd = ev->Get<TEvPipeCache::TEvForward>();
                    if (fwd->Ev && fwd->Ev->Type() == NEvents::TDataEvents::TEvWrite::EventType) {
                        if (fwd->TabletId == splitShard && !heldSplitShardWrite) {
                            // Keep the split-shard batch uncommitted: it pins the query
                            // open while the split runs and, once re-delivered to the
                            // removed tablet, forces the re-resolve that re-routes it.
                            heldSplitShardWrite.reset(ev.Release());
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        if (fwd->TabletId == unaffectedShard) {
                            with_lock(cookiesMutex) {
                                observedS2Cookies.insert(ev->Cookie);
                            }
                        }
                        if (!initialShards.contains(fwd->TabletId)) {
                            newShardSends++;
                        }
                    }
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            TStringBuilder upsert;
            upsert << "UPSERT INTO `/Root/SplitKV` (Key, Value) VALUES ";
            for (ui32 key = 1; key <= 20; ++key) {
                if (key > 1) {
                    upsert << ", ";
                }
                upsert << "(" << key << "u, \"V" << key << "\")";
            }
            upsert << ", (3000000000u, \"Big\")";

            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(Q_(TString(upsert)), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync(); });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    bool s2Seen = false;
                    with_lock(cookiesMutex) {
                        s2Seen = !observedS2Cookies.empty();
                    }
                    return heldSplitShardWrite.get() != nullptr && s2Seen;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(heldSplitShardWrite.get() != nullptr, "no write to the split shard was observed");
            }

            THashSet<ui64> initialS2Cookies;
            with_lock(cookiesMutex) {
                initialS2Cookies = observedS2Cookies;
            }
            UNIT_ASSERT_C(!initialS2Cookies.empty(), "no write to the unaffected shard was observed");

            // Disable the readiness gate that would otherwise reject a split of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            // Split the shard that owns the small keys: it is replaced by two new
            // shards, while the big-key shard (tablet id) survives untouched.
            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKV", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            // Re-deliver the intercepted write to the split-shard tablet id: it is gone
            // after the split, so the delivery fails and the write actor falls back to
            // re-resolve. The controller re-routes the batch to the new shards.
            runtime.Send(heldSplitShardWrite.release());

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return newShardSends > 0;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                // Persistently failing writes to the removed shard drive the re-resolve;
                // the re-route then writes to the brand-new shard tablet ids.
                UNIT_ASSERT_C(newShardSends > 0,
                    "no write was re-routed to the new shards after the split");
            }

            {
                with_lock(cookiesMutex) {
                    for (const ui64 cookie : observedS2Cookies) {
                        UNIT_ASSERT_C(initialS2Cookies.contains(cookie),
                            "the unaffected shard was re-written after the partitioning change");
                    }
                }
            }

            runtime.SetObserverFunc(saveObserver);

            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKV` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(R"([[1u;["V1"]];[2u;["V2"]];[3u;["V3"]];[4u;["V4"]];[5u;["V5"]];[6u;["V6"]];[7u;["V7"]];[8u;["V8"]];[9u;["V9"]];[10u;["V10"]];[11u;["V11"]];[12u;["V12"]];[13u;["V13"]];[14u;["V14"]];[15u;["V15"]];[16u;["V16"]];[17u;["V17"]];[18u;["V18"]];[19u;["V19"]];[20u;["V20"]];[3000000000u;["Big"]]])",
                    FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(InconsistentWritePartitioningChangeRoutesOnlyDeletedShards) {
        TInconsistentWritePartitioningChangeRoutesOnlyDeletedShards tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A reroute after a split removes the shard's record from the write controller
    // (an inconsistent (streaming) write has no TxManager state to transfer), but
    // events referencing the dead tablet can still arrive afterwards — a pipe
    // delivery problem, a scheduled retry, a stale result. None of them may fail
    // the write: the reroute already re-sent the pending batches to the covering
    // shards, so late events for the dead tablet are stale by definition. The query
    // must succeed and every row must be applied exactly once.
    class TInconsistentWriteLateEventsForRemovedShard : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            // The split re-route relies on paced retries against the removed shard;
            // keep the backoff short so the re-resolve rounds stay fast.
            auto& writeActorSettings = *settings.AppConfig.MutableTableServiceConfig()->MutableWriteActorSettings();
            writeActorSettings.SetStartRetryDelayMs(100);
            writeActorSettings.SetMaxRetryDelayMs(1000);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVInconsistentLate` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVInconsistentLate");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKVInconsistentLate to have 2 shards");
            const ui64 splitShard = shards[0];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            with_lock(wire.Mutex) {
                wire.HoldWritesTo = {splitShard};
            }

            bool queryRequestPatched = false;
            auto observer = [&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
                // IsStreamingQuery=true makes the sink compile with InconsistentTx=true.
                if (!queryRequestPatched &&
                    ev->GetTypeRewrite() == TEvKqp::TEvQueryRequest::EventType) {
                    queryRequestPatched = true;
                    auto* req = ev->Get<TEvKqp::TEvQueryRequest>();
                    auto userCtx = MakeIntrusive<TUserRequestContext>("", "/Root", "");
                    userCtx->IsStreamingQuery = true;
                    req->SetUserRequestContext(std::move(userCtx));
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
                return wire.Observe(ev);
            };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(
                    Q_(BuildKvUpsertQuery("/Root/SplitKVInconsistentLate", 20)),
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync(); });

            // The split-shard write is held: it pins the query open while the split runs.
            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    with_lock(wire.Mutex) {
                        return wire.HeldWrite != nullptr;
                    }
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.HeldWrite != nullptr, "no write to the split shard was observed");
            }
            UNIT_ASSERT_C(wire.WriteActorIdSnapshot() != TActorId(), "the write actor id was not observed on a data write");

            // Disable the readiness gate that would otherwise reject a split of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            // Quiet split: the removed shard is replaced by two new shards.
            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVInconsistentLate", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            const auto shardsAfterSplit = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVInconsistentLate");
            THashSet<ui64> newShards;
            for (const ui64 shardId : shardsAfterSplit) {
                if (!initialShards.contains(shardId)) {
                    newShards.insert(shardId);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(newShards.size(), 2u, "the split must produce two new shards");

            // Re-deliver the intercepted write to the split-shard tablet id: it is gone
            // after the split, so the delivery fails and the paced retries fall back to
            // re-resolve, which re-routes the batch to the new shards. Hold the
            // re-routed batch undelivered: the write actor stays in-flight with the
            // reroute fully applied while the late events for the removed shard are
            // injected.
            with_lock(wire.Mutex) {
                wire.HoldWritesTo = newShards;
            }
            runtime.Send(wire.HeldWrite.release());

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    with_lock(wire.Mutex) {
                        return wire.HeldWrite != nullptr;
                    }
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.HeldWrite != nullptr, "the re-routed batch was not observed on a new shard");
            }

            // The reroute is complete: the removed shard is erased from the write
            // controller. Inject the late events for it.
            const TActorId writeActorId = wire.WriteActorIdSnapshot();
            runtime.Send(new IEventHandle(
                writeActorId, writeActorId,
                new TEvPipeCache::TEvDeliveryProblem(splitShard, /* notDelivered */ true)));
            auto lateError = NEvents::TDataEvents::TEvWriteResult::BuildError(
                splitShard, 0, NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR,
                "late result for a shard removed by a reroute");
            runtime.Send(new IEventHandle(
                writeActorId, writeActorId, lateError.release()));
            auto lateCompleted = NEvents::TDataEvents::TEvWriteResult::BuildCompleted(splitShard);
            runtime.Send(new IEventHandle(
                writeActorId, writeActorId, lateCompleted.release()));

            // Release the held re-routed batch: the write proceeds to the new shard
            // and the query completes.
            with_lock(wire.Mutex) {
                wire.HoldWritesTo.clear();
            }
            runtime.Send(wire.HeldWrite.release());

            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            runtime.SetObserverFunc(saveObserver);

            // Every row is applied exactly once.
            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVInconsistentLate` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(BuildExpectedKvYson(20), FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(InconsistentWriteLateEventsForRemovedShard) {
        TInconsistentWriteLateEventsForRemovedShard tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A partition change (split) during an in-flight consistent (WriteSeqNum) write must
    // re-route only the removed shard's pending batch to the new shards (the unaffected
    // shard's in-flight write is never re-sent). The re-routed batch preserves the original
    // WriteSeqNum and carries OriginalShard = the removed shard, validates as a duplicate
    // of the transferred ancestor chain (the rows are not re-applied), the new shards join
    // the distributed commit (their prepare carries the ancestor lock of the removed shard)
    // and the transaction commits with every row applied exactly once.
    class TUncommittedWriteSeqNumPartitioningChangeReroutesOnlyDeletedShards : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVSeq` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVSeq");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKVSeq to have 2 shards");
            const ui64 splitShard = shards[0];
            const ui64 unaffectedShard = shards[1];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            // The split shard's write is applied but its result is suppressed, so the
            // batch stays pending while the split runs (the shard's lock and WriteSeqNum
            // chain are created and later transferred to the new shards).
            wire.SuppressResultsFrom = {splitShard};
            // The unaffected shard's send cookies prove it is never re-sent.
            wire.CookieTablet = unaffectedShard;
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync().GetTransaction(); });

            // The SELECT must read only the unaffected shard's key: the split shard's
            // lock has to stay write-only for the split to transfer it. The query is
            // pinned open by the suppressed split-shard write result.
            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(
                    Q_(BuildKvUpsertQuery("/Root/SplitKVSeq", 20) + "; SELECT Key, Value FROM `/Root/SplitKVSeq` WHERE Key IN (3000000000u);"),
                    TTxControl::Tx(tx)).ExtractValueSync(); });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return wire.SuppressedAckCompleted.load() >= 1 && !wire.ObservedCookiesSnapshot().empty();
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.SuppressedAckCompleted.load() >= 1, "the split shard's write was not applied");
            }
            const THashSet<ui64> initialUnaffectedCookies = wire.ObservedCookiesSnapshot();
            UNIT_ASSERT_C(!initialUnaffectedCookies.empty(), "no write to the unaffected shard was observed");

            // Disable the readiness gate that would otherwise reject a split of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            // Split the shard that owns the small keys: it is replaced by two new
            // shards, while the big-key shard (tablet id) survives untouched. The split
            // transfers the tx's write-only lock (with its WriteSeqNum chain) to the new
            // shards, so the re-routed batch can validate later. The removed tablet
            // breaks the pipe of the pending write, driving the paced retries into the
            // re-resolve that re-routes the batch.
            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVSeq", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return wire.NewShardSends.load() > 0;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.NewShardSends.load() > 0,
                    "no write was re-routed to the new shards after the split");
            }

            // The write phase survives the split: the re-routed batch is answered as a
            // duplicate of the transferred chain (the rows were applied on the removed
            // shard before the split and moved to the new shards) and the query completes.
            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // Wire checks: operations to the original shards carry no OriginalShard; the
            // re-routed operations preserve the original WriteSeqNum and carry
            // OriginalShard of the removed shard; the new shards answered as duplicates
            // (the transferred rows are not re-applied); the unaffected shard was never
            // re-sent (its cookie set did not grow).
            const auto writeOps = wire.WriteOpsSnapshot();
            THashSet<ui64> rerouteDestinations;
            for (const auto& [dest, seqNum, originalShard] : writeOps) {
                if (initialShards.contains(dest)) {
                    UNIT_ASSERT_VALUES_EQUAL_C(originalShard, dest,
                        "operations to the original shards must carry OriginalShard");
                } else {
                    rerouteDestinations.insert(dest);
                    UNIT_ASSERT_VALUES_EQUAL_C(originalShard, splitShard,
                        "a re-routed operation must carry OriginalShard of the removed shard");
                    UNIT_ASSERT_VALUES_EQUAL_C(seqNum, 1u,
                        "the re-routed batch must preserve the original WriteSeqNum");
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(rerouteDestinations.size(), 2u,
                "the split must re-route the batch to exactly two new shards");
            {
                const auto duplicates = wire.DuplicateRespondersSnapshot();
                for (const ui64 dest : rerouteDestinations) {
                    UNIT_ASSERT_C(duplicates.contains(dest),
                        "the re-routed batch must be answered as a duplicate of the transferred chain");
                }
            }
            {
                for (const ui64 cookie : wire.ObservedCookiesSnapshot()) {
                    UNIT_ASSERT_C(initialUnaffectedCookies.contains(cookie),
                        "the unaffected shard was re-written after the partitioning change");
                }
            }

            // Commit participation: the new shards are ReceivingShards of the distributed
            // commit and their prepare carries the ancestor lock of the removed shard.
            // The transaction commits through the split.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            runtime.SetObserverFunc(saveObserver);

            {
                const auto prepareAncestors = wire.PrepareAncestorsSnapshot();
                const auto prepareReceiving = wire.PrepareReceivingSnapshot();
                for (const ui64 dest : rerouteDestinations) {
                    UNIT_ASSERT_C(prepareAncestors.contains(dest),
                        "no prepare message was sent to a new shard");
                    UNIT_ASSERT_C(prepareAncestors.at(dest).contains(splitShard),
                        "the new shard's prepare must carry the ancestor lock of the removed shard");
                    bool receivingContainsDest = false;
                    for (const auto& [prepareDest, receiving] : prepareReceiving) {
                        if (receiving.contains(dest)) {
                            receivingContainsDest = true;
                            break;
                        }
                    }
                    UNIT_ASSERT_C(receivingContainsDest,
                        "the new shard must be a ReceivingShard of the distributed commit");
                }
            }

            // Every row is applied exactly once (the re-routed batch was a duplicate).
            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVSeq` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(BuildExpectedKvYson(20), FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumPartitioningChangeReroutesOnlyDeletedShards) {
        TUncommittedWriteSeqNumPartitioningChangeReroutesOnlyDeletedShards tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A split observed only after the first write to the shard has already succeeded must
    // behave like the in-flight case: the next write lands on the brand-new shards through
    // the same re-resolve + re-route path and continues the transferred ancestor WriteSeqNum
    // chain (the write actor persists across the queries of the transaction, so the chain
    // position is not restarted from 1). A subsequent fresh write goes straight to the new
    // shards and starts their own current chains from 1. The transaction commits through the
    // split with every row applied exactly once.
    class TUncommittedWriteSeqNumSplitAfterFirstWrite : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVSeqLocks` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVSeqLocks");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKVSeqLocks to have 2 shards");
            const ui64 splitShard = shards[0];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            // Read Committed: the pre-prepare re-resolve round runs for this tx, which
            // also flushes the deferred fresh-write batches as observable data-mode
            // messages before the distributed prepare.
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::ReadCommittedRW()).ExtractValueSync().GetTransaction(); });

            // The first write fully succeeds: both shards acknowledge, recording the tx's
            // lock and WriteSeqNum chain (position 1 for the single multi-row batch) on
            // the shard that is about to be split. The SELECT must read only the
            // unaffected shard's key so the split shard's lock stays write-only for the
            // split to transfer it.
            {
                auto result = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(
                        Q_(BuildKvUpsertQuery("/Root/SplitKVSeqLocks", 20) + "; SELECT Key, Value FROM `/Root/SplitKVSeqLocks` WHERE Key IN (3000000000u);"),
                        TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            // Disable the readiness gate that would otherwise reject a split of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            // Split the shard that owns the small keys after its write has already been
            // acknowledged: the removed shard's lock and uncommitted chain move to the
            // new shards.
            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVSeqLocks", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            const auto shardsAfterSplit = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVSeqLocks");
            THashSet<ui64> newShards;
            for (const ui64 shardId : shardsAfterSplit) {
                if (!initialShards.contains(shardId)) {
                    newShards.insert(shardId);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(newShards.size(), 2u, "the split must produce two new shards");

            // The second write targets keys of the removed shard through the stale
            // partitioning: the re-route continues the transferred ancestor chain
            // (WriteSeqNum 2 == 1 + 1, the first write's batch took position 1,
            // OriginalShard = the removed shard). The write actor persists across the
            // queries of the transaction, so the chain position is not restarted from 1.
            {
                auto result = Kikimr->RunCall([&] { return session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/SplitKVSeqLocks` (Key, Value) VALUES
                        (21u, "V21"), (22u, "V22"), (23u, "V23"), (24u, "V24"), (25u, "V25");
                    SELECT Key, Value FROM `/Root/SplitKVSeqLocks` WHERE Key IN (3000000000u);
                )"), TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            {
                const auto writeOps = wire.WriteOpsSnapshot();
                THashSet<ui64> rerouteDestinations;
                for (const auto& [dest, seqNum, originalShard] : writeOps) {
                    if (initialShards.contains(dest)) {
                        UNIT_ASSERT_VALUES_EQUAL_C(originalShard, dest,
                            "operations to the original shards must carry OriginalShard");
                    } else {
                        rerouteDestinations.insert(dest);
                        UNIT_ASSERT_VALUES_EQUAL_C(originalShard, splitShard,
                            "a re-routed operation must carry OriginalShard of the removed shard");
                        UNIT_ASSERT_VALUES_EQUAL_C(seqNum, 2u,
                            "the re-routed batch must continue the transferred chain at 2");
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL_C(rerouteDestinations.size(), 1u,
                    "keys 21..25 must be re-routed to a single new shard");
            }

            // The third write routes by the updated partitioning straight to the new
            // shards: fresh current chains starting at WriteSeqNum 1 with no
            // OriginalShard (the frozen ancestors accept them because they arrive after
            // all ancestor-continuation batches of the same destination).
            {
                auto result = Kikimr->RunCall([&] { return session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/SplitKVSeqLocks` (Key, Value) VALUES (5u, "V5b"), (12u, "V12b");
                )"), TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            // The fresh write (UPSERT-only statement) is buffered and flushed at commit
            // time, so its wire operations are observed during the commit below.

            // The transaction commits through the split: every participant (both new
            // shards and the unaffected one) prepares with the matching ancestor locks.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            // The commit-time flush routed keys 5 and 12 by the updated partitioning
            // straight to the new shards: fresh current chains starting at WriteSeqNum 1
            // with no OriginalShard (the frozen ancestors accept them because they arrive
            // after all ancestor-continuation batches of the same destination).
            {
                const auto writeOps = wire.WriteOpsSnapshot();
                THashSet<ui64> freshDestinations;
                for (const auto& [dest, seqNum, originalShard] : writeOps) {
                    if (newShards.contains(dest) && originalShard == dest) {
                        freshDestinations.insert(dest);
                        UNIT_ASSERT_VALUES_EQUAL_C(seqNum, 1u,
                            "a fresh batch to a new shard must start its own chain at 1");
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL_C(freshDestinations.size(), 2u,
                    "keys 5 and 12 must land on both new shards as fresh writes");
            }

            runtime.SetObserverFunc(saveObserver);

            {
                const auto prepareAncestors = wire.PrepareAncestorsSnapshot();
                const auto prepareReceiving = wire.PrepareReceivingSnapshot();
                for (const ui64 dest : newShards) {
                    UNIT_ASSERT_C(prepareAncestors.contains(dest),
                        "no prepare message was sent to a new shard");
                    UNIT_ASSERT_C(prepareAncestors.at(dest).contains(splitShard),
                        "the new shard's prepare must carry the ancestor lock of the removed shard");
                }
                bool receivingContainsNewShards = false;
                for (const auto& [prepareDest, receiving] : prepareReceiving) {
                    if (receiving.contains(*newShards.begin())) {
                        receivingContainsNewShards = true;
                        break;
                    }
                }
                UNIT_ASSERT_C(receivingContainsNewShards,
                    "the new shards must be ReceivingShards of the distributed commit");
            }

            // A fresh session read sees all rows applied exactly once.
            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVSeqLocks` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,
                            21u,22u,23u,24u,25u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(BuildExpectedKvYson(25, {{5, "V5b"}, {12, "V12b"}}), FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumSplitAfterFirstWrite) {
        TUncommittedWriteSeqNumSplitAfterFirstWrite tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // The quiet case of the pre-prepare re-resolve: a Read Committed transaction has
    // all of its writes acknowledged, then the shard splits (nothing is in flight, so
    // no write retry would ever notice the split). The commit must still survive: the
    // pre-prepare re-resolve round (Read Committed only) re-resolves the partitioning,
    // the removed shard is dropped from the participant set and its lock is transferred
    // to the new shards as an ancestor lock, and the distributed commit succeeds.
    class TUncommittedWriteSeqNumQuietSplitBeforeReadCommittedCommit : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVRound` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVRound");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKVRound to have 2 shards");
            const ui64 splitShard = shards[0];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::ReadCommittedRW()).ExtractValueSync().GetTransaction(); });

            // One write query, fully acknowledged (Read Committed flushes every
            // statement's effects immediately and waits for the acknowledgements).
            {
                auto result = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(BuildKvUpsertQuery("/Root/SplitKVRound", 20)),
                        TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            UNIT_ASSERT_VALUES_EQUAL_C(wire.NewShardSends.load(), 0u, "no re-sends are expected before the split");

            // Disable the readiness gate that would otherwise reject a split of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            // Quiet split: all writes are already acknowledged, nothing is in flight.
            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVRound", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            const auto shardsAfterSplit = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVRound");
            THashSet<ui64> newShards;
            for (const ui64 shardId : shardsAfterSplit) {
                if (!initialShards.contains(shardId)) {
                    newShards.insert(shardId);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(newShards.size(), 2u, "the split must produce two new shards");

            // The commit survives the quiet split through the pre-prepare re-resolve
            // round (Read Committed only).
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            runtime.SetObserverFunc(saveObserver);

            // No data was re-sent anywhere (nothing was pending); both new shards joined
            // the commit and their prepare carries the ancestor lock of the removed shard.
            UNIT_ASSERT_VALUES_EQUAL_C(wire.NewShardSends.load(), 0u, "no data re-send is expected in the quiet split case");
            {
                const auto prepareAncestors = wire.PrepareAncestorsSnapshot();
                const auto prepareReceiving = wire.PrepareReceivingSnapshot();
                for (const ui64 dest : newShards) {
                    UNIT_ASSERT_C(prepareAncestors.contains(dest),
                        "no prepare message was sent to a new shard");
                    UNIT_ASSERT_C(prepareAncestors.at(dest).contains(splitShard),
                        "the new shard's prepare must carry the ancestor lock of the removed shard");
                    bool receivingContainsDest = false;
                    for (const auto& [prepareDest, receiving] : prepareReceiving) {
                        if (receiving.contains(dest)) {
                            receivingContainsDest = true;
                            break;
                        }
                    }
                    UNIT_ASSERT_C(receivingContainsDest,
                        "the new shard must be a ReceivingShard of the distributed commit");
                }
            }

            // The rows were transferred by the split and committed exactly once.
            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVRound` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(BuildExpectedKvYson(20), FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumQuietSplitBeforeReadCommittedCommit) {
        TUncommittedWriteSeqNumQuietSplitBeforeReadCommittedCommit tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A split where all of the removed shard's rows land in one of the two new shards
    // leaves the other one lock-only: it holds the transferred ancestor lock but
    // receives no rows and no re-routed batch. It still must join the distributed
    // commit (the transferred lock makes it a participant; the prepare reaches it through
    // the external-shards path) so that the ancestor lock is validated and cleaned by
    // the commit.
    class TUncommittedWriteSeqNumLockOnlyShardJoinsCommit : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVLockOnly` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVLockOnly");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKVLockOnly to have 2 shards");
            const ui64 splitShard = shards[0];
            const ui64 unaffectedShard = shards[1];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            // The split shard's write is applied but its result is suppressed, so the
            // batch stays pending and is re-routed after the split.
            wire.SuppressResultsFrom = {splitShard};
            wire.CookieTablet = unaffectedShard;
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::ReadCommittedRW()).ExtractValueSync().GetTransaction(); });

            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(
                    Q_(BuildKvUpsertQuery("/Root/SplitKVLockOnly", 20) + "; SELECT Key, Value FROM `/Root/SplitKVLockOnly` WHERE Key IN (3000000000u);"),
                    TTxControl::Tx(tx)).ExtractValueSync(); });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return wire.SuppressedAckCompleted.load() >= 1;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.SuppressedAckCompleted.load() >= 1, "the split shard's write was not applied");
            }

            // Disable the readiness gate that would otherwise reject a split of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            // Split high: every written key is below the boundary, so all rows land in
            // the left new shard and the right one receives no rows (lock-only).
            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVLockOnly", splitShard, 100u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return wire.NewShardSends.load() > 0;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.NewShardSends.load() > 0,
                    "no write was re-routed to the new shards after the split");
            }

            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // All re-routed rows must land in one new shard; the other one is lock-only.
            const auto writeOps = wire.WriteOpsSnapshot();
            THashSet<ui64> rerouteDestinations;
            for (const auto& [dest, seqNum, originalShard] : writeOps) {
                if (initialShards.contains(dest)) {
                    UNIT_ASSERT_VALUES_EQUAL_C(originalShard, dest,
                        "operations to the original shards must carry OriginalShard");
                } else {
                    rerouteDestinations.insert(dest);
                    UNIT_ASSERT_VALUES_EQUAL_C(originalShard, splitShard,
                        "a re-routed operation must carry OriginalShard of the removed shard");
                    UNIT_ASSERT_VALUES_EQUAL_C(seqNum, 1u,
                        "the re-routed batch must preserve the original WriteSeqNum");
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(rerouteDestinations.size(), 1u,
                "all rows must be re-routed to a single new shard");

            const auto shardsAfterSplit = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVLockOnly");
            ui64 lockOnlyShard = 0;
            for (const ui64 shardId : shardsAfterSplit) {
                if (!initialShards.contains(shardId) && !rerouteDestinations.contains(shardId)) {
                    UNIT_ASSERT_VALUES_EQUAL_C(lockOnlyShard, 0u, "more than one lock-only shard");
                    lockOnlyShard = shardId;
                }
            }
            UNIT_ASSERT_C(lockOnlyShard != 0, "no lock-only shard was found");
            {
                const auto duplicates = wire.DuplicateRespondersSnapshot();
                UNIT_ASSERT_C(duplicates.contains(*rerouteDestinations.begin()),
                    "the re-routed batch must be answered as a duplicate of the transferred chain");
                UNIT_ASSERT_C(!duplicates.contains(lockOnlyShard),
                    "the lock-only shard must not answer any data write");
            }

            // The lock-only shard joins the distributed commit: its prepare carries the
            // ancestor lock of the removed shard, which is validated and cleaned by the
            // commit. The transaction commits through the split.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            runtime.SetObserverFunc(saveObserver);

            {
                const auto prepareAncestors = wire.PrepareAncestorsSnapshot();
                UNIT_ASSERT_C(prepareAncestors.contains(lockOnlyShard),
                    "no prepare message was sent to the lock-only shard");
                UNIT_ASSERT_C(prepareAncestors.at(lockOnlyShard).contains(splitShard),
                    "the lock-only shard's prepare must carry the ancestor lock of the removed shard");
                UNIT_ASSERT_C(prepareAncestors.contains(*rerouteDestinations.begin()),
                    "no prepare message was sent to the data destination shard");
            }

            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVLockOnly` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(BuildExpectedKvYson(20), FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumLockOnlyShardJoinsCommit) {
        return; // TODO
        TUncommittedWriteSeqNumLockOnlyShardJoinsCommit tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A merge of the transaction's two shards while both of its writes are in flight
    // (applied, results suppressed) must re-route both pending batches to the merged
    // shard: two independent ancestor chains (one per removed shard) are validated in
    // the same destination and the transaction commits with every row applied exactly
    // once.
    class TUncommittedWriteSeqNumMergeReroutesBothShards : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVMerge` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
                        UNIFORM_PARTITIONS = 3
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVMerge");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 3u, "expected /Root/SplitKVMerge to have 3 shards");
            const ui64 splitShard = shards[0];
            const ui64 unaffectedShard = shards[1];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            // Both writes are applied but their results are suppressed, so both batches
            // stay pending while the merge runs. No SELECT in the query: a read would
            // mark the locks with read tables and break the lock transfer.
            wire.SuppressResultsFrom = {splitShard, unaffectedShard};
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::ReadCommittedRW()).ExtractValueSync().GetTransaction(); });

            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/SplitKVMerge` (Key, Value) VALUES
                        (1u, "V1"), (2u, "V2"), (3u, "V3"), (4u, "V4"), (5u, "V5"),
                        (6u, "V6"), (7u, "V7"), (8u, "V8"), (9u, "V9"), (10u, "V10"),
                        (2000000000u, "VMid"), (3000000000u, "Big");
                )"), TTxControl::Tx(tx)).ExtractValueSync(); });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return wire.SuppressedAckCompleted.load() >= 2;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.SuppressedAckCompleted.load() >= 2, "the writes were not applied on both merged shards");
            }

            // Disable the readiness gate that would otherwise reject a merge of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            // Merge the two shards holding the pending writes: both are removed,
            // replaced by a single new shard that receives the transferred locks (two
            // ancestor chains) and rows of both. The third shard survives, so the
            // commit stays distributed.
            const ui64 mergeTxId = AsyncMergeTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVMerge", {splitShard, unaffectedShard});
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, mergeTxId);

            const auto shardsAfterMerge = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVMerge");
            UNIT_ASSERT_VALUES_EQUAL_C(shardsAfterMerge.size(), 2u, "the merge must leave two shards");
            const ui64 mergedShard = *std::find_if(shardsAfterMerge.begin(), shardsAfterMerge.end(),
                [&](ui64 shardId) { return !initialShards.contains(shardId); });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return wire.NewShardSends.load() > 0;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.NewShardSends.load() > 0,
                    "no write was re-routed to the merged shard after the merge");
            }

            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // Both pending batches were re-routed to the merged shard with their
            // original chains preserved, and answered as duplicates of the transferred
            // ancestor chains.
            {
                const auto writeOps = wire.WriteOpsSnapshot();
                THashSet<ui64> routedOriginalShards;
                THashSet<ui64> routedSeqNums;
                for (const auto& [dest, seqNum, originalShard] : writeOps) {
                    if (dest == mergedShard) {
                        UNIT_ASSERT_VALUES_EQUAL_C(seqNum, 1u,
                            "the re-routed batches must preserve the original WriteSeqNums");
                        routedOriginalShards.insert(originalShard);
                        routedSeqNums.insert(seqNum);
                    } else {
                        UNIT_ASSERT_C(initialShards.contains(dest),
                            "no data writes outside the merged shard are expected");
                    }
                }
                UNIT_ASSERT_C(routedOriginalShards.contains(splitShard),
                    "the merged shard must receive the batch of the first removed shard");
                UNIT_ASSERT_C(routedOriginalShards.contains(unaffectedShard),
                    "the merged shard must receive the batch of the second removed shard");
                UNIT_ASSERT_C(wire.DuplicateRespondersSnapshot().contains(mergedShard),
                    "the re-routed batches must be answered as duplicates of the transferred chains");
            }

            // The transaction commits through the merge.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            runtime.SetObserverFunc(saveObserver);

            {
                const auto prepareAncestors = wire.PrepareAncestorsSnapshot();
                UNIT_ASSERT_C(prepareAncestors.contains(mergedShard),
                    "no prepare message was sent to the merged shard");
                UNIT_ASSERT_C(prepareAncestors.at(mergedShard).contains(splitShard),
                    "the merged shard's prepare must carry the ancestor lock of the first removed shard");
                UNIT_ASSERT_C(prepareAncestors.at(mergedShard).contains(unaffectedShard),
                    "the merged shard's prepare must carry the ancestor lock of the second removed shard");
            }

            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVMerge` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,2000000000u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(R"([[1u;["V1"]];[2u;["V2"]];[3u;["V3"]];[4u;["V4"]];[5u;["V5"]];[6u;["V6"]];[7u;["V7"]];[8u;["V8"]];[9u;["V9"]];[10u;["V10"]];[2000000000u;["VMid"]];[3000000000u;["Big"]]])",
                    FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumMergeReroutesBothShards) {
        return; // TODO
        TUncommittedWriteSeqNumMergeReroutesBothShards tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A split whose boundary leaves one covering target with no re-routed rows and no
    // transferred locks (a plain UPSERT has no buffer lookup, so the removed shard's
    // TxManager entry is lockless once its write ack is suppressed) must still join the
    // commit: the target holds the DataShard-side transferred chain of the removed
    // shard, so it receives a covering prepare (and the transferred lock is cleaned).
    // Without the empty-target registration the commit waits for the target forever.
    class TUncommittedWriteSeqNumSplitWithEmptyTargetJoinsCommit : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVEmptyTarget` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVEmptyTarget");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKVEmptyTarget to have 2 shards");
            const ui64 splitShard = shards[0];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            wire.SuppressResultsFrom = {splitShard};
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::ReadCommittedRW()).ExtractValueSync().GetTransaction(); });

            // A plain UPSERT without a SELECT: no buffer-table lookup, so no lock echo
            // besides the write ack (suppressed below) reaches the removed shard.
            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(Q_(BuildKvUpsertQuery("/Root/SplitKVEmptyTarget", 20)),
                    TTxControl::Tx(tx)).ExtractValueSync(); });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return wire.SuppressedAckCompleted.load() >= 1;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.SuppressedAckCompleted.load() >= 1, "the split shard's write was not applied");
            }

            SetSplitMergePartCountLimit(&runtime, -1);

            // Split high: every written key is below the boundary, so all rows land in
            // the left new shard and the right one receives nothing (the empty target).
            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVEmptyTarget", splitShard, 100u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            const auto shardsAfterSplit = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVEmptyTarget");
            THashSet<ui64> newShards;
            for (const ui64 shardId : shardsAfterSplit) {
                if (!initialShards.contains(shardId)) {
                    newShards.insert(shardId);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(newShards.size(), 2u, "the split must produce two new shards");

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return wire.NewShardSends.load() > 0;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.NewShardSends.load() > 0,
                    "no write was re-routed to the new shards after the split");
            }

            // All re-routed rows must land in one target; the other one is the empty
            // target (it receives no data writes).
            ui64 emptyTarget = 0;
            {
                THashSet<ui64> dataDestinations;
                for (const auto& [dest, seqNum, originalShard] : wire.WriteOpsSnapshot()) {
                    if (newShards.contains(dest)) {
                        dataDestinations.insert(dest);
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL_C(dataDestinations.size(), 1u,
                    "all rows must be re-routed to a single new shard");
                for (const ui64 shardId : newShards) {
                    if (!dataDestinations.contains(shardId)) {
                        emptyTarget = shardId;
                    }
                }
                UNIT_ASSERT_C(emptyTarget != 0, "no empty target shard was found");
            }

            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            {
                const auto writeOps = wire.WriteOpsSnapshot();
                for (const auto& [dest, seqNum, originalShard] : writeOps) {
                    UNIT_ASSERT_C(dest != emptyTarget,
                        "the empty target must not receive data writes");
                }
            }

            // The transaction commits through the split: the empty target joins the
            // commit with a covering prepare.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            runtime.SetObserverFunc(saveObserver);

            {
                const auto prepareAncestors = wire.PrepareAncestorsSnapshot();
                UNIT_ASSERT_C(prepareAncestors.contains(emptyTarget),
                    "no prepare message was sent to the empty target shard");
            }

            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVEmptyTarget` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(BuildExpectedKvYson(20), FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumSplitWithEmptyTargetJoinsCommit) {
        return; // TODO
        TUncommittedWriteSeqNumSplitWithEmptyTargetJoinsCommit tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // An INSERT-only Read Committed statement defers its affected-rows flush to the
    // commit, so the commit takes the Flush path. A split during that flush (the write
    // applied but unacknowledged) must still be survived: the flush's retry re-resolve
    // re-routes the batch and transfers the removed shard, and the pre-prepare resolve
    // round after the flush re-resolves the partitioning before the distributed prepare.
    class TUncommittedWriteSeqNumInsertSplitDuringFlushSurvives : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVInsert` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVInsert");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKVInsert to have 2 shards");
            const ui64 splitShard = shards[0];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            wire.SuppressResultsFrom = {splitShard};
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::ReadCommittedRW()).ExtractValueSync().GetTransaction(); });

            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(Q_(R"(
                    INSERT INTO `/Root/SplitKVInsert` (Key, Value) VALUES
                        (1u, "V1"), (2u, "V2"), (3u, "V3"), (4u, "V4"), (5u, "V5"),
                        (6u, "V6"), (7u, "V7"), (8u, "V8"), (9u, "V9"), (10u, "V10"),
                        (11u, "V11"), (12u, "V12"), (13u, "V13"), (14u, "V14"), (15u, "V15"),
                        (16u, "V16"), (17u, "V17"), (18u, "V18"), (19u, "V19"), (20u, "V20"),
                        (3000000000u, "Big");
                )"), TTxControl::Tx(tx)).ExtractValueSync(); });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return wire.SuppressedAckCompleted.load() >= 1;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.SuppressedAckCompleted.load() >= 1, "the split shard's insert was not applied");
            }

            SetSplitMergePartCountLimit(&runtime, -1);

            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVInsert", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // The transaction commits through the split (the commit flush path: the
            // INSERT's flush plus the pre-prepare resolve round).
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            runtime.SetObserverFunc(saveObserver);

            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVInsert` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(BuildExpectedKvYson(20), FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumInsertSplitDuringFlushSurvives) {
        TUncommittedWriteSeqNumInsertSplitDuringFlushSurvives tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A single-shard Read Committed transaction (exactly one write action) whose only
    // shard is quietly split after the write was acknowledged: the pre-prepare resolve
    // round transfers the shard's participant state to both covering shards
    // (MoveShardTo) without adding actions, so the transaction stops being
    // single-shard while ActionsCount stays 1. The commit must fall back from the
    // immediate commit to the distributed prepare (both covering shards join) and
    // succeed with every row applied exactly once.
    class TUncommittedWriteSeqNumSingleShardQuietSplitSurvives : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVSingleShard` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
                        UNIFORM_PARTITIONS = 1
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVSingleShard");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 1u, "expected /Root/SplitKVSingleShard to have a single shard");
            const ui64 splitShard = shards[0];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::ReadCommittedRW()).ExtractValueSync().GetTransaction(); });

            // A plain UPSERT touching only the single shard: exactly one write action
            // (ActionsCount == 1). No SELECT: a read would add a second action. The
            // statement completes with its write acknowledged — the split below is
            // quiet (no in-flight traffic, no pending batches).
            auto result = Kikimr->RunCall([&] {
                return session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/SplitKVSingleShard` (Key, Value) VALUES
                        (1u, "V1"), (2u, "V2"), (3u, "V3"), (4u, "V4"), (5u, "V5"),
                        (6u, "V6"), (7u, "V7"), (8u, "V8"), (9u, "V9"), (10u, "V10"),
                        (11u, "V11"), (12u, "V12"), (13u, "V13"), (14u, "V14"), (15u, "V15"),
                        (16u, "V16"), (17u, "V17"), (18u, "V18"), (19u, "V19"), (20u, "V20");
                )"), TTxControl::Tx(tx)).ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            SetSplitMergePartCountLimit(&runtime, -1);

            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVSingleShard", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            const auto shardsAfterSplit = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVSingleShard");
            THashSet<ui64> newShards;
            for (const ui64 shardId : shardsAfterSplit) {
                if (!initialShards.contains(shardId)) {
                    newShards.insert(shardId);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(newShards.size(), 2u, "the split must produce two new shards");

            // The commit must survive the quiet split: the resolve round reveals it,
            // MoveShardTo transfers the single shard's state to both covering shards
            // and the commit falls back to the distributed prepare.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            runtime.SetObserverFunc(saveObserver);

            {
                const auto prepareReceiving = wire.PrepareReceivingSnapshot();
                for (const ui64 shardId : newShards) {
                    UNIT_ASSERT_C(prepareReceiving.contains(shardId),
                        "no prepare message was sent to a covering shard");
                }
            }

            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVSingleShard` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,
                            11u,12u,13u,14u,15u,16u,17u,18u,19u,20u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(R"([[1u;["V1"]];[2u;["V2"]];[3u;["V3"]];[4u;["V4"]];[5u;["V5"]];[6u;["V6"]];[7u;["V7"]];[8u;["V8"]];[9u;["V9"]];[10u;["V10"]];[11u;["V11"]];[12u;["V12"]];[13u;["V13"]];[14u;["V14"]];[15u;["V15"]];[16u;["V16"]];[17u;["V17"]];[18u;["V18"]];[19u;["V19"]];[20u;["V20"]]])",
                    FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumSingleShardQuietSplitSurvives) {
        TUncommittedWriteSeqNumSingleShardQuietSplitSurvives tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // Multi-hop re-route: the first re-route sends the batch to a brand-new shard with
    // OriginalShard = the original removed shard; the batch is held undelivered there,
    // the intermediate shard is split as well, and the second re-route must keep the
    // ORIGINAL OriginalShard (not the intermediate shard) and the original WriteSeqNum
    // (invariant 14). The ancestor chain transferred through the intermediate shard
    // lets the final destination apply the batch as a continuation, and the commit
    // succeeds.
    class TUncommittedWriteSeqNumMultiHopReroute : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVMultiHop` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVMultiHop");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKVMultiHop to have 2 shards");
            const ui64 splitShard = shards[0];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::ReadCommittedRW()).ExtractValueSync().GetTransaction(); });

            // The first write fully succeeds: the lock and the WriteSeqNum chain (1..20)
            // exist on the shard that is about to be split.
            {
                auto result = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(
                        Q_(BuildKvUpsertQuery("/Root/SplitKVMultiHop", 20) + "; SELECT Key, Value FROM `/Root/SplitKVMultiHop` WHERE Key IN (3000000000u);"),
                        TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            // Disable the readiness gate that would otherwise reject a split of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            // Split #1: the removed shard is replaced by two new shards.
            const ui64 splitTxId1 = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVMultiHop", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId1);

            const auto shardsAfterSplit1 = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVMultiHop");
            THashSet<ui64> newShards1;
            for (const ui64 shardId : shardsAfterSplit1) {
                if (!initialShards.contains(shardId)) {
                    newShards1.insert(shardId);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(newShards1.size(), 2u, "the first split must produce two new shards");

            // Hold the re-routed batch undelivered on whichever new shard it lands on
            // (keys 21..25 all belong to one of them): it stays pending on the
            // intermediate shard.
            with_lock(wire.Mutex) {
                wire.HoldWritesTo = newShards1;
            }

            // The second write targets the removed shard through the stale partitioning
            // and is re-routed to the intermediate shard (held undelivered).
            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/SplitKVMultiHop` (Key, Value) VALUES
                        (21u, "V21"), (22u, "V22"), (23u, "V23"), (24u, "V24"), (25u, "V25");
                    SELECT Key, Value FROM `/Root/SplitKVMultiHop` WHERE Key IN (3000000000u);
                )"), TTxControl::Tx(tx)).ExtractValueSync(); });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    with_lock(wire.Mutex) {
                        return wire.HeldWrite != nullptr;
                    }
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.HeldWrite != nullptr, "the re-routed batch was not observed on an intermediate shard");
            }
            const ui64 intermediateShard = wire.HeldWriteTablet;
            UNIT_ASSERT_C(newShards1.contains(intermediateShard),
                "the first re-route must target a new shard");
            with_lock(wire.Mutex) {
                wire.HoldWritesTo.clear();
            }

            // Split #2: the intermediate shard is removed as well, replaced by two
            // shards; the transferred ancestor chains (including the original removed
            // shard's chain) move to all of its destinations.
            const ui64 splitTxId2 = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVMultiHop", intermediateShard, 15u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId2);

            const auto shardsAfterSplit2 = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVMultiHop");
            THashSet<ui64> finalShardCandidates;
            for (const ui64 shardId : shardsAfterSplit2) {
                if (!initialShards.contains(shardId) && shardId != intermediateShard) {
                    finalShardCandidates.insert(shardId);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(finalShardCandidates.size(), 3u,
                "the second split must produce two more new shards");

            // Re-deliver the held batch to the now-removed intermediate tablet: the
            // delivery problem drives the paced retries into the re-resolve and the
            // second re-route to the final destination.
            runtime.Send(wire.HeldWrite.release());

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    for (const auto& [dest, seqNum2, originalShard] : wire.WriteOpsSnapshot()) {
                        if (originalShard == splitShard && finalShardCandidates.contains(dest)) {
                            return true;
                        }
                    }
                    return false;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
            }
            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // The second re-route preserved the ORIGINAL OriginalShard (invariant 14:
            // not overwritten with the intermediate shard) and the original WriteSeqNum.
            {
                const auto writeOps = wire.WriteOpsSnapshot();
                THashSet<ui64> finalDestinations;
                for (const auto& [dest, seqNum, originalShard] : writeOps) {
                    if (finalShardCandidates.contains(dest) && originalShard == splitShard) {
                        finalDestinations.insert(dest);
                        UNIT_ASSERT_VALUES_EQUAL_C(seqNum, 2u,
                            "the twice re-routed batch must preserve the original WriteSeqNum");
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL_C(finalDestinations.size(), 1u,
                    "keys 21..25 must land on a single final destination");
            }

            // The transaction commits through both splits.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            runtime.SetObserverFunc(saveObserver);

            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVMultiHop` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,
                            21u,22u,23u,24u,25u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(BuildExpectedKvYson(25), FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumMultiHopReroute) {
        TUncommittedWriteSeqNumMultiHopReroute tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A reroute (split/merge) erases the removed shard's records from the write
    // actor's controller and from the TxManager. Events referencing the dead tablet
    // can still arrive afterwards — a pipe delivery problem, a stale error result,
    // a stale acknowledgement. None of them may fail the transaction: the reroute
    // already transferred the pending batches and the participant state to the
    // covering shards, so late events for the dead tablet are stale by definition.
    // The transaction must commit and every row must be applied exactly once.
    class TUncommittedWriteSeqNumLateEventsForRemovedShard : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVLateEvents` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVLateEvents");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKVLateEvents to have 2 shards");
            const ui64 splitShard = shards[0];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::ReadCommittedRW()).ExtractValueSync().GetTransaction(); });

            // The first write fully succeeds: the lock and the WriteSeqNum chain (1..20)
            // exist on the shard that is about to be split.
            {
                auto result = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(
                        Q_(BuildKvUpsertQuery("/Root/SplitKVLateEvents", 20) + "; SELECT Key, Value FROM `/Root/SplitKVLateEvents` WHERE Key IN (3000000000u);"),
                        TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            const TActorId writeActorId = wire.WriteActorIdSnapshot();
            UNIT_ASSERT_C(writeActorId != TActorId(), "the write actor id was not observed on a data write");

            // Disable the readiness gate that would otherwise reject a split of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            // Quiet split: the removed shard is replaced by two new shards.
            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVLateEvents", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            const auto shardsAfterSplit = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVLateEvents");
            THashSet<ui64> newShards;
            for (const ui64 shardId : shardsAfterSplit) {
                if (!initialShards.contains(shardId)) {
                    newShards.insert(shardId);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(newShards.size(), 2u, "the split must produce two new shards");

            // Hold the re-routed batch undelivered on the new shards: the write actor
            // stays in-flight with the reroute fully applied while the late events
            // for the removed shard are injected.
            with_lock(wire.Mutex) {
                wire.HoldWritesTo = newShards;
            }

            // The second write targets the removed shard through the stale partitioning
            // and is re-routed to a new shard (held undelivered).
            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/SplitKVLateEvents` (Key, Value) VALUES
                        (21u, "V21"), (22u, "V22"), (23u, "V23"), (24u, "V24"), (25u, "V25");
                    SELECT Key, Value FROM `/Root/SplitKVLateEvents` WHERE Key IN (3000000000u);
                )"), TTxControl::Tx(tx)).ExtractValueSync(); });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    with_lock(wire.Mutex) {
                        return wire.HeldWrite != nullptr;
                    }
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.HeldWrite != nullptr, "the re-routed batch was not observed on a new shard");
            }

            // The reroute is complete: the removed shard is erased from the write
            // actor's controller and TxManager state. Inject the late events for it.
            runtime.Send(new IEventHandle(
                writeActorId, writeActorId,
                new TEvPipeCache::TEvDeliveryProblem(splitShard, /* notDelivered */ true)));
            auto lateError = NEvents::TDataEvents::TEvWriteResult::BuildError(
                splitShard, 0, NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR,
                "late result for a shard removed by a reroute");
            runtime.Send(new IEventHandle(
                writeActorId, writeActorId, lateError.release()));
            auto lateCompleted = NEvents::TDataEvents::TEvWriteResult::BuildCompleted(splitShard);
            runtime.Send(new IEventHandle(
                writeActorId, writeActorId, lateCompleted.release()));

            // Release the held re-routed batch: the write proceeds to the new shard
            // and the transaction completes.
            with_lock(wire.Mutex) {
                wire.HoldWritesTo.clear();
            }
            runtime.Send(wire.HeldWrite.release());

            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // The transaction commits through the split.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            runtime.SetObserverFunc(saveObserver);

            // Every row is applied exactly once.
            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVLateEvents` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,
                            21u,22u,23u,24u,25u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(BuildExpectedKvYson(25), FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumLateEventsForRemovedShard) {
        TUncommittedWriteSeqNumLateEventsForRemovedShard tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A Read Committed SELECT over the splitting shard's keys must not prevent the
    // write-stage split handling: RC reads take no read locks (no read-tables on the
    // tx's lock), so the write-only lock still transfers with its uncommitted chain
    // and the re-routed batch validates. The transaction survives with every row
    // applied exactly once.
    class TUncommittedWriteSeqNumReadFromSplittingShardStillTransfers : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVReadBreak` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVReadBreak");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKVReadBreak to have 2 shards");
            const ui64 splitShard = shards[0];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            wire.SuppressResultsFrom = {splitShard};
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::ReadCommittedRW()).ExtractValueSync().GetTransaction(); });

            // The SELECT reads a key of the splitting shard; the write's result is
            // suppressed so the batch stays pending for the re-route.
            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(
                    Q_(BuildKvUpsertQuery("/Root/SplitKVReadBreak", 20) + "; SELECT Key, Value FROM `/Root/SplitKVReadBreak` WHERE Key IN (1u, 3000000000u);"),
                    TTxControl::Tx(tx)).ExtractValueSync(); });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return wire.SuppressedAckCompleted.load() >= 1;
                });
                runtime.DispatchEvents(opts, TDuration::Seconds(30));
                UNIT_ASSERT_C(wire.SuppressedAckCompleted.load() >= 1, "the split shard's write was not applied");
            }

            // Disable the readiness gate that would otherwise reject a split of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVReadBreak", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            // The write phase survives the split: RC reads do not mark the lock with
            // read tables, so the split transfers it and the re-routed batch validates.
            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // The transaction commits through the split.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

            runtime.SetObserverFunc(saveObserver);

            // Every row is applied exactly once.
            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVReadBreak` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(BuildExpectedKvYson(20), FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumReadFromSplittingShardStillTransfers) {
        TUncommittedWriteSeqNumReadFromSplittingShardStillTransfers tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // The pre-prepare re-resolve round is gated on Read Committed: for other isolation
    // levels a quiet split between the acknowledged write and the commit still breaks
    // the distributed commit (the removed shard stays a participant and its prepare
    // cannot be delivered). Documented current behavior.
    class TUncommittedWriteSeqNumQuietSplitNonReadCommittedCommitFails : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            SetupUncommittedWriteSeqNum(settings);
        }

        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();
            auto client = Kikimr->GetQueryClient();

            auto create = Kikimr->RunCall([&] {
                return client.ExecuteQuery(Q_(R"(
                    CREATE TABLE `/Root/SplitKVNonRC` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        AUTO_PARTITIONING_BY_SIZE = DISABLED,
                        AUTO_PARTITIONING_BY_LOAD = DISABLED,
                        UNIFORM_PARTITIONS = 2
                    );
                )"), TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

            auto edgeActor = runtime.AllocateEdgeActor();
            const auto shards = GetTableShards(&Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVNonRC");
            UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 2u, "expected /Root/SplitKVNonRC to have 2 shards");
            const ui64 splitShard = shards[0];
            const THashSet<ui64> initialShards(shards.begin(), shards.end());

            TUncommittedWriteWire wire;
            wire.InitialShards = initialShards;
            auto observer = [&wire](TAutoPtr<IEventHandle>& ev) { return wire.Observe(ev); };
            auto saveObserver = runtime.SetObserverFunc(observer);

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync().GetTransaction(); });

            // One write query, fully acknowledged: the SELECT forces the deferred
            // effects flush and reads the unaffected shard only.
            {
                auto result = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(
                        Q_(BuildKvUpsertQuery("/Root/SplitKVNonRC", 20) + "; SELECT Key, Value FROM `/Root/SplitKVNonRC` WHERE Key IN (3000000000u);"),
                        TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            UNIT_ASSERT_VALUES_EQUAL_C(wire.NewShardSends.load(), 0u, "no re-sends are expected before the split");

            // Disable the readiness gate that would otherwise reject a split of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            // Quiet split of the shard that owns the small keys.
            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVNonRC", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            runtime.SetObserverFunc(saveObserver);

            // Without the pre-prepare re-resolve round the commit cannot survive: the
            // covering prepare for the removed shard cannot be delivered.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::UNAVAILABLE,
                TStringBuilder() << commitResult.GetStatus() << ": " << commitResult.GetIssues().ToString());

            // The cluster stays usable and the uncommitted rows are not applied.
            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVNonRC` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(R"([])", FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumQuietSplitNonReadCommittedCommitFails) {
        TUncommittedWriteSeqNumQuietSplitNonReadCommittedCommitFails tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }
}

} // namespace NKqp
} // namespace NKikimr
