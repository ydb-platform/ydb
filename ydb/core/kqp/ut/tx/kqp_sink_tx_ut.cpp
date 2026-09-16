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

    // A partition change (split) during an in-flight consistent (WriteSeqNum) write must
    // behave like the inconsistent case: the removed shard's pending batch is re-routed to
    // the new shards after the re-resolve, while the unaffected shard's in-flight write
    // stays untouched. The transaction must then commit with every row applied exactly once.
    class TUncommittedWriteSeqNumPartitioningChangeReroutesOnlyDeletedShards : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetEnableDataShardUncommittedWriteSeqNum(true);
            // Keep the paced retries short so the re-resolve + re-route rounds stay fast.
            auto& writeActorSettings = *settings.AppConfig.MutableTableServiceConfig()->MutableWriteActorSettings();
            writeActorSettings.SetStartRetryDelayMs(100);
            writeActorSettings.SetMaxRetryDelayMs(1000);
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
            THashSet<ui64> initialShards(shards.begin(), shards.end());

            std::unique_ptr<IEventHandle> heldSplitShardWrite;
            bool splitShardWriteHeld = false;
            TMutex cookiesMutex;
            THashSet<ui64> observedS2Cookies;
            std::atomic<ui64> newShardSends{0};

            auto observer = [&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
                if (ev->GetTypeRewrite() == TEvPipeCache::EvForward) {
                    auto* fwd = ev->Get<TEvPipeCache::TEvForward>();
                    if (fwd->Ev && fwd->Ev->Type() == NEvents::TDataEvents::TEvWrite::EventType) {
                        if (fwd->TabletId == splitShard && !splitShardWriteHeld) {
                            // Keep one split-shard batch uncommitted: it pins the query
                            // open while the split runs and, once re-delivered to the
                            // removed tablet, forces the re-resolve that re-routes it.
                            // Only the first write is held; the later paced resends must
                            // reach the pipe cache so the dead tablet surfaces them.
                            splitShardWriteHeld = true;
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
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync().GetTransaction(); });

            TStringBuilder upsert;
            upsert << "UPSERT INTO `/Root/SplitKVSeq` (Key, Value) VALUES ";
            for (ui32 key = 1; key <= 20; ++key) {
                if (key > 1) {
                    upsert << ", ";
                }
                upsert << "(" << key << "u, \"V" << key << "\")";
            }
            upsert << ", (3000000000u, \"Big\")";

            // The batch of the split shard is held while the SELECT pins the txn open,
            // so the split happens while the write is still in flight.
            auto future = Kikimr->RunInThreadPool([&] {
                return session.ExecuteQuery(Q_(TString(upsert) + "; SELECT Key, Value FROM `/Root/SplitKVSeq` WHERE Key IN (1u, 3000000000u);"), TTxControl::Tx(tx)).ExtractValueSync(); });

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
            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVSeq", splitShard, 10u);
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

            // The write phase survives the split: the removed shard's batch is re-routed to
            // the new shards and the UPSERT+SELECT flushes and completes. But the commit of a
            // SerializableRW transaction across a split aborts on the distributed-tx side,
            // because the participant set captured at plan time contains the removed shard.
            auto result = runtime.WaitFuture(future, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // The distributed commit must fail (the participant set still contains the
            // removed shard, which is unreachable after the split): it aborts on the
            // distributed-tx side, so the uncommitted rows are never applied.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_C(commitResult.GetStatus() == EStatus::ABORTED,
                TStringBuilder() << commitResult.GetStatus() << ": " << commitResult.GetIssues().ToString());

            // The cluster stays usable: a fresh session read succeeds and the aborted rows
            // are gone.
            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVSeq` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(R"([])", FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumPartitioningChangeReroutesOnlyDeletedShards) {
        return; // TODO: needs consistent txs rerouting to be enabled
        TUncommittedWriteSeqNumPartitioningChangeReroutesOnlyDeletedShards tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    // A split observed only after the first write to the shard has already succeeded must
    // behave like the in-flight case: the next write lands on the brand-new shards through
    // the same re-resolve + re-route path, and the transaction keeps working.
    class TUncommittedWriteSeqNumSplitAfterFirstWrite : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetEnableDataShardUncommittedWriteSeqNum(true);
            // Keep the paced retries short so the re-resolve + re-route rounds stay fast.
            auto& writeActorSettings = *settings.AppConfig.MutableTableServiceConfig()->MutableWriteActorSettings();
            writeActorSettings.SetStartRetryDelayMs(100);
            writeActorSettings.SetMaxRetryDelayMs(1000);
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

            auto session = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto tx = Kikimr->RunCall([&] {
                return session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync().GetTransaction(); });

            // The first write fully succeeds: both shards acknowledge, recording the tx's
            // lock and WriteSeqNum chain on the shard that is about to be split.
            {
                auto result = Kikimr->RunCall([&] { return session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/SplitKVSeqLocks` (Key, Value) VALUES
                        (1u, "V1"), (2u, "V2"), (3u, "V3"), (4u, "V4"), (5u, "V5"),
                        (6u, "V6"), (7u, "V7"), (8u, "V8"), (9u, "V9"), (10u, "V10"),
                        (11u, "V11"), (12u, "V12"), (13u, "V13"), (14u, "V14"), (15u, "V15"),
                        (16u, "V16"), (17u, "V17"), (18u, "V18"), (19u, "V19"), (20u, "V20"),
                        (3000000000u, "Big");
                    SELECT Key, Value FROM `/Root/SplitKVSeqLocks` WHERE Key IN (1u, 3000000000u);
                )"), TTxControl::Tx(tx)).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            // Disable the readiness gate that would otherwise reject a split of a
            // freshly created table whose shards have not reported stats yet.
            SetSplitMergePartCountLimit(&runtime, -1);

            // Split the shard that owns the small keys after its write has already been
            // acknowledged: the removed shard's lock and uncommitted chain are gone.
            const ui64 splitTxId = AsyncSplitTable(Kikimr->GetTestServer(), edgeActor, "/Root/SplitKVSeqLocks", splitShard, 10u);
            WaitTxNotification(Kikimr->GetTestServer(), edgeActor, splitTxId);

            // The next write targets keys that now belong to the brand-new shards. It must
            // be re-routed to them (like the in-flight case), not corrupt data or hang.
            auto result = Kikimr->RunCall([&] { return session.ExecuteQuery(Q_(R"(
                UPSERT INTO `/Root/SplitKVSeqLocks` (Key, Value) VALUES
                    (21u, "V21"), (22u, "V22"), (23u, "V23"), (24u, "V24"), (25u, "V25");
                SELECT Key, Value FROM `/Root/SplitKVSeqLocks` WHERE Key IN (21u, 22u, 23u, 24u, 25u);
            )"), TTxControl::Tx(tx)).ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            // The transaction must not hang or burn retries against the dead tablet; the
            // distributed commit must fail (the participant set captured at plan time still
            // contains the removed shard): it aborts on the distributed-tx side or surfaces
            // UNAVAILABLE while trying to prepare the removed shard, so the uncommitted
            // rows are never applied.
            auto commitResult = Kikimr->RunCall([&] { return tx.Commit().ExtractValueSync(); });
            UNIT_ASSERT_C(commitResult.GetStatus() == EStatus::UNAVAILABLE,
                TStringBuilder() << commitResult.GetStatus() << ": " << commitResult.GetIssues().ToString());

            // The cluster stays usable: a fresh session read succeeds and nothing was applied.
            {
                auto check = Kikimr->RunCall([&] {
                    return session.ExecuteQuery(Q_(R"(
                        SELECT Key, Value FROM `/Root/SplitKVSeqLocks` WHERE Key IN (
                            1u,2u,3u,4u,5u,6u,7u,8u,9u,10u,11u,12u,13u,14u,15u,16u,17u,18u,19u,20u,
                            21u,22u,23u,24u,25u,3000000000u
                        ) ORDER BY Key;
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
                UNIT_ASSERT_VALUES_EQUAL_C(check.GetStatus(), EStatus::SUCCESS, check.GetIssues().ToString());
                CompareYson(R"([])", FormatResultSetYson(check.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(UncommittedWriteSeqNumSplitAfterFirstWrite) {
        return; // TODO: needs consistent txs rerouting to be enabled
        TUncommittedWriteSeqNumSplitAfterFirstWrite tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }
}

} // namespace NKqp
} // namespace NKikimr
