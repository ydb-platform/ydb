#include "kqp_sink_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpDisableOnlineRO) {
    class TDisableOnlineROBase : public TTableDataModificationTester {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetDisableOnlineRO(true);
        }
    };

    class TDisableOnlineROOlap : public TDisableOnlineROBase {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session = client.GetSession().GetValueSync().GetSession();

            {
                auto result = session.ExecuteQuery(Q_(R"(
                    SELECT * FROM `/Root/KV`;
                )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
                result.GetIssues().PrintTo(Cerr);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                    "Read from column-oriented tables is not supported in Online Read-Only or Stale Read-Only transaction modes.");
            }

            {
                auto result = session.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/KV` (Key, Value) VALUES (1u, "New");
                )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
                result.GetIssues().PrintTo(Cerr);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                    "Read from column-oriented tables is not supported in Online Read-Only or Stale Read-Only transaction modes.");
            }
        }
    };

    class TDisableOnlineROTableService : public TDisableOnlineROBase {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetDisableOnlineRO(true);
            SetFillTables(false);
        }

        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            {
                auto result = client.ExecuteQuery(R"(
                    CREATE TABLE `/Root/TestKV` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        UNIFORM_PARTITIONS = 100
                    );
                )", TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            }
            {
                auto result = client.ExecuteQuery(R"(
                    REPLACE INTO `TestKV` (Key, Value) VALUES (1u, "One");
                )", TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            }

            auto db = Kikimr->GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            {
                auto result = session.ExecuteDataQuery(R"(
                    SELECT * FROM `/Root/TestKV`;
                )", NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
                result.GetIssues().PrintTo(Cerr);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            {
                auto result = session.ExecuteDataQuery(R"(
                    UPSERT INTO `/Root/TestKV` (Key, Value) VALUES (1u, "New");
                )", NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
                result.GetIssues().PrintTo(Cerr);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Operation 'Upsert' can't be performed in read only transaction");
            }
        }
    };

    class TDisableOnlineROAllowInconsistentReads : public TDisableOnlineROBase {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetDisableOnlineRO(true);
        }

        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            Kikimr->RunCall([&](){
                auto result = client.ExecuteQuery(R"(
                    CREATE TABLE `/Root/TestKV` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        UNIFORM_PARTITIONS = 100
                    );
                )", TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            });

            auto db = Kikimr->GetTableClient();
            auto session = Kikimr->RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

            auto& runtime = *Kikimr->GetTestServer().GetRuntime();

            bool snapshotChecked = false;
            auto grab = [&](TAutoPtr<IEventHandle>& ev) -> auto {
                if (ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvRead::EventType) {
                    auto* evRead = ev->Get<NKikimr::TEvDataShard::TEvRead>();
                    UNIT_ASSERT_C(evRead->Record.GetSnapshot().GetStep() != 0,
                        "Snapshot step must be non-zero when DisableOnlineRO converts OnlineRO to SnapshotRO");
                    UNIT_ASSERT_C(evRead->Record.GetSnapshot().GetTxId() != 0,
                        "Snapshot txId must be non-zero when DisableOnlineRO converts OnlineRO to SnapshotRO");
                    snapshotChecked = true;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };

            auto saveObserver = runtime.SetObserverFunc(grab);
            Y_DEFER {
                runtime.SetObserverFunc(saveObserver);
            };

            auto future = Kikimr->RunInThreadPool([&] {
                auto txc = NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::OnlineRO(
                    NYdb::NTable::TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx();
                return session.ExecuteDataQuery(R"(
                    SELECT Value FROM `/Root/TestKV` WHERE Key = 1u OR Key = 4294967295u;
                )", txc).ExtractValueSync();
            });

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_C(snapshotChecked, "No TEvRead was intercepted to verify snapshot acquisition");
        }
    };

    class TDisableOnlineROBeginTx : public TDisableOnlineROBase {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetDisableOnlineRO(true);
            SetFillTables(false);
        }

        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            {
                auto result = client.ExecuteQuery(R"(
                    CREATE TABLE `/Root/TestKV` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    );
                )", TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            }

            auto db = Kikimr->GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            auto txResult = session.BeginTransaction(NYdb::NTable::TTxSettings::OnlineRO()).ExtractValueSync();
            txResult.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL_C(txResult.GetStatus(), EStatus::BAD_REQUEST, txResult.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(txResult.GetIssues().ToString(), "open transactions not supported for transaction mode OnlineReadOnly");
        }
    };

    class TDisableOnlineROInteractiveTransaction : public TDisableOnlineROBase {
    protected:
        void Setup(TKikimrSettings& settings) override {
            settings.AppConfig.MutableFeatureFlags()->SetDisableOnlineRO(true);
            SetFillTables(false);
        }

        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            {
                auto result = client.ExecuteQuery(R"(
                    CREATE TABLE `/Root/TestKV` (
                        Key Uint32 not null,
                        Value String,
                        PRIMARY KEY (Key)
                    );
                )", TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            }
            {
                auto result = client.ExecuteQuery(R"(
                    REPLACE INTO `TestKV` (Key, Value) VALUES (1u, "One");
                )", TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            }

            auto db = Kikimr->GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/TestKV` WHERE Key = 1u;
            )", NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::OnlineRO())).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "open transactions not supported for transaction mode: OnlineReadOnly");
        }
    };

    Y_UNIT_TEST(DisableOnlineRO_Olap) {
        TDisableOnlineROOlap tester;
        tester.SetIsOlap(true);
        tester.SetFillTables(true);
        tester.Execute();
    }

    Y_UNIT_TEST(DisableOnlineRO_TableService) {
        TDisableOnlineROTableService tester;
        tester.Execute();
    }

    Y_UNIT_TEST(DisableOnlineRO_AllowInconsistentReads) {
        TDisableOnlineROAllowInconsistentReads tester;
        tester.SetFillTables(true);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    Y_UNIT_TEST(DisableOnlineRO_BeginTransaction) {
        TDisableOnlineROBeginTx tester;
        tester.Execute();
    }

    Y_UNIT_TEST(DisableOnlineRO_InteractiveTransaction) {
        TDisableOnlineROInteractiveTransaction tester;
        tester.Execute();
    }
}

}
}
