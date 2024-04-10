#include "export_reboots_common.h"

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/core/protos/msgbus_kv.pb.h>

namespace NSchemeShardUT_Private {
namespace NExportReboots {

void Run(const TVector<TString>& tables, const TString& request, TTestWithReboots& t) {
    t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
        {
            TInactiveZone inactive(activeZone);

            TSet<ui64> toWait;
            for (const auto& table : tables) {
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", table);
                toWait.insert(t.TxId);
            }
            t.TestEnv->TestWaitNotification(runtime, toWait);
        }

        TestExport(runtime, ++t.TxId, "/MyRoot", request);
        const ui64 exportId = t.TxId;

        t.TestEnv->TestWaitNotification(runtime, exportId);

        {
            TInactiveZone inactive(activeZone);

            auto response = TestGetExport(runtime, exportId, "/MyRoot", {
                Ydb::StatusIds::SUCCESS,
                Ydb::StatusIds::NOT_FOUND
            });

            if (response.GetResponse().GetEntry().GetStatus() == Ydb::StatusIds::NOT_FOUND) {
                return;
            }

            TestForgetExport(runtime, ++t.TxId, "/MyRoot", exportId);
            t.TestEnv->TestWaitNotification(runtime, exportId);

            TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        }
    });
}

void Cancel(const TVector<TString>& tables, const TString& request, TTestWithReboots& t) {
    t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
        {
            TInactiveZone inactive(activeZone);

            TSet<ui64> toWait;
            for (const auto& table : tables) {
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", table);
                toWait.insert(t.TxId);
            }
            t.TestEnv->TestWaitNotification(runtime, toWait);
        }

        TestExport(runtime, ++t.TxId, "/MyRoot", request);
        const ui64 exportId = t.TxId;

        t.TestEnv->ReliablePropose(runtime, CancelExportRequest(++t.TxId, "/MyRoot", exportId), {
            Ydb::StatusIds::SUCCESS,
            Ydb::StatusIds::NOT_FOUND
        });
        t.TestEnv->TestWaitNotification(runtime, exportId);

        {
            TInactiveZone inactive(activeZone);

            auto response = TestGetExport(runtime, exportId, "/MyRoot", {
                Ydb::StatusIds::SUCCESS,
                Ydb::StatusIds::CANCELLED,
                Ydb::StatusIds::NOT_FOUND
            });

            if (response.GetResponse().GetEntry().GetStatus() == Ydb::StatusIds::NOT_FOUND) {
                return;
            }

            TestForgetExport(runtime, ++t.TxId, "/MyRoot", exportId);
            t.TestEnv->TestWaitNotification(runtime, exportId);

            TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        }
    });
}

void Forget(const TVector<TString>& tables, const TString& request, TTestWithReboots& t) {
    t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
        {
            TInactiveZone inactive(activeZone);

            TSet<ui64> toWait;
            for (const auto& table : tables) {
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", table);
                toWait.insert(t.TxId);
            }
            t.TestEnv->TestWaitNotification(runtime, toWait);

            TestExport(runtime, ++t.TxId, "/MyRoot", request);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
        }

        const ui64 exportId = t.TxId;

        t.TestEnv->ReliablePropose(runtime, ForgetExportRequest(++t.TxId, "/MyRoot", exportId), {
            Ydb::StatusIds::SUCCESS,
        });
        t.TestEnv->TestWaitNotification(runtime, exportId);

        {
            TInactiveZone inactive(activeZone);
            TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        }
    });
}

} // NExportReboots
} // NSchemeShardUT_Private
