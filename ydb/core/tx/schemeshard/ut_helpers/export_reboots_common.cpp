#include "export_reboots_common.h"

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/core/protos/msgbus_kv.pb.h>

using namespace NKikimrSchemeOp;

namespace NSchemeShardUT_Private {
namespace NExportReboots {

void TestCreate(TTestActorRuntime& runtime, ui64 txId, const TString& scheme, NKikimrSchemeOp::EPathType pathType) {
    using TTestCreateFunc = ui64(*)(TTestActorRuntime&, ui64, const TString&, const TString&, 
        const TVector<TExpectedResult>&, const TApplyIf&);

    static const THashMap<NKikimrSchemeOp::EPathType, TTestCreateFunc> functions = {
        {EPathTypeTable, &TestSimpleCreateTable},
        {EPathTypeView, &TestCreateView},
        {EPathTypeCdcStream, &TestCreateCdcStream},
    };

    auto it = functions.find(pathType);
    if (it != functions.end()) {
        it->second(runtime, txId, "/MyRoot", scheme, {NKikimrScheme::StatusAccepted}, {});
    } else {
        UNIT_FAIL("export is not implemented for the scheme object type: " << pathType);
    }
}

void CreateSchemeObjects(TTestWithReboots& t, TTestActorRuntime& runtime, const TVector<TTypedScheme>& schemeObjects) {
    TSet<ui64> toWait;
    for (const auto& [type, scheme, _] : schemeObjects) {
        TestCreate(runtime, ++t.TxId, scheme, type);
        toWait.insert(t.TxId);
    }
    t.TestEnv->TestWaitNotification(runtime, toWait);
}

void Run(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t) {
    t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
        runtime.GetAppData().FeatureFlags.SetEnableViewExport(true);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        {
            TInactiveZone inactive(activeZone);
            CreateSchemeObjects(t, runtime, schemeObjects);
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

void Cancel(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t) {
    t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
        runtime.GetAppData().FeatureFlags.SetEnableViewExport(true);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        {
            TInactiveZone inactive(activeZone);
            CreateSchemeObjects(t, runtime, schemeObjects);
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

void Forget(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t) {
    t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
        runtime.GetAppData().FeatureFlags.SetEnableViewExport(true);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        {
            TInactiveZone inactive(activeZone);
            CreateSchemeObjects(t, runtime, schemeObjects);

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
