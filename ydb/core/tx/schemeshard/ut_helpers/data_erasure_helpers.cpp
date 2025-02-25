#include "data_erasure_helpers.h"
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

namespace NSchemeShardUT_Private {

TFakeBSController::TFakeBSController(const NActors::TActorId& tablet, NKikimr::TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, nullptr)
{}

void TFakeBSController::DefaultSignalTabletActive(const NActors::TActorContext&) {
    // must be empty
}

void TFakeBSController::OnActivateExecutor(const NActors::TActorContext&) {
    Become(&TThis::StateWork);
    SignalTabletActive(SelfId());
}

void TFakeBSController::OnDetach(const NActors::TActorContext& ctx) {
    Die(ctx);
}

void TFakeBSController::OnTabletDead(NKikimr::TEvTablet::TEvTabletDead::TPtr&, const NActors::TActorContext& ctx) {
    Die(ctx);
}

void TFakeBSController::Handle(NKikimr::TEvBlobStorage::TEvControllerShredRequest::TPtr& ev, const NActors::TActorContext& ctx) {
    auto record = ev->Get()->Record;
    if (record.GetGeneration() > Generation) {
        Generation = record.GetGeneration();
        Completed = false;
        Progress = 0;
    } else if (record.GetGeneration() == Generation) {
        if (!Completed) {
            Progress += 5000;
            if (Progress >= 10000) {
                Progress = 10000;
                Completed = true;
            }
        }
    }
    ctx.Send(ev->Sender, new NKikimr::TEvBlobStorage::TEvControllerShredResponse(Generation, Completed, Progress));
}

ui64 CreateTestSubdomain(NActors::TTestActorRuntime& runtime, TTestEnv& env, ui64* txId, const TString& name) {
    TestCreateExtSubDomain(runtime, ++(*txId), "/MyRoot", Sprintf(R"(
        Name: "%s"
    )", name.c_str()));
    env.TestWaitNotification(runtime, *txId);

    TestAlterExtSubDomain(runtime, ++(*txId), "/MyRoot", Sprintf(R"(
        PlanResolution: 50
        Coordinators: 1
        Mediators: 1
        TimeCastBucketsPerMediator: 2
        ExternalSchemeShard: true
        ExternalHive: false
        Name: "%s"
        StoragePools {
            Name: "name_%s_kind_hdd-1"
            Kind: "common"
        }
        StoragePools {
            Name: "name_%s_kind_hdd-2"
            Kind: "external"
        }
    )", name.c_str(), name.c_str(), name.c_str()));
    env.TestWaitNotification(runtime, *txId);

    ui64 schemeshardId;
    TestDescribeResult(DescribePath(runtime, TStringBuilder() << "/MyRoot/" << name), {
        NLs::PathExist,
        NLs::ExtractTenantSchemeshard(&schemeshardId)
    });

    TestCreateTable(runtime, schemeshardId, ++(*txId), TStringBuilder() << "/MyRoot/" << name,
        R"____(
            Name: "Simple"
            Columns { Name: "key1"  Type: "Uint32"}
            Columns { Name: "Value" Type: "Utf8"}
            KeyColumnNames: ["key1"]
            UniformPartitionsCount: 2
        )____");
    env.TestWaitNotification(runtime, *txId, schemeshardId);

    return schemeshardId;
}

} // NSchemeShardUT_Private
