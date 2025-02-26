#pragma once
#include <util/system/types.h>
#include <util/generic/fwd.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/blobstorage/base/blobstorage_shred_events.h>

namespace NActors {

class TTestActorRuntime;

}

namespace NSchemeShardUT_Private {

class TTestEnv;

ui64 CreateTestSubdomain(NActors::TTestActorRuntime& runtime, TTestEnv& env, ui64* txId, const TString& name);

class TFakeBSController : public NActors::TActor<TFakeBSController>, public NKikimr::NTabletFlatExecutor::TTabletExecutedFlat {
    void DefaultSignalTabletActive(const NActors::TActorContext&) override;
    void OnActivateExecutor(const NActors::TActorContext&) override;
    void OnDetach(const NActors::TActorContext& ctx) override;
    void OnTabletDead(NKikimr::TEvTablet::TEvTabletDead::TPtr&, const NActors::TActorContext& ctx) override;

public:
    TFakeBSController(const NActors::TActorId& tablet, NKikimr::TTabletStorageInfo *info);
    void Handle(NKikimr::TEvBlobStorage::TEvControllerShredRequest::TPtr& ev, const NActors::TActorContext& ctx);

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::TEvBlobStorage::TEvControllerShredRequest, Handle);
        }
    }


public:
    ui64 Generation = 0;
    bool Completed = true;
    ui32 Progress = 10000;
};

} // NSchemeShardUT_Private
