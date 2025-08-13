#pragma once
#include <ydb/core/blobstorage/base/blobstorage_shred_events.h>
#include <ydb/core/tablet_flat/defs.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/fwd.h>
#include <util/system/types.h>

namespace NActors {

class TTestActorRuntime;

}

namespace NSchemeShardUT_Private {

class TTestEnv;

struct TSchemeObject {
    bool Table = true;
    bool Topic = true;
};

ui64 CreateTestExtSubdomain(NActors::TTestActorRuntime& runtime, TTestEnv& env, ui64* txId, const TString& name, const TSchemeObject& createSchemeObject = {});

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
